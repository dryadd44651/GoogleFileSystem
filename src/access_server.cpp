#include "access_server.h"

#include <Poco/Logger.h>
#include <Poco/Util/HelpFormatter.h>
#include <Poco/Environment.h>
#include <Poco/UUIDGenerator.h>
#include <Poco/Net/HTTPClientSession.h>
#include <algorithm>
#include <random>

namespace DistFS {

class GetFileRequestHandler: public HTTPRequestHandler {
public:
    void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response) {
        Application& app = Application::instance();
        AccessServer& server = dynamic_cast<AccessServer&>(app);
        
        std::map<std::string, std::string> query_map = getQueryMap(URI(request.getURI()));

        std::string filename = query_map["filename"];
        int64_t begin_pos = 0;
        int64_t end_pos = -1;

        if(query_map.find("begin_pos") != query_map.end()) {
            begin_pos = std::stoi(query_map["begin_pos"]);
        }
        if(query_map.find("end_pos") != query_map.end()) {
            end_pos = std::stoi(query_map["end_pos"]);
        }

        std::string meta_server_addr = server.meta_server_addr;

        // Request meta info from meta server
        JSON::Object::Ptr file_meta = getFileMeta(meta_server_addr, filename);
        if(file_meta.isNull()) {
            response.setStatusAndReason(HTTPResponse::HTTP_NOT_FOUND);
            response.send();
            return;
        }

        JSON::Array::Ptr chunks_json = file_meta->getArray("chunks");
        JSON::Object::Ptr chunk_servers_json = file_meta->getObject("chunk_servers");

        int64_t length = file_meta->getValue<int64_t>("length");
        int64_t chunk_size = file_meta->getValue<int64_t>("chunk_size");
        
        if(end_pos == -1) {
            end_pos = length;
        }

        if(begin_pos < 0) {
            begin_pos = 0;
        }
        if(end_pos > length) {
            end_pos = length;
        }

        std::vector<std::string> required_chunks;
        
        if(end_pos != 0) {
            for(int64_t i=begin_pos/chunk_size; i<=(end_pos-1)/chunk_size; i++) {
                required_chunks.push_back(chunks_json->getElement<std::string>((unsigned int)i));
            }
        }
        

        /*
        std::cout << "required_chunks ("<<required_chunks.size() << "): " << std::endl;
        for(auto it=required_chunks.begin(); it!=required_chunks.end(); ++it) {
            std::cout << "    " << (*it) << std::endl;
        }
        //*/

        bool ok = true;
        std::vector<std::pair<std::string, std::string>> selected_chunk_servers;
        for(int i=0; i<required_chunks.size(); i++) {
            JSON::Array::Ptr servers_json = chunk_servers_json->getArray(required_chunks[i]);
            if(servers_json.isNull() || servers_json->size() == 0) {
                ok = false;
                break;
            }
            int idx = std::rand() % servers_json->size();
            JSON::Object::Ptr server_json = servers_json->getObject(idx);
            std::string id = server_json->getValue<std::string>("id");
            std::string address = server_json->getValue<std::string>("address");

            selected_chunk_servers.push_back({id, address});
        }
        if(!ok) {
            response.setStatusAndReason(HTTPResponse::HTTP_SERVICE_UNAVAILABLE);
            response.send();
            return;
        }

        ok = true;

        response.setStatusAndReason(HTTPResponse::HTTP_OK);
        std::ostream& resp = response.send();
        for(int i=0; i<required_chunks.size(); i++) {
            std::vector<uint8_t> content = getChunk(selected_chunk_servers[i].second, required_chunks[i]);
            if(i != required_chunks.size()-1 && content.size() != chunk_size) {
                ok = false;
            }

            if(i == 0) {
                // Cut the beginning
                auto first = content.cbegin() + (begin_pos - (begin_pos/chunk_size)*chunk_size);
                auto last = content.cend();
                std::vector<uint8_t> tmp(first, last);
                content = tmp;
            }

            if(i == required_chunks.size()-1) {
                auto first = content.cbegin();
                // Not include the byte at end_pos. E.g. begin=3, end=4 will contains only 1 byte data.
                // std iterator is also not include the element at end(), so we don't need to -1 here.
                auto last = content.cbegin() + (end_pos - (end_pos/chunk_size)*chunk_size);
                std::vector<uint8_t> tmp(first, last);
                content = tmp;
            }
            resp.write((char*)content.data(), content.size());
        }
    }
};

class WriteFileRequestHandler: public HTTPRequestHandler {
public:
    void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response) {
        Application& app = Application::instance();
        AccessServer& server = dynamic_cast<AccessServer&>(app);
        
        std::map<std::string, std::string> query_map = getQueryMap(URI(request.getURI()));

        std::string filename = query_map["filename"];

        int64_t resize = -1;
        if(query_map.find("resize") != query_map.end()) {
            resize = std::stoi(query_map["resize"]);
        }
        
        int64_t begin_pos = 0;
        if(query_map.find("begin_pos") != query_map.end()) {
            begin_pos = std::stoi(query_map["begin_pos"]);
        }
        std::istream& istr = request.stream();
        
        std::vector<uint8_t> content;
        std::copy(std::istream_iterator<uint8_t>(istr), std::istream_iterator<uint8_t>(), std::back_inserter(content));

        std::string meta_server_addr = server.meta_server_addr;
        
        // Request meta info from meta server
        JSON::Object::Ptr file_meta = getFileMeta(meta_server_addr, filename);
        if(file_meta.isNull()) {
            // File not exist, create one.
            int resp_code = requestCreateFile(meta_server_addr, filename);

            if(resp_code != HTTPResponse::HTTP_OK) {
                response.setStatusAndReason(HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
                response.send();
                return;
            }
            JSON::Object::Ptr file_meta = getFileMeta(meta_server_addr, filename);
            if(file_meta.isNull()) {
                response.setStatusAndReason(HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
                response.send();
                return;
            }
        }

        int64_t chunk_size = file_meta->getValue<int64_t>("chunk_size");
        int64_t original_length = file_meta->getValue<int64_t>("length");
        
        
        int64_t replica_count = file_meta->getValue<int64_t>("replica_count");
        
        int64_t end_pos = begin_pos + content.size();   // not include this pos
        int64_t first_chunk_idx = begin_pos/chunk_size;
        int64_t last_chunk_idx = (end_pos-1)/chunk_size;    // included
        int64_t chunk_num = last_chunk_idx-first_chunk_idx+1;

        if(begin_pos > original_length) {
            // Only allow append, does not allow to expand the file.
            response.setStatusAndReason(HTTPResponse::HTTP_BAD_REQUEST);
            response.send();
            return;
        }

        std::vector<std::string> chunk_ids;
        UUIDGenerator uuidGen;
        for(int i=0; i<chunk_num; i++) {
            chunk_ids.push_back(uuidGen.createOne().toString());
        }

        std::vector<std::vector<uint8_t>> chunks;

        // first chunk
        auto first = content.begin();
        auto last = first;

        if(chunk_size - (begin_pos%chunk_size) <= content.size()) {
            last = first + (chunk_size - (begin_pos%chunk_size));
        } else {
            last = content.end();
        }
        
        
        for(int i=0; i<chunk_num; i++) {
            chunks.push_back(std::vector<uint8_t>(first, last));
            first = last;
            if(i+1<chunk_num-1) {
                last += chunk_size;
            } else {
                last = content.end();
            }
        }
        std::cout << content.size() << std::endl;

        app.logger().information("Writing " + std::to_string(chunk_num) + " chunks.");

        bool all_ok = true;     // Operation finished on every chunk servers
        bool some_ok = true;    // At least one chunk server finished our operation for each chunks
        
        int64_t begin_chunks_idx = (begin_pos/chunk_size);

        JSON::Array::Ptr orig_chunks_json = file_meta->getArray("chunks");
        JSON::Object::Ptr chunk_servers_json = file_meta->getObject("chunk_servers");
        
        int64_t i;
        for(i=begin_chunks_idx; i<orig_chunks_json->size(); i++) {

            if(i-begin_chunks_idx < chunks.size()) {
                // Update the chunks we already have (and rename them)
                std::string orig_chunk_id = orig_chunks_json->getElement<std::string>(i);
                std::string new_chunk_id = chunk_ids[i-begin_chunks_idx];
                std::vector<uint8_t>& content = chunks[i-begin_chunks_idx];
                JSON::Array::Ptr servers_json = chunk_servers_json->getArray(orig_chunk_id);
                
                bool chunk_some_ok = false;
                for(int j=0; j<servers_json->size(); j++) {
                    JSON::Object::Ptr server_json = servers_json->getObject(j);
                    std::string chunk_server_id = server_json->getValue<std::string>("id");
                    std::string chunk_server_addr = server_json->getValue<std::string>("address");

                    int64_t content_begin_pos = 0;
                    if(i == begin_chunks_idx) {
                        content_begin_pos = begin_pos - begin_chunks_idx*chunk_size;
                    }
                    int64_t cur_chunk_length = content.size();
                    
                    std::cout << "Requesting update chunk on addr " + chunk_server_addr + " new_chunk id " + new_chunk_id << " content length " << content.size() << std::endl;
                    int resp_code = requestUpdateChunk(chunk_server_addr, orig_chunk_id, new_chunk_id, content_begin_pos, content);
                    if(resp_code != HTTPResponse::HTTP_OK) {
                        all_ok = false;
                    } else {
                        chunk_some_ok = true;
                    }

                }
                if(!chunk_some_ok) {
                    some_ok = false;
                }
            } else {
                // Maybe add a parameter to allow truncate the file directly using this api?
                // We'll leave this else case here, and jump out of the loop for now.
                i=orig_chunks_json->size();
                break;
            }
            
        }

        std::vector<std::pair<std::string, std::string>> chunk_servers = requestGetActiveChunkServersList(server.meta_server_addr);

        int replica = (int)replica_count;
        if(replica > chunk_servers.size()) {
            replica = chunk_servers.size();
        }

        for(;i-begin_chunks_idx < chunk_ids.size(); i++) {
            // create extra chunks on servers
            std::string chunk_id = chunk_ids[i-begin_chunks_idx];
            std::vector<uint8_t>& content = chunks[i-begin_chunks_idx];

            std::vector<int> indexes;
            for(int j=0; j<chunk_servers.size(); j++) {
                indexes.push_back(j);
            }
            std::shuffle(indexes.begin(), indexes.end(), std::default_random_engine());

            bool chunk_some_ok = false;
            for(int j=0; j<replica; j++) {
                std::cout << "Requesting create chunk on addr " + chunk_servers[indexes[j]].second + " chunk id " + chunk_id << " content length " << content.size() << std::endl;
                int resp_code = requestCreateChunk(chunk_servers[indexes[j]].second, chunk_id, content);

                if(resp_code != HTTPResponse::HTTP_OK) {
                    all_ok = false;
                } else {
                    chunk_some_ok = true;
                }
            }
            if(!chunk_some_ok) {
                some_ok = false;
            }

        }

        if(some_ok) {
            // commit and update chunk list
            URI uri("http://"+meta_server_addr);
            uri.setPath("/update_file_meta");

            HTTPRequest request(HTTPRequest::HTTP_POST, uri.getPathAndQuery());

            HTTPClientSession session(uri.getHost(), uri.getPort());
            JSON::Object::Ptr req_json(new JSON::Object);

            req_json->set("filename", filename);
            if(end_pos > original_length) {
                req_json->set("length", end_pos);
            } else {
                req_json->set("length", original_length);
            }

            JSON::Array::Ptr chunks_json(new JSON::Array);
            for(int i=0; i<begin_chunks_idx; i++) {
                std::string chunk_id = orig_chunks_json->getElement<std::string>(i);
                chunks_json->add(chunk_id);
            }
            for(int i=0; i < chunk_ids.size(); i++) {
                chunks_json->add(chunk_ids[i]);
            }
            for(int i=begin_chunks_idx+chunks.size(); i<orig_chunks_json->size(); i++) {
                std::string chunk_id = orig_chunks_json->getElement<std::string>(i);
                chunks_json->add(chunk_id);
            }

            req_json->set("chunks", chunks_json);

            req_json->stringify(session.sendRequest(request));

            HTTPResponse resp;
            std::istream& resp_stream = session.receiveResponse(resp);
            if(resp.getStatus() != HTTPResponse::HTTP_OK) {
                app.logger().information("Failed to update metadata, response code: "+std::to_string(resp.getStatus()));

                response.setStatusAndReason(HTTPResponse::HTTP_SERVICE_UNAVAILABLE);
                response.send();
                return;
            }

            response.setStatusAndReason(HTTPResponse::HTTP_OK);
            response.send();
        } else {
            response.setStatusAndReason(HTTPResponse::HTTP_SERVICE_UNAVAILABLE);
            response.send();
        }

    }
};

AccessServer::AccessServer() {
    help_requested = false;
    request_handler_factory = new AccessServerRequestHandlerFactory(this);
}

AccessServer::~AccessServer() {
    delete request_handler_factory;
}

void AccessServer::initialize(Application& self) {
    loadConfiguration();
    ServerApplication::initialize(self);
}

void AccessServer::uninitialize() {
    ServerApplication::uninitialize();
}

void AccessServer::defineOptions(OptionSet& options) {
    ServerApplication::defineOptions(options);

    options.addOption(
        Option("help", "h", "display help information")
            .required(false)
            .repeatable(false)
            .callback(OptionCallback<AccessServer>(this, &AccessServer::handleHelp))
    );
    options.addOption(
        Option("port", "p", "listening port")
            .required(false)
            .repeatable(false)
            .argument("port")
            .binding("AccessServer.port")
    );
    options.addOption(
        Option("metaserver", "m", "metadata server address")
            .required(false)
            .repeatable(false)
            .argument("metadata_server")
            .binding("AccessServer.meta_server_address")
    );
}

int AccessServer::main(const std::vector<std::string>& args) {
    if(help_requested) {
        return Application::EXIT_OK;
    }

    uint16_t port = (uint16_t)config().getInt("AccessServer.port", 22000);
    
    SocketAddress listen_addr(port);
    server_id = Environment::nodeName() + ":" + std::to_string(listen_addr.port());
    meta_server_addr = config().getString("AccessServer.meta_server_address", "");

    logger().information("DistFS AccessServer " + server_id + " starting...");
    logger().information("Metadata server address: " + meta_server_addr);
    if(meta_server_addr == "") {
        logger().warning("Metadata Server not defined, use -m \"ADDRESS:PORT\" to set metadata server address.");
    }

    ServerSocket server_socket(listen_addr);
    http_server = new HTTPServer(request_handler_factory, server_socket, new HTTPServerParams);

    http_server->start();
    waitForTerminationRequest();
    http_server->stop();

    return Application::EXIT_OK;
}

void AccessServer::handleHelp(const std::string& name, const std::string& value) {
    HelpFormatter help_formatter(options());
    help_formatter.setCommand(commandName());
    help_formatter.setUsage("OPTIONS");
    help_formatter.setHeader("DistFS Access Server.");
    help_formatter.format(std::cout);
    stopOptionsProcessing();
    help_requested = true;
}

AccessServerRequestHandlerFactory::AccessServerRequestHandlerFactory(AccessServer* srv) {
    this->server = srv;
}

HTTPRequestHandler* AccessServerRequestHandlerFactory::createRequestHandler(const HTTPServerRequest& request) {
    URI uri(request.getURI());
    if(uri.getPath() == "/ping") {
        return new PingRequestHandler();
    } else if(uri.getPath() == "/get_file") {
        return new GetFileRequestHandler();
    } else if(uri.getPath() == "/write_file") {
        return new WriteFileRequestHandler();
    }
}

}