#include "chunk_server.h"

#include <Poco/Logger.h>
#include <Poco/Util/HelpFormatter.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/Environment.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/StreamCopier.h>
#include <iostream>
#include <fstream>
#include <iterator>
#include <algorithm>

namespace DistFS {

class GetChunkRequestHandler: public HTTPRequestHandler {
public:
    void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response) {
        Application& app = Application::instance();
        ChunkServer& server = dynamic_cast<ChunkServer&>(app);

        std::map<std::string, std::string> query_map = getQueryMap(URI(request.getURI()));
        std::string chunk_id = query_map["chunk_id"];

        /* Use json to get chunk id
        JSON::Parser jsonParser;
        JSON::Object::Ptr req_json = jsonParser.parse(request.stream()).extract<JSON::Object::Ptr>();
        std::string chunk_id = req_json->getValue<std::string>("chunk_id");
        //*/

        File chunk_file(Path(server.chunk_directory).append(chunk_id));
        
        if(!chunk_file.exists()  || !chunk_file.isFile()) {
            response.setStatusAndReason(HTTPResponse::HTTP_NOT_FOUND);
            return;
        }

        response.setStatusAndReason(HTTPResponse::HTTP_OK);
        response.setContentType("application/octet-stream");

        { // ifile scope
            std::ifstream ifile(chunk_file.path().c_str(), std::ios::binary);
            std::ostream& ostr = response.send();
            ostr << ifile.rdbuf();
            ifile.close();
        }
    }
};

class ForcePushChunksListRequestHandler: public HTTPRequestHandler {
public:
    void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response) {
        Application& app = Application::instance();
        ChunkServer& server = dynamic_cast<ChunkServer&>(app);

        std::string meta_server_addr = server.meta_server_addr;

        app.logger().information("Pushing chunks list to "+ meta_server_addr+"...");

        std::vector<std::string> chunks_list = server.getChunksList();
        int resp_code = requestUpdateChunksList(meta_server_addr, server.server_id, chunks_list);

        app.logger().information("Push chunks list response code: "+std::to_string(resp_code));
        response.setStatusAndReason(HTTPResponse::HTTP_OK);
        response.send();
    }
};

class CreateChunkRequestHandler: public HTTPRequestHandler {
public:
    void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response) {
        Application& app = Application::instance();
        ChunkServer& server = dynamic_cast<ChunkServer&>(app);
        std::map<std::string, std::string> query_map = getQueryMap(URI(request.getURI()));

        std::string chunk_id = query_map["chunk_id"];

        File chunk_file(Path(server.chunk_directory).append(chunk_id));
        
        { // ofile scope
            std::ofstream ofile(chunk_file.path().c_str(), std::ios::out|std::ios::binary);
            std::istream& istr = request.stream();
            StreamCopier copier;
            copier.copyStream(istr, ofile);
            ofile.close();
        }

        response.setStatusAndReason(HTTPResponse::HTTP_OK);
        response.setContentType("application/json");
        JSON::Object::Ptr resp_json(new JSON::Object);
        resp_json->set("status", "success");
        std::ostream& ostr = response.send();
        resp_json->stringify(ostr);

        //std::vector<std::string> chunks_list = server.getChunksList();
        //requestUpdateChunksList(server.meta_server_addr, server.server_id, chunks_list);
    } 
};

class UpdateChunkRequestHandler: public HTTPRequestHandler {
public:
    void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response) {
        Application& app = Application::instance();
        ChunkServer& server = dynamic_cast<ChunkServer&>(app);
        std::map<std::string, std::string> query_map = getQueryMap(URI(request.getURI()));

        std::string chunk_id = query_map["chunk_id"];
        std::string new_id = query_map["new_id"];
        int64_t begin_pos = std::stoi(query_map["begin_pos"]);

        File chunk_file(Path(server.chunk_directory).append(chunk_id));
        
        { // file scope
            std::fstream file(chunk_file.path().c_str(), std::ios::in|std::ios::out|std::ios::binary);
            file.seekp(begin_pos);
            std::istream& istr = request.stream();
            std::cout << istr.tellg();
            //file << istr.rdbuf();
            //file.close();
            StreamCopier copier;
            copier.copyStream(istr, file);
            file.close();
            std::cout << istr.tellg();
        }
        // rename the file
        chunk_file.copyTo(Path(server.chunk_directory).append(new_id).toString());
        //chunk_file.renameTo(Path(server.chunk_directory).append(new_id).toString());

        response.setStatusAndReason(HTTPResponse::HTTP_OK);
        response.setContentType("application/json");
        JSON::Object::Ptr resp_json(new JSON::Object);
        resp_json->set("status", "success");
        std::ostream& ostr = response.send();
        resp_json->stringify(ostr);

        //std::vector<std::string> chunks_list = server.getChunksList();
        //requestUpdateChunksList(server.meta_server_addr, server.server_id, chunks_list);
    } 
};

class DeleteChunkRequestHandler: public HTTPRequestHandler {
public:
    void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response) {
        Application& app = Application::instance();
        ChunkServer& server = dynamic_cast<ChunkServer&>(app);
        JSON::Parser jsonParser;
        JSON::Object::Ptr req_json = jsonParser.parse(request.stream()).extract<JSON::Object::Ptr>();
        std::string chunk_id = req_json->getValue<std::string>("chunk_id");

        File chunk_file(Path(server.chunk_directory).append(chunk_id));
        
        if(!chunk_file.exists()  || !chunk_file.isFile()) {
            response.setStatusAndReason(HTTPResponse::HTTP_NOT_FOUND);
            return;
        }

        chunk_file.remove();
        response.setStatusAndReason(HTTPResponse::HTTP_OK);
    }
};

class ListChunksRequestHandler: public HTTPRequestHandler {
public:
    void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response) {
        Application& app = Application::instance();
        ChunkServer& server = dynamic_cast<ChunkServer&>(app);
        
        JSON::Object::Ptr resp_json(new JSON::Object);

        std::vector<std::string> chunks_list = server.getChunksList();

        JSON::Array::Ptr chunks_list_json(new JSON::Array());
        for(auto it=chunks_list.begin(); it!=chunks_list.end(); ++it) {
            chunks_list_json->add(*it);
        }

        resp_json->set("status", "success");
        resp_json->set("chunks", chunks_list_json);

        response.setStatusAndReason(HTTPResponse::HTTP_OK);
        std::ostream& ostr = response.send();
        resp_json->stringify(ostr);
    }
};

ChunkServer::ChunkServer() {
    help_requested = false;
    request_handler_factory = new ChunkServerRequestHandlerFactory(this);
}

ChunkServer::~ChunkServer() {

}

std::vector<std::string> ChunkServer::getChunksList() {
    return listDirectory(chunk_directory);
}

void ChunkServer::initialize(Application& self) {
    loadConfiguration();
    ServerApplication::initialize(self);
}

void ChunkServer::uninitialize() {
    ServerApplication::uninitialize();
}

void ChunkServer::defineOptions(OptionSet& options)
{
    ServerApplication::defineOptions(options);

    options.addOption(
        Option("help", "h", "display help information")
            .required(false)
            .repeatable(false)
            .callback(OptionCallback<ChunkServer>(this, &ChunkServer::handleHelp))
    );
    options.addOption(
        Option("port", "p", "listening port")
            .required(false)
            .repeatable(false)
            .argument("port")
            .binding("ChunkServer.port")
    );
    options.addOption(
        Option("root_directory", "d", "root directory")
            .required(false)
            .repeatable(false)
            .argument("root_directory")
            .binding("ChunkServer.root_directory")
    );
    options.addOption(
        Option("metaserver", "m", "metadata server address")
            .required(false)
            .repeatable(false)
            .argument("metadata_server")
            .binding("ChunkServer.meta_server_address")
    );
}

//==========add by Hua
class heartBeatSender :public Poco::Runnable {
private:
	int n;
	ChunkServer* server;
public:
	heartBeatSender(int n, ChunkServer *server) { 
		this->server = server;
		this->n = n; 
	}
	virtual void run() {
		while (true)
		{
			std::vector<std::string> chunks_list = server->getChunksList();
			try
			{
				requestUpdateChunksList(server->meta_server_addr, server->server_id, chunks_list);
			}
			catch (const std::exception&)
			{
				std::cout<<"update ChunksList fail"<<std::endl;
			}
			

			Thread::sleep(this->n * 1000);
		}
	}

};

int ChunkServer::main(const std::vector<std::string>& args) {
    if(help_requested) {
        return Application::EXIT_OK;
    }

    uint16_t port = (uint16_t)config().getInt("ChunkServer.port", 21000);
    std::string config_root_directory = config().getString("ChunkServer.root_directory", "files/");
    root_directory = Path(config_root_directory);
    chunk_directory = Path(root_directory).pushDirectory("chunks");
    meta_server_addr = config().getString("ChunkServer.meta_server_address", "");

    SocketAddress listen_addr(port);
    server_id = Environment::nodeName() + ":" + std::to_string(listen_addr.port());

    logger().information("DistFS ChunkServer " + server_id + " starting...");
    logger().information("Metadata server address: " + meta_server_addr);
    if(meta_server_addr == "") {
        logger().warning("Metadata Server not defined, use -m \"ADDRESS:PORT\" to set metadata server address.");
    }
    makeDirectories(root_directory);
    makeDirectories(chunk_directory);

    ServerSocket server_socket(listen_addr);
    http_server = new HTTPServer(request_handler_factory, server_socket, new HTTPServerParams);


	//==========add by Hua
	heartBeatSender sender(1,this);
	Thread heartBeat;
	heartBeat.start(sender);


    http_server->start();
    waitForTerminationRequest();
    http_server->stop();

    return Application::EXIT_OK;
}

void ChunkServer::handleHelp(const std::string& name, const std::string& value) {
    HelpFormatter help_formatter(options());
    help_formatter.setCommand(commandName());
    help_formatter.setUsage("OPTIONS");
    help_formatter.setHeader("DistFS Metadata Server.");
    help_formatter.format(std::cout);
    stopOptionsProcessing();
    help_requested = true;
}

ChunkServerRequestHandlerFactory::ChunkServerRequestHandlerFactory(ChunkServer* srv) {
    this->server = srv;
}

HTTPRequestHandler* ChunkServerRequestHandlerFactory::createRequestHandler(const HTTPServerRequest& request) {
    URI uri(request.getURI());
    if(uri.getPath() == "/ping") {
        return new PingRequestHandler();
    } else if(uri.getPath() == "/get_chunk") {
        return new GetChunkRequestHandler();
    } else if(uri.getPath() == "/force_push_chunks_list") {
        return new ForcePushChunksListRequestHandler();
    } else if(uri.getPath() == "/create_chunk") {
        return new CreateChunkRequestHandler();
    } else if(uri.getPath() == "/update_chunk") {
        return new UpdateChunkRequestHandler();
    } else if(uri.getPath() == "/delete_chunk") {
        return new DeleteChunkRequestHandler();
    } else if(uri.getPath() == "/list_chunks") {
        return new ListChunksRequestHandler();
    }
    return nullptr;
}

}