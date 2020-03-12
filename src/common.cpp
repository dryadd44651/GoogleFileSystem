#include "common.h"

#include <Poco/DirectoryIterator.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/StreamCopier.h>

using namespace DistFS;

Connection::Connection(StreamSocket& sock):
    fifoIn(BUFFER_SIZE),
    fifoOut(BUFFER_SIZE)
{
    socket = sock;
    socket.setBlocking(false);
    peer_id = "";
    peer_role = "unknown";

    bracket_level = -1;
    handshake_received = false;
}

Connection::~Connection() {
    // No need to close socket, because Poco will close the actual sockImpl if no socket reference to it.
}

std::string Connection::peerID() const {
    return peer_id;
}

std::string Connection::peerRole() const {
    return peer_role;
}

SocketAddress Connection::address() const {
    return socket.address();
}

SocketAddress Connection::peerAddress() const {
    return socket.peerAddress();
}

void Connection::sendMessage(JSON::Object::Ptr msg) {
    std::stringstream outbuf;
    msg->stringify(outbuf);

    std::string msg_str = outbuf.str();
    if(fifoOut.available() < msg_str.size()) {
        fifoOut.resize(fifoOut.used()+msg_str.size(), true);
    }

    fifoOut.mutex().lock();
    fifoOut.write(msg_str.c_str(), msg_str.size());
    fifoOut.mutex().unlock();
}

void Connection::onHandshake(JSON::Object::Ptr msg) {
    try {
        std::string command = msg->getValue<std::string>("command");
        if(command != "handshake") {
            return;
        }
        std::string id = msg->getValue<std::string>("id");
        std::string role = msg->getValue<std::string>("role");
        peer_id = id;
        peer_role = role;
        handshake_received = true;
    } catch(...) {
        return;
    }
}

void Connection::sendHandshake(std::string id, std::string role) {
    JSON::Object::Ptr msg(new JSON::Object());
    msg->set("command", "handshake");
    msg->set("id", id);
    msg->set("role", role);

    JSON::Object::Ptr params(new JSON::Object());
    std::string peer_addr = socket.peerAddress().toString();
    msg->set("params", params);

    sendMessage(msg);
}

bool Connection::processIO() {
    if(fifoOut.isReadable()) {
        size_t byte_sent = socket.sendBytes(fifoOut);
    }

    int bytes_received = 0;

    try {
        bytes_received = socket.receiveBytes(fifoIn);
        // socket is in non-blocking mode.
        // a negative return value will be returned (-1 on windows) if blocked.
    } catch(ConnectionAbortedException e) {
        // Connection aborted
        try {
            Application& app = Application::instance();
            app.logger().warning(e.displayText());
        } catch(...) {
            // Do nothing if can't log.
        }

        onMessage(peerClosedMsg());
        return false;
    } catch(IOException e) {
        onMessage(peerClosedMsg());
        return false;
    }

    if(fifoIn.isReadable()) {
        try {
            processInput();
        } catch(JSON::JSONException) {
            onMessage(peerClosedMsg());
            return false;
        }
        
    }

    if(bytes_received == 0) {
        // Connection closed by peer.
        onMessage(peerClosedMsg());
        return false;
    }
    
    return true;
}

void Connection::flush() {
    try {
        if(fifoOut.isReadable()) {
            size_t byte_sent = socket.sendBytes(fifoOut);
            fifoOut.drain(byte_sent);
        }
    } catch(...) {

    }
}

void Connection::processInput() {
    while(fifoIn.isReadable()) {
        char bbuf;
        fifoIn.read(&bbuf, 1);

        if(bracket_level == -1) {
            if(bbuf == '{') {
                bracket_level = 1;
                jsonBuffer << bbuf;
            }
        } else {
            if(bbuf == '{') {
                bracket_level += 1;
            } else if(bbuf == '}') {
                bracket_level -= 1;
            }
            jsonBuffer << bbuf;
        }

        if(bracket_level == 0) {
            Var json_msg = jsonParser.parse(jsonBuffer);
            
            bracket_level = -1;
            jsonBuffer.clear();

            JSON::Object::Ptr msg = json_msg.extract<JSON::Object::Ptr>();

            // Handshake must be send before any message.
            if(!handshake_received) {
                onHandshake(msg);
            } else {
                onMessage(msg);
            }
        }
    }
}

JSON::Object::Ptr Connection::peerClosedMsg() {
    JSON::Object::Ptr msg(new JSON::Object());
    msg->set("command", "closed");

    JSON::Object::Ptr params(new JSON::Object());
    params->set("peer_id", peer_id);    // can be empty string if handshake failed
    std::string peer_addr = socket.peerAddress().toString();
    params->set("peer_addr", peer_addr);

    msg->set("params", params);
    return msg;
}

namespace DistFS {

void PingRequestHandler::handleRequest(HTTPServerRequest& request, HTTPServerResponse& response) {
    Application& app = Application::instance();

    JSON::Object::Ptr json_resp(new JSON::Object);
    json_resp->set("status", "success");
    std::ostream& ostr = response.send();
    json_resp->stringify(ostr);
}

JSON::Object::Ptr ChunkInfo::toJSON() {
    JSON::Object::Ptr json(new JSON::Object);
    json->set("chunk_id", chunk_id);
    json->set("chunk_size", chunk_size);
    json->set("content_length", content_length);

    JSON::Array::Ptr locations(new JSON::Array);
    for(auto it=locations->begin(); it!=locations->end(); ++it) {
        locations->add(*it);
    }
    json->set("locations", locations);
    return json;
}

ChunkInfo* ChunkInfo::fromJSON(JSON::Object::Ptr json) {
    ChunkInfo* obj = new ChunkInfo();
    obj->chunk_id = json->getValue<std::string>("chunk_id");
    obj->chunk_size = json->getValue<int64_t>("chunk_size");
    obj->content_length = json->getValue<int64_t>("content_length");
    
    JSON::Array::Ptr locations = json->getArray("locations");

    for(int i=0; i<locations->size(); i++) {
        obj->locations.push_back(locations->getElement<std::string>(i));
    }
    return obj;
}

JSON::Object::Ptr FileInfo::toJSON() {
    JSON::Object::Ptr json(new JSON::Object);
    json->set("filename", filename);
    json->set("length", length);
    json->set("chunk_size", chunk_size);
    json->set("chunk_count", chunk_count);
    
    JSON::Array::Ptr chunks(new JSON::Array);
    for(auto it=chunks->begin(); it!=chunks->end(); ++it) {
        chunks->add(*it);
    }
    json->set("chunks", chunks);
    return json;
}

FileInfo* fromJSON(JSON::Object::Ptr json) {
    FileInfo* obj = new FileInfo();
    obj->filename = json->getValue<std::string>("filename");
    obj->length = json->getValue<int64_t>("length");
    obj->chunk_size = json->getValue<int64_t>("chunk_size");
    obj->chunk_count = json->getValue<int64_t>("chunk_count");

    JSON::Array::Ptr chunks = json->getArray("chunks");
    for(int i=0; i<chunks->size(); i++) {
        obj->chunks.push_back(chunks->getElement<std::string>(i));
    }
    return obj;
}

std::vector<std::string> listDirectory(Path& path) {
    DirectoryIterator end;
    std::vector<std::string> list;

    for(DirectoryIterator it(path); it != end; ++it) {
        list.push_back(it.name());
    }

    return list;
}

bool makeDirectories(Path& path) {
    File dir(path);
    if(dir.exists()) {
        if(dir.isDirectory()) {
            return true;
        }
        return false;
    }
    dir.createDirectories();
    return true;
}

std::map<std::string, std::string> getQueryMap(const URI uri) {
    std::map<std::string, std::string> ret;
    auto para = uri.getQueryParameters();

    for(auto it=para.begin(); it!=para.end(); ++it) {
        ret[it->first] = it->second;
    }
    return ret;
}

std::vector<uint8_t> getChunk(std::string& address, std::string chunk_id) {
    URI uri("http://"+address);
    uri.setPath("/get_chunk");
    URI::QueryParameters param = {
        {"chunk_id", chunk_id}
    };

    uri.setQueryParameters(param);
    HTTPRequest request(HTTPRequest::HTTP_GET, uri.getPathAndQuery(), HTTPMessage::HTTP_1_1);

    HTTPClientSession session(uri.getHost(), uri.getPort());
    std::ostream& out = session.sendRequest(request);

    HTTPResponse response;
    std::istream& resp_stream = session.receiveResponse(response);

    std::vector<uint8_t> content;

    if(response.getStatus() != HTTPResponse::HTTP_OK) {
        // return empty content
        return content;
    }

    std::istream_iterator<uint8_t> isit(resp_stream);
    std::copy(isit, std::istream_iterator<uint8_t>(), std::back_inserter(content));
    return content;
}

bool writeChunksOnServers(std::vector<std::string>& addresses, std::string chunk_id, std::istream& content) {
    HTTPRequest request(HTTPRequest::HTTP_POST, "/create_chunk", HTTPMessage::HTTP_1_1);
    request.setContentType("application/octet-stream");

    for(auto it=addresses.begin(); it!=addresses.end(); ++it) {
        std::string address = (*it);
        URI uri(address);
        HTTPClientSession session(uri.getHost(), uri.getPort());

        session.setKeepAlive(true);
        std::ostream& out = session.sendRequest(request);
        content.seekg(0);
        StreamCopier::copyStream(content, out);
    }

    return true;
}

JSON::Object::Ptr getFileMeta(std::string address, std::string filename) {
    URI uri("http://"+address);
    uri.setPath("/get_file_meta");
    URI::QueryParameters param = {
        {"filename", filename}
    };
    uri.setQueryParameters(param);
    HTTPRequest request(HTTPRequest::HTTP_GET, uri.getPathAndQuery(), HTTPMessage::HTTP_1_1);
    
    HTTPClientSession session(uri.getHost(), uri.getPort());

    std::ostream& out = session.sendRequest(request);

    HTTPResponse response;
    std::istream& resp_stream = session.receiveResponse(response);

    if(response.getStatus() != HTTPResponse::HTTP_OK) {
        return JSON::Object::Ptr();
    }

    JSON::Parser jsonParser;
    JSON::Object::Ptr resp_json = jsonParser.parse(resp_stream).extract<JSON::Object::Ptr>();

    return resp_json;
}

int requestCreateFile(std::string address, std::string filename) {
    URI uri("http://"+address);
    uri.setPath("/create_file");
    URI::QueryParameters param = {
        {"filename", filename}
    };
    uri.setQueryParameters(param);
    HTTPRequest request(HTTPRequest::HTTP_GET, uri.getPathAndQuery(), HTTPMessage::HTTP_1_1);

    HTTPClientSession session(uri.getHost(), uri.getPort());

    std::ostream& out = session.sendRequest(request);

    HTTPResponse response;
    std::istream& istr = session.receiveResponse(response);
    
    return response.getStatus();
}

int requestCreateChunk(std::string address, std::string chunk_id, std::vector<uint8_t>& content) {
    URI uri("http://"+address);
    uri.setPath("/create_chunk");
    URI::QueryParameters param = {
        {"chunk_id", chunk_id}
    };
    uri.setQueryParameters(param);
    HTTPRequest request(HTTPRequest::HTTP_POST, uri.getPathAndQuery());
    request.setContentType("application/octet-stream");

    HTTPClientSession session(uri.getHost(), uri.getPort());

    std::ostream& out = session.sendRequest(request);
    StreamCopier copier;
    out.write((char*)content.data(), content.size());
    out.flush();
    
    HTTPResponse response;
    std::istream& istr = session.receiveResponse(response);

    return response.getStatus();
}

int requestUpdateChunk(std::string address, std::string chunk_id, std::string new_id, int64_t begin_pos, std::vector<uint8_t>& content) {
    URI uri("http://"+address);
    uri.setPath("/update_chunk");
    URI::QueryParameters param = {
        {"chunk_id", chunk_id},
        {"new_id", new_id},
        {"begin_pos", std::to_string(begin_pos)},
    };
    uri.setQueryParameters(param);
    HTTPRequest request(HTTPRequest::HTTP_POST, uri.getPathAndQuery());
    request.setContentType("application/octet-stream");

    HTTPClientSession session(uri.getHost(), uri.getPort());

    std::ostream& out = session.sendRequest(request);

    out.write((char*)content.data(), content.size());
    out.flush();

    HTTPResponse response;
    std::istream& istr = session.receiveResponse(response);

    return response.getStatus();
}

int requestUpdateChunksList(std::string address, std::string chunk_server_id, std::vector<std::string> chunks_list) {
    URI uri("http://"+address);
    uri.setPath("/update_chunks_list");

    HTTPRequest request(HTTPRequest::HTTP_POST, uri.getPathAndQuery());

    HTTPClientSession session(uri.getHost(), uri.getPort());
    JSON::Object::Ptr req_json(new JSON::Object);

    req_json->set("server_id", chunk_server_id);
    req_json->set("timestamp", DateTime().timestamp().utcTime());

    JSON::Array::Ptr chunks_json(new JSON::Array);
    for(auto it=chunks_list.begin(); it!=chunks_list.end(); ++it) {
        chunks_json->add(*it);
    }
    req_json->set("chunks", chunks_json);

    try {
        std::ostream& out = session.sendRequest(request);
        req_json->stringify(out);
    } catch(NetException e) {
        std::cout << e.displayText() << std::endl;
        std::cout << e.message() << std::endl;
    }

    HTTPResponse response;
    std::istream& istr = session.receiveResponse(response);

    return response.getStatus();

}

std::vector<std::pair<std::string, std::string>> requestGetActiveChunkServersList(std::string address) {
    URI uri("http://"+address);
    uri.setPath("/get_active_chunk_servers");

    HTTPRequest request(HTTPRequest::HTTP_GET, uri.getPathAndQuery(), HTTPMessage::HTTP_1_1);
    
    HTTPClientSession session(uri.getHost(), uri.getPort());

    std::ostream& out = session.sendRequest(request);

    HTTPResponse response;
    std::istream& resp_stream = session.receiveResponse(response);

    std::vector<std::pair<std::string, std::string>> result;
    if(response.getStatus() != HTTPResponse::HTTP_OK) {
        return result;
    }

    JSON::Parser jsonParser;
    JSON::Object::Ptr resp_json = jsonParser.parse(resp_stream).extract<JSON::Object::Ptr>();

    JSON::Array::Ptr servers_json = resp_json->getArray("chunk_servers");

    for(int i=0; i<servers_json->size(); i++) {
        std::string id = servers_json->getObject(i)->getValue<std::string>("id");
        std::string addr = servers_json->getObject(i)->getValue<std::string>("address");
        result.push_back({id, addr});
    }
    return result;
}

}
