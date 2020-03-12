#ifndef DISTFS_CHUNK_SERVER_H
#define DISTFS_CHUNK_SERVER_H

#include "common.h"

#include <Poco/Util/Subsystem.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/ServerApplication.h>
#include <Poco/Util/Option.h>
#include <Poco/Util/OptionSet.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketStream.h>
#include <Poco/Net/SocketReactor.h>
#include <Poco/Net/SocketAcceptor.h>
#include <Poco/Net/SocketNotification.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/NetException.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>

namespace DistFS {

using namespace Poco;
using namespace Poco::Util;
using namespace Poco::Net;

class ChunkServer;
class ChunkServerRequestHandlerFactory;

class ChunkServer: public ServerApplication {
public:
    ChunkServer();
    virtual ~ChunkServer();
    std::vector<std::string> getChunksList();

    Path root_directory;
    Path chunk_directory;
    std::string server_id;
    std::string meta_server_addr;

protected:
    void initialize(Application& self) override;
    void uninitialize() override;
    void defineOptions(OptionSet& options) override;
    int main(const std::vector<std::string>& args) override;

    void handleHelp(const std::string& name, const std::string& value);

    bool help_requested;

    HTTPServer* http_server;
    ChunkServerRequestHandlerFactory* request_handler_factory;

};

class ChunkServerRequestHandlerFactory: public HTTPRequestHandlerFactory {
public:
    ChunkServerRequestHandlerFactory(ChunkServer* srv);
    HTTPRequestHandler* createRequestHandler(const HTTPServerRequest& request) override;

protected:
    ChunkServer* server;
};

}
#endif