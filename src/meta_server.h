#ifndef DISTFS_METADATA_SERVER_H
#define DISTFS_METADATA_SERVER_H

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

class MetaServer;
class MetaServerRequestHandlerFactory;

class MetaServer: public ServerApplication {
public:
    MetaServer();
    virtual ~MetaServer();
    Path root_directory;
    Path meta_directory;
    int64_t default_chunk_size = 4096;
    int64_t default_replica_count = 3;

    Mutex chunks_map_mutex;
    std::map<std::string, std::vector<std::string>> server_chunks_map;
    std::map<std::string, std::vector<std::string>> chunk_servers_map;

    std::vector<std::string> live_chunk_servers;
    std::map<std::string, std::string> servers_id_address_map;
    std::map<std::string, int64_t> chunk_server_last_heatbeat;
	//====add by hua
	std::map<std::string, int64_t> chunk_servers_time_map;

protected:
    void initialize(Application& self) override;
    void uninitialize() override;
    void defineOptions(OptionSet& options) override;
    int main(const std::vector<std::string>& args) override;

    void handleHelp(const std::string& name, const std::string& value);
    void loadServersList();

    std::string server_id;
    bool help_requested;

    HTTPServer* http_server;
    MetaServerRequestHandlerFactory* request_handler_factory;

};

class MetaServerRequestHandlerFactory: public HTTPRequestHandlerFactory {
public:
    MetaServerRequestHandlerFactory(MetaServer* srv);
    HTTPRequestHandler* createRequestHandler(const HTTPServerRequest& request) override;

protected:
    MetaServer* server;
};

}
#endif