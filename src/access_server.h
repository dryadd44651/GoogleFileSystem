#ifndef DISTFS_ACCESS_SERVER_H
#define DISTFS_ACCESS_SERVER_H

#include "common.h"

namespace DistFS {

using namespace Poco;
using namespace Poco::Util;
using namespace Poco::Net;

class AccessServer;
class AccessServerRequestHandlerFactory;

class AccessServer: public ServerApplication {
public:
    AccessServer();
    virtual ~AccessServer();

    std::string meta_server_addr;

protected:
    void initialize(Application& self) override;
    void uninitialize() override;
    void defineOptions(OptionSet& options) override;
    int main(const std::vector<std::string>& args) override;

    void handleHelp(const std::string& name, const std::string& value);
    
    std::string server_id;
    bool help_requested;

    HTTPServer* http_server;
    AccessServerRequestHandlerFactory* request_handler_factory;
};

class AccessServerRequestHandlerFactory: public HTTPRequestHandlerFactory {
public:
    AccessServerRequestHandlerFactory(AccessServer* srv);
    HTTPRequestHandler* createRequestHandler(const HTTPServerRequest& request) override;

protected:
    AccessServer* server;
};

}
#endif