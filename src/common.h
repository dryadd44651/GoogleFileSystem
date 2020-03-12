#ifndef DISTFS_COMMON_H
#define DISTFS_COMMON_H

#include <vector>
#include <string>
#include <queue>
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
#include <Poco/Net/NetException.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>
#include <Poco/NObserver.h>
#include <Poco/AutoPtr.h>
#include <Poco/Dynamic/Var.h>
#include <Poco/Dynamic/Struct.h>
#include <Poco/Thread.h>
#include <Poco/FIFOBuffer.h>
#include <Poco/Delegate.h>
#include <Poco/URI.h>

namespace DistFS {

using namespace Poco;
using namespace Poco::Util;
using namespace Poco::Net;
using Poco::Dynamic::Var;

class Connection {

public:
    Connection(StreamSocket& sock);
    virtual ~Connection();

    std::string peerID() const;
    std::string peerRole() const;
    SocketAddress address() const;
    SocketAddress peerAddress() const;

    virtual void sendMessage(JSON::Object::Ptr msg);
    virtual void onMessage(JSON::Object::Ptr msg) = 0;
    virtual void onHandshake(JSON::Object::Ptr msg);

    void sendHandshake(std::string id, std::string role);

    bool processIO();
    void flush();

protected:

    void processInput();

    JSON::Object::Ptr peerClosedMsg();

    std::string peer_id;
    std::string peer_role;
    StreamSocket socket;

    const size_t BUFFER_SIZE = 1024;
    FIFOBuffer fifoIn;
    FIFOBuffer fifoOut;

    // variable used in processInput()
    int bracket_level;
    std::stringstream jsonBuffer;
    JSON::Parser jsonParser;

    // handshake
    bool handshake_received;
};

template <class TConn> class ConnectionManager: public Runnable {
public:
    ConnectionManager() {
        stop_requested = false;
    }
    virtual ~ConnectionManager() {
        // all clean up are done in run().
    }

    void setListenAddress(SocketAddress addr) {
        this->addr = addr;
    }

    SocketAddress listenAddress() {
        return this->addr;
    }

    void run() override {
        stop_requested = false;

        socket.bind(addr);
        socket.setBlocking(false);
        socket.listen();

        while(!stop_requested) {
            try {
                while(socket.poll(Timespan(0), Socket::SELECT_READ)) {
                    StreamSocket s = socket.acceptConnection();
                    try {
                        TConn& conn = makeConnection(s);
                        addConnection(conn);
                        Application& app = Application::instance();
                        app.logger().information("Accepting connection from "+conn.peerAddress().toString()+".");
                    } catch(Exception e) {
                        Application& app = Application::instance();
                        app.logger().warning(e.displayText());
                    } catch(...) {
                        Application& app = Application::instance();
                        app.logger().warning("Unknown exception caught while accepting connection");
                    }
                }
                // Lets slow down the thread to reduce CPU requirement when testing.
                Thread::sleep(1);
            } catch(IOException e) {
                if(e.code() == POCO_EWOULDBLOCK) {
                    // Socket will block.
                    // This shouldn't happen since we uses poll.
                    Thread::sleep(1);
                } else {
                    // TODO: Handle it gracefully.
                    e.rethrow();
                }
            } catch(...) {
                Application& app = Application::instance();
                app.logger().warning("Unknown exception caught while polling socket.");
            }
            // Do connection loop
            { // connection_map_mutex lock scope
                ScopedLock<Mutex> connection_map_lock(connection_map_mutex);
                for(auto it=connection_map.begin(); it != connection_map.end();) {
                    bool ok = it->second->processIO();
                    if(!ok) {
                        Application& app = Application::instance();
                        app.logger().information("Connection to "+it->second->peerAddress().toString()+" closed");
                        delete it->second;
                        it = connection_map.erase(it);
                        
                    } else {
                        ++it;
                    }
                }
            }
        }

        // Graceful shutdown
        { // connection_map_mutex lock scope
            ScopedLock<Mutex> connection_map_lock(connection_map_mutex);
            for(auto it=connection_map.begin(); it != connection_map.end(); ++it) {
                it->second->flush();
                delete it->second;
            }
            connection_map.clear();
        }
    }

    void stop() {
        stop_requested = true;
    }

    void addConnection(TConn& conn) {
        SocketAddress peer_addr = conn.peerAddress();
        ScopedLock<Mutex> lock(connection_map_mutex);
        connection_map[peer_addr] = &conn;
    }

    TConn* getConnection(SocketAddress addr) {
        TConn* t;
        try {
            t = connection_map.at(addr);
        } catch(std::out_of_range) {
            t = nullptr;
        }
        return t;
    }

    // removeConnection just remove the connection from the manager, but does not destroy the connection.
    void removeConnection(TConn& conn) {
        ScopedLock<Mutex> lock(connection_map_mutex);
        for(auto it=connection_map.begin(); it != connection_map.end();) {
            if(it->second == &conn) {
                connection_map.erase(it);
                break;
            } else {
                ++it;
            }
        }
    }

    TConn* connectTo(SocketAddress addr) {
        StreamSocket socket; // Poco::Socket is reference counted, so no worry of the object scope.
        socket.connect(addr);
        TConn& conn = makeConnection(socket);
        addConnection(conn);
        return &conn;
    }

    const std::map<SocketAddress, TConn*>& connectionMap() {
        return connection_map;
    }

    virtual TConn& makeConnection(StreamSocket& sock) = 0;

    Mutex connection_map_mutex;

protected:
    ServerSocket socket;
    SocketAddress addr;
    bool stop_requested;
    
    std::map<SocketAddress, TConn*> connection_map;
};

class PingRequestHandler: public HTTPRequestHandler {
public:
    void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response) override;
};

class ChunkInfo {
public:
    std::string chunk_id;
    int64_t chunk_size;
    int64_t content_length;
    std::vector<std::string> locations;
    
    JSON::Object::Ptr toJSON();
    static ChunkInfo* fromJSON(JSON::Object::Ptr obj);
};

class FileInfo {
public:
    std::string filename;
    int64_t length;
    int64_t chunk_size;
    int64_t chunk_count;
    std::vector<std::string> chunks;

    JSON::Object::Ptr toJSON();
    static FileInfo* fromJSON(JSON::Object::Ptr obj);
};

std::vector<std::string> listDirectory(Path& path);
bool makeDirectories(Path& path);
std::map<std::string, std::string> getQueryMap(const URI uri);
std::vector<uint8_t> getChunk(std::string& address, std::string chunk_id);

bool writeChunksOnServers(std::vector<std::string>& addresses, std::string chunk_id, std::istream& content);
JSON::Object::Ptr getFileMeta(std::string address, std::string filename);
int requestCreateFile(std::string address, std::string filename);
int requestCreateChunk(std::string address, std::string chunk_id, std::vector<uint8_t>& content);
int requestUpdateChunk(std::string address, std::string chunk_id, std::string new_id, int64_t begin_pos, std::vector<uint8_t>& content);
int requestUpdateChunksList(std::string address, std::string chunk_server_id, std::vector<std::string> chunks_list);
std::vector<std::pair<std::string, std::string>> requestGetActiveChunkServersList(std::string address);
}
#endif