#include "meta_server.h"

#include <Poco/Logger.h>
#include <Poco/Util/HelpFormatter.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/Environment.h>
#include <Poco/UUIDGenerator.h>
#include <iostream>
#include <fstream>

namespace DistFS {

	class ListDirectoryRequestHandler : public HTTPRequestHandler {
	public:
		void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response) {
			Application& app = Application::instance();
			MetaServer& server = dynamic_cast<MetaServer&>(app);

			std::vector<std::string> files_list = listDirectory(server.meta_directory);

			JSON::Array::Ptr files(new JSON::Array);
			for (int i = 0; i < files_list.size(); i++) {
				files->set(i, files_list[i]);
			}

			response.setStatusAndReason(HTTPResponse::HTTP_OK);
			response.setContentType("application/json");

			JSON::Object::Ptr json_resp(new JSON::Object);
			json_resp->set("status", "success");
			json_resp->set("files", files);
			std::ostream& ostr = response.send();
			json_resp->stringify(ostr);
		}
	};

	//added by Hua
	class heartBeatChecker :public Poco::Runnable {
	private:
		MetaServer *server;
		int n;
		
	public:
		heartBeatChecker(int n, MetaServer *server) {
			this->server = server;
			this->n = n;
		}
		template < typename T>
		void Delet(std::vector<T> &src,T target) {
			std::vector<T>::iterator it;
			for (it = src.begin(); it != src.end(); it++) {
				if (*it == target)
				{
					src.erase(it);
					break;
				}
			}
		}
		virtual void run() {
			while (true)
			{
				//std::vector<std::string> chunks_list = server->getChunksList();
				try
				{
					//requestUpdateChunksList(server->meta_server_addr, server->server_id, chunks_list);
					//server->chunk_servers_time_map[server_id]
					int64_t timestamp = DateTime().timestamp().utcTime();
					int timeDiff = 0;
					std::map<std::string, int64_t>::iterator it;
					//std::map<std::string, int64_t>::iterator begin = server->chunk_servers_time_map.cbegin();
					auto begin = server->chunk_servers_time_map.cbegin();
					auto end = server->chunk_servers_time_map.cend();
					//for (it = begin; it != end; it++) {
					for (auto it = begin; it != end;) {
						std::cout << it->first << " => " << it->second << '\n';
						//std::cout << (timestamp - it->second) / 10000000 << '\n';
						timeDiff = (timestamp - it->second) / 10000000;
						if (timeDiff >= 5) {
							Delet(server->live_chunk_servers, it->first);
							server->chunk_servers_time_map.erase(it++);
						}
						else
						{
							++it;
						}
					}
						
						
				}
				catch (const std::exception&)
				{
					std::cout << "error" << std::endl;
				}


				Thread::sleep(this->n * 1000);
			}
		}

	};

	class UpdateChunksListRequestHandler : public HTTPRequestHandler {
	public:
		void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response) {
			Application& app = Application::instance();
			MetaServer& server = dynamic_cast<MetaServer&>(app);

			JSON::Parser jsonParser;
			JSON::Object::Ptr json_req = jsonParser.parse(request.stream()).extract<JSON::Object::Ptr>();

			std::string server_id = json_req->getValue<std::string>("server_id");
			int64_t timestamp = json_req->getValue<int64_t>("timestamp");

			JSON::Array::Ptr chunks_list_json = json_req->getArray("chunks");

			{
				// This api will update all chunk record for that server.
				// We need to delete old records related to that server first.
				ScopedLock<Mutex> chunks_map_lock(server.chunks_map_mutex);

				// remove from chunk_servers_map
				std::vector<std::string>& chunks_list = server.server_chunks_map[server_id];
				for (auto it = chunks_list.begin(); it != chunks_list.end(); ++it) {
					std::vector<std::string>& vec = server.chunk_servers_map[*it];
					vec.erase(std::remove(vec.begin(), vec.end(), server_id), vec.end());
				}
				// remove from server_chunks_map
				server.server_chunks_map[server_id].clear();

				// update the new chunks list
				for (int i = 0; i < chunks_list_json->size(); i++) {
					std::string chunk_id = chunks_list_json->getElement<std::string>(i);
					server.server_chunks_map[server_id].push_back(chunk_id);
					server.chunk_servers_map[chunk_id].push_back(server_id);
				}
				
				//add by Hua
				std::map<std::string, int64_t>::const_iterator it = server.chunk_servers_time_map.find(server_id);
				if (it == server.chunk_servers_time_map.end())
					server.chunk_servers_time_map.insert(std::pair<std::string, int64_t>(server_id, timestamp));
				else
					server.chunk_servers_time_map[server_id] = timestamp;
				//std::cout << server.chunk_servers_time_map[server_id] << std::endl;
				//std::vector<int> test = {1,2,3,4};
				//std::vector<int>::iterator it = test.begin();
				//for (int t : test) {
				//	if (t == 2)
				//		test.erase(it);
				//}
				//
			}

			if (std::find(server.live_chunk_servers.begin(), server.live_chunk_servers.end(), server_id) == server.live_chunk_servers.end()) {
				server.live_chunk_servers.push_back(server_id);
			}
			
			

			response.setStatusAndReason(HTTPResponse::HTTP_OK);
			JSON::Object::Ptr json_resp(new JSON::Object);
			json_resp->set("status", "success");
			std::ostream& ostr = response.send();
			json_resp->stringify(ostr);
		}
	};

	class GetActiveChunkServersRequestHandler : public HTTPRequestHandler {
	public:
		void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response) {
			Application& app = Application::instance();
			MetaServer& server = dynamic_cast<MetaServer&>(app);

			JSON::Object::Ptr resp_json(new JSON::Object);
			resp_json->set("status", "success");

			JSON::Array::Ptr servers_json(new JSON::Array);
			for (auto it = server.live_chunk_servers.begin(); it != server.live_chunk_servers.end(); ++it) {
				JSON::Object::Ptr server_json(new JSON::Object);
				server_json->set("id", *it);
				if (server.servers_id_address_map.find(*it) != server.servers_id_address_map.end()) {
					server_json->set("address", server.servers_id_address_map[*it]);
				}
				else {
					// try to use its id as address.
					server_json->set("address", *it);
				}
				servers_json->add(server_json);
			}
			resp_json->set("chunk_servers", servers_json);

			response.setStatusAndReason(HTTPResponse::HTTP_OK);
			resp_json->stringify(response.send());
		}
	};

	class GetChunkChunkServersRequestHandler : public HTTPRequestHandler {
	public:
		void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response) {
			Application& app = Application::instance();
			MetaServer& server = dynamic_cast<MetaServer&>(app);

			JSON::Parser jsonParser;
			JSON::Object::Ptr json_req = jsonParser.parse(request.stream()).extract<JSON::Object::Ptr>();

			std::string chunk_id = json_req->getValue<std::string>("chunk_id");

			JSON::Array::Ptr servers_list_json(new JSON::Array);
			{
				ScopedLock<Mutex> chunks_map_lock(server.chunks_map_mutex);
				std::vector<std::string>& servers = server.chunk_servers_map[chunk_id];
				for (auto it = servers.begin(); it != servers.end(); ++it) {
					servers_list_json->add(*it);
				}
			}

			JSON::Object::Ptr json_resp(new JSON::Object);
			json_resp->set("status", "success");
			json_resp->set("servers", servers_list_json);
			std::ostream& ostr = response.send();
			json_resp->stringify(ostr);
		}
	};

	class CreateFileRequestHandler : public HTTPRequestHandler {
	public:
		void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response) {
			Application& app = Application::instance();
			MetaServer& server = dynamic_cast<MetaServer&>(app);

			std::map<std::string, std::string> query_map = getQueryMap(URI(request.getURI()));
			std::string filename = query_map["filename"];

			int64_t chunk_size;
			if (query_map.find("chunk_size") != query_map.end()) {
				chunk_size = std::stoi(query_map["chunk_size"]);
			}
			else {
				chunk_size = server.default_chunk_size;
			}

			int64_t replica_count = server.default_replica_count;

			File meta_file(Path(server.meta_directory).append(filename));

			if (meta_file.exists()) {
				response.setStatusAndReason(HTTPResponse::HTTP_CONFLICT);
				response.send();
				return;
			}

			JSON::Object::Ptr meta_json(new JSON::Object);
			meta_json->set("filename", filename);
			meta_json->set("length", 0);
			meta_json->set("chunk_size", chunk_size);
			meta_json->set("replica_count", replica_count);

			JSON::Array::Ptr chunks_json(new JSON::Array);
			//std::string first_chunk = UUIDGenerator().createOne().toString();
			//chunks_json->add(first_chunk);

			meta_json->set("chunks", chunks_json);

			{
				std::ofstream ofile(meta_file.path().c_str(), std::ios::out | std::ios::binary);
				meta_json->stringify(ofile, 4);
				ofile.close();
			}

			int64_t choose_servers_count = replica_count;

			int64_t live_chunk_servers_count = (int64_t)server.live_chunk_servers.size();
			if (live_chunk_servers_count < replica_count) {
				choose_servers_count = live_chunk_servers_count;
			}

			std::vector<std::string> choosen_servers_id;
			for (int i = 0; i < choose_servers_count; i++) {

				choosen_servers_id.push_back(server.live_chunk_servers[i]);
			}

			std::vector<std::string> choosen_servers_addr;
			for (auto it = choosen_servers_id.begin(); it != choosen_servers_id.end(); ++it) {
				if (server.servers_id_address_map.find(*it) != server.servers_id_address_map.end()) {
					choosen_servers_addr.push_back(server.servers_id_address_map[*it]);
				}
				else {
					app.logger().warning("Server " + (*it) + " address unknown, trying to use its id as address.");
					choosen_servers_addr.push_back(*it);
				}
			}

			std::istringstream content("");

			JSON::Object::Ptr resp_json(new JSON::Object());
			resp_json->set("status", "success");
			resp_json->set("filename", filename);
			resp_json->set("chunk_size", chunk_size);
			resp_json->set("chunks", chunks_json);

			response.setStatusAndReason(HTTPResponse::HTTP_OK);
			resp_json->stringify(response.send());
		}
	};

	class GetFileMetaRequestHandler : public HTTPRequestHandler {
	public:
		void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response) {
			Application& app = Application::instance();
			MetaServer& server = dynamic_cast<MetaServer&>(app);

			std::map<std::string, std::string> query_map = getQueryMap(URI(request.getURI()));

			std::string filename = query_map["filename"];
			File meta_file(Path(server.meta_directory).append(filename));

			if (!meta_file.exists() || !meta_file.isFile()) {
				response.setStatusAndReason(HTTPResponse::HTTP_NOT_FOUND);
				response.send();
				return;
			}

			JSON::Object::Ptr file_meta;

			{
				std::ifstream ifile(meta_file.path().c_str(), std::ios::binary);
				JSON::Parser jsonParser;
				file_meta = jsonParser.parse(ifile).extract<JSON::Object::Ptr>();
				ifile.close();
			}

			JSON::Array::Ptr chunks_json = file_meta->getArray("chunks");

			JSON::Object::Ptr resp_json(new JSON::Object);
			resp_json->set("filename", file_meta->get("filename"));
			resp_json->set("length", file_meta->get("length"));
			resp_json->set("chunk_size", file_meta->get("chunk_size"));
			resp_json->set("chunks", chunks_json);
			resp_json->set("replica_count", file_meta->get("replica_count"));

			// return the chunk to server map so the client don't need to send another request.
			std::vector<std::string> chunks_list;
			for (int i = 0; i < chunks_json->size(); i++) {
				chunks_list.push_back(chunks_json->getElement<std::string>(i));
			}

			JSON::Object::Ptr chunk_servers(new JSON::Object);
			for (auto it = chunks_list.begin(); it != chunks_list.end(); ++it) {

				JSON::Array::Ptr servers_json(new JSON::Array);
				std::vector<std::string> servers_list = server.chunk_servers_map[*it];
				for (auto jt = servers_list.begin(); jt != servers_list.end(); ++jt) {
					JSON::Object::Ptr server_json(new JSON::Object);
					server_json->set("id", *jt);
					server_json->set("address", server.servers_id_address_map[*jt]);
					servers_json->add(server_json);
				}
				chunk_servers->set(*it, servers_json);
			}

			resp_json->set("chunk_servers", chunk_servers);

			response.setStatusAndReason(HTTPResponse::HTTP_OK);
			resp_json->stringify(response.send());
		}
	};

	class UpdateFileMetaRequestHandler : public HTTPRequestHandler {
	public:
		void handleRequest(HTTPServerRequest& request, HTTPServerResponse& response) {
			Application& app = Application::instance();
			MetaServer& server = dynamic_cast<MetaServer&>(app);

			JSON::Parser jsonParser;
			JSON::Object::Ptr json_req = jsonParser.parse(request.stream()).extract<JSON::Object::Ptr>();

			std::string filename = json_req->getValue<std::string>("filename");
			File meta_file(Path(server.meta_directory).append(filename));

			if (!meta_file.exists() || !meta_file.isFile()) {
				response.setStatusAndReason(HTTPResponse::HTTP_NOT_FOUND);
				response.send();
				return;
			}

			JSON::Object::Ptr file_meta;

			{
				std::ifstream ifile(meta_file.path().c_str(), std::ios::binary);
				JSON::Parser jsonParser;
				file_meta = jsonParser.parse(ifile).extract<JSON::Object::Ptr>();
				ifile.close();
			}

			if (json_req->has("length")) {
				file_meta->set("length", json_req->getValue<int64_t>("length"));
			}
			if (json_req->has("chunk_size")) {
				file_meta->set("chunk_size", json_req->getValue<std::string>("chunk_size"));
			}
			if (json_req->has("chunks")) {
				file_meta->set("chunks", json_req->getArray("chunks"));
			}

			{
				std::ofstream ofile(meta_file.path().c_str(), std::ios::out | std::ios::binary);
				file_meta->stringify(ofile, 4);
				ofile.close();
			}

			response.setStatusAndReason(HTTPResponse::HTTP_OK);
			response.send();
		}
	};

	MetaServer::MetaServer() {
		help_requested = false;
		request_handler_factory = new MetaServerRequestHandlerFactory(this);
	}

	MetaServer::~MetaServer() {
		delete request_handler_factory;
	}

	void MetaServer::initialize(Application& self) {
		loadConfiguration();
		ServerApplication::initialize(self);
	}

	void MetaServer::uninitialize() {
		ServerApplication::uninitialize();
	}

	void MetaServer::defineOptions(OptionSet& options) {
		ServerApplication::defineOptions(options);

		options.addOption(
			Option("help", "h", "display help information")
			.required(false)
			.repeatable(false)
			.callback(OptionCallback<MetaServer>(this, &MetaServer::handleHelp))
		);
		options.addOption(
			Option("port", "p", "listening port")
			.required(false)
			.repeatable(false)
			.argument("port")
			.binding("MetaServer.port")
		);
		options.addOption(
			Option("root_directory", "d", "root directory")
			.required(false)
			.repeatable(false)
			.argument("root_directory")
			.binding("MetaServer.root_directory")
		);
	}

	int MetaServer::main(const std::vector<std::string>& args) {
		if (help_requested) {
			return Application::EXIT_OK;
		}

		uint16_t port = (uint16_t)config().getInt("MetaServer.port", 20000);
		std::string config_root_directory = config().getString("MetaServer.root_directory", "files/");
		root_directory = Path(config_root_directory);
		meta_directory = Path(root_directory).pushDirectory("metas");

		SocketAddress listen_addr(port);
		server_id = Environment::nodeName() + ":" + std::to_string(listen_addr.port());

		logger().information("DistFS MetaServer " + server_id + " starting...");

		makeDirectories(root_directory);
		makeDirectories(meta_directory);

		loadServersList();
		ServerSocket server_socket(listen_addr);
		http_server = new HTTPServer(request_handler_factory, server_socket, new HTTPServerParams);


		//====added by Hua
		heartBeatChecker checker(5, this);
		Thread check_sever_live;
		check_sever_live.start(checker);

		http_server->start();
		waitForTerminationRequest();
		http_server->stop();

		return Application::EXIT_OK;
	}

	void MetaServer::handleHelp(const std::string& name, const std::string& value) {
		HelpFormatter help_formatter(options());
		help_formatter.setCommand(commandName());
		help_formatter.setUsage("OPTIONS");
		help_formatter.setHeader("DistFS Metadata Server.");
		help_formatter.format(std::cout);
		stopOptionsProcessing();
		help_requested = true;
	}

	void MetaServer::loadServersList() {
		File servers_list_file(Path(root_directory).append("servers_list.json"));
		if (!servers_list_file.exists() || !servers_list_file.isFile()) {
			return;
		}

		JSON::Parser jsonParser;
		JSON::Object::Ptr servers_json;

		{
			std::ifstream ifile(servers_list_file.path().c_str(), std::ios::binary);
			JSON::Parser jsonParser;
			servers_json = jsonParser.parse(ifile).extract<JSON::Object::Ptr>();
			ifile.close();
		}

		if (servers_json->has("chunk_servers")) {
			JSON::Array::Ptr chunk_servers_json = servers_json->getArray("chunk_servers");
			for (int i = 0; i < chunk_servers_json->size(); i++) {
				JSON::Object::Ptr server_json = chunk_servers_json->getObject(i);
				std::string id = server_json->getValue<std::string>("id");
				std::string addr = server_json->getValue<std::string>("address");

				servers_id_address_map[id] = addr;
			}
		}

	}

	MetaServerRequestHandlerFactory::MetaServerRequestHandlerFactory(MetaServer* srv) {
		this->server = srv;
	}

	HTTPRequestHandler* MetaServerRequestHandlerFactory::createRequestHandler(const HTTPServerRequest& request) {
		URI uri(request.getURI());
		if (uri.getPath() == "/ping") {
			return new PingRequestHandler();
		}
		else if (uri.getPath() == "/files") {
			return new ListDirectoryRequestHandler();
		}
		else if (uri.getPath() == "/update_chunks_list") {
			return new UpdateChunksListRequestHandler();
		}
		else if (uri.getPath() == "/get_active_chunk_servers") {
			return new GetActiveChunkServersRequestHandler();
		}
		else if (uri.getPath() == "/get_chunk_chunk_servers") {
			return new GetChunkChunkServersRequestHandler();
		}
		else if (uri.getPath() == "/create_file") {
			return new CreateFileRequestHandler();
		}
		else if (uri.getPath() == "/get_file_meta") {
			return new GetFileMetaRequestHandler();
		}
		else if (uri.getPath() == "/update_file_meta") {
			return new UpdateFileMetaRequestHandler();
		}
		return nullptr;
	}

}
