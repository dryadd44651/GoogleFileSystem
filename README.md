# DistFS3

A distributed file system demo, with GoogleFS.

## Summary

We implemented a HTTP based GoogleFS server cluster. In our implementation, different files can have different chunk size and replica count. The default size is 4096. Chunk servers and clients can join or leave the cluster on-the-fly, but there's only one metadata server each cluster so it can't leave the cluster.

When a client want to read a file, it go ask the meta server for the chunk information. Meta server will also tell the client which chunk servers contains each chunk. Then the client will randomly choose a server for each chunk and read the content of the chunk from that server.

When a client want to write a file, it does not write on the original chunks, because if it writes on the original chunks, we have to keep a chunk version or some similar mechanism to determine whose chunks is newer. Otherwise if a chunk server leave the cluster for a moment, the write happens, and the chunk server join back, it can't determine if it need to update its chunks. Instead each time we update a chunk, we give it a new id. (The implementation allows in-place modification so if the chunks is huge and we only modify a tiny bit of it, we don't need to transmit the whole chunk) Then chunk servers can use the metadata from meta server to determine which chunk is needed and discard the unneeded chunks when it rejoin the cluster. The client will issue command to clean the unused chunks immediately if it successfully committed an operation. Because chunks never changes, and only the client who created the new chunks knows its id before it update the metadata, so we don't need to use any global lock. As long as we make sure the metadata update is atomic, we will be fine. And since there's only one meta server, it's pretty easy to maintain the consistence of the metadata. Appending is very similar to write. We choose some random live chunk servers, create chunks, and if everyone is OK, update the metadata and commit.

Our implementation allows the client to perform a write even if not all chunk servers are agreed to write. As long as for every chunks related to an operation, there's at least one chunk server successfully performed the write, we will allow the operation to finish. (This behavior can be configured and can be turn off). And we can still maintain consistence in this situation. To achieve this, two-phase commit are implemented in a different way in our implementation. Chunk servers are not aware of the commit state. The only thing they do is maintain the chunks. In our implementation, phase 1 is  similar to normal two-phase commit: all chunk servers first store the chunks in somewhere invisible to others (because no one know the new chunk's id), then tell the client (2-phase commit cohort). The client received all responses and decide to commit or not. If commit, it will push the file information to meta server, so the modification are in effect now. Then it will ask the chunk servers to clean up previous chunks. This can fail but it won't influence the consistence of the system and the chunk servers can also clean up themselves just like what they do upon joining the cluster. If the client decide not to commit, it will not push the file information to meta server, so the modification will be discard. It will then ask the chunk servers to clean up the newly created chunks. If the client unresponsive in this process, chunk servers will keep both chunks for a moment. 

In our implementation, servers use HTTP to communicate. We used TCP before, but if we continue using TCP, there's no point we implement heartbeats on top of TCP. TCP already have heartbeats. So instead we use HTTP (which is actually based on TCP but HTTP is a stateless protocal, so it is actually a downgrade here). The main reason we choose HTTP is its easy to debug.


## Build

As before, this project use cmake to build everything.

```shell
mkdir build
cd build
cmake ..
cmake --build .
```

The output executables are in `build/DistFS`.

- `difsqs` is the access server, it will serve chunk files in its working directory's `files/chunks` folder.
- `difsms` is the meta server, it will store file meta information in `files/metas` and serve this information to access server and chunk server. Meta server is the heart of the whole system.
- `difsas` is the access server (client), it provides file access API.

Both the servers supports a command line argument `-p {port}` (or `/p={port}` on windows) to specify its listen port.

Chunk server and access server supports a command line argumant `-m {meta_server_address}` (or `/m={meta_server_address}` on windows) to specify the meta server's address. You can start the chunk server using this command: `./difscs -m "127.0.0.1:20000"`.

Use `-h` `--help` to see all command line options available.

The code should work on windows (tested), Linux (tested) and MacOS (not tested).

## Dependencies

We only requires Poco for networking.

All dependencies are in `external` folder.

Poco is statically linked to our main program, so there's no extra runtime dependency other than standard c++ runtime libraries.

## API List

### AccessServer

These APIs are designed to be exposed to client application.

- `GET /get_file`

  Parameters:

  - `filename` Filename.
  - `begin_pos` Where to start read.
  - `end_pos` Where to stop read.

  Return: If operation succeed, then it will return the content of the file, from `begin_pos` to `end_pos`. Otherwise a non-200 HTTP response code will be returned.

- `POST /write_file`

  Parameters:

  - `filename` Filename.
  - `begin_pos` Where to start write.

  Request Body: `application/octet-stream` content to write.

  Return: Standard HTTP code indicating if the operation is succeed or not.