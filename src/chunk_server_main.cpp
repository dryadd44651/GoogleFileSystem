#include "chunk_server.h"

using namespace DistFS;

int main(int argc, char** argv)
{
    ChunkServer server;
    return server.run(argc, argv);
}