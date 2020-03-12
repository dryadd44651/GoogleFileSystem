#include "meta_server.h"

using namespace DistFS;

int main(int argc, char** argv)
{
    MetaServer server;
    return server.run(argc, argv);
}