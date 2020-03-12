#include "access_server.h"

using namespace DistFS;

int main(int argc, char** argv)
{
    AccessServer server;
    return server.run(argc, argv);
}