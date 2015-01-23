#include "kafka.h"

namespace csi
{
    namespace kafka
    {
        std::string to_string(const broker_address& ba)
        {
            std::string str = ba.host_name + ":" + std::to_string(ba.port);
            return str;
        }
    };
};

