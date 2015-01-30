#include <boost/thread.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <csi_kafka/kafka.h>
#include <csi_kafka/internal/async.h>

int main(int argc, char** argv)
{
    std::vector<csi::async::async_function> work;
    
    work.push_back([](csi::async::async_callback cb)
    {
        std::cerr << "f1" << std::endl;
        cb(make_error_code(boost::system::errc::success));
    });

    work.push_back([](csi::async::async_callback cb)
    {
        std::cerr << "f2" << std::endl;
        cb(make_error_code(boost::system::errc::success));
    });


    work.push_back([](csi::async::async_callback cb)
    {
        std::cerr << "f3" << std::endl;
        cb(make_error_code(boost::system::errc::already_connected));
    });

    csi::async::waterfall(work.begin(), work.end(), [](const boost::system::error_code& ec)
    {
        std::cerr << "done ec=" << ec.message() << std::endl;
    });

    std::cerr << "version 2" << std::endl;
    csi::async::waterfall(work, [](const boost::system::error_code& ec)
    {
        std::cerr << "done ec=" << ec.message() << std::endl;
    });

    std::cerr << "version 3" << std::endl;
    csi::async::parallel(work, [](std::vector<boost::system::error_code> ec)
    {
        std::cerr << "all done" << std::endl;
        size_t index = 0; 
        for (std::vector<boost::system::error_code>::const_iterator i = ec.begin(); i != ec.end(); ++i, ++index)
        {
            std::cerr << "index = " << index << " ec=" << (*i).message() << std::endl;
        }
    });


}
