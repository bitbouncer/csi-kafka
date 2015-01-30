#include <vector>
#include <boost/function.hpp>
#include <boost/asio.hpp>

#pragma once

namespace csi
{
    namespace async
    {
        //use this with shared_ptr to get many children's last callback to tell parent when done
        //    std::shared_ptr<destructor_callback> final_cb(new destructor_callback(cb)
        //for (int i = 0; i != partitions; ++i)
        //{
        //    _partition2producers[i]->send_async(NULL, [i, final_cb]()
        //    {
        //        //std::cerr << "ready partition:" << i << std::endl;
        //    });
        //}
        class destructor_callback
        {
        public:
            destructor_callback(boost::function <void()>  callback) : cb(callback) {}
            ~destructor_callback() { cb(); }
        private:
            boost::function <void()> cb;
        };

        typedef boost::function <void(std::vector<boost::system::error_code>)>  async_vcallback;
        typedef boost::function <void(const boost::system::error_code&)>        async_callback;
        typedef boost::function <void(async_callback)>                          async_function;
        //typedef std::pair<async_function, async_callback>                       async_work;

        template<typename RAIter>
        void waterfall(RAIter begin, RAIter end, async_callback cb)
        {
            if (begin == end)
            {
                cb(make_error_code(boost::system::errc::success));
                return;
            }
            (*begin)([begin, end, cb](const boost::system::error_code& ec)
            {
                if (!ec)
                    waterfall(begin + 1, end, cb);
                else
                    cb(ec); // add iterator here...
            });
        }

        void waterfall(const std::vector<async_function>& v, async_callback cb)
        {
            waterfall(v.begin(), v.end(), cb);
        }

        class parallel_result
        {
        public:
            parallel_result(size_t nr_of_tasks, async_vcallback callback) : _cb(callback), _result(nr_of_tasks, make_error_code(boost::system::errc::success)) {}
            ~parallel_result() { _cb(_result); }
            boost::system::error_code& operator[](size_t i)             { return _result[i]; }
            const boost::system::error_code& operator[](size_t i) const { return _result[i]; }
        private:
            async_vcallback                        _cb;
            std::vector<boost::system::error_code> _result;
        };

        template<typename RAIter>
        void parallel(RAIter begin, RAIter end, async_vcallback cb)
        {
            std::shared_ptr<parallel_result>     result(new parallel_result(end - begin, cb));
            size_t index = 0;
            for (RAIter i = begin; i != end; ++i, ++index)
            {
                (*i)([i, result, index](const boost::system::error_code& ec)
                {
                    (*result)[index] = ec;
                });
            }
        }

        void parallel(const std::vector<async_function>& v, async_vcallback cb)
        {
            parallel(v.begin(), v.end(), cb);
        }

        //template<typename RAIter, typename resultT>
        //void waterfall(RAIter begin, RAIter end, const resultT& res, boost::function<void, boost::function<void, const resultT&> cb>)
        //{
        //    if (begin == end)
        //    {
        //        cb(res);
        //        return;
        //    }

        //    (*begin)([&res](const resultT& ec)
        //    {
        //        res = ec;  // += would make sense??
        //        if (!ec)
        //            waterfall(begin++, end, cb);
        //        else
        //            cb(res); // add iterator here...
        //    });
        //}
    };
};


/*
template<typename resultT>
void waterfall(const std::vector<boost::function<void(boost::function void<const resultT& result >> > &v, boost::function<void(boost::function void<const resultT& result >> cb)
{

cb()
}
*/

/*
template<typename F, typename result>
void do_next(boost::asio::io_service& ios; const std::vector<F>& v, std::vector<F>::iterator cursor)
{

}

template<typename F, typename result>
void waterfall(boost::asio::io_service& ios; const std::vector<F>& v)
{
std::vector<F>::iterator cursor = v.begin();
ios.post([&ios, &v, cursor](){ (*cursor)([&ios, &v, cursor++])
{

});
});
}
*/

//
//
//#include <iostream>
//#include <vector>
//#include <algorithm>
//#include <numeric>
//#include <future>
//
//template <typename RAIter>
//int parallel_sum(RAIter beg, RAIter end)
//{
//    auto len = std::distance(beg, end);
//    if (len < 1000)
//        return std::accumulate(beg, end, 0);
//
//    RAIter mid = beg + len / 2;
//    auto handle = std::async(std::launch::async,
//        parallel_sum<RAIter>, mid, end);
//    int sum = parallel_sum(beg, mid);
//    return sum + handle.get();
//}
//
//int main()
//{
//    std::vector<int> v(10000, 1);
//    std::cout << "The sum is " << parallel_sum(v.begin(), v.end()) << '\n';
//}