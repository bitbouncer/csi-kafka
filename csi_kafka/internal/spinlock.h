#pragma once

#include <boost/smart_ptr/detail/spinlock.hpp>

/*
* Inspired by or more like cut and pasted from ...
* http://boost.2283326.n4.nabble.com/shared-ptr-thread-spinlock-initialization-td4636282.html
*/

namespace csi
{
    namespace kafka
    {
        class spinlock
        {
        public:
            spinlock();
            inline bool try_lock() 	{ return sl_.try_lock(); }
            inline void lock() 		{ sl_.lock(); }
            inline void unlock()	{ sl_.unlock(); }

            class scoped_lock
            {
            public:
                inline explicit scoped_lock(spinlock & sp) : sp_(sp)  { sp.lock(); }
                inline ~scoped_lock() 									 { sp_.unlock(); }

            private:
                scoped_lock(scoped_lock const &);
                scoped_lock & operator=(scoped_lock const &);
                spinlock& sp_;
            };

        private:
            boost::detail::spinlock sl_;
            spinlock(spinlock const&);
            spinlock & operator=(spinlock const&);
        };
    }
}