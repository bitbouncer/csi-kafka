/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#ifndef SYSLOG_H_870978010__H_
#define SYSLOG_H_870978010__H_


#include <sstream>
#include "boost/any.hpp"
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"

namespace sample {
struct syslog {
    int32_t pri;
    int32_t version;
    std::string timestamp;
    std::string hostname;
    std::string appname;
    std::string procid;
    std::string msgid;
    std::string message;
    syslog() :
        pri(int32_t()),
        version(int32_t()),
        timestamp(std::string()),
        hostname(std::string()),
        appname(std::string()),
        procid(std::string()),
        msgid(std::string()),
        message(std::string())
        { }
};

}
namespace avro {
template<> struct codec_traits<sample::syslog> {
    static void encode(Encoder& e, const sample::syslog& v) {
        avro::encode(e, v.pri);
        avro::encode(e, v.version);
        avro::encode(e, v.timestamp);
        avro::encode(e, v.hostname);
        avro::encode(e, v.appname);
        avro::encode(e, v.procid);
        avro::encode(e, v.msgid);
        avro::encode(e, v.message);
    }
    static void decode(Decoder& d, sample::syslog& v) {
        avro::decode(d, v.pri);
        avro::decode(d, v.version);
        avro::decode(d, v.timestamp);
        avro::decode(d, v.hostname);
        avro::decode(d, v.appname);
        avro::decode(d, v.procid);
        avro::decode(d, v.msgid);
        avro::decode(d, v.message);
    }
};

}
#endif
