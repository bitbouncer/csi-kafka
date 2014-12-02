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


#ifndef CONTACT_INFO_KEY_H_3318477099__H_
#define CONTACT_INFO_KEY_H_3318477099__H_


#include <sstream>
#include "boost/any.hpp"
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"

namespace sample {
struct contact_info_key {
    std::string md5;
    contact_info_key() :
        md5(std::string())
        { }
};

}
namespace avro {
template<> struct codec_traits<sample::contact_info_key> {
    static void encode(Encoder& e, const sample::contact_info_key& v) {
        avro::encode(e, v.md5);
    }
    static void decode(Decoder& d, sample::contact_info_key& v) {
        avro::decode(d, v.md5);
    }
};

}
#endif
