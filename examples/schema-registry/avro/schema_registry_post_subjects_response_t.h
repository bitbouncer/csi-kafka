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


#ifndef AVRO_SCHEMA_REGISTRY_POST_SUBJECTS_RESPONSE_T_H_1377359960__H_
#define AVRO_SCHEMA_REGISTRY_POST_SUBJECTS_RESPONSE_T_H_1377359960__H_


#include <sstream>
#include <boost/any.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/make_shared.hpp>
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"
#include "avro/Compiler.hh"

struct schema_registry_post_subjects_response_t {
    std::string uuid;
    schema_registry_post_subjects_response_t() :
        uuid(std::string())
        { }
//  avro extension
    static inline const boost::uuids::uuid      schema_hash()      { static const boost::uuids::uuid _hash(boost::uuids::string_generator()("51d77ed8-eca5-4975-a7cf-3c6a937ea570")); return _hash; }
    static inline const char*                   schema_as_string() { return "{\"type\":\"record\",\"name\":\"schema_registry_post_subjects_response_t\",\"fields\":[{\"name\":\"uuid\",\"type\":\"string\"}]}"; } 
    static boost::shared_ptr<avro::ValidSchema> valid_schema()     { static const boost::shared_ptr<avro::ValidSchema> _validSchema(boost::make_shared<avro::ValidSchema>(avro::compileJsonSchemaFromString(schema_as_string()))); return _validSchema; }
};

namespace avro {
template<> struct codec_traits<schema_registry_post_subjects_response_t> {
    static void encode(Encoder& e, const schema_registry_post_subjects_response_t& v) {
        avro::encode(e, v.uuid);
    }
    static void decode(Decoder& d, schema_registry_post_subjects_response_t& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.uuid);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.uuid);
        }
    }
};

}
#endif
