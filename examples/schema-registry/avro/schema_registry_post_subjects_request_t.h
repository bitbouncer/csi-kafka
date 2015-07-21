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


#ifndef AVRO_SCHEMA_REGISTRY_POST_SUBJECTS_REQUEST_T_H_1377359960__H_
#define AVRO_SCHEMA_REGISTRY_POST_SUBJECTS_REQUEST_T_H_1377359960__H_


#include <sstream>
#include <boost/any.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/make_shared.hpp>
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"
#include "avro/Compiler.hh"

struct schema_registry_post_subjects_request_t {
    std::string schema;
    schema_registry_post_subjects_request_t() :
        schema(std::string())
        { }
//  avro extension
    static inline const boost::uuids::uuid      schema_hash()      { static const boost::uuids::uuid _hash(boost::uuids::string_generator()("5c700a69-3ff7-5cd0-65e1-6b350de295c0")); return _hash; }
    static inline const char*                   schema_as_string() { return "{\"type\":\"record\",\"name\":\"schema_registry_post_subjects_request_t\",\"fields\":[{\"name\":\"schema\",\"type\":\"string\"}]}"; } 
    static boost::shared_ptr<avro::ValidSchema> valid_schema()     { static const boost::shared_ptr<avro::ValidSchema> _validSchema(boost::make_shared<avro::ValidSchema>(avro::compileJsonSchemaFromString(schema_as_string()))); return _validSchema; }
};

namespace avro {
template<> struct codec_traits<schema_registry_post_subjects_request_t> {
    static void encode(Encoder& e, const schema_registry_post_subjects_request_t& v) {
        avro::encode(e, v.schema);
    }
    static void decode(Decoder& d, schema_registry_post_subjects_request_t& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.schema);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.schema);
        }
    }
};

}
#endif
