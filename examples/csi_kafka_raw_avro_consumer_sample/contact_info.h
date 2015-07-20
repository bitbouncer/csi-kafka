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


#ifndef CONTACT_INFO_H_712309146__H_
#define CONTACT_INFO_H_712309146__H_


#include <sstream>
#include "boost/any.hpp"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/string_generator.hpp>
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"
#include "avro/Compiler.hh"

namespace sample {
struct contact_info_json_Union__0__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    std::string get_string() const;
    void set_string(const std::string& v);
    bool is_null() const {
        return (idx_ == 1);
    }
    void set_null() {
        idx_ = 1;
        value_ = boost::any();
    }
    contact_info_json_Union__0__();
};

struct contact_info_json_Union__1__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    std::string get_string() const;
    void set_string(const std::string& v);
    bool is_null() const {
        return (idx_ == 1);
    }
    void set_null() {
        idx_ = 1;
        value_ = boost::any();
    }
    contact_info_json_Union__1__();
};

struct contact_info_json_Union__2__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    std::string get_string() const;
    void set_string(const std::string& v);
    bool is_null() const {
        return (idx_ == 1);
    }
    void set_null() {
        idx_ = 1;
        value_ = boost::any();
    }
    contact_info_json_Union__2__();
};

struct contact_info_json_Union__3__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    std::string get_string() const;
    void set_string(const std::string& v);
    bool is_null() const {
        return (idx_ == 1);
    }
    void set_null() {
        idx_ = 1;
        value_ = boost::any();
    }
    contact_info_json_Union__3__();
};

struct contact_info_json_Union__4__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    std::string get_string() const;
    void set_string(const std::string& v);
    bool is_null() const {
        return (idx_ == 1);
    }
    void set_null() {
        idx_ = 1;
        value_ = boost::any();
    }
    contact_info_json_Union__4__();
};

struct contact_info_json_Union__5__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    std::string get_string() const;
    void set_string(const std::string& v);
    bool is_null() const {
        return (idx_ == 1);
    }
    void set_null() {
        idx_ = 1;
        value_ = boost::any();
    }
    contact_info_json_Union__5__();
};

struct contact_info_json_Union__6__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    std::string get_string() const;
    void set_string(const std::string& v);
    bool is_null() const {
        return (idx_ == 1);
    }
    void set_null() {
        idx_ = 1;
        value_ = boost::any();
    }
    contact_info_json_Union__6__();
};

struct contact_info_json_Union__7__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    std::string get_string() const;
    void set_string(const std::string& v);
    bool is_null() const {
        return (idx_ == 1);
    }
    void set_null() {
        idx_ = 1;
        value_ = boost::any();
    }
    contact_info_json_Union__7__();
};

struct contact_info_json_Union__8__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    std::string get_string() const;
    void set_string(const std::string& v);
    bool is_null() const {
        return (idx_ == 1);
    }
    void set_null() {
        idx_ = 1;
        value_ = boost::any();
    }
    contact_info_json_Union__8__();
};

struct contact_info_json_Union__9__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    std::string get_string() const;
    void set_string(const std::string& v);
    bool is_null() const {
        return (idx_ == 1);
    }
    void set_null() {
        idx_ = 1;
        value_ = boost::any();
    }
    contact_info_json_Union__9__();
};

struct contact_info {
    typedef contact_info_json_Union__0__ email_t;
    typedef contact_info_json_Union__1__ nin_t;
    typedef contact_info_json_Union__2__ given_name_t;
    typedef contact_info_json_Union__3__ family_name_t;
    typedef contact_info_json_Union__4__ date_of_birth_t;
    typedef contact_info_json_Union__5__ care_of_t;
    typedef contact_info_json_Union__6__ street_address_t;
    typedef contact_info_json_Union__7__ postal_code_t;
    typedef contact_info_json_Union__8__ city_t;
    typedef contact_info_json_Union__9__ country_t;
    email_t email;
    nin_t nin;
    given_name_t given_name;
    family_name_t family_name;
    date_of_birth_t date_of_birth;
    care_of_t care_of;
    street_address_t street_address;
    postal_code_t postal_code;
    city_t city;
    country_t country;
    contact_info() :
        email(email_t()),
        nin(nin_t()),
        given_name(given_name_t()),
        family_name(family_name_t()),
        date_of_birth(date_of_birth_t()),
        care_of(care_of_t()),
        street_address(street_address_t()),
        postal_code(postal_code_t()),
        city(city_t()),
        country(country_t())
        { }
//  avro extension
    static inline const boost::uuids::uuid schema_hash()      { static const boost::uuids::uuid _hash(boost::uuids::string_generator()("f1934b69-f795-1baa-2460-1497003e3f8f")); return _hash; }
    static inline const char*              schema_as_string() { return "{\"type\":\"record\",\"name\":\"contact_info\",\"fields\":[{\"name\":\"email\",\"type\":[\"string\",\"null\"]},{\"name\":\"nin\",\"type\":[\"string\",\"null\"]},{\"name\":\"given_name\",\"type\":[\"string\",\"null\"]},{\"name\":\"family_name\",\"type\":[\"string\",\"null\"]},{\"name\":\"date_of_birth\",\"type\":[\"string\",\"null\"]},{\"name\":\"care_of\",\"type\":[\"string\",\"null\"]},{\"name\":\"street_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"postal_code\",\"type\":[\"string\",\"null\"]},{\"name\":\"city\",\"type\":[\"string\",\"null\"]},{\"name\":\"country\",\"type\":[\"string\",\"null\"]}]}"; } 
    static const avro::ValidSchema         valid_schema()     { static const avro::ValidSchema _validSchema(avro::compileJsonSchemaFromString(schema_as_string())); return _validSchema; }
};

inline
std::string contact_info_json_Union__0__::get_string() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::string >(value_);
}

inline
void contact_info_json_Union__0__::set_string(const std::string& v) {
    idx_ = 0;
    value_ = v;
}

inline
std::string contact_info_json_Union__1__::get_string() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::string >(value_);
}

inline
void contact_info_json_Union__1__::set_string(const std::string& v) {
    idx_ = 0;
    value_ = v;
}

inline
std::string contact_info_json_Union__2__::get_string() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::string >(value_);
}

inline
void contact_info_json_Union__2__::set_string(const std::string& v) {
    idx_ = 0;
    value_ = v;
}

inline
std::string contact_info_json_Union__3__::get_string() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::string >(value_);
}

inline
void contact_info_json_Union__3__::set_string(const std::string& v) {
    idx_ = 0;
    value_ = v;
}

inline
std::string contact_info_json_Union__4__::get_string() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::string >(value_);
}

inline
void contact_info_json_Union__4__::set_string(const std::string& v) {
    idx_ = 0;
    value_ = v;
}

inline
std::string contact_info_json_Union__5__::get_string() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::string >(value_);
}

inline
void contact_info_json_Union__5__::set_string(const std::string& v) {
    idx_ = 0;
    value_ = v;
}

inline
std::string contact_info_json_Union__6__::get_string() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::string >(value_);
}

inline
void contact_info_json_Union__6__::set_string(const std::string& v) {
    idx_ = 0;
    value_ = v;
}

inline
std::string contact_info_json_Union__7__::get_string() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::string >(value_);
}

inline
void contact_info_json_Union__7__::set_string(const std::string& v) {
    idx_ = 0;
    value_ = v;
}

inline
std::string contact_info_json_Union__8__::get_string() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::string >(value_);
}

inline
void contact_info_json_Union__8__::set_string(const std::string& v) {
    idx_ = 0;
    value_ = v;
}

inline
std::string contact_info_json_Union__9__::get_string() const {
    if (idx_ != 0) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::string >(value_);
}

inline
void contact_info_json_Union__9__::set_string(const std::string& v) {
    idx_ = 0;
    value_ = v;
}

inline contact_info_json_Union__0__::contact_info_json_Union__0__() : idx_(0), value_(std::string()) { }
inline contact_info_json_Union__1__::contact_info_json_Union__1__() : idx_(0), value_(std::string()) { }
inline contact_info_json_Union__2__::contact_info_json_Union__2__() : idx_(0), value_(std::string()) { }
inline contact_info_json_Union__3__::contact_info_json_Union__3__() : idx_(0), value_(std::string()) { }
inline contact_info_json_Union__4__::contact_info_json_Union__4__() : idx_(0), value_(std::string()) { }
inline contact_info_json_Union__5__::contact_info_json_Union__5__() : idx_(0), value_(std::string()) { }
inline contact_info_json_Union__6__::contact_info_json_Union__6__() : idx_(0), value_(std::string()) { }
inline contact_info_json_Union__7__::contact_info_json_Union__7__() : idx_(0), value_(std::string()) { }
inline contact_info_json_Union__8__::contact_info_json_Union__8__() : idx_(0), value_(std::string()) { }
inline contact_info_json_Union__9__::contact_info_json_Union__9__() : idx_(0), value_(std::string()) { }
}
namespace avro {
template<> struct codec_traits<sample::contact_info_json_Union__0__> {
    static void encode(Encoder& e, sample::contact_info_json_Union__0__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_string());
            break;
        case 1:
            e.encodeNull();
            break;
        }
    }
    static void decode(Decoder& d, sample::contact_info_json_Union__0__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                std::string vv;
                avro::decode(d, vv);
                v.set_string(vv);
            }
            break;
        case 1:
            d.decodeNull();
            v.set_null();
            break;
        }
    }
};

template<> struct codec_traits<sample::contact_info_json_Union__1__> {
    static void encode(Encoder& e, sample::contact_info_json_Union__1__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_string());
            break;
        case 1:
            e.encodeNull();
            break;
        }
    }
    static void decode(Decoder& d, sample::contact_info_json_Union__1__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                std::string vv;
                avro::decode(d, vv);
                v.set_string(vv);
            }
            break;
        case 1:
            d.decodeNull();
            v.set_null();
            break;
        }
    }
};

template<> struct codec_traits<sample::contact_info_json_Union__2__> {
    static void encode(Encoder& e, sample::contact_info_json_Union__2__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_string());
            break;
        case 1:
            e.encodeNull();
            break;
        }
    }
    static void decode(Decoder& d, sample::contact_info_json_Union__2__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                std::string vv;
                avro::decode(d, vv);
                v.set_string(vv);
            }
            break;
        case 1:
            d.decodeNull();
            v.set_null();
            break;
        }
    }
};

template<> struct codec_traits<sample::contact_info_json_Union__3__> {
    static void encode(Encoder& e, sample::contact_info_json_Union__3__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_string());
            break;
        case 1:
            e.encodeNull();
            break;
        }
    }
    static void decode(Decoder& d, sample::contact_info_json_Union__3__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                std::string vv;
                avro::decode(d, vv);
                v.set_string(vv);
            }
            break;
        case 1:
            d.decodeNull();
            v.set_null();
            break;
        }
    }
};

template<> struct codec_traits<sample::contact_info_json_Union__4__> {
    static void encode(Encoder& e, sample::contact_info_json_Union__4__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_string());
            break;
        case 1:
            e.encodeNull();
            break;
        }
    }
    static void decode(Decoder& d, sample::contact_info_json_Union__4__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                std::string vv;
                avro::decode(d, vv);
                v.set_string(vv);
            }
            break;
        case 1:
            d.decodeNull();
            v.set_null();
            break;
        }
    }
};

template<> struct codec_traits<sample::contact_info_json_Union__5__> {
    static void encode(Encoder& e, sample::contact_info_json_Union__5__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_string());
            break;
        case 1:
            e.encodeNull();
            break;
        }
    }
    static void decode(Decoder& d, sample::contact_info_json_Union__5__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                std::string vv;
                avro::decode(d, vv);
                v.set_string(vv);
            }
            break;
        case 1:
            d.decodeNull();
            v.set_null();
            break;
        }
    }
};

template<> struct codec_traits<sample::contact_info_json_Union__6__> {
    static void encode(Encoder& e, sample::contact_info_json_Union__6__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_string());
            break;
        case 1:
            e.encodeNull();
            break;
        }
    }
    static void decode(Decoder& d, sample::contact_info_json_Union__6__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                std::string vv;
                avro::decode(d, vv);
                v.set_string(vv);
            }
            break;
        case 1:
            d.decodeNull();
            v.set_null();
            break;
        }
    }
};

template<> struct codec_traits<sample::contact_info_json_Union__7__> {
    static void encode(Encoder& e, sample::contact_info_json_Union__7__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_string());
            break;
        case 1:
            e.encodeNull();
            break;
        }
    }
    static void decode(Decoder& d, sample::contact_info_json_Union__7__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                std::string vv;
                avro::decode(d, vv);
                v.set_string(vv);
            }
            break;
        case 1:
            d.decodeNull();
            v.set_null();
            break;
        }
    }
};

template<> struct codec_traits<sample::contact_info_json_Union__8__> {
    static void encode(Encoder& e, sample::contact_info_json_Union__8__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_string());
            break;
        case 1:
            e.encodeNull();
            break;
        }
    }
    static void decode(Decoder& d, sample::contact_info_json_Union__8__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                std::string vv;
                avro::decode(d, vv);
                v.set_string(vv);
            }
            break;
        case 1:
            d.decodeNull();
            v.set_null();
            break;
        }
    }
};

template<> struct codec_traits<sample::contact_info_json_Union__9__> {
    static void encode(Encoder& e, sample::contact_info_json_Union__9__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            avro::encode(e, v.get_string());
            break;
        case 1:
            e.encodeNull();
            break;
        }
    }
    static void decode(Decoder& d, sample::contact_info_json_Union__9__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            {
                std::string vv;
                avro::decode(d, vv);
                v.set_string(vv);
            }
            break;
        case 1:
            d.decodeNull();
            v.set_null();
            break;
        }
    }
};

template<> struct codec_traits<sample::contact_info> {
    static void encode(Encoder& e, const sample::contact_info& v) {
        avro::encode(e, v.email);
        avro::encode(e, v.nin);
        avro::encode(e, v.given_name);
        avro::encode(e, v.family_name);
        avro::encode(e, v.date_of_birth);
        avro::encode(e, v.care_of);
        avro::encode(e, v.street_address);
        avro::encode(e, v.postal_code);
        avro::encode(e, v.city);
        avro::encode(e, v.country);
    }
    static void decode(Decoder& d, sample::contact_info& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.email);
                    break;
                case 1:
                    avro::decode(d, v.nin);
                    break;
                case 2:
                    avro::decode(d, v.given_name);
                    break;
                case 3:
                    avro::decode(d, v.family_name);
                    break;
                case 4:
                    avro::decode(d, v.date_of_birth);
                    break;
                case 5:
                    avro::decode(d, v.care_of);
                    break;
                case 6:
                    avro::decode(d, v.street_address);
                    break;
                case 7:
                    avro::decode(d, v.postal_code);
                    break;
                case 8:
                    avro::decode(d, v.city);
                    break;
                case 9:
                    avro::decode(d, v.country);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.email);
            avro::decode(d, v.nin);
            avro::decode(d, v.given_name);
            avro::decode(d, v.family_name);
            avro::decode(d, v.date_of_birth);
            avro::decode(d, v.care_of);
            avro::decode(d, v.street_address);
            avro::decode(d, v.postal_code);
            avro::decode(d, v.city);
            avro::decode(d, v.country);
        }
    }
};

}
#endif
