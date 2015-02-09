// -*- mode:c++ -*-
#pragma once
#include "bson.h"
#include "mongo.h"
#include <stack>
#include <iostream>
#include <iomanip>
#include <ctime>
#include <limits>

/**
 * Dumps out BSON values in mongodb-extended-json
 * (http://docs.mongodb.org/manual/reference/mongodb-extended-json/)
 * In the mongo driver they are defined at:
 *
 * src/mongo/bson/bsonelement.cpp
 */
namespace okmongo {

template <typename Parent>
class BsonDumper : public Parent {
    std::ostream *tgt_;
    std::stack<BsonTag> stack_;
    bool in_lit_ = false;
    BindataSubtype subtype_;
    bool InArray() { return stack_.top() == BsonTag::kArray; }

    void PrintStringFrag(const char *s, int32_t len) {
        assert(len >= 0);
        for (int32_t i = 0; i < len; i++) {
            const char c = s[i];
            if (c == '\n') {
                *tgt_ << "\\n";
            } else if (c == '\t') {
                *tgt_ << "\\t";
            } else if (c == '\"') {
                *tgt_ << "\\\"";
            } else if (std::isprint(c)) {
                *tgt_ << c;
            } else {
                // We don't use iostream because it triggers unspecified
                // behaviour warnings with clang's sanitizer.
                // http://lists.cs.uiuc.edu/pipermail/cfe-dev/2013-January/
                // 027401.html
                char buff[3];
                snprintf(buff, 3, "%.2x", static_cast<unsigned char>(c));
                *tgt_ << "\\x" << buff;
            }
        }
    }

    bool first_elt_ = true;

    void PrintNl(bool pop) {
        if (!pop && !first_elt_) {
            *tgt_ << ",";
        }
        first_elt_ = false;
        *tgt_ << "\n" << std::string(2 * (stack_.size()), ' ');
    }

public:
    explicit BsonDumper(std::ostream *tgt = &std::cout) : tgt_(tgt) {}

    void EmitError(const char *msg) {
        std::cerr << "Bson parsing error: " << msg << std::endl;
    }

    void EmitOpenDoc() {
        stack_.push(BsonTag::kDocument);
        *tgt_ << "{";
        first_elt_ = true;
    }

    void EmitOpenArray() {
        stack_.push(BsonTag::kArray);
        *tgt_ << "[";
        first_elt_ = true;
    }

    void EmitClose() {
        const char c = InArray() ? ']' : '}';
        stack_.pop();
        PrintNl(true);
        *tgt_ << c;
        if (stack_.size() == 0) {
            *tgt_ << std::endl;
        }
    }

    void EmitInt32(int32_t i) { *tgt_ << i; }

    void EmitInt64(int64_t i) {
        *tgt_ << "{ \"$numberLong\": \"" << i << "\" }";
    }

    void EmitUtcDatetime(int64_t i) {
        *tgt_ << "{ \"$date\": ";
        if (i >= 0 && i <= std::numeric_limits<time_t>::max()) {
            struct tm t;
            gmtime_r(&i, &t);

            char buf[32];
            strftime(buf, sizeof(buf), "\"%Y-%m-%dT%H:%M:%SZ\"", &t);
            *tgt_ << buf;
        } else {
            EmitInt64(i);
        }
        *tgt_ << " }";
    }

    void EmitTimestamp(int64_t i) {
        uint32_t seconds = i & static_cast<uint32_t>(-1);
        uint32_t increments = (i >> 32) & static_cast<uint32_t>(-1);
        *tgt_ << "{ \"$timestamp\": { \"i\": " << increments
              << ", \"s\": " << seconds << " }}";
    }

    void EmitBool(bool b) { *tgt_ << (b ? "true" : "false"); }

    void EmitDouble(double d) { *tgt_ << d; }

    void EmitNull() { *tgt_ << "null"; }

    void EmitUtf8(const char *s, const int32_t len) {
        assert(len >= 0);
        if (!in_lit_) {
            *tgt_ << '"';
            in_lit_ = true;
        }
        PrintStringFrag(s, len);
        if (len == 0) {
            *tgt_ << "\"";
            in_lit_ = false;
        }
    }

    void EmitBindata(const char *s, const int32_t len) {
        assert(len >= 0);
        PrintStringFrag(s, len);
        if (len == 0) {
            char buff[3];
            snprintf(buff, 3, "%.2x", static_cast<unsigned char>(subtype_));
            *tgt_ << "\", \"$type\": \"" << buff << "\" }";
        }
    }

    // TODO: figure out how this should actually be printed
    void EmitBindataSubtype(BindataSubtype st) {
        subtype_ = st;
        *tgt_ << "{ \"$binary\": \"";
    }

    void EmitJs(const char *s, const int32_t len) {
        assert(len >= 0);
        if (!in_lit_) {
            *tgt_ << "{ \"$code\": \"";
            in_lit_ = true;
        }
        PrintStringFrag(s, len);
        if (len == 0) {
            *tgt_ << "\" }";
            in_lit_ = false;
        }
    }

    void EmitFieldName(const char *data, const int32_t len) {
        assert(len >= 0);
        if (!in_lit_) {
            PrintNl(false);
        }
        if (!InArray()) {
            if (!in_lit_) {
                *tgt_ << '"';
            }
            PrintStringFrag(data, len);
            if (len == 0) {
                *tgt_ << "\": ";
            }
        }
        in_lit_ = len > 0;
    }

    void EmitObjectId(const char *s) {
        // The oid is hex encoded:
        //   \xff \xff \xff ... -> ffffffff...
        *tgt_ << "{ \"$oid\": \"";
        for (int i = 0; i < kObjectIdLen; ++i) {
            char buff[3];
            snprintf(buff, 3, "%.2x", static_cast<unsigned char>(s[i]));
            *tgt_ << buff;
        }
        *tgt_ << "\" }";
    }

    void EmitDocumentStart(int32_t idx) {
        if (idx > 0) {
            *tgt_ << "=================" << std::endl;
        }
    }

    void EmitStart(const ResponseHeader &hdr) {
        *tgt_ << "flags: " << hdr.response_flags << std::endl;
    }

};  // class BsonDumper

class BsonDocDumper
        : public okmongo::BsonDumper<okmongo::BsonReader<BsonDocDumper>> {
public:
    using BsonDumper::BsonDumper;
};

bool Print(const BsonValue &v, BsonDocDumper *d);
}  // namespace okmongo
