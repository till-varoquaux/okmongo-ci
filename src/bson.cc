#include "bson.h"
#include <iostream>

namespace okmongo {

BsonWriter::~BsonWriter() {
    if (!DataIsInline()) {
        data_. ~unique_ptr<char[]>();
    }
}

void BsonWriter::Clear() {
    doc_start_ = 0;
    pos_ = 0;
}

BsonTag ToBsonTag(char c) {
    if (c <= static_cast<signed char>(BsonTag::kMinKey) ||
        c >= static_cast<signed char>(BsonTag::kMaxKey)) {
        return BsonTag::kMinKey;
    }
    BsonTag res = static_cast<BsonTag>(c);
    switch (res) {
        case BsonTag::kDouble:
        case BsonTag::kUtf8:
        case BsonTag::kDocument:
        case BsonTag::kArray:
        case BsonTag::kBinData:
        case BsonTag::kObjectId:
        case BsonTag::kBool:
        case BsonTag::kUtcDatetime:
        case BsonTag::kNull:
        case BsonTag::kInt32:
        case BsonTag::kTimestamp:
        case BsonTag::kInt64:
        case BsonTag::kRegexp:
        case BsonTag::kJs:
        case BsonTag::kScopedJs:
            return res;
        case BsonTag::kMinKey:
        case BsonTag::kMaxKey:
            break;
    }
    return BsonTag::kMinKey;
}

/**
 * @returns -1 in case of error.
 */
static int32_t GetValueLength(BsonTag tag, const char *data, int32_t size) {
    int32_t res;
    bool null_terminated = false;
    switch (tag) {
        case BsonTag::kDocument:
        case BsonTag::kArray:
            if (size < static_cast<int32_t>(sizeof(int32_t)) + 1) {
                return -1;
            }
            std::memcpy(&res, data, sizeof(int32_t));
            if (res <= 0) {
                return -1;
            }
            null_terminated = true;
            break;
        case BsonTag::kJs:
        case BsonTag::kUtf8:
        case BsonTag::kBinData:
            if (size < static_cast<int32_t>(sizeof(int32_t)) + 1) {
                return -1;
            }
            std::memcpy(&res, data, sizeof(int32_t));
            if (res <= 0) {
                return -1;
            }
            res += 4;
            null_terminated = true;
            break;
        case BsonTag::kDouble:
            res = sizeof(double);
            break;
        case BsonTag::kObjectId:
            res = 12;
            break;
        case BsonTag::kBool:
            res = 1;
            break;
        case BsonTag::kInt32:
            res = sizeof(int32_t);
            break;
        case BsonTag::kInt64:
        case BsonTag::kUtcDatetime:
        case BsonTag::kTimestamp:
            res = sizeof(int64_t);
            break;
        case BsonTag::kNull:
            res = 0;
            break;
        case BsonTag::kRegexp:
        case BsonTag::kScopedJs:
        case BsonTag::kMinKey:
        case BsonTag::kMaxKey:
            return -1;
    }
    if (res > size) {
        return -1;
    }
    if (null_terminated && data[res - 1] != '\000') {
        return -1;
    }
    return res;
}

// Random access interface...
BsonValue::BsonValue(const char *data, int32_t size, BsonTag tag)
    : BsonValue() {
    const int32_t real_size = GetValueLength(tag, data, size);
    if (real_size == -1) {
        return;
    }
    data_ = data;
    tag_ = tag;
    size_ = real_size;
}

// For documents...
BsonValue BsonValue::GetField(const char *needle) const {
    if (tag_ != BsonTag::kDocument) {
        return BsonValue();
    }
    const char *end = data_ + size_;
    const char *curs = data_ + sizeof(int32_t);
    while (curs < end) {
        const BsonTag tag = ToBsonTag(*curs);
        if (tag == BsonTag::kMinKey) {
            return BsonValue();
        }
        ++curs;
        const char *match_cpy = needle;
        while (*curs == *match_cpy && *curs) {
            ++curs;
            ++match_cpy;
            if (curs >= end - 1) {
                return BsonValue();
            }
        }
        const bool matched = *curs == '\000' && *match_cpy == '\000';
        while (*curs) {
            ++curs;
            if (curs >= end - 1) {
                return BsonValue();
            }
        }
        ++curs;
        const int32_t size =
                GetValueLength(tag, curs, static_cast<int32_t>(end - curs));
        if (size == -1) {
            return BsonValue();
        }
        if (matched) {
            BsonValue res;
            res.data_ = curs;
            res.tag_ = tag;
            res.size_ = size;
            return res;
        }
        curs += size;
    }
    return BsonValue();
}

void BsonValueIt::Invalidate() {
    data_ = nullptr;
    tag_ = BsonTag::kMinKey;
    size_ = 0;
    key_ = nullptr;
}

void BsonValueIt::MoveTo(const char *curs) {
    if (curs >= end_ - 1) {
        // Error
        return Invalidate();
    }
    const BsonTag tag = ToBsonTag(*curs);
    if (tag == BsonTag::kMinKey) {
        return Invalidate();
    }
    ++curs;
    const char *key = curs;
    while (*curs) {
        ++curs;
        if (curs >= end_ - 1) {
            return Invalidate();
        }
    }

    ++curs;

    auto size =
            GetValueLength(tag, curs, static_cast<int32_t>(end_ - curs) - 1);

    if (size == -1) {
        return Invalidate();
    }

    data_ = curs;
    tag_ = tag;
    size_ = size;
    key_ = key;
}

BsonValueIt::BsonValueIt() { Invalidate(); }

BsonValueIt::BsonValueIt(const BsonValue &v) {
    if (v.tag() != BsonTag::kArray && v.tag() != BsonTag::kDocument) {
        Invalidate();
        return;
    }
    end_ = v.data() + v.size();
    MoveTo(v.data() + sizeof(int32_t));
}

void BsonValueIt::next() {
    if (!Empty()) {
        MoveTo(data_ + size_);
    }
}

}  // namespace okmongo
