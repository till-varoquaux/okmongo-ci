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
        case BsonTag::kBindata:
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
 * How much bytes must we add to the `length` to get the full field length
 * (including terminating null etc)...
 */
static int32_t TagLengthOffset(BsonTag tag) {
    switch (tag) {
        case BsonTag::kJs:
        case BsonTag::kUtf8:
            return 4;  // length
        case BsonTag::kBindata:
            return 5;  // length + subtype
        case BsonTag::kDocument:
        case BsonTag::kArray:
        default:
            return 0;
    }
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
        case BsonTag::kJs:
        case BsonTag::kUtf8:
        case BsonTag::kBindata:
            if (size < static_cast<int32_t>(sizeof(int32_t)) + 1) {
                return -1;
            }
            std::memcpy(&res, data, sizeof(int32_t));
            if (res <= 0) {
                return -1;
            }
            res += TagLengthOffset(tag);
            null_terminated = (tag != BsonTag::kBindata);
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

namespace {
    template <typename T, BsonTag Tgt, T fallback>
    T GetV(const BsonValue &b, const char *data) {
        if (b.Tag() != Tgt) {
            return fallback;
        }
        T retval;
        std::memcpy(&retval, data, sizeof(T));
        return retval;
    }  // namespace

    template <typename T, BsonTag Tgt>
    bool GetV(const BsonValue &b, const char *data, T *out) {
        if (b.Tag() != Tgt) {
            return false;
        }
        std::memcpy(out, data, sizeof(T));
        return true;
    }
}  // namespace

int64_t BsonValue::GetInt64() const {
    return GetV<int64_t, BsonTag::kInt64, -1>(*this, data_);
}

int64_t BsonValue::GetTimestamp() const {
    return GetV<int64_t, BsonTag::kTimestamp, -1>(*this, data_);
}

int64_t BsonValue::GetUtcDatetime() const {
    return GetV<int64_t, BsonTag::kUtcDatetime, -1>(*this, data_);
}

int32_t BsonValue::GetInt32() const {
    return GetV<int32_t, BsonTag::kInt32, -1>(*this, data_);
}

double BsonValue::GetDouble() const {
    double retv;
    if (!GetV<double, BsonTag::kDouble>(*this, data_, &retv)) {
        return std::numeric_limits<double>::quiet_NaN();
    } else {
        return retv;
    }
}

BindataSubtype BsonValue::GetBinSubstype() const {
    if (tag_ == BsonTag::kBindata) {
        return static_cast<BindataSubtype>(data_[4]);
    }
    return BindataSubtype::kGeneric;
}

bool BsonValue::GetBool() const {
    return GetV<char, BsonTag::kBool, 0>(*this, data_) == 1;
}

const char *BsonValue::GetData() const {
    switch (tag_) {
        case BsonTag::kObjectId:
            return data_;
        case BsonTag::kUtf8:
        case BsonTag::kJs:
            return data_ + 4;
        case BsonTag::kBindata:
            return data_ + 5;
        default:
            return nullptr;
    }
}

int32_t BsonValue::GetDataSize() const {
    switch (tag_) {
        case BsonTag::kObjectId:
            return 9;
        case BsonTag::kUtf8:
        case BsonTag::kJs:
        case BsonTag::kBindata:
            return size_ - 5;
        default:
            return -1;
    }
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
    auto size = GetValueLength(tag, curs, static_cast<int32_t>(end_ - curs));

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
    if (v.Tag() != BsonTag::kArray && v.Tag() != BsonTag::kDocument) {
        Invalidate();
        return;
    }
    end_ = v.data_ + v.size_;
    MoveTo(v.data_ + sizeof(int32_t));
}

void BsonValueIt::next() {
    if (!Empty()) {
        MoveTo(data_ + size_);
    }
}

}  // namespace okmongo
