// -*- mode:c++ -*-
#pragma once
#include <cassert>
#include <cstring>
#include <cstdint>
#include <algorithm>

namespace okmongo {

// TODO: use a combination of `offset_of` and BsonTag to implement a parser that
// works directly on a data structure.
// The reader class would only work for standard_layout classes
// (std::is_standard_layout), we have to assert the address of the last field is
// < max_int32_t maintain a bit-field of all the fields seen so far
// and construct objects of type t...
//
// struct fld_info {
//     BsonTag typ_;
//     int32_t offset_;
// };

template <typename T>
struct StringMatcherAction {
    const char *match;
    T val;
};

inline constexpr size_t ConstexprStrlen(const char *s) {
    // __builtin_strlen is not constexpr until clang 3.6
    return (*s) ? 1 + ConstexprStrlen(s + 1) : 0;
}

inline constexpr size_t ConstexprMax(size_t a, size_t b) {
    return a > b ? a : b;
}

template <typename T>
constexpr size_t GetMaxMatch(const StringMatcherAction<T> *k) {
    return (k->match)
                   ? ConstexprMax(ConstexprStrlen(k->match), GetMaxMatch(k + 1))
                   : 0;
}

template <typename T>
constexpr size_t GetNumActions(const StringMatcherAction<T> *k) {
    return (k->match) ? 1 + GetNumActions(k + 1) : 0;
}

// This class is designed to be very small (usually 8 butes) and make no memory
// allocation.
// It only works for keywords less than 256 char long and with list of less than
// 256 keywords
//
//
// The keywords* needs to point to a `constexpr` array of alphabetically sorted
// `StringMatcherAction`. The last action should have the `match` field set to
// `nullptr`. Its value is the default value that will be returned when we
// aren't matching anything...
template <typename T, const StringMatcherAction<T> *keywords>
class StringMatcher {
    static constexpr size_t totlen_ = GetNumActions(keywords);
    static_assert(totlen_ > 0, "Too few kwds...");
    static_assert(totlen_ < 256, "Too many kwds...");
    static_assert(GetMaxMatch(keywords) < 256,
                  "One of the kwds is too long...");

    uint8_t pos_ = 0;  // We could figure out how many bytes we need for that by
                       // finding the length of the longest string

    uint8_t min_ = 0;  // If we have more that 255 keywords we should use a
                       // larger type. we could figure this out at compile time
                       // by looking at totlen_
    uint8_t max_ = totlen_ - 1;
    enum State : uint8_t { kRunning, kSuccess, kFailed } state_ = kRunning;

public:
    // Advance the matcher by one character
    void AddChar(const char c_in) {
        switch (state_) {
            case kFailed:
                return;
            case kSuccess:
                assert(false);
                return;
            case kRunning:
                break;
        }

        while (min_ < max_ && keywords[min_].match[pos_] != c_in) {
            ++min_;
        }

        while (min_ < max_ && keywords[max_].match[pos_] != c_in) {
            --max_;
        }

        if (min_ == max_) {
            if (keywords[max_].match[pos_] != c_in) {
                state_ = kFailed;
                return;
            } else if (c_in == '\000') {
                state_ = kSuccess;
                return;
            }
        }
        ++pos_;
    }

    T GetResult() const {
        return keywords[(state_ == kSuccess) ? min_ : totlen_].val;
    }

    void Reset() {
        state_ = kRunning;
        pos_ = 0;
        min_ = 0;
        max_ = totlen_ - 1;
    }
};

}  // namespace okmongo
