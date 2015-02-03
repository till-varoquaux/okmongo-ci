#include "string_matcher.h"
#include <iostream>

constexpr okmongo::StringMatcherAction<int> kwds[] = {{"moretest", 1},
                                                      {"test", 2},
                                                      {"test1", 3},
                                                      {"test1234", 4},
                                                      {nullptr, 10}};

// class BsonReaderInterface {
//     virtual bool EmitOpenDoc() { return false; }

//     virtual bool EmitClose() { return false; }

//     virtual bool EmitOpenArray() { return false; }

//     virtual bool EmitInt32(int32_t) { return false; }

//     virtual bool EmitInt64(int64_t) { return false; }

//     virtual bool EmitBool (bool) { return false; }

//     virtual bool EmitDouble(double) { return false; }

//     virtual bool EmitNull() { return false; }

//     // Will be called back with a length of 0 when done
//     virtual bool EmitUtf8(const char *, int32_t) { return false; }

//     virtual bool EmitJs(const char *, int32_t) { return false; }

//     virtual bool EmitUtcDatetime(const int64_t) { return false; }

//     virtual bool EmitTimestamp(const int64_t) { return false; }

//     virtual bool EmitFieldName(const char *, const int32_t) { return false; }

//     virtual bool EmitObjectId(const char *) { return false; }
// };

// struct test_t {
//     bool ok;
//     int32_t v;
// };

int main() {
    const char* needle = "test12";
    const char* pos = needle;
    okmongo::StringMatcher<int, kwds> sm;
    while (*pos) {
        sm.AddChar(*pos);
        ++pos;
    }
    sm.AddChar('\000');
    // Add all the chars including the trailing '\000'
    std::cout << sizeof(sm) << std::endl;
    std::cout << sm.GetResult() << std::endl;
}
