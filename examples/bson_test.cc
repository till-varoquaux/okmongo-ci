#include "bson.h"
#include "bson_dumper.h"
#include <sstream>
#include <iostream>

static std::string SpoonFeed(const std::string &s) {
    constexpr size_t kChunkSize = 5;
    std::ostringstream ss;
    okmongo::BsonDocDumper r(&ss);
    const char *dt = s.data();
    int32_t len = 0;
    for (const int32_t max_len =
                 static_cast<int32_t>((s.size() / kChunkSize) * kChunkSize);
         len < max_len; len += kChunkSize) {
        const int32_t consumed = r.Consume(dt + len, kChunkSize);
        assert(consumed == kChunkSize);
    }
    if (len < static_cast<int32_t>(s.size())) {
        const int32_t remaining = s.size() % kChunkSize;
        const int32_t consumed = r.Consume(dt + len, remaining);
        assert(consumed == remaining);
    }
    return ss.str();
}

static std::string PrintBsonValue(const std::string &s) {
    okmongo::BsonValue v(s.data(), static_cast<int32_t>(s.size()));
    std::ostringstream ss;
    okmongo::BsonDocDumper d(&ss);
    Print(v, &d);
    return ss.str();
}

// kDocument
// kArray
// kUtf8
// kDouble
// kObjectId
// kBool
// kInt32
// kInt64
// kUtcDatetime
// kNull
// kTimestamp
// kJs
// kBinData
//============================
// Still untested:
//
// kRegexp
// kScopedJs
//============================
// kMinKey
// kMaxKey

int main() {
    okmongo::BsonWriter w;
    const char oid[okmongo::kObjectIdLen] = {};
    w.Document();
    {
        w.Element("int32", 1);
        w.Element("int64", static_cast<int64_t>(1));
        w.Element("double", 1.9);
        w.Element("null", nullptr);
        w.Element("bool", true);
        w.Element("bool2", false);
        w.Element("string",
                  "Why hire programmers when you could have a million "
                  "monkeys?");
        w.ElementUtcDatetime("date", time(nullptr));
        w.ElementObjectId("objectid", oid);
        w.ElementTimestamp("timestamp", 0);
        {
            const char* bin = "Some bin data 123";
            w.ElementBindata("bin_data", okmongo::BindataSubtype::kGeneric, bin,
                             static_cast<int>(strlen(bin)));
        }
        w.PushArray("long_array_name");
        {
            w.Element(0, "world");
            w.Element(1, 1.2);
            w.Element(2, true);
            w.Element(3, false);
            w.PushDocument(4);
            { w.Element("null", nullptr); }
            w.Pop();
        }
        w.Pop();
    }
    w.Pop();
    std::string res = w.ToString();
    {
        std::string v1 = SpoonFeed(res);
        std::string v2 = PrintBsonValue(res);
        if (v1 != v2) {
            std::cerr << "Values for v1 and v2 didn't match" << std::endl;
            std::cerr << "============================================\n" << v1
                      << "============================================\n" << v2;
            exit(1);
        }
        std::cout << v1;
    }

    // Crude getfield test
    {
        okmongo::BsonValue v(res.data(), static_cast<int32_t>(res.size()));
        okmongo::BsonValueIt It(v);
        while (!It.Done()) {
            okmongo::BsonValue v2 = v.GetField(It.key());
            assert(!v2.Empty());
            It.next();
        }
    }
    // Fuzz test...
    for (size_t i = 0; i < res.size(); ++i) {
        char &c = res[i];
        const char backup = c;
        std::cout << i << "/" << res.size() << std::endl;
        for (c = std::numeric_limits<char>::min();
             c < std::numeric_limits<char>::max(); ++c) {
            (void)PrintBsonValue(res);
        }
        c = backup;
    }
}
