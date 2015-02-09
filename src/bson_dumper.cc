#include "bson_dumper.h"

namespace okmongo {

bool Print(const BsonValue &v, BsonDocDumper *d) {
    BsonTag tag = v.Tag();
    switch (tag) {
        case BsonTag::kDouble:
            d->EmitDouble(v.GetDouble());
            return true;
        case BsonTag::kInt32:
            d->EmitInt32(v.GetInt32());
            return true;
        case BsonTag::kInt64:
            d->EmitInt64(v.GetInt64());
            return true;
        case BsonTag::kUtcDatetime:
            d->EmitUtcDatetime(v.GetUtcDatetime());
            return true;
        case BsonTag::kTimestamp:
            d->EmitTimestamp(v.GetTimestamp());
            return true;
        case BsonTag::kBool:
            d->EmitBool(v.GetBool());
            return true;
        case BsonTag::kNull:
            d->EmitNull();
            return true;
        case BsonTag::kDocument:
        case BsonTag::kArray: {
            BsonValueIt it(v);
            bool ok = true;
            if (tag == BsonTag::kArray) {
                d->EmitOpenArray();
            } else {
                d->EmitOpenDoc();
            }
            while (!it.Done()) {
                d->EmitFieldName(it.key(),
                                 static_cast<int32_t>(strlen(it.key())));
                d->EmitFieldName(nullptr, 0);
                ok = Print(it, d) && ok;
                ok = it.next() && ok;
            }
            d->EmitClose();
            return ok;
        }
        case BsonTag::kObjectId:
            d->EmitObjectId(v.GetData());
            return true;
        case BsonTag::kUtf8:
            d->EmitUtf8(v.GetData(), v.GetDataSize());
            d->EmitUtf8(nullptr, 0);
            return true;
        case BsonTag::kJs:
            d->EmitJs(v.GetData(), v.GetDataSize());
            d->EmitJs(nullptr, 0);
            return true;
        case BsonTag::kBindata:
            d->EmitBindataSubtype(v.GetBinSubstype());
            d->EmitBindata(v.GetData(), v.GetDataSize());
            d->EmitBindata(nullptr, 0);
            return true;
        case BsonTag::kScopedJs:
        case BsonTag::kRegexp:
        case BsonTag::kMinKey:
        case BsonTag::kMaxKey:
            // assert (false);
            return false;
    }
}

}  //  namespace okmongo
