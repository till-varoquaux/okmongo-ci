#include "mongo.h"

namespace okmongo {
constexpr StringMatcherAction<OpResponseParser::BaseField>
        OpResponseParser::sma_[];

constexpr StringMatcherAction<OpResponseParser::ErrorField>
        OpResponseParser::ema_[];

void OpResponseParser::EmitFieldName(const char *data, const int32_t len) {
    assert(len >= 0);
    if (depth_ == 1) {
        if (base_field_ != BaseField::kField) {
            base_field_ = BaseField::kField;
            new (&base_matcher_) BaseMatcher();
        }
        for (int32_t i = 0; i < len; i++) {
            base_matcher_.AddChar(data[i]);
        }
        if (len == 0) {
            base_matcher_.AddChar('\000');
            base_field_ = base_matcher_.GetResult();
            base_matcher_.~BaseMatcher();
        }
    } else if (IsError()) {
        if (error_field_ != ErrorField::kField) {
            error_field_ = ErrorField::kField;
            new (&error_matcher_) ErrorMatcher();
        }

        for (int32_t i = 0; i < len; i++) {
            error_matcher_.AddChar(data[i]);
        }
        if (len == 0) {
            error_matcher_.AddChar('\000');
            error_field_ = error_matcher_.GetResult();
            error_matcher_.~ErrorMatcher();
        }
    }
}

void OpResponseParser::EmitOpenDoc() {
    ++depth_;
    if (IsError()) {
        res_.errors.push_back(CmdError{});
        if (base_field_ == BaseField::kWriteConcernErrors) {
            res_.errors.back().type = CmdError::Type::WriteConcernError;
        }
    }
}

void OpResponseParser::EmitInt32(int32_t i) {
    if (depth_ == 1) {
        switch (base_field_) {
            case BaseField::kField:
            case BaseField::kWriteConcernErrors:
            case BaseField::kWriteErrors:
            case BaseField::kUnknown:
                return;
            case BaseField::kOk:
                res_.ok = i;
                break;
            case BaseField::kN:
                res_.n = i;
                break;
            case BaseField::kNModified:
                res_.nModified = i;
                break;
        }
    } else if (IsError()) {
        switch (error_field_) {
            case ErrorField::kCode:
                res_.errors.back().code = i;
                break;
            case ErrorField::kIndex:
                res_.errors.back().index = i;
                break;
            case ErrorField::kField:
            case ErrorField::kUnknown:
            case ErrorField::kErrMsg:
            case ErrorField::kErrInfo:
                break;
        }
    }
}

bool OpResponseParser::IsError() const {
    return (depth_ == 3 && (base_field_ == BaseField::kWriteErrors ||
                            base_field_ == BaseField::kWriteConcernErrors));
}

void OpResponseParser::EmitUtf8(const char *cnt, int32_t len) {
    assert(len >= 0);
    if (len == 0) {
        return;
    }
    if (IsError()) {
        switch (error_field_) {
            case ErrorField::kErrMsg:
                res_.errors.back().msg.append(cnt, static_cast<size_t>(len));
                break;
            case ErrorField::kErrInfo:
                res_.errors.back().info.append(cnt, static_cast<size_t>(len));
                break;
            case ErrorField::kField:
            case ErrorField::kUnknown:
            case ErrorField::kCode:
            case ErrorField::kIndex:
                break;
        }
    }
}

void OpResponseParser::EmitError(const char *msg) {
    CmdError e = {};
    e.msg = msg;
    e.type = CmdError::Type::ParseError;
    res_.errors.push_back(e);
}

void AppendCommandHeader(BsonWriter *w, int32_t requestid, const char *db) {
    w->AppendRaw(MsgHeader(requestid, MongoOpcode::kQuery));
    w->AppendRaw<int32_t>(0);  // flags

    w->AppendRawBytes(db, static_cast<int32_t>(strlen(db)));
    w->AppendCstring(".$cmd");

    w->AppendRaw<int32_t>(0);   // Start
    w->AppendRaw<int32_t>(-1);  // Number to return
}

void AppendWriteConcern(BsonWriter *w) {
    w->PushDocument("WriteConcern");
    // w->Element("j", true);
    w->Element("wtimeout", 100);
    w->Element("w", 1);
    w->Pop();
}

bool FillIsMasterOp(BsonWriter *w, int32_t requestid) {
    AppendCommandHeader(w, requestid, "admin");
    w->Document();
    w->Element("ismaster", 1);
    w->Pop();
    w->FlushLen();
    return true;
}

bool FillGetMoreOp(BsonWriter *w, int32_t requestid, const char *db,
                   const char *collection, int64_t cursorid) {
    w->AppendRaw(MsgHeader(requestid, MongoOpcode::kGetMore));
    w->AppendRaw<int32_t>(0);  // Zero

    w->AppendRawBytes(db, static_cast<int32_t>(strlen(db)));
    w->AppendRawBytes(".", 1);
    w->AppendCstring(collection);

    w->AppendRaw<int32_t>(0);         // Number to return
    w->AppendRaw<int64_t>(cursorid);  // Cursorid
    w->FlushLen();
    return true;
}

bool FillKillCursorsOp(BsonWriter *w, int32_t requestid, int64_t cursorid) {
    w->AppendRaw(MsgHeader(requestid, MongoOpcode::kKillCursors));
    w->AppendRaw<int32_t>(0);         // Zero
    w->AppendRaw<int32_t>(1);         // Num cursor
    w->AppendRaw<int64_t>(cursorid);  // Cursorid
    w->FlushLen();
    return true;
}

}  // namespace okmongo
