// -*- mode:c++ -*-

/**
 * @file
 * @brief Helpers to read the mongo protocol
 */

#pragma once
#include "bson.h"
#include "string_matcher.h"

namespace okmongo {

/**
 * @defgroup mng_cmd_wrt writing mongo commands
 * @{
 */

template <typename... Values>
bool FillInsertOp(BsonWriter *w, int32_t requestid, const char *db,
                  const char *collection, const Values &... values);

/**
 * Insert a range of documents
 *
 * Updates `It` to point to the first document that couldn't fit in the query.
 */
template <typename It>
bool FillInsertRangeOp(BsonWriter *w, int32_t requestid, const char *db,
                       const char *collection, It *start, const It end);

template <typename T>
bool FillQueryOp(BsonWriter *w, int32_t requestid, const char *db,
                 const char *collection, const T &qry, int32_t limit = 0);

template <typename T, typename FldSelector>
bool FillQueryOp(BsonWriter *w, int32_t requestid, const char *db,
                 const char *collection, const T &qry, const FldSelector &sel,
                 int32_t limit = 0);

template <typename Select, typename Operation>
bool FillUpdateOp(BsonWriter *w, int32_t requestid, const char *db,
                  const char *collection, const Select &qry,
                  const Operation &op, bool upsert = false);

template <typename T>
bool FillDeleteOp(BsonWriter *w, int32_t requestid, const char *db,
                  const char *collection, const T &qry);

bool FillGetMoreOp(BsonWriter *w, int32_t requestid, const char *db,
                   const char *collection, int64_t cursorid);

bool FillIsMasterOp(BsonWriter *w, int32_t requestid);

/**
 * @todo iterators for Kill cursors
 */
bool FillKillCursorsOp(BsonWriter *w, int32_t requestid, int64_t cursorid);

/** @} */

enum class MongoOpcode : int32_t {
    kReply = 1,         /**< Reply to a client request. responseTo is set */
    kMsg = 1000,        /**< generic msg command followed by a string */
    kUpdate = 2001,     /**< update document */
    kInsert = 2002,     /**< insert new document */
    kQuery = 2004,      /**< query a collection */
    kGetMore = 2005,    /**< Get more data from a query. See Cursors */
    kDelete = 2006,     /**<  Delete documents */
    kKillCursors = 2007 /**< Tell database client is done with a cursor */
};

#pragma pack(push, 1)
/**
 * Mongo's message header.
 *
 * This header is included at the beginning of every message to and from the
 * mongo database.
 *
 * defined in:
 * http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/
 */
struct MsgHeader {
    int32_t message_length; /**< total message size, including this */
    int32_t request_id;     /**< identifier for this message */
    int32_t response_to;    /**< requestID from the original request
                                (used in reponses from db) */
    int32_t op_code;        /**< request type - (See `MongoOpCode`) */

    MsgHeader() {}
    MsgHeader(int32_t id, MongoOpcode op)
        : request_id(id), response_to(0), op_code(static_cast<int32_t>(op)) {}
};
static_assert(sizeof(MsgHeader) == 4 * sizeof(int32_t), "Packing failed");
#pragma pack(pop)

// Specialize this template to use the functions in this header...
template <typename T>
bool BsonWriteFields(BsonWriter *w, const T &);

//------------------------------------------------------------------------------
// Reading...
//------------------------------------------------------------------------------

#pragma pack(push, 1)

/**
 * Common header for all responses from the db.
 */
struct ResponseHeader : public MsgHeader {
    int32_t response_flags;   ///< bit vector - see details below
    int64_t cursor_id;        ///< cursor id if client needs to do get more's
    int32_t starting_from;    ///< where in the cursor this reply is starting
    int32_t number_returned;  ///< number of documents in the reply
};

enum ResponseFlags {
    kCursorNotFound = 1,  /// Set when getMore is called but the cursor id is
                          /// not valid at the server. Returned with zero
                          /// results.

    kQueryFailure = 2,  /// Set when query failed.Results consist of one
    /// document  containing an “$err” field describing the
    /// failure .

    kShardConfigStale = 4,  /// Drivers should ignore this.Only mongos will ever
                            /// see this set, in which case, it needs to update
                            /// config from the server .
    kAwaitCapable =
            8  /// Set when the server supports the AwaitData Query option. If
    /// it doesn’t, a client should sleep a little between getMore’s
    /// of a Tailable cursor.Mongod version 1.6 supports AwaitData
    /// and thus always sets AwaitCapable .

    // 4 - 31 Reserved Ignore
};

static_assert(sizeof(ResponseHeader) ==
                      sizeof(MsgHeader) + sizeof(int32_t) * 3 + sizeof(int64_t),
              "Packing pb");
#pragma pack(pop)

/**
 * This is the base class that you need to specialise in order to read values of
 * the network.
 */
template <typename Implementation>
class ResponseReader : public BsonReader<Implementation> {
protected:
    ResponseHeader header_ = {};
    int32_t doc_count_ = 0;
    typedef BsonReader<Implementation> Parent;

    const char *NextDocument(const char *s, const char *end);

public:
    static constexpr typename Parent::State kInitialState = Parent::State::kHdr;

    ResponseReader(const ResponseHeader &header);

    ResponseReader();

    // Read the header
    const char *ConsumeHdr(const char *s, const char *end);

    // CRTP specializable
    const char *DocumentStart(const char *s, const char *end) {
        return Parent::ConsumeValueDocument(s, end);
    }

    const char *DocumentDone(const char *s, const char *end) {
        return NextDocument(s, end);
    }

    void EmitDocumentDone() {}

    void EmitDocumentStart(int32_t) {}

    void EmitStop() {}

    void EmitStart(const ResponseHeader &) {}

    void Clear();

    const ResponseHeader &Header() const { return header_; }
};

//------------------------------------------------------------------------------
/**
 * This is a specialised response reader that read in `BsonValue`'s
 *
 *
 */
template <typename Implementation>
class BsonValueResponseReader : public ResponseReader<Implementation> {
protected:
    std::string buf_;
    typedef ResponseReader<Implementation> Parent;

public:
    const char *DocumentStart(const char *s, const char *end);

    // usr1 consumes the int32_t at the start of the document that says how big
    // the whole doc is. Usr2 grabs the rest.
    const char *ConsumeUsr1(const char *s, const char *end);

    const char *ConsumeUsr2(const char *s, const char *end);
};

//------------------------------------------------------------------------------
// $cmd responses...
//------------------------------------------------------------------------------

struct CmdError {
    int32_t code = 0;
    int32_t index = 0;
    std::string msg;
    std::string info;
    enum Type {
        WriteError,
        WriteConcernError,
        ParseError  ///< This is not a mongo error per-se
    } type;
};

struct OperationResponse {
    int32_t ok = 0;
    int32_t n = 0;
    int32_t nModified = 0;
    std::vector<CmdError> errors;
};

/**
 * This class reads the result of operations (in the new mongo protocol)
 *
 * The result is parsed in an `OperationResponse`
 */
class OpResponseParser : public ResponseReader<OpResponseParser> {
    enum class BaseField : uint8_t {
        kField,
        kOk,
        kNModified,
        kN,
        kUnknown,
        kWriteConcernErrors,
        kWriteErrors
    } base_field_ = BaseField::kUnknown;

    enum class ErrorField : uint8_t {
        kField,
        kIndex,
        kErrMsg,
        kErrInfo,
        kCode,
        kUnknown
    } error_field_ = ErrorField::kUnknown;

    uint8_t depth_ = 0;

    static constexpr StringMatcherAction<BaseField> sma_[] = {
            {"ok", BaseField::kOk},
            {"nModified", BaseField::kNModified},
            {"n", BaseField::kN},
            {"writeErrors", BaseField::kWriteErrors},
            {"writeConcernErrors", BaseField::kWriteConcernErrors},
            {nullptr, BaseField::kUnknown}};

    static constexpr StringMatcherAction<ErrorField> ema_[] = {
            {"index", ErrorField::kIndex},
            {"errmsg", ErrorField::kErrMsg},
            {"errInfo", ErrorField::kErrInfo},
            {"kcode", ErrorField::kCode},
            {nullptr, ErrorField::kUnknown}};

    typedef StringMatcher<BaseField, sma_> BaseMatcher;
    typedef StringMatcher<ErrorField, ema_> ErrorMatcher;

    union {
        BaseMatcher base_matcher_;
        ErrorMatcher error_matcher_;
    };

    OperationResponse res_;

public:
    OpResponseParser() {}

    void EmitFieldName(const char *data, const int32_t len);

    void EmitClose() { --depth_; }

    void EmitDocumentDone() {}

    void EmitDocumentStart(int32_t) {}

    void EmitError(const char *msg);

    void EmitOpenDoc();

    void EmitOpenArray() { ++depth_; }

    void EmitInt32(int32_t);

    void EmitInt64(int64_t) {}

    void EmitBool(bool) {}

    void EmitDouble(double) {}

    void EmitNull() {}

    bool IsError() const;

    void EmitUtf8(const char *cnt, int32_t len);

    void EmitObjectId(const char *) {}

    void EmitStart(const ResponseHeader &) {}

    void EmitStop() {}

    const OperationResponse &Result() const { return res_; }
};

//------------------------------------------------------------------------------
// Implementation

void AppendCommandHeader(BsonWriter *w, int32_t requestid, const char *db);
void AppendWriteConcern(BsonWriter *w);

// Inner function...
template <int32_t cnt = 0>
bool InsertDocuments(BsonWriter *) {
    return true;
}

template <int32_t cnt = 0, typename Arg, typename... Rest>
bool InsertDocuments(BsonWriter *w, const Arg &v, const Rest &... rest) {
    w->PushDocument(cnt);
    if (!BsonWriteFields<Arg>(w, v)) {
        return false;
    }
    w->Pop();

    InsertDocuments<cnt + 1>(w, rest...);
    return true;
}

template <typename... Values>
bool FillInsertOp(BsonWriter *w, int32_t requestid, const char *db,
                  const char *collection, const Values &... values) {
    AppendCommandHeader(w, requestid, db);

    w->Document();
    {
        w->Element("insert", collection);
        w->PushArray("documents");
        {
            if (!InsertDocuments(w, values...)) {
                return false;
            }
        }
        w->Pop();

        AppendWriteConcern(w);
    }
    w->Pop();

    w->FlushLen();
    return true;
}

/**
 * The maximum number of documents allowed in one write command.
 *
 *  Can be obtained from the db via: `db.isMaster().maxWriteBatchSize`
 */
constexpr int32_t kMaxWriteBatchSize = 1000;

template <typename It>
bool FillInsertRangeOp(BsonWriter *w, int32_t requestid, const char *db,
                       const char *collection, It *curs, const It end) {
    AppendCommandHeader(w, requestid, db);

    w->Document();
    {
        w->Element("insert", collection);
        w->PushArray("documents");
        {
            int32_t cnt = 0;
            while (*curs != end && cnt < kMaxWriteBatchSize) {
                w->PushDocument(cnt);
                // <typename It::value_type>
                if (!BsonWriteFields(w, *(*curs))) {
                    return false;
                };
                w->Pop();
                ++(*curs), ++cnt;
            }
        }
        w->Pop();

        AppendWriteConcern(w);
    }
    w->Pop();

    w->FlushLen();
    return true;
}

template <typename T>
bool FillQueryOp(BsonWriter *w, int32_t requestid, const char *db,
                 const char *collection, const T &qry, int32_t limit) {
    w->AppendRaw(MsgHeader(requestid, MongoOpcode::kQuery));
    w->AppendRaw<int32_t>(0);  // flags
    w->AppendRawBytes(db, static_cast<int32_t>(strlen(db)));
    w->AppendRawBytes(".", 1);
    w->AppendCstring(collection);

    if (limit > 0) {
        limit = -limit;
    }

    w->AppendRaw<int32_t>(0);      // Start
    w->AppendRaw<int32_t>(limit);  // Number to return

    w->Document();
    if (!BsonWriteFields<T>(w, qry)) {
        return false;
    }
    w->Pop();

    w->FlushLen();
    return true;
}

template <typename T, typename FldSelector>
bool FillQueryOp(BsonWriter *w, int32_t requestid, const char *db,
                 const char *collection, const T &qry, const FldSelector &sel,
                 int32_t limit) {
    w->AppendRaw(MsgHeader(requestid, MongoOpcode::kQuery));
    w->AppendRaw<int32_t>(0);  // flags
    w->AppendRawBytes(db, static_cast<int32_t>(strlen(db)));
    w->AppendRawBytes(".", 1);
    w->AppendCstring(collection);

    if (limit > 0) {
        limit = -limit;
    }

    w->AppendRaw<int32_t>(0);      // Start
    w->AppendRaw<int32_t>(limit);  // Number to return

    w->Document();
    if (!BsonWriteFields<T>(w, qry)) {
        return false;
    }
    w->Pop();

    w->Document();
    if (!BsonWriteFields<FldSelector>(w, sel)) {
        return false;
    }
    w->Pop();

    w->FlushLen();
    return true;
}

template <typename Select, typename Operation>
bool FillUpdateOp(BsonWriter *w, int32_t requestid, const char *db,
                  const char *collection, const Select &qry,
                  const Operation &op, bool upsert) {
    AppendCommandHeader(w, requestid, db);

    w->Document();
    {
        w->Element("update", collection);

        w->PushArray("updates");
        {
            w->PushDocument(0);
            {
                w->PushDocument("q");
                {
                    if (!BsonWriteFields<Select>(w, qry)) {
                        return false;
                    }
                }
                w->Pop();

                w->PushDocument("u");
                {
                    if (!BsonWriteFields<Operation>(w, op)) {
                        return false;
                    }
                }
                w->Pop();

                if (upsert) {
                    w->Element("upsert", true);
                }
            }
            w->Pop();
        }
        w->Pop();

        AppendWriteConcern(w);
    }
    w->Pop();

    w->FlushLen();
    return true;
}

template <typename T>
bool FillDeleteOp(BsonWriter *w, int32_t requestid, const char *db,
                  const char *collection, const T &qry) {
    AppendCommandHeader(w, requestid, db);

    w->Document();
    {
        w->Element("delete", collection);

        w->PushArray("deletes");
        {
            w->PushDocument(0);
            {
                w->PushDocument("q");
                {
                    if (!BsonWriteFields<T>(w, qry)) {
                        return false;
                    }
                }
                w->Pop();

                w->Element("limit", 0);
            }
            w->Pop();
        }
        w->Pop();

        AppendWriteConcern(w);
    }
    w->Pop();

    w->FlushLen();
    return true;
}

template <typename Implementation>
const char *ResponseReader<Implementation>::NextDocument(const char *s,
                                                         const char *end) {
    if (doc_count_ > 0) {
        Parent::impl().EmitDocumentDone();
    }

    if (doc_count_ != header_.number_returned) {
        Parent::impl().EmitDocumentStart(doc_count_);
        ++doc_count_;
        this->typ_ = BsonTag::kDocument;
        return Parent::impl().DocumentStart(s, end);
    } else {
        Parent::impl().EmitStop();
        return Parent::DocumentDone(s, end);
    }
}

template <typename Implementation>
const char *ResponseReader<Implementation>::ConsumeHdr(const char *s,
                                                       const char *end) {
    bool done;
    s = Parent::ReadBytes(&done, s, end, sizeof(ResponseHeader),
                          reinterpret_cast<char *>(&header_),
                          Parent::State::kHdr);
    if (!done) {
        return s;
    }
    Parent::impl().EmitStart(header_);
    return NextDocument(s, end);
}

template <typename Implementation>
ResponseReader<Implementation>::ResponseReader(const ResponseHeader &header)
    : header_(header) {
    Parent::bytes_seen_ = sizeof(ResponseHeader);
}

template <typename Implementation>
ResponseReader<Implementation>::ResponseReader() {}

template <typename Implementation>
const char *BsonValueResponseReader<Implementation>::DocumentStart(
        const char *s, const char *end) {
    assert(Parent::partial_ == 0);
    return ConsumeUsr1(s, end);
}

template <typename Implementation>
void ResponseReader<Implementation>::Clear() {
    Parent::Clear();
    header_ = {};
    doc_count_ = 0;
}

template <typename Implementation>
const char *BsonValueResponseReader<Implementation>::ConsumeUsr1(
        const char *s, const char *end) {
    bool done;
    s = Parent::ReadBytes(&done, s, end, sizeof(int32_t), Parent::scratch_,
                          Parent::State::kUsr1);
    if (!done) {
        return s;
    }
    int32_t len;
    std::memcpy(&len, Parent::scratch_, sizeof(int32_t));
    if (len < 5) {
        return Parent::Error("Document length too small");
    }
    buf_.clear();
    buf_.reserve(len);
    buf_.append(reinterpret_cast<const char *>(&len), 4);
    Parent::partial_ = len - 4;
    return ConsumeUsr2(s, end);
}

template <typename Implementation>
const char *BsonValueResponseReader<Implementation>::ConsumeUsr2(
        const char *s, const char *end) {
    assert(end >= s);
    int32_t inlen = static_cast<int32_t>(end - s);
    if (inlen < Parent::partial_) {
        Parent::state_ = Parent::State::kUsr2;
        Parent::partial_ -= inlen;
        buf_.append(s, inlen);
        return end;
    }
    buf_.append(s, Parent::partial_);
    assert(s[Parent::partial_ - 1] == '\000');
    const BsonValue bv(buf_.data(), buf_.length());
    Parent::impl().EmitBsonValue(bv);
    s += Parent::partial_;
    Parent::partial_ = 0;
    return Parent::NextDocument(s, end);
}

}  // namespace okmongo
