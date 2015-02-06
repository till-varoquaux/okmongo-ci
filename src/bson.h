// -*- mode:c++ -*-
/**
 * @file
 * @brief Reading and writing BSON values
 */
#pragma once
#include <memory>
#include <cstring>
#include <algorithm>
#include <cassert>
#include <cstddef>

namespace okmongo {

/**
 * Size (in bytes) of an objectid.
 */
static constexpr const int kObjectIdLen = 12;

/**
 * Used to identify the type of a field in a BSON document.
 *
 * @sa http://bsonspec.org/spec.html
 */
enum class BsonTag : signed char {
    kDouble = '\x01',       ///< Ieee 754 double precision float
    kUtf8 = '\x02',         ///< UTF8 string
    kDocument = '\x03',     ///< Embedded document
    kArray = '\x04',        ///< Array
    kBindata = '\x05',      ///< Binary data
    kObjectId = '\x07',     ///< Mongo Object id
    kBool = '\x08',         ///< Boolean
    kUtcDatetime = '\x09',  ///< UTC datetime (seconds since epoch)
    kNull = '\x0a',         ///< Null value
    kRegexp = '\x0b',       ///< Regular expression (Not supported)
    kJs = '\x0d',           ///< Javascript code (Not supported)
    kScopedJs = '\x0f',     ///< Scoped Javascript code (Not supported)
    kInt32 = '\x10',        ///< 32 bit integer
    kTimestamp = '\x11',    ///< Timestamp (Used internally by mongo in the
                            /// logs. Use a UtcDatetime if you want a real
                            /// timestamp).
    kInt64 = '\x12',        ///< 64 bit integer
    kMinKey = -1,           ///< Min key
                            // The bson spec says '\xff' but that is either 255
                            // or -1 depending on whether the compiler uses
                            // signed char or unsigned char for chars...
    kMaxKey = 127           ///< Max key
};

/**
 * Subtype for binary data.
 */
enum class BindataSubtype : unsigned char {
    kGeneric = '\x00',   ///< Generic binary subtype
    kFunction = '\x01',  ///< Function
    kBinary = '\x02',    ///< Binary (Old)
                         /// This used to be the default subtype, but was
                         /// deprecated in favor of \\x00. Drivers and tools
                         /// should be sure to handle \\x02 appropriately.
                         /// The structure of the binary data (the byte* array
                         /// in the binary non-terminal) must be an int32
                         /// followed by a (byte*). The int32 is the number of
                         /// bytes in the repetition.
    kUuidOld = '\x03',   ///< UUID (Old)
                         /// This used to be the UUID subtype, but was
                         /// deprecated in favor of \\x04.
                         /// Drivers and tools for languages with a native UUID
                         /// type should handle \\x03 appropriately.
    kUuid = '\x04',      ///< UUID
    kMd5 = '\x05',       ///< MD5
    kMinCustom = 0x80,   ///< Lowest tag acceptable for user defined subtypes.
                         /// The binary data can be anything.
    kMaxCustom = 0xFF
};

/**
 * Cast a char to a `BsonTag`
 *
 * @return `BsonTag::kMinKey` if the input was not a valid `BsonTag`
 */
BsonTag ToBsonTag(char c);

/**
 * Specialise this template to enable `BsonWriter` to use the type `T` as a
 * key in document.
 *
 * You need to provide two functions:
 * > static int32_t len(const T&);
 * > static const char *data(const T&);
 *
 * This template is already specialised for `const char*` and `std::string`.
 */
template <typename T>
struct KeyHelper {};

/**
 * A helper class to write bson values.
 *
 * This class writes to an internally managed buffer (which can be read through
 * `data()`. This buffer is the only thing that needs to get allocated in
 * this class.
 *
 * @note this driver only works on little endian architectures.
 * @todo use allocators
 *
 */
class BsonWriter {
public:
    BsonWriter() {}

    ~BsonWriter();

    /**
     * Reset the parser to its initial state.
     *
     * After being reset a parser can be reused to parse another value.
     */
    void Clear();

    /**
     * @defgroup bsw_fields Writing fields in arrays/documents
     * @{
     */

    /**
     * Close a document or an array.
     */
    void Pop();

    /**
     * Start a bson array.
     *
     * The `pop` call should be used to close the array when you are done. Keys
     * in an array should be `int32_t` in an increasing order.
     *
     * @see Pop()
     * @see PushDocument()
     * @param key an `int32_t` if we are in an array or a `char *`
     */
    template <typename K>
    void PushArray(const K key);

    /**
     * Start a field containing bson document.
     *
     * The `Pop()` call should be used to close the document when you are done.
     *
     * @note keys should be unique.
     * @see Pop()
     * @see PushArray()
     * @param key see `Element()` for an explanation
     */
    template <typename K>
    void PushDocument(const K key);

    /**
     * Holly overloaded madness...
     */
    template <typename K>
    void Element(const K key, const std::string &value);

    /** @overload */
    template <typename K>
    void Element(const K key, const char *value);

    /** @overload */
    template <typename K>
    void Element(const K key, const int32_t value);

    /** @overload */
    template <typename K>
    void Element(const K key, const int64_t value);

    /** @overload */
    template <typename K>
    void Element(const K key, const double value);

    /** @overload */
    template <typename K>
    void Element(const K key, const bool value);

    /** @overload */
    template <typename K>
    void Element(const K key, std::nullptr_t);

    /** @overload */
    template <typename K>
    void Element(const K key, const char *value, const int32_t value_len);

    template <typename K>
    void ElementUtcDatetime(const K key, const int64_t val);

    template <typename K>
    void ElementTimestamp(const K key, const int64_t val);

    template <typename K>
    void ElementObjectId(const K key, const char val[kObjectIdLen]);

    template <typename K>
    void ElementBindata(const K key, const BindataSubtype, const char *value,
                        const int32_t value_len);
    /** @} */

    /// Writes the current len of the buffer in the first field (as an int32).
    /// This should only be used IFF the first value of the buffer is an int32
    void FlushLen();

    /**
     * @defgroup bsw_raw BsonWriter raw values
     * These functions are used to write "raw" values in a `BsonWriter`.
     *
     * Unlike the fields in an array or a document those values are not tagged
     * and require you to know their types when reading them back in. Those
     * functions are useful to implement the commands in mongodb's wire protocol
     *
     * @{
     */

    template <typename T>
    void AppendRaw(const T &v);

    void AppendRawBytes(const char *cnt, int32_t len);

    void AppendCstring(const char *cnt, int32_t len);

    /** @overload */
    void AppendCstring(const char *cnt);

    /**
     * Start a bson document. The document must be closed with `Pop()`
     * @see Pop()
     */
    void Document();

    /** @} */

    /**
     * Get the underlying buffer
     */
    const char *data() const;

    /**
     * Get the length of the underlying buffer.
     */
    int32_t len() const { return pos_; }

    /**
     * Copy the content of the class to a `std::string`.
     *
     */
    std::string ToString() const;

protected:
    bool DataIsInline() const { return size_ == kMinSize_; }

    void StartDocument();

    static constexpr int32_t kMinSize_ = 240;

    // Short string optimization
    // for messages less than kMinSize_ in length we use an inline buffer
    // otherwise
    // we use a heap allocated one.
    union {
        char inline_data_[kMinSize_];
        std::unique_ptr<char[]> data_;
    };

    // Doc start points to the offset where the current document starts
    // because the length of the doc needs to be filled when we close the
    // document
    // we use this address to hold the previous doc_start_ value.
    int32_t doc_start_ = 0;
    int32_t pos_ = 0;
    int32_t size_ = kMinSize_;

    /**
     * Count the number of digits appearing in the decimal representation of n
     *
     * `n` must be positive
     */
    static inline int32_t CountDigits(int32_t n);

    char *WritableData();

    char *curs();

    char *StartField(const BsonTag tag, const char *k, const int32_t klen,
                     const int32_t cntlen);

    template <typename T>
    char *StartField(const BsonTag tag, const T, const int32_t cntlen);

    char *StartField(const BsonTag tag, int32_t k, const int32_t cntlen);

    void Reserve(int32_t r);

    template <BsonTag TAG, typename K, typename T>
    void Element(const K k, const T v);
};

/**
 * The BSON reader is a reentrant state machine that uses CRTP for the
 * callbacks.
 *
 */
template <typename Implementation>
class BsonReader {
protected:
    /**
     * A helper class to align the buffer.
     *
     * we could use `std::max_align_t` but
     * https://gcc.gnu.org/bugzilla/show_bug.cgi?id=56019
     * is preventing us from that so, instead, we use a union of all the types
     * we might want to put in here...
     */
    typedef union {
        double d;
        int32_t i32;
        int64_t i64;
        char oid[kObjectIdLen];
    } align_t;

    static constexpr int8_t kScratchLen_ = sizeof(align_t);

    alignas(align_t) char scratch_[kScratchLen_];

    /**
     * State the DFA is in...
     */
    enum class State : char {
        kFieldTyp,
        kFieldName,
        kReadInt32,
        kReadInt64,
        kReadDouble,
        kReadBool,
        kReadString,
        kReadStringTerm,
        kReadBinSubtype,
        kReadObjectId,
        kDone,
        kError,
        kHdr,   ///< Used by mongo packet readers...
        kUsr1,  ///< Value reserved for user defined state
        kUsr2,  ///< Value reserved for user defined state
        kUsr3,  ///< Value reserved for user defined state
        kUsr4   ///< Value reserved for user defined state
    } state_;

    BsonTag typ_;
    int8_t depth_;

    int32_t partial_;
    int32_t bytes_seen_;

    // This is capped at 100 by mongo anyway...
    int8_t depth() const { return depth_; }

public:
    /**
     * Reset the parser to its initial state.
     *
     * After being reset a parser can be reused to parse another value.
     */
    void Clear();

    /**
     * State the parser starts in.
     *
     * Override this via CRTP to define custom parsers that start in a different
     * State.
     * (Initially set to int32 for the length of a document).
     */
    static constexpr State kInitialState = State::kReadInt32;

    BsonReader();

    /**
     * Report an error.
     */
    template <typename Error_type>
    const char *Error(Error_type msg);

    /**
     * Tells us whether we are done parsing.
     */
    bool Done() const;
    /**
     * Return the number of bytes read...
     */
    int32_t Consume(const char *s, int32_t len);

protected:
    /**
     * Helper placeholder function that is used as the default value for
     * functions that should have been specialised by the user.
     */
    const char *Missing() {
        assert(false);
        return nullptr;
    }

    /**
     * This method is overriden by `ResponseReader` using CRTP
     */
    const char *ConsumeHdr(const char *, const char *) { return Missing(); }

    // "Virtual" function that the user can override in CRTP inherited classes
    // associated with the state kUsr1...4
    const char *ConsumeUsr1(const char *, const char *) { return Missing(); }

    const char *ConsumeUsr2(const char *, const char *) { return Missing(); }

    const char *ConsumeUsr3(const char *, const char *) { return Missing(); }

    const char *ConsumeUsr4(const char *, const char *) { return Missing(); }

    const char *DocumentDone(const char *s, const char *) {
        state_ = State::kDone;
        return s;
    }

    //--------------------------------------------------------------------------

    // CRTP...
    Implementation &impl() { return *static_cast<Implementation *>(this); }

    const Implementation &impl() const {
        return *static_cast<const Implementation *>(this);
    }

    //--------------------------------------------------------------------------

    /**
     * @defgroup bson_reader_state_machine reading state machine
     * @{
     */

    const char *ConsumeFieldTyp(const char *s, const char *end);
    const char *ConsumeValueInt32Cnt(const char *s, const char *end, int32_t t);
    const char *ConsumeValueInt32(const char *s, const char *end);
    const char *ConsumeValueInt64Cnt(const char *s, const char *end, int64_t t);
    const char *ConsumeValueInt64(const char *s, const char *end);
    const char *ConsumeValueBinSubtype(const char *s, const char *end);
    const char *ConsumeValueBool(const char *s, const char *end);
    const char *ConsumeValueDoubleCnt(const char *s, const char *end, double d);
    const char *ConsumeValueDouble(const char *s, const char *end);
    const char *ConsumeValueStringTerm(const char *s, const char *end);
    const char *ConsumeValueString(const char *s, const char *end);
    const char *ConsumeValue(const char *s, const char *end);
    const char *ConsumeValueObjectId(const char *s, const char *end);
    const char *ConsumeFieldName(const char *s, const char *end);

    void DispatchStringData(const char *s, const int32_t inlen);
    /**
     * @}
     */
    const char *ReadBytes(bool *done, const char *s, const char *end,
                          int32_t sz, char *dst, State state);

    // Consume a fixed length and pass it to the continuation (cont)
    template <typename T, State state,
              const char *(BsonReader::*cont)(const char *s, const char *end,
                                              T)>
    const char *ReadVal(const char *s, const char *end);

    /**
     * @defgroup bsr_crtp BsonReader CRTP
     * `BsonReader`'s function that can overloaded to customize the behaviour.
     * @{
     */
    void EmitOpenDoc() {}

    void EmitClose() {}

    void EmitOpenArray() {}

    void EmitInt32(int32_t) {}

    void EmitInt64(int64_t) {}

    void EmitBool(bool) {}

    void EmitDouble(double) {}

    void EmitNull() {}

    // Will be called back with a length of 0 when done
    void EmitUtf8(const char *, int32_t) {}

    void EmitBindataSubtype(BindataSubtype) {}

    // A length of zero means that it's the last call
    void EmitBindata(const char *, int32_t) {}

    void EmitJs(const char *, int32_t) {}

    void EmitUtcDatetime(const int64_t) {}

    void EmitTimestamp(const int64_t) {}

    void EmitFieldName(const char *, const int32_t) {}

    void EmitObjectId(const char *) {}
    /** @}*/
};  // BsonReader

// Random access interface...
class BsonValue {
protected:
    const char *data_;
    BsonTag tag_;
    int32_t size_;

    friend class BsonValueIt;

public:
    BsonTag Tag() const { return tag_; }
    int32_t GetDataSize() const;
    const char *GetData() const;

    BsonValue() : data_(nullptr), tag_(BsonTag::kMinKey), size_(0) {}
    BsonValue(const char *data, int32_t size, BsonTag tag = BsonTag::kDocument);
    BsonValue(const BsonValue &) = default;
    BsonValue(BsonValue &&) = default;
    bool Empty() const { return data_ == nullptr; }

    // Only for documents...
    BsonValue GetField(const char *needle) const;

    int64_t GetInt64() const;
    int64_t GetTimestamp() const;
    int64_t GetUtcDatetime() const;
    int32_t GetInt32() const;
    double GetDouble() const;
    bool GetBool() const;
    BindataSubtype GetBinSubstype() const;
};

/**
 An iterator for `BsonValue`s
 */
class BsonValueIt : public BsonValue {
protected:
    const char *end_ = nullptr;
    const char *key_ = nullptr;
    void Invalidate();
    void MoveTo(const char *);

public:
    BsonValueIt();
    explicit BsonValueIt(const BsonValue &v);
    bool Done() const;
    const char *key() const { return key_; }
    void next();
};

//------------------------------------------------------------------------------
// Implementation
//------------------------------------------------------------------------------

template <>
struct KeyHelper<const char *> {
    static int32_t len(const char *c) {
        return static_cast<int32_t>(strlen(c));
    }
    static const char *data(const char *c) { return c; }
};

inline void BsonWriter::Pop() {
    Reserve(1);
    *curs() = '\000';
    ++pos_;
    int32_t doc_len = pos_ - doc_start_;
    char *start_pos = WritableData() + doc_start_;
    std::memcpy(&doc_start_, start_pos, 4);
    std::memcpy(start_pos, &doc_len, 4);
}

template <typename K>
void BsonWriter::PushArray(const K key) {
    StartField(BsonTag::kArray, key, 10);
    StartDocument();
}

template <typename K>
void BsonWriter::PushDocument(const K key) {
    StartField(BsonTag::kDocument, key, 10);
    StartDocument();
}

template <typename K>
void BsonWriter::Element(const K key, const std::string &value) {
    Element(key, value.data(), static_cast<int32_t>(value.size()));
}

template <typename K>
void BsonWriter::Element(const K key, const char *value) {
    Element(key, value, static_cast<int32_t>(strlen(value)));
}

template <typename K>
void BsonWriter::Element(const K key, const int32_t value) {
    Element<BsonTag::kInt32>(key, value);
}

template <typename K>
void BsonWriter::Element(const K key, const int64_t value) {
    Element<BsonTag::kInt64>(key, value);
}

template <typename K>
void BsonWriter::Element(const K key, const double value) {
    Element<BsonTag::kDouble>(key, value);
}

template <typename K>
void BsonWriter::Element(const K key, const bool value) {
    Element<BsonTag::kBool>(key, value ? '\1' : '\0');
}

template <typename K>
void BsonWriter::ElementUtcDatetime(const K key, const int64_t value) {
    Element<BsonTag::kUtcDatetime>(key, value);
}

template <typename K>
void BsonWriter::ElementTimestamp(const K key, const int64_t value) {
    Element<BsonTag::kTimestamp>(key, value);
}

template <typename K>
void BsonWriter::ElementObjectId(const K key, const char value[kObjectIdLen]) {
    char *out = StartField(BsonTag::kObjectId, key, kObjectIdLen);
    std::memcpy(out, value, kObjectIdLen);
    pos_ += kObjectIdLen;
}

template <typename K>
void BsonWriter::ElementBindata(const K key, const BindataSubtype st,
                                const char *value, const int32_t value_len) {
    const int32_t flen = 4 + 1 + value_len;
    char *out = StartField(BsonTag::kBindata, key, flen);
    std::memcpy(out, &value_len, 4);
    out[4] = static_cast<char>(st);
    std::memcpy(out + 5, value, static_cast<size_t>(value_len));
    pos_ += flen;
}


template <typename K>
void BsonWriter::Element(const K key, std::nullptr_t) {
     StartField(BsonTag::kNull, key, 0);
}

template <typename K>
void BsonWriter::Element(const K key, const char *value,
                         const int32_t value_len) {
    const int32_t flen = 4 + value_len + 1;
    char *out = StartField(BsonTag::kUtf8, key, flen);
    // includes the trailing null...
    const int32_t wlen = value_len + 1;
    std::memcpy(out, &wlen, 4);
    std::memcpy(out + 4, value, static_cast<size_t>(value_len));
    out[value_len + 4] = '\000';
    pos_ += flen;
}

inline void BsonWriter::FlushLen() {
    int32_t doc_len = pos_;
    // We shouldn't have to use memcpy here: we know the alignment is fine.
    std::memcpy(WritableData(), &doc_len, sizeof(int32_t));
}

inline void BsonWriter::AppendRawBytes(const char *cnt, int32_t len) {
    Reserve(len);
    char *out = curs();
    std::memcpy(out, cnt, static_cast<size_t>(len));
    pos_ += len;
}

inline void BsonWriter::AppendCstring(const char *cnt, int32_t len) {
    Reserve(len + 1);
    char *out = curs();
    std::memcpy(out, cnt, static_cast<size_t>(len));
    out[len] = '\000';
    pos_ += len + 1;
}

/** @overload */
inline void BsonWriter::AppendCstring(const char *cnt) {
    return AppendCstring(cnt, static_cast<int32_t>(strlen(cnt)));
}

template <typename T>
void BsonWriter::AppendRaw(const T &v) {
    Reserve(sizeof(T));
    std::memcpy(curs(), &v, sizeof(T));
    pos_ += sizeof(T);
}

inline void BsonWriter::Document() {
    Reserve(5);
    StartDocument();
}

inline const char *BsonWriter::data() const {
    return DataIsInline() ? inline_data_ : data_.get();
}

inline std::string BsonWriter::ToString() const {
    return std::string(data(), static_cast<size_t>(pos_));
}

inline void BsonWriter::StartDocument() {
    std::memcpy(curs(), &doc_start_, 4);
    doc_start_ = pos_;
    pos_ += 4;
}

inline int32_t BsonWriter::CountDigits(int32_t n) {
    int32_t res = 1;
    for (;;) {
        if (n < 10) return res;
        if (n < 100) return res + 1;
        if (n < 1000) return res + 2;
        if (n < 10000) return res + 3;
        n /= 10000u;
        res += 4;
    }
}

inline char *BsonWriter::WritableData() {
    return DataIsInline() ? inline_data_ : data_.get();
}

inline char *BsonWriter::curs() { return WritableData() + pos_; }

inline char *BsonWriter::StartField(const BsonTag tag, const char *key_data,
                                    const int32_t key_len,
                                    const int32_t cnt_len) {
    const int32_t tot_len = 1 + key_len + 1 + cnt_len;
    Reserve(tot_len);
    char *out = curs();
    *out = static_cast<char>(tag);
    std::memcpy(out + 1, key_data, static_cast<size_t>(key_len));
    out[key_len + 1] = '\000';
    pos_ += key_len + 2;
    return out + key_len + 2;
}

template <typename K>
inline char *BsonWriter::StartField(const BsonTag tag, const K k,
                                    const int32_t cntlen) {
    typedef KeyHelper<K> kh;
    return StartField(tag, kh::data(k), kh::len(k), cntlen);
}

inline char *BsonWriter::StartField(const BsonTag tag, int32_t k,
                                    const int32_t cntlen) {
    assert(k >= 0);
    int32_t klen = CountDigits(k);
    const int32_t totlen = 1 + klen + 1 + cntlen;
    Reserve(totlen);
    char *out = curs();
    *out = static_cast<char>(tag);
    out += klen;
    do {
        *out = k % 10 + '0';
        k /= 10;
        out--;
    } while (k);
    out[klen + 1] = '\000';
    pos_ += klen + 2;
    return out + klen + 2;
}

inline void BsonWriter::Reserve(int32_t r) {
    if (size_ < pos_ + r) {
        auto new_size = std::max(2 * size_, size_ + r + 2);
        std::unique_ptr<char[]> new_doc(new char[new_size]);
        std::memcpy(new_doc.get(), data(), static_cast<size_t>(pos_));
        if (DataIsInline()) {
            new (&data_) std::unique_ptr<char[]>(std::move(new_doc));
        } else {
            data_ = std::move(new_doc);
        }
        size_ = new_size;
    }
}

template <BsonTag TAG, typename K, typename V>
void BsonWriter::Element(const K k, const V v) {
    char *out = StartField(TAG, k, sizeof(V));
    std::memcpy(out, &v, sizeof(V));
    pos_ += sizeof(V);
}

template <typename T>
BsonReader<T>::BsonReader() {
    Clear();
}

template <typename T>
void BsonReader<T>::Clear() {
    state_ = T::kInitialState;
    typ_ = BsonTag::kDocument;
    depth_ = 0;
    partial_ = 0;
    bytes_seen_ = 0;
}

template <typename T>
template <typename Error_type>
const char *BsonReader<T>::Error(Error_type msg) {
    state_ = State::kError;
    impl().EmitError(msg);
    return nullptr;
}

template <typename T>
bool BsonReader<T>::Done() const {
    return state_ == State::kDone || state_ == State::kError;
}

template <typename T>
int32_t BsonReader<T>::Consume(const char *s, int32_t len) {
    assert(len >= 0);
    if (len == 0) {
        return 0;
    }

    const char *end = nullptr;
    switch (state_) {
        case State::kError:
        case State::kDone:
            return 0;
        case State::kFieldTyp:
            end = ConsumeFieldTyp(s, s + len);
            break;
        case State::kFieldName:
            end = ConsumeFieldName(s, s + len);
            break;
        case State::kReadInt32:
            end = ConsumeValueInt32(s, s + len);
            break;
        case State::kReadInt64:
            end = ConsumeValueInt64(s, s + len);
            break;
        case State::kReadBool:
            end = ConsumeValueBool(s, s + len);
            break;
        case State::kReadDouble:
            end = ConsumeValueDouble(s, s + len);
            break;
        case State::kReadString:
            end = ConsumeValueString(s, s + len);
            break;
        case State::kReadStringTerm:
            end = ConsumeValueStringTerm(s, s + len);
            break;
        case State::kReadBinSubtype:
            end = ConsumeValueBinSubtype(s, s + len);
            break;
        case State::kReadObjectId:
            end = ConsumeValueObjectId(s, s + len);
            break;
        case State::kHdr:
            end = impl().ConsumeHdr(s, s + len);
            break;
        case State::kUsr1:
            end = impl().ConsumeUsr1(s, s + len);
            break;
        case State::kUsr2:
            end = impl().ConsumeUsr2(s, s + len);
            break;
        case State::kUsr3:
            end = impl().ConsumeUsr3(s, s + len);
            break;
        case State::kUsr4:
            end = impl().ConsumeUsr4(s, s + len);
            break;
    }
    if (end == nullptr) {
        return -1;
    }
    int32_t read = static_cast<int32_t>(end - s);
    bytes_seen_ += read;
    return read;
}

template <typename T>
const char *BsonReader<T>::ConsumeFieldTyp(const char *s, const char *end) {
    if (s == end) {
        state_ = State::kFieldTyp;
        return end;
    }
    if (*s == '\000') {
        --depth_;
        impl().EmitClose();
        if (depth_ == 0) {
            return impl().DocumentDone(s + 1, end);
        } else {
            return ConsumeFieldTyp(s + 1, end);
        }
    }
    typ_ = ToBsonTag(*s);
    return ConsumeFieldName(s + 1, end);
}

template <typename T>
const char *BsonReader<T>::ConsumeValueInt32Cnt(const char *s, const char *end,
                                                int32_t t) {
    switch (typ_) {
        case BsonTag::kDocument:
            ++depth_;
            impl().EmitOpenDoc();
            return ConsumeFieldTyp(s, end);
        case BsonTag::kArray:
            ++depth_;
            impl().EmitOpenArray();
            return ConsumeFieldTyp(s, end);
        case BsonTag::kInt32:
            impl().EmitInt32(t);
            return ConsumeFieldTyp(s, end);
        case BsonTag::kUtf8:
        case BsonTag::kJs:
            if (t < 1) {
                return Error("Negative length!");
            }
            partial_ = t - 1;
            return ConsumeValueString(s, end);
        case BsonTag::kBindata:
            if (t < 0) {
                return Error("Negative length!");
            }
            partial_ = t;
            return ConsumeValueBinSubtype(s, end);
        default:
            assert(false);
            return Error("internal error");
    }
}

template <typename T>
const char *BsonReader<T>::ConsumeValueInt32(const char *s, const char *end) {
    return ReadVal<int32_t, State::kReadInt32,
                   &BsonReader::ConsumeValueInt32Cnt>(s, end);
}

template <typename T>
const char *BsonReader<T>::ConsumeValueInt64Cnt(const char *s, const char *end,
                                                int64_t t) {
    switch (typ_) {
        case BsonTag::kInt64:
            impl().EmitInt64(t);
            break;
        case BsonTag::kUtcDatetime:
            impl().EmitUtcDatetime(t);
            break;
        case BsonTag::kTimestamp:
            impl().EmitTimestamp(t);
            break;
        default:
            assert(false);
    }
    return (ConsumeFieldTyp(s, end));
}

template <typename T>
const char *BsonReader<T>::ConsumeValueInt64(const char *s, const char *end) {
    return ReadVal<int64_t, State::kReadInt64,
                   &BsonReader::ConsumeValueInt64Cnt>(s, end);
}

template <typename T>
const char *BsonReader<T>::ConsumeValueBool(const char *s, const char *end) {
    if (s == end) {
        state_ = State::kReadBool;
        return end;
    }
    impl().EmitBool(*s > 0);
    return (ConsumeFieldTyp(s + 1, end));
}

template <typename T>
const char *BsonReader<T>::ConsumeValueDoubleCnt(const char *s, const char *end,
                                                 double d) {
    impl().EmitDouble(d);
    return (ConsumeFieldTyp(s, end));
}

template <typename T>
const char *BsonReader<T>::ConsumeValueDouble(const char *s, const char *end) {
    return ReadVal<double, State::kReadDouble,
                   &BsonReader::ConsumeValueDoubleCnt>(s, end);
}

template <typename T>
const char *BsonReader<T>::ConsumeValueStringTerm(const char *s,
                                                  const char *end) {
    if (s == end) {
        state_ = State::kReadStringTerm;
        return end;
    }

    partial_ = 0;

    if (*s != '\000') {
        return Error("expected null byte");
    }
    return ConsumeFieldTyp(s + 1, end);
}

template <typename T>
const char *BsonReader<T>::ConsumeValueBinSubtype(const char *s,
                                                  const char *end) {
    if (s == end) {
        state_ = State::kReadBinSubtype;
        return end;
    }
    impl().EmitBindataSubtype(BindataSubtype(*s));
    return ConsumeValueString(s + 1, end);
}

template <typename T>
void BsonReader<T>::DispatchStringData(const char *s, const int32_t inlen) {
    switch (typ_) {
        case BsonTag::kUtf8:
            impl().EmitUtf8(s, inlen);
            break;
        case BsonTag::kJs:
            impl().EmitJs(s, inlen);
            break;
        case BsonTag::kBindata:
            impl().EmitBindata(s, inlen);
            break;
        default:
            assert(false);
    }
}

template <typename T>
const char *BsonReader<T>::ConsumeValueString(const char *s, const char *end) {
    int32_t inlen = static_cast<int32_t>(end - s);
    if (inlen < partial_) {
        state_ = State::kReadString;
        partial_ -= inlen;
        if (inlen > 0) {
            DispatchStringData(s, inlen);
        }
        return end;
    }
    DispatchStringData(s, partial_);
    DispatchStringData(nullptr, 0);
    if (typ_ == BsonTag::kBindata) {
        s += partial_;
        partial_ = 0;
        return ConsumeFieldTyp(s, end);
    } else {
        return ConsumeValueStringTerm(s + partial_, end);
    }
}

template <typename T>
const char *BsonReader<T>::ConsumeValue(const char *s, const char *end) {
    switch (typ_) {
        case BsonTag::kInt32:
        case BsonTag::kArray:
        case BsonTag::kDocument:
        case BsonTag::kUtf8:
        case BsonTag::kJs:
        case BsonTag::kBindata:
            return ConsumeValueInt32(s, end);
        case BsonTag::kInt64:
        case BsonTag::kUtcDatetime:
        case BsonTag::kTimestamp:
            return ConsumeValueInt64(s, end);
        case BsonTag::kBool:
            return ConsumeValueBool(s, end);
        case BsonTag::kDouble:
            return ConsumeValueDouble(s, end);
        case BsonTag::kNull:
            impl().EmitNull();
            return ConsumeFieldTyp(s, end);
        case BsonTag::kObjectId:
            return ConsumeValueObjectId(s, end);
        case BsonTag::kRegexp:
        case BsonTag::kScopedJs:
            return Error("Field type no handled");
        case BsonTag::kMinKey:
            return Error("Invalid bson tag");
        case BsonTag::kMaxKey:
            assert(false);
            return Error("Internal error");
    }
}

template <typename T>
const char *BsonReader<T>::ConsumeValueObjectId(const char *s,
                                                const char *end) {
    bool done;
    s = ReadBytes(&done, s, end, kObjectIdLen, scratch_, State::kReadObjectId);
    if (!done) {
        return s;
    }
    impl().EmitObjectId(scratch_);
    return ConsumeFieldTyp(s, end);
}

template <typename T>
const char *BsonReader<T>::ConsumeFieldName(const char *s, const char *end) {
    const int32_t max_len = static_cast<int32_t>(end - s);
    int32_t i = 0;
    //  strnlen....
    while (i < max_len) {
        if (!s[i]) {
            if (i > 0) impl().EmitFieldName(s, i);
            impl().EmitFieldName(nullptr, 0);
            return ConsumeValue(s + i + 1, end);
        }
        ++i;
    }
    if (i > 0) {
        impl().EmitFieldName(s, i);
    }
    state_ = State::kFieldName;
    return end;
}

template <typename T>
const char *BsonReader<T>::ReadBytes(bool *done, const char *s, const char *end,
                                     int32_t sz, char *dst, State state) {
    auto i = partial_;
    while (i < sz && s < end) {
        dst[i] = *s;
        ++s;
        ++i;
    }
    if (i < sz) {
        partial_ = i;
        state_ = state;
        *done = false;
        return end;
    }
    *done = true;
    partial_ = 0;
    return s;
}

template <typename Implementation>
template <typename T, typename BsonReader<Implementation>::State state,
          const char *(BsonReader<Implementation>::*cont)(const char *s,
                                                          const char *end, T)>
const char *BsonReader<Implementation>::ReadVal(const char *s,
                                                const char *end) {
    static_assert(sizeof(T) <= sizeof(scratch_), "Type too big");
    T t;
    bool done;
    s = ReadBytes(&done, s, end, sizeof(T), scratch_, state);
    if (!done) {
        return s;
    }
    // Gah: this *could* mess up tail call optimization:
    // http://david.wragg.org/blog/2014/02/c-tail-calls-1.html
    std::memcpy(&t, scratch_, sizeof(t));
    return (this->*cont)(s, end, t);
}

inline bool BsonValueIt::Done() const { return tag_ == BsonTag::kMinKey; }
}  // namespace okmongo
