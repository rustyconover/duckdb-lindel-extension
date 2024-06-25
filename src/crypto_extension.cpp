#define DUCKDB_EXTENSION_MAIN

#include "crypto_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// Include the declarations of things from Rust.
#include "rust.h"

namespace duckdb {

inline void CryptoScalarHashFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &hash_name_vector = args.data[0];
    auto &value_vector = args.data[1];

    BinaryExecutor::Execute<string_t, string_t, string_t>(
        hash_name_vector, value_vector, result, args.size(),
        [&](string_t hash_name, string_t value)
        {
            // AddString will retain the pointer, but really Rust allocated the string.
            auto hash_result = hashing_varchar(hash_name.GetData(), hash_name.GetSize(), value.GetData(), value.GetSize());
            if (hash_result.tag == ResultCString::Tag::Err) {
                throw InvalidInputException(hash_result.err._0);
            }

            auto output = StringVector::AddString(result, hash_result.ok._0);
            return output;
        });
}

inline void CryptoScalarHmacFun(DataChunk &args, ExpressionState &state, Vector &result) {
    // This is called with three arguments:
    // 1. The hash function name
    // 2. The key
    // 3. The value
    //
    // The return value is the hex-encoded HMAC.
    auto &hash_function_name_vector = args.data[0];
    auto &key_vector = args.data[1];
    auto &value_vector = args.data[2];

    TernaryExecutor::Execute<string_t, string_t, string_t, string_t>(
        hash_function_name_vector, key_vector, value_vector, result, args.size(),
        [&](string_t hash_function_name, string_t key, string_t value)
        {
            // AddString will retain the pointer, but really Rust allocated the string.
            auto hmac_result = hmac_varchar(hash_function_name.GetData(), hash_function_name.GetSize(),
            key.GetData(), key.GetSize(),
            value.GetData(), value.GetSize());
            if (hmac_result.tag == ResultCString::Tag::Err) {
                throw InvalidInputException(hmac_result.err._0);
            }

            auto output = StringVector::AddString(result, hmac_result.ok._0);
            return output;
        });
}



static void LoadInternal(DatabaseInstance &instance) {
    auto crypto_hash_scalar_function = ScalarFunction("crypto_hash", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, CryptoScalarHashFun);
    ExtensionUtil::RegisterFunction(instance, crypto_hash_scalar_function);

    auto crypto_hmac_scalar_function = ScalarFunction("crypto_hmac", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, CryptoScalarHmacFun);
    ExtensionUtil::RegisterFunction(instance, crypto_hmac_scalar_function);
}

void CryptoExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string CryptoExtension::Name() {
	return "crypto";
}

std::string CryptoExtension::Version() const {
#ifdef EXT_VERSION_QUACK
	return EXT_VERSION_QUACK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void crypto_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::CryptoExtension>();
}

DUCKDB_EXTENSION_API const char *crypto_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
