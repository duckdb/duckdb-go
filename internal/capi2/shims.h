#pragma once

// Include duckdb_extension_v2.h before this header to dispatch these same bodies
// through the host table. cgo calls real C functions, not function-pointer macros.
#include "include/duckdb_v2.h"

static inline DUCKDB_V2_ERROR capi2_environment_create(duckdb_v2_environment_handle *out, duckdb_v2_error_info_handle *err) {
	return duckdb_v2_environment_create(out, err);
}
static inline DUCKDB_V2_ERROR capi2_environment_destroy(duckdb_v2_environment_handle *env) {
	return duckdb_v2_environment_destroy(env);
}
static inline DUCKDB_V2_ERROR capi2_instance_create(duckdb_v2_environment_handle env, duckdb_v2_instance_handle *out, duckdb_v2_error_info_handle *err) {
	// Disable network-assisted loading before the instance starts. The bootstrap
	// must supply the statically linked extensions itself.
	const duckdb_v2_str names[] = {{"autoload_known_extensions", 25}, {"autoinstall_known_extensions", 28}};
	const duckdb_v2_str values[] = {{"false", 5}, {"false", 5}};
	return duckdb_v2_instance_create_with_options(env, names, values, 2, out, err);
}
static inline DUCKDB_V2_ERROR capi2_instance_attach(duckdb_v2_instance_handle instance, const char *path, idx_t size, duckdb_v2_error_info_handle *err) {
	const duckdb_v2_str view = {path, size};
	return duckdb_v2_instance_attach(instance, NULL, &view, NULL, true, err);
}
static inline DUCKDB_V2_ERROR capi2_instance_destroy(duckdb_v2_instance_handle *instance) {
	return duckdb_v2_instance_destroy(instance);
}
static inline DUCKDB_V2_ERROR capi2_connection_create(duckdb_v2_instance_handle instance, duckdb_v2_connection_handle *out, duckdb_v2_error_info_handle *err) {
	return duckdb_v2_connection_create(instance, out, err);
}
static inline DUCKDB_V2_ERROR capi2_connection_destroy(duckdb_v2_connection_handle *conn) {
	return duckdb_v2_connection_destroy(conn);
}
static inline DUCKDB_V2_ERROR capi2_parse(duckdb_v2_connection_handle conn, const char *sql, duckdb_v2_statement_iterator_handle *out, duckdb_v2_error_info_handle *err) {
	return duckdb_v2_parse_sql(conn, sql, true, out, err);
}
static inline DUCKDB_V2_ERROR capi2_iterator_next(duckdb_v2_statement_iterator_handle iterator, duckdb_v2_sql_statement_handle *out, duckdb_v2_error_info_handle *err) {
	return duckdb_v2_statement_iterator_next(iterator, out, err);
}
static inline DUCKDB_V2_ERROR capi2_iterator_destroy(duckdb_v2_statement_iterator_handle *iterator) {
	return duckdb_v2_statement_iterator_destroy(iterator);
}
static inline DUCKDB_V2_ERROR capi2_statement_destroy(duckdb_v2_sql_statement_handle *statement) {
	return duckdb_v2_sql_statement_destroy(statement);
}
static inline DUCKDB_V2_ERROR capi2_execute(duckdb_v2_connection_handle conn, duckdb_v2_sql_statement_handle statement, duckdb_v2_result_handle *out, duckdb_v2_error_info_handle *err) {
	return duckdb_v2_statement_execute(conn, statement, NULL, NULL, 0, out, err);
}
static inline DUCKDB_V2_ERROR capi2_result_destroy(duckdb_v2_result_handle *result) {
	return duckdb_v2_result_destroy(result);
}
static inline DUCKDB_V2_ERROR capi2_drain(duckdb_v2_result_handle result, idx_t *changed, duckdb_v2_error_info_handle *err) {
	return duckdb_v2_result_drain(result, changed, err);
}
static inline DUCKDB_V2_ERROR capi2_fetch(duckdb_v2_result_handle result, duckdb_v2_data_chunk_handle *out, duckdb_v2_error_info_handle *err) {
	return duckdb_v2_result_fetch_chunk(result, out, err);
}
static inline DUCKDB_V2_ERROR capi2_chunk_destroy(duckdb_v2_data_chunk_handle *chunk) {
	return duckdb_v2_data_chunk_destroy(chunk);
}
static inline DUCKDB_V2_ERROR capi2_chunk_size(duckdb_v2_data_chunk_handle chunk, idx_t *out, duckdb_v2_error_info_handle *err) {
	return duckdb_v2_data_chunk_get_size(chunk, out, err);
}
static inline DUCKDB_V2_ERROR capi2_chunk_vector(duckdb_v2_data_chunk_handle chunk, idx_t column, duckdb_v2_vector_handle *out, duckdb_v2_error_info_handle *err) {
	return duckdb_v2_data_chunk_get_vector(chunk, column, out, err);
}
static inline DUCKDB_V2_ERROR capi2_vector_value(duckdb_v2_vector_handle vector, idx_t row, duckdb_v2_value_handle *out, duckdb_v2_error_info_handle *err) {
	return duckdb_v2_vector_get_value(vector, row, out, err);
}
static inline DUCKDB_V2_ERROR capi2_value_bigint(duckdb_v2_value_handle value, int64_t *out, duckdb_v2_error_info_handle *err) {
	return duckdb_v2_value_get_bigint(value, out, err);
}
static inline DUCKDB_V2_ERROR capi2_value_destroy(duckdb_v2_value_handle *value) {
	return duckdb_v2_value_destroy(value);
}
static inline DUCKDB_V2_ERROR capi2_error_text(duckdb_v2_error_info_handle info, duckdb_v2_str *out) {
	return duckdb_v2_error_info_get_text(info, out);
}
static inline DUCKDB_V2_ERROR capi2_error_destroy(duckdb_v2_error_info_handle *info) {
	return duckdb_v2_error_info_destroy(info);
}
