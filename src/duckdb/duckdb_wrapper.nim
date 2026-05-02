import nimterop/cimport
import nimterop/build
import os

when defined(buildDuckDb):
  const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v1.5.2/libduckdb-src.zip"

else:
  when defined(Linux):
    when defined(arm64):
      const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v1.5.2/libduckdb-linux-arm64.zip"
    else:
      const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v1.5.2/libduckdb-linux-amd64.zip"
    const duckdbLib = "libduckdb.so"
  when defined(macosx):
    const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v1.5.2/libduckdb-osx-universal.zip"
    const duckdbLib = "libduckdb.dylib"
  when defined(Windows):
    const duckdbLib = "duckdb.dll"
    when defined(cpu64):
      when defined(arm64):
        const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v1.5.2/libduckdb-windows-arm64.zip"
      else:
        const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v1.5.2/libduckdb-windows-amd64.zip"
    when defined(cpu32):
      const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v0.10.0/libduckdb-windows-i386.zip"

const baseDir = getProjectCacheDir("duckdb") & "/"

static:
  # cDebug()

  const duckdbZip = lastPathPart(duckdbUrl)
  downloadUrl(duckdbUrl, baseDir)

  const outdir = "."
  extractZip(normalizedPath(baseDir & duckdbzip), outdir)

cPlugin:
  proc onSymbol*(sym: var Symbol) {.exportc, dynlib.} =
    case sym.name:
    of "_Bool": sym.name = "bool"
    of "__deprecated_data": sym.name = "deprecated_data"
    of "__deprecated_nullmask": sym.name = "deprecated_nullmask"
    of "__deprecated_type": sym.name = "deprecated_type"
    of "__deprecated_name": sym.name = "deprecated_name"
    of "__deprecated_column_count": sym.name = "deprecated_column_count"
    of "__deprecated_row_count": sym.name = "deprecated_row_count"
    of "__deprecated_rows_changed": sym.name = "deprecated_rows_changed"
    of "__deprecated_columns": sym.name = "deprecated_columns"
    of "__deprecated_error_message": sym.name = "deprecated_error_message"
    of "_duckdb_vector": sym.name = "duckdb_vector"
    of "__vctr": sym.name = "vctr"
    of "_duckdb_database": sym.name = "duckdb_database"
    of "__db": sym.name = "db"
    of "_duckdb_connection": sym.name = "duckdb_connection"
    of "__conn": sym.name = "conn"
    of "_duckdb_prepared_statement": sym.name = "duckdb_prepared_statement"
    of "__prep": sym.name = "prep"
    of "_duckdb_extracted_statements": sym.name = "duckdb_extracted_statements"
    of "__extrac": sym.name = "extrac"
    of "_duckdb_pending_result": sym.name = "duckdb_pending_result"
    of "__pend": sym.name = "pend"
    of "_duckdb_appender": sym.name = "duckdb_appender"
    of "__appn": sym.name = "appn"
    of "_duckdb_config": sym.name = "duckdb_config"
    of "_duckdb_logical_type": sym.name = "duckdb_logical_type"
    of "__cnfg": sym.name = "cnfg"
    of "__lglt": sym.name = "lglt"
    of "_duckdb_data_chunk": sym.name = "duckdb_data_chunk"
    of "__dtck": sym.name = "dtck"
    of "_duckdb_value": sym.name = "duckdb_value"
    of "__val": sym.name = "val"
    of "_duckdb_arrow": sym.name = "duckdb_arrow"
    of "__arrw": sym.name = "arrw"
    of "_duckdb_arrow_stream": sym.name = "duckdb_arrow_stream"
    of "__arrwstr": sym.name = "arrwstr"
    of "_duckdb_arrow_schema": sym.name = "duckdb_arrow_schema"
    of "__arrs": sym.name = "arrs"
    of "_duckdb_arrow_array": sym.name = "duckdb_arrow_array"
    of "__arra": sym.name = "arra"
    # Upgrade the duckdb API from v0.10.0 to v1.5.2
    of "_duckdb_selection_vector": sym.name = "duckdb_selection_vector"
    of "_duckdb_instance_cache": sym.name = "duckdb_instance_cache"
    of "_duckdb_client_context": sym.name = "duckdb_client_context"
    of "_duckdb_table_description": sym.name = "duckdb_table_description"
    of "_duckdb_config_option": sym.name = "duckdb_config_option"
    of "_duckdb_create_type_info": sym.name = "duckdb_create_type_info"
    of "_duckdb_profiling_info": sym.name = "duckdb_profiling_info"
    of "_duckdb_error_data": sym.name = "duckdb_error_data"
    of "_duckdb_expression": sym.name = "duckdb_expression"
    of "_duckdb_extension_info": sym.name = "duckdb_extension_info"
    of "_duckdb_function_info": sym.name = "duckdb_function_info"
    of "_duckdb_bind_info": sym.name = "duckdb_bind_info"
    of "_duckdb_init_info": sym.name = "duckdb_init_info"
    of "_duckdb_scalar_function": sym.name = "duckdb_scalar_function"
    of "_duckdb_scalar_function_set": sym.name = "duckdb_scalar_function_set"
    of "_duckdb_aggregate_function": sym.name = "duckdb_aggregate_function"
    of "_duckdb_aggregate_function_set": sym.name = "duckdb_aggregate_function_set"
    of "_duckdb_aggregate_state": sym.name = "duckdb_aggregate_state"
    of "_duckdb_table_function": sym.name = "duckdb_table_function"
    of "_duckdb_copy_function": sym.name = "duckdb_copy_function"
    of "_duckdb_copy_function_bind_info": sym.name = "duckdb_copy_function_bind_info"
    of "_duckdb_copy_function_global_init_info": sym.name = "duckdb_copy_function_global_init_info"
    of "_duckdb_copy_function_sink_info": sym.name = "duckdb_copy_function_sink_info"
    of "_duckdb_copy_function_finalize_info": sym.name = "duckdb_copy_function_finalize_info"
    of "_duckdb_cast_function": sym.name = "duckdb_cast_function"
    of "_duckdb_replacement_scan_info": sym.name = "duckdb_replacement_scan_info"
    of "_duckdb_arrow_converted_schema": sym.name = "duckdb_arrow_converted_schema"
    of "_duckdb_arrow_options": sym.name = "duckdb_arrow_options"
    of "_duckdb_file_open_options": sym.name = "duckdb_file_open_options"
    of "_duckdb_file_system": sym.name = "duckdb_file_system"
    of "_duckdb_file_handle": sym.name = "duckdb_file_handle"
    of "_duckdb_catalog": sym.name = "duckdb_catalog"
    of "_duckdb_catalog_entry": sym.name = "duckdb_catalog_entry"
    of "_duckdb_log_storage": sym.name = "duckdb_log_storage"
    else: discard

const duckDbH = normalizedPath(baseDir & "duckdb.h")

when defined(buildDuckDb):
  const duckDbSource = normalizedPath(baseDir & "duckdb.cpp")
  cPassL("-lstdc++ -lpthread")
  cCompile(duckDbSource)
  cImport(duckDbH)

else:
  const duckDbLibPath = normalizedPath(baseDir & duckdbLib)
  cImport(duckDbH, recurse = true, dynlib = duckDbLibPath)
