import nimterop/cimport
import nimterop/build
import os

when defined(buildDuckDb):
  const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v0.10.0/libduckdb-src.zip"

else:
  when defined(Linux):
    const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v0.10.0/libduckdb-linux-amd64.zip"
    const duckdbLib = "libduckdb.so"
  when defined(macosx):
    const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v0.10.0/libduckdb-osx-universal.zip"
    const duckdbLib = "libduckdb.dylib"
  when defined(Windows):
    const duckdbLib = "duckdb.dll"
    when defined(cpu64):
      const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v0.10.0/libduckdb-windows-amd64.zip"
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
