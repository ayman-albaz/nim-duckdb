import nimterop/cimport
import nimterop/build
import os

when defined(buildDuckDb):
  const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v0.5.0/libduckdb-src.zip"

else:
  when defined(Linux):
    const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v0.5.0/libduckdb-linux-amd64.zip"

  when defined(macosx):
    const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v0.5.0/libduckdb-osx-amd64.zip"

  when defined(Windows):
    when defined(cpu64):
      const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v0.5.0/libduckdb-windows-amd64.zip"
    when defined(cpu32):
      const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v0.5.0/libduckdb-windows-i386.zip"

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
    else: discard

const duckDbH = normalizedPath(baseDir & "duckdb.h")

when defined(buildDuckDb):
  const duckDbSource = normalizedPath(baseDir & "duckdb.cpp")
  cPassL("-lstdc++ -lpthread")
  cCompile(duckDbSource)
  cImport(duckDbH)

else:
  const duckDbLib = normalizedPath(baseDir & "libduckdb.so")
  cImport(duckDbH, recurse = true, dynlib = duckDbLib)
