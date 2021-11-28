import nimterop/cimport
import nimterop/build
import os

when defined(Linux):
  const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v0.2.9/libduckdb-linux-amd64.zip"

when defined(Windows):
  when defined(cpu64):
    const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v0.2.9/libduckdb-windows-amd64.zip"
  when defined(cpu32):
    const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v0.2.93/libduckdb-windows-i386.zip"

const baseDir = getProjectCacheDir("duckdb") & "/"

static:
  # Uncomment to see output
  cDebug()

  const duckdbZip = lastPathPart(duckdbUrl)
  downloadUrl(duckdbUrl, baseDir)

  const outdir = "."
  extractZip(normalizedPath(baseDir & duckdbzip), outdir)

# Workaround due to nimterop and #include <stdbool.h> not being compatible
# See https://github.com/nimterop/nimterop/issues/260
cPlugin:
  proc onSymbol*(sym: var Symbol) {.exportc, dynlib.} =
    if sym.name == "_Bool": sym.name = "bool"

const duckDbH = normalizedPath(baseDir & "duckdb.h")
const duckDbLib = normalizedPath(baseDir & "libduckdb.so")
cImport(duckDbH, recurse = true, dynlib = duckDbLib)


