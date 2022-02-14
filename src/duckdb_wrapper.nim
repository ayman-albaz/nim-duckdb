import nimterop/cimport
import nimterop/build
import os

when defined(buildDuckDb):
  const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v0.3.1/libduckdb-src.zip"

else:
  when defined(Linux):
    const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v0.3.1/libduckdb-linux-amd64.zip"

  when defined(macosx):
    const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v0.3.1/libduckdb-osx-amd64.zip"

  when defined(Windows):
    when defined(cpu64):
      const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v0.3.1/libduckdb-windows-amd64.zip"
    when defined(cpu32):
      const duckdbUrl = "https://github.com/duckdb/duckdb/releases/download/v0.3.1/libduckdb-windows-i386.zip"

const baseDir = getProjectCacheDir("duckdb") & "/"

static:
  cDebug()

  const duckdbZip = lastPathPart(duckdbUrl)
  downloadUrl(duckdbUrl, baseDir)

  const outdir = "."
  extractZip(normalizedPath(baseDir & duckdbzip), outdir)

cPlugin:
  proc onSymbol*(sym: var Symbol) {.exportc, dynlib.} =
    if sym.name == "_Bool": sym.name = "bool"

const duckDbH = normalizedPath(baseDir & "duckdb.h")

when defined(buildDuckDb):
  const duckDbSource = normalizedPath(baseDir & "duckdb.cpp")
  cPassL("-lstdc++ -lpthread")
  cCompile(duckDbSource)
  cImport(duckDbH)

else:
  const duckDbLib = normalizedPath(baseDir & "libduckdb.so")
  cImport(duckDbH, recurse = true, dynlib = duckDbLib)
