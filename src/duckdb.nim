import duckdb/duckdb_wrapper

type
  DuckDBBaseError* = object of CatchableError
  DuckDBConnectionError* = object of DuckDBBaseError
  DuckDBOperationError* = object of DuckDBBaseError
  DuckDBDatabase* = duckdb_database
  DuckDBConnection* = duckdb_connection
  DuckDBPreparedStatement = duckdb_prepared_statement
  DuckDBResult = duckdb_result
  DuckDBAppender = duckdb_appender
  DuckDBState* = duckdb_state
  DuckDBRow* = seq[string]

template cFree(duckDBResult: DuckDBResult, body: untyped) =
  try: body
  finally: duckdbDestroyResult(duckDBResult.addr)

template cFree(valueVarchar: cstring, body: untyped) =
  try: body
  finally: duckdbFree(valueVarchar)

template cFree(duckDBPreparedStatement: DuckDBPreparedStatement, body: untyped) =
  try: body
  finally: duckdbDestroyPrepare(duckDBPreparedStatement.addr)

proc isStateSuccessful(duckDBState: DuckDBState): bool =
  result = duckDBState == DuckDBSuccess

proc checkStateSuccessful(duckDBState: DuckDBState) =
  if not isStateSuccessful(duckDBState): raise newException(DuckDBConnectionError, "DuckDB operation did not complete sucessfully.")

proc checkStateSuccessful(duckDBState: DuckDBState, duckDBResult: DuckDBResult) =
  let errorMessage = duckDBResult.unsafeAddr.duckdbResultError()
  if not isStateSuccessful(duckDBState): raise newException(DuckDBOperationError, "DuckDB operation did not complete sucessfully. Reason:\n" & $errorMessage)

proc checkStateSuccessful(duckDBState: DuckDBState, duckDBResult: DuckDBPreparedStatement) =
  let errorMessage = duckDBResult.unsafeAddr.duckDBPrepareError()
  if not isStateSuccessful(duckDBState): raise newException(DuckDBOperationError, "DuckDB operation did not complete sucessfully. Reason:\n" & $errorMessage)

proc close*(duckDBDatabase: DuckDBDatabase) =
  ## Closes a duckDB database.
  duckdbClose(duckDBDatabase.unsafeAddr)

proc disconnect*(duckDBConnection: DuckDBConnection) =
  ## Disconnects the connection to a duckDB database.
  duckdbDisconnect(duckDBConnection.unsafeAddr)

proc openDuckDB*(path: string): DuckDBDatabase =
  ## Opens a DuckDB database
  ## `path` is the path of the output DuckDB. Set to ":memory:" to open an in-memory database.
  var duckDBState = duckdbOpen(path.cstring, result.addr)
  checkStateSuccessful(duckDBState)

proc openDuckDB*(): DuckDBDatabase =
  ## Opens an in-memory DuckDB database.
  result = openDuckDB(":memory:")

proc connect*(duckDBDatabase: DuckDBDatabase): DuckDBConnection =
  ## Creates a connection to a duckDB database.
  var duckDBState = duckdbConnect(duckDBDatabase, result.addr)
  checkStateSuccessful(duckDBState)

proc executeWithoutArgs(duckDBConnection: DuckDBConnection, sqlQuery: string) =
  ## Executes a SQL query to a duckDB database.
  var duckDBResult: DuckDBResult
  cFree(duckDBResult):
    var duckDBState = duckdbQuery(duckDBConnection, sqlQuery.cstring, duckDBResult.addr)
    checkStateSuccessful(duckDBState, duckDBResult)

proc executeWithArgs(duckDBConnection: DuckDBConnection, sqlQuery: string, args: varargs[string, `$`]) =
  ## Executes a SQL query to a duckDB database.
  var duckDBState: DuckDBState
  var duckDBPreparedStatement: DuckDBPreparedStatement
  cFree(duckDBPreparedStatement):
    
    # Create prepared statement
    duckDBState = duckdbPrepare(duckDBConnection, sqlQuery.cstring, duckDBPreparedStatement.addr)
    checkStateSuccessful(duckDBState, duckDBPreparedStatement)
    
    # Parse prepared statement
    for i, arg in args:
      duckDBState = duckdbBindVarchar(duckDBPreparedStatement, (i + 1).idx_t, arg.cstring)
      checkStateSuccessful(duckDBState)

    # Result handler
    var duckDBResult: DuckDBResult
    cFree(duckDBResult):

      duckDBState = duckdbExecutePrepared(duckDBPreparedStatement, duckDBResult.addr)
      checkStateSuccessful(duckDBState, duckDBResult)

proc execute*(duckDBConnection: DuckDBConnection, sqlQuery: string, args: varargs[string, `$`]) =
  if args.len() == 0: duckDBConnection.executeWithoutArgs(sqlQuery)
  else: duckDBConnection.executeWithArgs(sqlQuery, args)

iterator getRows(duckDBResult: var DuckDBResult): DuckDBRow =
  var rowCount = duckDBResult.addr.duckdbRowCount()
  var columnCount = duckDBResult.addr.duckdbColumnCount()
  var duckDBRow = newSeq[string](columnCount)
  for idxRow in 0..<rowCount:
    for idxCol in 0..<columnCount:
      var valueVarchar = duckdbValueVarchar(duckDBResult.addr, idxCol, idxRow)
      cFree(valueVarchar):
        duckDBRow[idxCol] = (
          if valueVarchar.isNil():
            "NULL"
          else:
            $valueVarChar
        )
    yield duckDBRow

iterator fetchWithoutArgs(duckDBConnection: DuckDBConnection, sqlQuery: string): DuckDBRow =
  ## Executes a SELECT SQL query to a duckDB database.
  var duckDBResult: DuckDBResult
  cFree(duckDBResult):
    var duckDBState = duckdbQuery(duckDBConnection, sqlQuery.cstring, duckDBResult.addr)
    checkStateSuccessful(duckDBState, duckDBResult)
    for duckDBRow in getRows(duckDBResult):
      yield duckDBRow

iterator fetchWithArgs(duckDBConnection: DuckDBConnection, sqlQuery: string, args: varargs[string, `$`]): DuckDBRow =
  ## Executes a prepared SELECT SQL query to a duckDB database.
  var duckDBState: DuckDBState
  var duckDBPreparedStatement: DuckDBPreparedStatement
  cFree(duckDBPreparedStatement):
    
    # Create prepared statement
    duckDBState = duckdbPrepare(duckDBConnection, sqlQuery.cstring, duckDBPreparedStatement.addr)
    checkStateSuccessful(duckDBState, duckDBPreparedStatement)
    
    # Parse prepared statement
    for i, arg in args:
      duckDBState = duckdbBindVarchar(duckDBPreparedStatement, (i + 1).idx_t, arg.cstring)
      checkStateSuccessful(duckDBState)

    # Result handler
    var duckDBResult: DuckDBResult
    cFree(duckDBResult):

      duckDBState = duckdbExecutePrepared(duckDBPreparedStatement, duckDBResult.addr)
      checkStateSuccessful(duckDBState, duckDBResult)

      for duckDBRow in getRows(duckDBResult):
        yield duckDBRow

iterator fetch*(duckDBConnection: DuckDBConnection, sqlQuery: string, args: varargs[string, `$`]): DuckDBRow =
  if args.len() == 0: 
    for duckDBRow in fetchWithoutArgs(duckDBConnection, sqlQuery):
      yield duckDBRow
  else:
    for duckDBRow in fetchWithArgs(duckDBConnection, sqlQuery, args):
      yield duckDBRow

template cFreeAppender(duckDBAppender: DuckDBAppender, body: untyped) =
  try: body
  finally: discard duckdbAppenderDestroy(duckDBAppender.addr)

proc fastInsert*(duckDBConnection: DuckDBConnection, table: string, ent: seq[DuckDBRow]) =
  var duckDBState: DuckDBState 
  var duckDBAppender: DuckDBAppender
  cFreeAppender(duckDBAppender):
    duckDBState = duckdbAppenderCreate(duckDBConnection, nil, table.cstring, duckDBAppender.addr)
    checkStateSuccessful(duckDBState)
    for row in ent:
      for column in row:
        if column == "NULL": duckDBState = duckdbAppendNull(duckdbAppender)
        else: duckDBState = duckdbAppendVarchar(duckdbAppender, column.cstring)
        checkStateSuccessful(duckDBState)
      duckDBState = duckdbAppenderEndRow(duckDBAppender)
      checkStateSuccessful(duckDBState)
