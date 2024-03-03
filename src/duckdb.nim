import duckdb/duckdb_wrapper

type
  DuckDBBaseError* = object of CatchableError
  DuckDBOperationError* = object of DuckDBBaseError
  DuckDBRow* = seq[string]
  DuckDBState* = duckdb_state
  # User defined objects
  DuckDBConn* = object
    database: duckdbDatabase
    connection: duckdbConnection
  DuckDBResult* = object
    result: duckdbResult
  DuckDBPreparedStatement = object
    statement: duckdbPreparedStatement
  DuckDBValueVarchar = object
    varchar: cstring
  DuckDBAppender = object
    appender: duckdbAppender

proc close*(conn: DuckDBConn) =
  ## Closes a duckDB database.
  duckdbClose(conn.database.addr)

proc disconnect*(conn: DuckDBConn) =
  ## Disconnects the connection to a duckDB database.
  duckdbDisconnect(conn.connection.addr)

proc `=destroy`(conn: var DuckDBConn) =
  if not isNil(conn.connection.addr):
    conn.disconnect()
  if not isNil(conn.database.addr):
    conn.close()

proc `=destroy`(result: var DuckDBResult) =
  if not isNil(result.result.addr):
    duckdbDestroyResult(result.result.addr)

proc `=destroy`(statement: var DuckDBPreparedStatement) =
  if not isNil(statement.statement.addr):
    duckdbDestroyPrepare(statement.statement.addr)

proc `=destroy`(varchar: var DuckDBValueVarchar) =
  if not isNil(varchar.varchar):
    duckdbFree(varchar.varchar)

proc `=destroy`(appender: var DuckDBAppender) =
  if not isNil(appender.appender.addr):
    discard duckdbAppenderDestroy(appender.appender.addr)

proc isStateSuccessful(state: DuckDBState): bool =
  result = state == DuckDBSuccess

proc checkStateSuccessful(state: DuckDBState) =
  if not isStateSuccessful(state):
    raise newException(DuckDBOperationError, "DuckDB operation did not complete sucessfully.")

proc checkStateSuccessful(state: DuckDBState, result: DuckDBResult) =
  if not isStateSuccessful(state):
    let errorMessage = result.result.addr.duckdbResultError()
    raise newException(DuckDBOperationError,
        "DuckDB operation did not complete sucessfully. Reason:\n" & $errorMessage)

proc checkStateSuccessful(state: DuckDBState,
    statement: DuckDBPreparedStatement) =
  if not isStateSuccessful(state):
    let errorMessage = statement.statement.duckdbPrepareError()
    raise newException(DuckDBOperationError,
        "DuckDB operation did not complete sucessfully. Reason:\n" & $errorMessage)

proc checkStateSuccessful(state: DuckDBState, appender: DuckDBAppender) =
  if not isStateSuccessful(state):
    let errorMessage = appender.appender.duckdbAppenderError()
    raise newException(DuckDBOperationError,
        "DuckDB operation did not complete sucessfully. Reason:\n" & $errorMessage)

proc connect*(path: string): DuckDBConn =
  ## Opens a DuckDB database
  ## `path` is the path of the output DuckDB. Set to ":memory:" to open an in-memory database.
  var state1 = duckdbOpen(path.cstring, result.database.addr)
  checkStateSuccessful(state1)
  var state2 = duckdbConnect(result.database, result.connection.addr)
  checkStateSuccessful(state2)

proc connect*(): DuckDBConn =
  ## Opens an in-memory DuckDB database.
  result = connect(":memory:")

proc execWithoutArgs(conn: DuckDBConn, sqlQuery: string) =
  ## Executes a SQL query to a duckDB database.
  var result = DuckDBResult()
  var state = duckdbQuery(conn.connection, sqlQuery.cstring, result.result.addr)
  checkStateSuccessful(state, result)

proc execWithArgs(conn: DuckDBConn, sqlQuery: string, args: varargs[string, `$`]) =
  ## Executes a SQL query to a duckDB database.
  var statement = DuckDBPreparedStatement()

  # Create prepared statement
  let state1 = duckdbPrepare(conn.connection, sqlQuery.cstring,
      statement.statement.addr)
  checkStateSuccessful(state1, statement)

  # Parse prepared statement
  for i, arg in args:
    let state2 = duckdbBindVarchar(statement.statement, (i + 1).idx_t, arg.cstring)
    checkStateSuccessful(state2)

  # Result handler
  let result = DuckDBResult()
  let state3 = duckdbExecutePrepared(statement.statement,
      result.result.addr)
  checkStateSuccessful(state3, result)

proc exec*(conn: DuckDBConn, sqlQuery: string, args: varargs[string, `$`]) =
  if args.len() == 0: conn.execWithoutArgs(sqlQuery)
  else: conn.execWithArgs(sqlQuery, args)

iterator getRows(result: DuckDBResult): DuckDBRow =
  var rowCount = result.result.addr.duckdbRowCount()
  var columnCount = result.result.addr.duckdbColumnCount()
  var duckDBRow = newSeq[string](columnCount)
  for idxRow in 0..<rowCount:
    for idxCol in 0..<columnCount:
      var varchar = DuckDBValueVarchar(varchar: duckdbValueVarchar(
          result.result.addr, idxCol, idxRow))
      duckDBRow[idxCol] = (
        if varchar.varchar.isNil():
        "NULL"
      else:
        $varchar.varchar
      )
    yield duckDBRow

iterator rowsWithoutArgs(conn: DuckDBConn, sqlQuery: string): DuckDBRow =
  ## Executes a SELECT SQL query to a duckDB database.

  var result = DuckDBResult()
  var state = duckdbQuery(conn.connection, sqlQuery.cstring, result.result.addr)
  checkStateSuccessful(state, result)
  for duckDBRow in getRows(result):
    yield duckDBRow

iterator rowsWithArgs(conn: DuckDBConn, sqlQuery: string, args: varargs[string,
    `$`]): DuckDBRow =
  ## Executes a prepared SELECT SQL query to a duckDB database.
  var statement = DuckDBPreparedStatement()

  # Create prepared statement
  let state1 = duckdbPrepare(conn.connection, sqlQuery.cstring,
      statement.statement.addr)
  checkStateSuccessful(state1, statement)

  # Parse prepared statement
  for i, arg in args:
    let state2 = duckdbBindVarchar(statement.statement, (i + 1).idx_t, arg.cstring)
    checkStateSuccessful(state2)

  # Result handler
  let result = DuckDBResult()
  let state3 = duckdbExecutePrepared(statement.statement,
      result.result.addr)
  checkStateSuccessful(state3, result)
  for duckDBRow in getRows(result):
    yield duckDBRow

iterator rows*(conn: DuckDBConn, sqlQuery: string, args: varargs[string,
    `$`]): DuckDBRow =
  if args.len() == 0:
    for duckDBRow in rowsWithoutArgs(conn, sqlQuery):
      yield duckDBRow
  else:
    for duckDBRow in rowsWithArgs(conn, sqlQuery, args):
      yield duckDBRow

proc fastInsert*(conn: DuckDBConn, table: string, ent: seq[DuckDBRow]) =
  var appender = DuckDBAppender()
  let state1 = duckdbAppenderCreate(conn.connection, nil, table.cstring,
      appender.appender.addr)
  checkStateSuccessful(state1, appender)
  for row in ent:
    for column in row:
      let state2 = (
        if column == "NULL": duckdbAppendNull(appender.appender)
        else: duckdbAppendVarchar(appender.appender, column.cstring)
      )
      checkStateSuccessful(state2)
    let state3 = duckdbAppenderEndRow(appender.appender)
    checkStateSuccessful(state3)
