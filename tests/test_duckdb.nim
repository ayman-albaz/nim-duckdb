import unittest
import ../src/duckdb


suite "tests":

  test "DuckDB init":
    var dbConn = connect()

  test "Bad result":
    var dbConn = connect()
    expect DuckDBOperationError:
      dbConn.exec("CREATE BAD COMMAND!")

  test "DuckDB exec":
    var dbConn = connect()
    dbConn.exec("CREATE TABLE integers(i INTEGER, j INTEGER);")
    dbConn.exec("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);")

  test "DuckDB exec prepared rows":
    var dbConn = connect()
    dbConn.exec("CREATE TABLE integers(i INTEGER, j INTEGER);")
    dbConn.exec("INSERT INTO integers VALUES (3, 4), (5, 6), (?, NULL);", 7)
    var items: seq[seq[string]]
    for item in dbConn.rows("SELECT * FROM integers"): items.add(item)
    check items == @[@["3", "4"], @["5", "6"], @["7", "NULL"]]

  test "DuckDB exec bad prepared rows":
    var dbConn = connect()
    dbConn.exec("CREATE TABLE integers(i INTEGER, j INTEGER);")
    expect DuckDBOperationError:
      dbConn.exec("INSERT INTO integers VALUES (3, 4), (5, 6), (?, NULL);", "NANA")

  test "DuckDB exec rows prepared":
    var dbConn = connect()
    dbConn.exec("CREATE TABLE integers(i INTEGER, j INTEGER);")
    dbConn.exec("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);")
    var items: seq[seq[string]]
    for item in dbConn.rows("SELECT * FROM integers WHERE i = ? or j = ?", 3, "6"): items.add(item)
    check items == @[@["3", "4"], @["5", "6"]]

  test "DuckDB exec rows bad prepared":
    var dbConn = connect()
    dbConn.exec("CREATE TABLE integers(i INTEGER, j INTEGER);")
    dbConn.exec("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);")
    var items: seq[seq[string]]
    expect DuckDBOperationError:
      for item in dbConn.rows("SELECT * FROM integers WHERE i = ? or j = ?", 3, "NANA"): items.add(item)

  test "DuckDB exec fast insert":
    var dbConn = connect()
    dbConn.exec("CREATE TABLE integers(i INTEGER, j INTEGER);")
    dbConn.exec("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);")
    dbConn.fastInsert(
      "integers",
      @[
        @["11", "NULL"]
      ],
    )
    var items: seq[seq[string]]
    for item in dbConn.rows(
      """SELECT i, j, i + j
      FROM integers"""
      ): items.add(item)

    check items == @[@["3", "4", "7"], @["5", "6", "11"], @["7", "NULL", "NULL"], @["11", "NULL", "NULL"]]

  test "DuckDB exec bad fast insert":
    var dbConn = connect()
    dbConn.exec("CREATE TABLE integers(i INTEGER, j INTEGER);")
    dbConn.exec("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);")
    expect DuckDBOperationError:
      dbConn.fastInsert(
        "integers",
        @[
          @["11", "NANA"]
        ],
      )
