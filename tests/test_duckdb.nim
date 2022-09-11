import unittest
import ../src/duckdb


suite "tests":

  test "DuckDB init":
    var db = openDuckDB("test.duckdb")
    var connection = db.connect()

  test "DuckDB execute fetch":
    var db = openDuckDB()
    var connection = db.connect()
    connection.execute("CREATE TABLE integers(i INTEGER, j INTEGER);")
    connection.execute("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);")
    var items: seq[seq[string]]
    for item in connection.fetch("SELECT * FROM integers"): items.add(item)
    check items == @[@["3", "4"], @["5", "6"], @["7", "NULL"]]

  test "DuckDB execute fetch prepared":
    var db = openDuckDB()
    var connection = db.connect()
    connection.execute("CREATE TABLE integers(i INTEGER, j INTEGER);")
    connection.execute("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);")
    var items: seq[seq[string]]
    for item in connection.fetch("SELECT * FROM integers WHERE i = ? or j = ?", 3, "6"): items.add(item)
    check items == @[@["3", "4"], @["5", "6"]]

  test "DuckDB execute fast insert":
    var db = openDuckDB()
    var connection = db.connect()
    connection.execute("CREATE TABLE integers(i INTEGER, j INTEGER);")
    connection.execute("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);")
    connection.fastInsert(
      "integers",
      @[
        @["11", "NULL"]
      ],

    )
    var items: seq[seq[string]]
    for item in connection.fetch(
      """SELECT i, j, i + j
      FROM integers"""
      ): items.add(item)

    check items == @[@["3", "4", "7"], @["5", "6", "11"], @["7", "NULL", "NULL"], @["11", "NULL", "NULL"]]

  teardown:
    connection.disconnect()
    db.close()