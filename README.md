![Linux Build Status (Github Actions)](https://github.com/ayman-albaz/nim-duckdb/actions/workflows/install_and_test.yml/badge.svg) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# nim-duckdb

This library is a DuckDB wrapper in nim. It uses nimterop to generate the C Bindings.


## Supported Functions

Opening/closing a database:
```Nim
import duckdb

# Open database and connection.
# Connection and database closed automatically with destructors
var dbConn = connect("mydb.db")
```

Opening/closing an in-memory database:
```Nim
import duckdb

# Open database and connection
# Connection and database closed automatically with destructors
var dbConn = connect()
```

Executing commands and fetching a prepared statement
```Nim
dbConn.exec("CREATE TABLE integers(i INTEGER, j INTEGER);")
dbConn.exec("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);")
var items: seq[seq[string]]
for item in dbConn.rows("SELECT * FROM integers WHERE i = ? or j = ?", 3, "6"):
  items.add(item)
assert items == @[@["3", "4"], @["5", "6"]]
```

Executing commands and fast inserting. Fast inserting is much faster than inserting in a loop.
```Nim
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
```

Null items are represented by `"NULL"`


## Acknowledgments
Special thanks to [@Clonkk](https://github.com/Clonkk/duckdb_wrapper) as I used his nimterop script to generate the wrapper.


## Contact
I can be reached at aymanalbaz98@gmail.com