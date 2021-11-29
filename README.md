![Linux Build Status (Github Actions)](https://github.com/ayman-albaz/nim-duckdb/actions/workflows/install_and_test.yml/badge.svg) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# nim-duckdb

This library is a DuckDB wrapper in nim. It uses nimterop to generate the C Bindings.


## Supported Functions

Opening/closing a database:
```Nim
import duckdb

# Open database and connection
var db = openDuckDB("test.duckdb")
var connection = db.connect()


# Close connection and database
connection.disconnect()
db.close()  
```

Opening/closing a database with config:
```Nim
import duckdb

# Open database and connection
var duckDBConfig = DuckDBConfig(accessMode: "READ_WRITE", threads: "8", maxMemory: "8GB", defaultOrder: "DESC")
var db = openDuckDB("test.duckdb", duckDBConfig)
var connection = db.connect()


# Close connection and database
connection.disconnect()
db.close()  

```

Opening/closing an in-memory database:
```Nim
import duckdb

# Open database and connection
var db = openDuckDB()
var connection = db.connect()


# Close connection and database
connection.disconnect()
db.close()  
```

Executing commands and fetching a prepared statement
```Nim
connection.execute("CREATE TABLE integers(i INTEGER, j INTEGER);")
connection.execute("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);")
var items: seq[seq[string]]
for item in connection.fetch("SELECT * FROM integers WHERE i = ? or j = ?", 3, "6"):
  items.add(item)
assert items == @[@["3", "4"], @["5", "6"]]
```

Executing commands and inserting
```Nim
connection.execute("CREATE TABLE integers(i INTEGER, j INTEGER);")
connection.execute("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);")
connection.fastInsert(
  "integers",
  @[
    @["11", ""]
  ],
)
var items: seq[seq[string]]
for item in connection.fetch(
  """SELECT i, j, i + j
  FROM integers"""
  ): items.add(item)

check items == @[@["3", "4", "7"], @["5", "6", "11"], @["7", "", ""], @["11", "", ""]]
```

Null items are represented by `""`. I'm aware this is not always optimal, and PR's are always welcome.


## Acknowledgments
Special thanks to [@Clonkk](https://github.com/Clonkk/duckdb_wrapper) as I used his nimterop script to generate the wrapper.


## Contact
I can be reached at aymanalbaz98@gmail.com