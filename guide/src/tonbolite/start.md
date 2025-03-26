# Getting Started


## Installation

### Prerequisite
To get started using tonbo you should make sure you have [Rust](https://www.rust-lang.org/tools/install) installed on your system. If you haven't alreadly done yet, try following the instructions [here](https://www.rust-lang.org/tools/install).

### Building

To build TonboLite as an extension, you should enable loadable_extension features

```sh
cargo build --release --features loadable_extension
```

Once building successfully, you will get a file named libsqlite_tonbo.dylib(`.dll` on windows, `.so` on most other unixes) in *target/release/*

```bash
target/release/
├── build
├── deps
├── incremental
├── libsqlite_tonbo.d
├── libsqlite_tonbo.dylib
└── libsqlite_tonbo.rlib
```

## Loading TonboLite

SQLite provide [`.load`](https://www.sqlite.org/cli.html#loading_extensions) command to load a SQLite extension. So, you can load TonboLite extension by running the following command:

```bash
.load target/release/libsqlite_tonbo
```

## Creating Table

After loading TonboLite extension successfully, you can [SQLite Virtual Table](https://www.sqlite.org/vtab.html) syntax to create a table:

```sql
CREATE VIRTUAL TABLE temp.tonbo USING tonbo(
    create_sql = 'create table tonbo(id bigint primary key, name varchar, like int)',
    path = 'db_path/tonbo'
);
```
- `create_sql` is a SQL statement that will be executed to create the table.
- `path` is the path to the database file.

## Inserting Data

After creating a table, you can start to insert data into it using the normal `INSERT INTO` statement:

```sql
INSERT INTO tonbo(id, name, like) VALUES(1, 'tonbo', 100);
```

## Querying Data

After inserting data, you can query them by using the `SELECT` statement:

```sql
SELECT * FROM tonbo;

1|tonbo|100
```

## Updating Data

You can update data in the table using the `UPDATE` statement:

```sql
UPDATE tonbo SET like = 123 WHERE id = 1;

SELECT * FROM tonbo;
1|tonbo|123
```

## Deleting Data

You can also delete data by using the `DELETE` statement:

```sql
DELETE FROM tonbo WHERE id = 1;
```

## Coding with extension

TonboLite extension can also be used in any place that supports loading SQLite extensions. Here is an example of using TonboLite extension in Python:

```py
import sqlite3

conn = sqlite3.connect(":memory")
conn.enable_load_extension(True)
# Load the tonbolite extension
conn.load_extension("target/release/libsqlite_tonbo.dylib")
con.enable_load_extension(False)

conn.execute("CREATE VIRTUAL TABLE temp.tonbo USING tonbo("
                "create_sql = 'create table tonbo(id bigint primary key, name varchar, like int)', "
                "path = 'db_path/tonbo'"
             ")")
conn.execute("INSERT INTO tonbo (id, name, like) VALUES (0, 'lol', 1)")
conn.execute("INSERT INTO tonbo (id, name, like) VALUES (1, 'lol', 100)")
rows = conn.execute("SELECT * FROM tonbo;")
for row in rows:
    print(row)
# ......
```
