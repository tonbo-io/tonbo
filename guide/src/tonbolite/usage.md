# Usage
<!-- toc -->
## Using as Extension

If you do not know how to build TonboLite, please refer to the [Building](./build.md) section.

### Loading TonboLite Extension

Once building successfully, you will get a file named libsqlite_tonbo.dylib(.dll on windows, .so on most other unixes) in *target/release/*(or *target/debug/*).

SQLite provide [`.load`](https://www.sqlite.org/cli.html#loading_extensions) command to load a SQLite extension. So, you can load TonboLite extension by running the following command:

```bash
.load target/release/libsqlite_tonbo
```

Or you can load TonboLite extension in Python or other languages.
```py
import sqlite3

conn = sqlite3.connect(":memory")
conn.enable_load_extension(True)
# Load the tonbolite extension
conn.load_extension("target/release/libsqlite_tonbo.dylib")
con.enable_load_extension(False)

# ......
```


After loading TonboLite successfully, you can start to use it.

### Create Table

Unlike Normal `CREATE TABLE` statement, TonboLite use [SQLite Virtual Table](https://www.sqlite.org/vtab.html) syntax to create a table:

```sql
CREATE VIRTUAL TABLE temp.tonbo USING tonbo(
    create_sql = 'create table tonbo(id bigint primary key, name varchar, like int)',
    path = 'db_path/tonbo'
);
```

### Select/Insert/Update/Delete

you can execute SQL statements just like normal SQL in the SQLite. Here is an example:

```sql
sqlite> .load target/release/libsqlite_tonbo

sqlite> CREATE VIRTUAL TABLE temp.tonbo USING tonbo(
    create_sql = 'create table tonbo(id bigint primary key, name varchar, like int)',
    path = 'db_path/tonbo'
);
sqlite> insert into tonbo (id, name, like) values (0, 'tonbo', 100);
sqlite> insert into tonbo (id, name, like) values (1, 'sqlite', 200);

sqlite> select * from tonbo;
0|tonbo|100
1|sqlite|200

sqlite> update tonbo set like = 123 where id = 0;

sqlite> select * from tonbo;
0|tonbo|123
1|sqlite|200

sqlite> delete from tonbo where id = 0;

sqlite> select * from tonbo;
1|sqlite|200
```

### Flush

TonboLite use LSM tree to store data, and it use a WAL buffer size to improve performance, so you may need to flush data to disk manually. But SQLite don't provide flush interface, so we choose to implement it in the [`pragma quick_check`](https://www.sqlite.org/pragma.html#pragma_quick_check).

```sql
PRAGMA tonbo.quick_check;
```

## Using in Rust

To use TonboLite in your application, you can import TonboLite in the *Cargo.toml* file.

```toml
tonbolite = { git = "https://github.com/tonbo-io/tonbolite" }
```

You can create use TonboLite just like in [Rusqlite](https://github.com/rusqlite/rusqlite), but you should create table using [SQLite Virtual Table](https://www.sqlite.org/vtab.html) syntax:

```rust
let _ = std::fs::create_dir_all("./db_path/test");

let db = rusqlite::Connection::open_in_memory()?;
crate::load_module(&db)?;

db.execute_batch(
    "CREATE VIRTUAL TABLE temp.tonbo USING tonbo(
            create_sql = 'create table tonbo(id bigint primary key, name varchar, like int)',
            path = 'db_path/test'
    );"
).unwrap();

db.execute(
    "INSERT INTO tonbo (id, name, like) VALUES (1, 'lol', 12)",
    [],
).unwrap();

let mut stmt = db.prepare("SELECT * FROM tonbo;")?;
let _rows = stmt.query([])?;
```
for more usage, you can refer to [Rusqlite](https://docs.rs/rusqlite).

One difference is that TonboLite extends [`pragma quick_check`](https://www.sqlite.org/pragma.html#pragma_quick_check) to flush WAL to disk. You can use it like this:

```rust
db.pragma(None, "quick_check", "tonbo", |_r| -> rusqlite::Result<()> {
    Ok(())
}).unwrap();
```

## Using in JavaScript

To use TonboLite in wasm, can should enable *wasm* feature.
```toml
tonbolite = { git = "https://github.com/tonbo-io/tonbolite", default-features = false, features = ["wasm"] }
```
After building successfully, you will get a *pkg* folder containing compiled js and wasm files. Copy it to your project and then you can start to use it. If you don't know how to build TonboLite on wasm, you can refer to [TonboLite](build.md#build-on-wasm).

Here is an example of how to use TonboLite in JavaScript:

```javascript
const tonbo = await import("./pkg/sqlite_tonbo.js");
await tonbo.default();

const db = new TonboLite('db_path/test');
await db.create(`CREATE VIRTUAL TABLE temp.tonbo USING tonbo(
  create_sql ='create table tonbo(id bigint primary key, name varchar, like int)',
  path = 'db_path/tonbo'
);`);

await db.insert('INSERT INTO tonbo (id, name, like) VALUES (1, \'lol\', 12)');
await conn.delete("DELETE FROM tonbo WHERE id = 4");
await conn.update("UPDATE tonbo SET name = 'tonbo' WHERE id = 6");

const rows = await db.select('SELECT * FROM tonbo limit 10;');
console.log(rows);

await db.flush();
```

<div class="warning">

TonboLite should be used in a [secure context](https://developer.mozilla.org/en-US/docs/Web/Security/Secure_Contexts) and [cross-origin isolated](https://developer.mozilla.org/en-US/docs/Web/API/Window/crossOriginIsolated), since it uses [`SharedArrayBuffer`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/SharedArrayBuffer) to share memory. Please refer to [this article](https://web.dev/articles/coop-coep) for a detailed explanation.

</div>
