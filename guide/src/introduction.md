# What is Tonbo?

[Tonbo](https://github.com/tonbo-io/tonbo) is an in-process KV database that can be embedded in data-intensive applications written in Rust, Python, or JavaScript (WebAssembly / Deno). It is designed for analytical processing. Tonbo can efficiently write data in real time in edge environments such as browsers and AWS Lambda, with the data stored in memory, on local disks, or in S3 using Apache Parquet format.

## Build with schema
Building data-intensive applications in Rust using Tonbo is convenient. You just need to declare the dependency in your `Cargo.toml` file and then create the embedded database. Tonbo supports:
```rust
#[derive(tonbo::Record)]
pub struct User {
    #[record(primary_key)]
    name: String,
    email: Option<String>,
    age: u8,
}

async fn main() {
    let db = tonbo::DB::new("./db_path/users".into(), TokioExecutor::default())
        .await
        .unwrap();
}
```

## All in Parquet

Tonbo organizes all stored data as Apache Parquet files. At each level, these files can reside in memory, on disk, or in S3. This design lets users process their data without any vendor lock-in, including with Tonbo.

```
			╔═tonbo═════════════════════════════════════════════════════╗
			║                                                           ║
			║    ┌──────╂─client storage─┐  ┌──────╂─client storage─┐   ║
			║    │ ┏━━━━▼━━━━┓           │  │ ┏━━━━▼━━━━┓           │   ║
			║    │ ┃ parquet ┃           │  │ ┃ parquet ┃           │   ║
			║    │ ┗━━━━┳━━━━┛           │  │ ┗━━━━┳━━━━┛           │   ║
			║    └──────╂────────────────┘  └──────╂────────────────┘   ║
			║           ┣━━━━━━━━━━━━━━━━━━━━━━━━━━┛                    ║
			║    ┌──────╂────────────────────────────────server ssd─┐   ║
			║    │      ┣━━━━━━━━━━━┓                               │   ║
			║    │ ┏━━━━▼━━━━┓ ┏━━━━▼━━━━┓                          │   ║
			║    │ ┃ parquet ┃ ┃ parquet ┃                          │   ║
			║    │ ┗━━━━┳━━━━┛ ┗━━━━┳━━━━┛                          │   ║
			║    └──────╂───────────╂───────────────────────────────┘   ║
			║    ┌──────╂───────────╂────────object storage service─┐   ║
			║    │      ┣━━━━━━━━━━━╋━━━━━━━━━━━┳━━━━━━━━━━━┓       │   ║
			║    │ ┏━━━━▼━━━━┓ ┏━━━━▼━━━━┓ ┏━━━━▼━━━━┓ ┏━━━━▼━━━━┓  │   ║
			║    │ ┃ parquet ┃ ┃ parquet ┃ ┃ parquet ┃ ┃ parquet ┃  │   ║
			║    │ ┗━━━━━━━━━┛ ┗━━━━━━━━━┛ ┗━━━━━━━━━┛ ┗━━━━━━━━━┛  │   ║
			║    └──────────────────────────────────────────────────┘   ║
			║                                                           ║
			╚═══════════════════════════════════════════════════════════╝
```

## Easy to be integrated
Compared to other analytical databases, Tonbo is extremely lightweight—only 1.3MB when compressed. In addition to being embedded directly as a KV database within applications, Tonbo can also serve as an analytical enhancement for existing OLTP databases.

For example, [Tonbolite](https://github.com/tonbo-io/tonbolite) is a SQLite plugin built on Tonbo that provides SQLite with highly compressed, analytical-ready tables using Arrow/Parquet to boost query efficiency. Moreover, it can run alongside SQLite in various environments such as browsers and Linux:
```
sqlite> .load target/release/libsqlite_tonbo

sqlite> CREATE VIRTUAL TABLE temp.tonbo USING tonbo(
    create_sql = 'create table tonbo(id bigint primary key, name varchar, like int)',
    path = 'db_path/tonbo'
);

sqlite> insert into tonbo (id, name, like) values (0, 'tonbo', 100);

sqlite> select * from tonbo;
0|tonbo|100
```

We are committed to providing the most convenient and efficient real-time analytical database for edge-first scenarios. In addition to Tonbolite, we will offer the following based on Tonbo:
1. Time-series data writing and querying for observability and other scenarios.
2. Real-time index building and search based on BM25 or vectors.

We are passionate about establishing Tonbo as an open-source, community-contributed project and are dedicated to building a community around it to develop features for all use cases.
