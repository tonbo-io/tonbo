# Tonbo Benchmarks

In order to measure the performance of Tonbo, we selected RocksDB, Sled to compare with.

## Testing environment

```
> uname -r
Linux work 5.15.0-136-generic #147-Ubuntu SMP Sat Mar 15 15:53:30 UTC 2025 x86_64 x86_64 x86_64 GNU/Linux

> cat /etc/os-release
PRETTY_NAME="Ubuntu 22.04.5 LTS"
NAME="Ubuntu"
VERSION_ID="22.04"
VERSION="22.04.5 LTS (Jammy Jellyfish)"
VERSION_CODENAME=jammy
ID=ubuntu
ID_LIKE=debian
HOME_URL="https://www.ubuntu.com/"
SUPPORT_URL="https://help.ubuntu.com/"
BUG_REPORT_URL="https://bugs.launchpad.net/ubuntu/"
PRIVACY_POLICY_URL="https://www.ubuntu.com/legal/terms-and-policies/privacy-policy"
UBUNTU_CODENAME=jammy


> cargo -V
cargo 1.85.1 (d73d2caf9 2024-12-31)
```

## Test Data

The benchmarks use `Customer` record structure with the following fields:

```Rust
#[derive(Record, Debug)]
pub struct Customer {
    #[record(primary_key)]
    pub c_custkey: i32,
    pub c_name: String,
    pub c_address: String,
    pub c_nationkey: i32,
    pub c_phone: String,
    pub c_acctbal: String,
    pub c_mktsegment: String,
    pub c_comment: String,
}
```

## Running the Benchmarks


### Read Benchmarks

In this test we will randomly read ranges of records from [Tonbo](https://github.com/tonbo-io/tonbo) and [RocksDB](https://github.com/facebook/rocksdb). We will compare the read performance with and without projection pushdown. You can run the command below to run the read benchmark:

```bash
# Run read performance benchmarks
cargo bench --bench read_bench --features=bench

# With data loading (requires load_tbl feature)
cargo bench --bench read_bench --features=bench,load_tbl
```

```
+--------------------------------------------+----------+---------+
|                                            | tonbo    | rocksdb |
+==================================================================
| random range reads                         | 324629ms | 17162ms |
|--------------------------------------------+----------+---------+
| random range reads                         | 313270ms | 17131ms |
|--------------------------------------------+----------+---------+
| random range projection reads              | 75861ms  | 17665ms |
|--------------------------------------------+----------+---------+
| random range projection reads              | 81807ms  | 17663ms |
|--------------------------------------------+----------+---------+
| random range projection reads (4 threads)  | 73021ms  | 17632ms |
|--------------------------------------------+----------+---------+
| random range projection reads (8 threads)  | 73732ms  | 17696ms |
|--------------------------------------------+----------+---------+
| random range projection reads (16 threads) | 57672ms  | 16640ms |
|--------------------------------------------+----------+---------+
| random range projection reads (32 threads) | 55181ms  | 16554ms |
+--------------------------------------------+----------+---------+
```

In the case of reading the entire record, tonbo's read performance is not as good as rocksdb. But the read performance of tonbo is significantly improved and has exceeded rocksdb when using projection pushdown.

You can find its source code [here](./read_bench.rs)

### Write Benchmarks

In this test we will randomly write a fixed number of records to [Tonbo](https://github.com/tonbo-io/tonbo) and [RocksDB](https://github.com/facebook/rocksdb). We will compare the write performance of Tonbo and RocksDB. You can run the command below to run the write benchmark:


```bash
cargo bench --bench write_bench --features=bench
```

```
+-------------------------------+--------+---------+----------+
|                               | tonbo  | rocksdb | tonbo_s3 |
+=============================================================+
| bulk load                     | 6311ms | 9558ms  | 6066ms   |
|-------------------------------+--------+---------+----------|
| individual writes             | 8151ms | 10631ms | 8051ms   |
|-------------------------------+--------+---------+----------|
| individual writes (4 threads) | 8020ms | 10634ms | 7975ms   |
|-------------------------------+--------+---------+----------|
| individual writes (8 threads) | 7629ms | 10608ms | 8713ms   |
|-------------------------------+--------+---------+----------|
| batch writes                  | 7729ms | 7057ms  | 9091ms   |
|-------------------------------+--------+---------+----------|
| batch writes (4 threads)      | 7702ms | 7068ms  | 9909ms   |
|-------------------------------+--------+---------+----------|
| batch writes (8 threads)      | 7775ms | 7058ms  | 10641ms  |
|-------------------------------+--------+---------+----------|
| removals                      | 3045ms | 3814ms  | 3484ms   |
+-------------------------------+--------+---------+----------+
```

In the case of writing record one by one, tonbo's write performance is better than rocksdb. But in the case of writing batches, rocksdb's performance is slightly better.

You can find its source code [here](./write_bench.rs)

### Criterion Benchmarks

In this test we will randomly write KV pairs to [Tonbo](https://github.com/tonbo-io/tonbo) and [Sled](https://github.com/spacejam/sled). We will compare the write performance of Tonbo and Sled. You can run the command below to run the write benchmark:

```bash
cargo bench --bench writes --features=sled
```

```
> cargo bench --bench writes --features=sled
write/Tonbo/1           time:   [3.8809 µs 4.7921 µs 5.8901 µs]
Found 10 outliers among 100 measurements (10.00%)
  10 (10.00%) high severe
write/Tonbo/16          time:   [83.793 µs 104.72 µs 129.44 µs]
Found 12 outliers among 100 measurements (12.00%)
  12 (12.00%) high severe
write/Tonbo/128         time:   [678.61 µs 831.29 µs 1.0089 ms]
Found 15 outliers among 100 measurements (15.00%)
  15 (15.00%) high severe
write/Sled/1            time:   [13.365 µs 13.487 µs 13.614 µs]
Found 8 outliers among 100 measurements (8.00%)
  5 (5.00%) low mild
  3 (3.00%) high mild
write/Sled/16           time:   [213.83 µs 217.68 µs 222.32 µs]
Found 4 outliers among 100 measurements (4.00%)
  2 (2.00%) low mild
  1 (1.00%) high mild
  1 (1.00%) high severe
Benchmarking write/Sled/128: Warming up for 3.0000 s
write/Sled/128          time:   [1.7196 ms 1.7411 ms 1.7618 ms]
Found 6 outliers among 100 measurements (6.00%)
  2 (2.00%) low severe
  3 (3.00%) low mild
  1 (1.00%) high mild
```

You can find its source code [here](./criterion/writes.rs)
