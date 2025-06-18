#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::{hash::Hasher, ops::Bound};

    use fusio::path::Path;
    use futures_util::StreamExt;
    use tempfile::TempDir;
    use tonbo::{executor::tokio::TokioExecutor, DbOption, Record, DB};

    const WRITE_TIMES: usize = 500_000;
    const STRING_SIZE: usize = 50;

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

    impl Customer {
        pub fn crc_hash(&self, hasher: &mut crc32fast::Hasher) {
            hasher.write_i32(self.c_custkey);
            hasher.write(self.c_name.as_bytes());
            hasher.write(self.c_address.as_bytes());
            hasher.write_i32(self.c_nationkey);
            hasher.write(self.c_phone.as_bytes());
            hasher.write(self.c_acctbal.as_bytes());
            hasher.write(self.c_mktsegment.as_bytes());
            hasher.write(self.c_comment.as_bytes());
        }
    }

    fn gen_string(rng: &mut fastrand::Rng, len: usize) -> String {
        let charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

        let random_string: String = (0..len)
            .map(|_| {
                let idx = rng.usize(0..charset.len());
                charset.chars().nth(idx).unwrap()
            })
            .collect();
        random_string
    }

    fn gen_record(rng: &mut fastrand::Rng, primary_key_count: &mut i32) -> Customer {
        *primary_key_count += 1;
        Customer {
            c_custkey: *primary_key_count,
            c_name: gen_string(rng, STRING_SIZE),
            c_address: gen_string(rng, STRING_SIZE),
            c_nationkey: rng.i32(..),
            c_phone: gen_string(rng, STRING_SIZE),
            c_acctbal: gen_string(rng, STRING_SIZE),
            c_mktsegment: gen_string(rng, STRING_SIZE),
            c_comment: gen_string(rng, STRING_SIZE),
        }
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_data_integrity() {
        let mut rng = fastrand::Rng::with_seed(42);
        let mut primary_key_count = 0;
        let mut write_hasher = crc32fast::Hasher::new();

        let temp_dir = TempDir::new().unwrap();
        let option = DbOption::new(Path::from_filesystem_path(temp_dir.path()).unwrap());

        let db: DB<Customer, TokioExecutor> =
            DB::new(option, TokioExecutor::current(), Customer::schema())
                .await
                .unwrap();

        for _ in 0..WRITE_TIMES {
            let customer = gen_record(&mut rng, &mut primary_key_count);

            customer.crc_hash(&mut write_hasher);
            db.insert(customer).await.unwrap();
        }
        println!("{} items written", WRITE_TIMES);

        let mut read_hasher = crc32fast::Hasher::new();
        let mut read_count = 0;
        let txn = db.transaction().await;

        let mut stream = txn
            .scan((Bound::Unbounded, Bound::Unbounded))
            .take()
            .await
            .unwrap();
        while let Some(result) = stream.next().await {
            let entry = result.unwrap();
            let customer_ref = entry.value().unwrap();

            Customer {
                c_custkey: customer_ref.c_custkey,
                c_name: customer_ref.c_name.unwrap().to_string(),
                c_address: customer_ref.c_address.unwrap().to_string(),
                c_nationkey: customer_ref.c_nationkey.unwrap(),
                c_phone: customer_ref.c_phone.unwrap().to_string(),
                c_acctbal: customer_ref.c_acctbal.unwrap().to_string(),
                c_mktsegment: customer_ref.c_mktsegment.unwrap().to_string(),
                c_comment: customer_ref.c_comment.unwrap().to_string(),
            }
            .crc_hash(&mut read_hasher);
            read_count += 1;
        }
        println!("{} items read", read_count);
        assert_eq!(write_hasher.finish(), read_hasher.finish());
    }
}
