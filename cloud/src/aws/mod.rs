use std::{env, sync::Arc};

use fusio::{
    remotes::aws::{fs::AmazonS3Builder, AwsCredential},
    DynFs,
};
use flume::{Sender, Receiver};
use tonbo::{executor::Executor, record::Record, DB};

use crate::TonboCloud;

pub struct AWSTonbo<R, E>  where R: Record, E: Executor {
    // TODO: Add tonbo DB instance
    tonbo: DB<R, E>, 
    s3_fs: Arc<dyn DynFs>,
}

impl<R, E> TonboCloud<R, E> for AWSTonbo<R, E> where R: Record, E: Executor {
    /// Creates new Tonbo cloud instance on S3
    fn new() {
        let key_id = env::var("AWS_ACCESS_KEY_ID").unwrap();
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap();

        let s3: Arc<dyn DynFs> = Arc::new(
            AmazonS3Builder::new("fusio-test".into())
                .credential(AwsCredential {
                    key_id,
                    secret_key,
                    token: None,
                })
                .region("ap-southeast-1".into())
                .sign_payload(true)
                .build(),
        );
        
        // AWSTonbo {
        //     s3_fs: s3,
        // }
    }
    
    // TODO: Use `DynRecord`
    fn write(&self, records: impl ExactSizeIterator<Item = R>) {
        
    }
    
    fn read() {
        todo!()
    }

    // TODO: Set up API for listening to write/read requests to Tonbo
    fn listen() {
        todo!()
    }

    // This will create the new metadata update that will be sent to S3
    fn update_metadata() {
        todo!()
    }
    
    // Writes SSTable to S3
    fn flush() {
        todo!()
    } 
}