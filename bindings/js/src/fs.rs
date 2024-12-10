use fusio::path::Path;
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};

#[wasm_bindgen]
#[derive(Debug, Clone)]
pub struct AwsCredential {
    #[wasm_bindgen(skip)]
    pub key_id: String,
    #[wasm_bindgen(skip)]
    pub secret_key: String,
    #[wasm_bindgen(skip)]
    pub token: Option<String>,
}

impl From<AwsCredential> for fusio::remotes::aws::AwsCredential {
    fn from(cred: AwsCredential) -> Self {
        fusio::remotes::aws::AwsCredential {
            key_id: cred.key_id,
            secret_key: cred.secret_key,
            token: cred.token,
        }
    }
}

#[wasm_bindgen]
impl AwsCredential {
    #[wasm_bindgen(constructor)]
    pub fn new(key_id: String, secret_key: String, token: Option<String>) -> Self {
        Self {
            key_id,
            secret_key,
            token,
        }
    }
}

#[wasm_bindgen]
#[derive(Debug, Clone)]
pub struct FsOptions {
    inner: FsOptionsInner,
}

impl FsOptions {
    pub(crate) fn path(&self, path: String) -> Result<Path, JsValue> {
        match self.inner {
            FsOptionsInner::Local => {
                Path::from_opfs_path(&path).map_err(|err| JsValue::from(err.to_string()))
            }
            FsOptionsInner::S3 { .. } => {
                Path::from_url_path(&path).map_err(|err| JsValue::from(err.to_string()))
            }
        }
    }
}

#[derive(Debug, Clone)]
enum FsOptionsInner {
    Local,
    S3 {
        bucket: String,
        credential: Option<AwsCredential>,
        region: Option<String>,
        sign_payload: Option<bool>,
        checksum: Option<bool>,
        endpoint: Option<String>,
    },
}

#[wasm_bindgen]
pub struct S3Builder {
    bucket: String,
    credential: Option<AwsCredential>,
    region: Option<String>,
    sign_payload: Option<bool>,
    checksum: Option<bool>,
    endpoint: Option<String>,
}

#[wasm_bindgen]
impl S3Builder {
    #[wasm_bindgen(constructor)]
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            credential: None,
            region: None,
            sign_payload: None,
            checksum: None,
            endpoint: None,
        }
    }

    pub fn credential(self, credential: AwsCredential) -> Self {
        Self {
            credential: Some(credential),
            ..self
        }
    }

    pub fn region(self, region: String) -> Self {
        Self {
            region: Some(region),
            ..self
        }
    }

    pub fn sign_payload(self, sign_payload: bool) -> Self {
        Self {
            sign_payload: Some(sign_payload),
            ..self
        }
    }

    pub fn checksum(self, checksum: bool) -> Self {
        Self {
            checksum: Some(checksum),
            ..self
        }
    }

    pub fn endpoint(self, endpoint: String) -> Self {
        Self {
            endpoint: Some(endpoint),
            ..self
        }
    }

    pub fn build(self) -> FsOptions {
        let S3Builder {
            bucket,
            credential,
            region,
            sign_payload,
            checksum,
            endpoint,
        } = self;

        FsOptions {
            inner: FsOptionsInner::S3 {
                bucket,
                credential,
                region,
                sign_payload,
                checksum,
                endpoint,
            },
        }
    }
}

#[wasm_bindgen]
impl FsOptions {
    pub fn local() -> Self {
        Self {
            inner: FsOptionsInner::Local,
        }
    }
}

impl FsOptions {
    pub(crate) fn into_fs_options(self) -> fusio_dispatch::FsOptions {
        match self.inner {
            FsOptionsInner::Local => fusio_dispatch::FsOptions::Local,
            FsOptionsInner::S3 {
                bucket,
                credential,
                region,
                sign_payload,
                checksum,
                endpoint,
            } => fusio_dispatch::FsOptions::S3 {
                bucket,
                credential: credential.map(fusio::remotes::aws::AwsCredential::from),
                endpoint,
                region,
                sign_payload,
                checksum,
            },
        }
    }
}
