pub(crate) mod manager;

use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

use fusio::{fs::OpenOptions, path::Path};
use once_cell::sync::OnceCell;
use ulid::{DecodeError, Ulid};

pub type FileId = Ulid;

static GENERATOR: OnceCell<std::sync::Mutex<ulid::Generator>> = OnceCell::new();

#[inline]
pub fn generate_file_id() -> FileId {
    // init
    let m = GENERATOR.get_or_init(|| std::sync::Mutex::new(ulid::Generator::new()));
    let mut guard = m
        .lock()
        .expect("global file id generator lock should not fail");

    guard.generate().expect("generator should not fail")
}

pub enum FileType {
    Wal,
    Parquet,
    Log,
}

impl Display for FileType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FileType::Wal => write!(f, "wal"),
            FileType::Parquet => write!(f, "parquet"),
            FileType::Log => write!(f, "log"),
        }
    }
}

impl FileType {
    pub(crate) fn open_options(&self, only_read: bool) -> OpenOptions {
        match self {
            FileType::Wal | FileType::Log => OpenOptions::default().create(true).read(true),
            FileType::Parquet => {
                if only_read {
                    OpenOptions::default().read(true)
                } else {
                    OpenOptions::default()
                        .create(true)
                        .write(true)
                        .truncate(true)
                }
            }
        }
    }
}

pub(crate) fn parse_file_id(path: &Path, suffix: FileType) -> Result<Option<FileId>, DecodeError> {
    path.filename()
        .map(|file_name| {
            let file_id = file_name
                .strip_suffix(&format!(".{suffix}"))
                .unwrap_or(file_name);
            FileId::from_str(file_id)
        })
        .transpose()
}
