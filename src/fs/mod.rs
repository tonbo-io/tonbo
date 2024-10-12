pub mod manager;

use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

use fusio::{fs::OpenOptions, path::Path};
use ulid::{DecodeError, Ulid};

pub(crate) type FileId = Ulid;

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

pub(crate) fn default_open_options(is_append: bool) -> OpenOptions {
    let options = OpenOptions::default().create(true).read(true);
    if is_append {
        options.append(true)
    } else {
        options.write(true)
    }
}

pub(crate) fn parse_file_id(path: &Path, suffix: FileType) -> Result<Option<FileId>, DecodeError> {
    path.filename()
        .map(|file_name| {
            let file_id = file_name
                .strip_suffix(&format!(".{}", suffix))
                .unwrap_or(file_name);
            FileId::from_str(file_id)
        })
        .transpose()
}
