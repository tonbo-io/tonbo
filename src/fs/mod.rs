pub(crate) mod manager;

use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

use fusio::{fs::OpenOptions, path::Path};
use once_cell::sync::OnceCell;
use ulid::{DecodeError, Ulid};

/// Identifier used for files.
pub type FileId = Ulid;

/// Process-global ULID generator (lazily initialized).
static GENERATOR: OnceCell<std::sync::Mutex<ulid::Generator>> = OnceCell::new();

/// Generate a new [`FileId`] using a process-global ULID generator.
///
/// This function is cheap, thread-safe, and yields lexicographically
/// time-ordered identifiers when formatted as strings.
#[inline]
pub fn generate_file_id() -> FileId {
    // init
    let m = GENERATOR.get_or_init(|| std::sync::Mutex::new(ulid::Generator::new()));
    let mut guard = m
        .lock()
        .expect("global file id generator lock should not fail");

    guard.generate().expect("generator should not fail")
}

/// Logical file categories with known on-disk conventions.
pub enum FileType {
    /// Write-ahead log file.
    Wal,
    /// Parquet data file.
    Parquet,
    /// Human-oriented or engine diagnostic log file.
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
    /// Construct appropriate [`OpenOptions`] for this file type.
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

/// Try to parse a [`FileId`] from the final path component, assuming a fixed suffix.
///
/// ### Examples
/// ```no_run
/// use fusio::path::Path;
/// use tonbo::storage::{FileType, parse_file_id};
///
/// let path = Path::from("/data/01J8QW8X2NV9K1T8Q3J8S2F3YZ.parquet");
/// let id = parse_file_id(&path, FileType::Parquet).unwrap();
/// assert!(id.is_some());
///
/// let wrong_suffix = Path::from("/data/01J8QW8X2NV9K1T8Q3J8S2F3YZ.wal");
/// assert_eq!(parse_file_id(&wrong_suffix, FileType::Parquet).unwrap(), None);
/// ```
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