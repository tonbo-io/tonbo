use std::ops::Bound;

use common::{
    util::{decode_value, value_le, value_lt},
    Keys,
};
use fusio::{SeqRead, Write};
use fusio_log::{Decode, Encode};
use ulid::Ulid;

pub type FileId = Ulid;

#[derive(Debug, Eq)]
pub(crate) struct Scope {
    pub(crate) min: Keys,
    pub(crate) max: Keys,
    pub(crate) gen: FileId,
    pub(crate) wal_ids: Option<Vec<FileId>>,
}

impl Scope {
    pub fn new(min: Keys, max: Keys, gen: FileId, wal_ids: Option<Vec<FileId>>) -> Self {
        Scope {
            min,
            max,
            gen,
            wal_ids,
        }
    }
}
impl PartialEq for Scope {
    fn eq(&self, other: &Self) -> bool {
        self.gen.eq(&other.gen())
            && self.wal_ids.eq(&other.wal_ids)
            && self.min.eq(&other.min)
            && self.max.eq(&other.max)
    }
}

impl Clone for Scope {
    fn clone(&self) -> Self {
        Scope {
            min: self.min.clone(),
            max: self.max.clone(),
            gen: self.gen,
            wal_ids: self.wal_ids.clone(),
        }
    }
}

impl Scope {
    pub(crate) fn contains(&self, key: &Keys) -> bool {
        value_le(&self.min, key) && value_le(key, &self.max)
    }

    #[allow(unused)]
    pub(crate) fn meets(&self, target: &Self) -> bool {
        self.contains(target.min.as_ref()) || self.contains(target.max.as_ref())
    }

    pub(crate) fn meets_range(&self, range: (Bound<&Keys>, Bound<&Keys>)) -> bool {
        let excluded_contains =
            |key| -> bool { value_lt(&self.min, key) && value_lt(&key, &self.max) };
        let included_by =
            |min, max| -> bool { value_le(min, &self.min) && value_le(&self.max, max) };

        match (range.0, range.1) {
            (Bound::Included(start), Bound::Included(end)) => {
                self.contains(start) || self.contains(end) || included_by(start, end)
            }
            (Bound::Included(start), Bound::Excluded(end)) => {
                start != end
                    && (self.contains(start) || excluded_contains(end) || included_by(start, end))
            }
            (Bound::Excluded(start), Bound::Included(end)) => {
                start != end
                    && (excluded_contains(start) || self.contains(end) || included_by(start, end))
            }
            (Bound::Excluded(start), Bound::Excluded(end)) => {
                start != end
                    && (excluded_contains(start)
                        || excluded_contains(end)
                        || included_by(start, end))
            }
            (Bound::Included(start), Bound::Unbounded) => start <= self.max.as_ref(),
            (Bound::Excluded(start), Bound::Unbounded) => start < self.max.as_ref(),
            (Bound::Unbounded, Bound::Included(end)) => end >= self.min.as_ref(),
            (Bound::Unbounded, Bound::Excluded(end)) => end > self.min.as_ref(),
            (Bound::Unbounded, Bound::Unbounded) => true,
        }
    }

    pub(crate) fn gen(&self) -> FileId {
        self.gen
    }
}

impl Encode for Scope {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: Write,
    {
        let len = self.min.len();
        (len as u32).encode(writer).await?;
        for min in self.min.iter() {
            min.encode(writer).await?;
        }
        for max in self.max.iter() {
            max.encode(writer).await?;
        }

        let (result, _) = writer.write_all(&self.gen.to_bytes()[..]).await;
        result?;

        match &self.wal_ids {
            None => {
                0u8.encode(writer).await?;
            }
            Some(ids) => {
                1u8.encode(writer).await?;
                (ids.len() as u32).encode(writer).await?;
                for id in ids {
                    let (result, _) = writer.write_all(&id.to_bytes()[..]).await;
                    result?;
                }
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        // ProcessUniqueId: usize + u64
        self.min.iter().fold(16, |acc, min| acc + min.size())
            + self.min.iter().fold(16, |acc, min| acc + min.size())
    }
}

impl Decode for Scope {
    async fn decode<R: SeqRead>(reader: &mut R) -> Result<Self, fusio::Error> {
        let mut buf = [0u8; 16];

        let len = u32::decode(reader).await? as usize;
        let mut min = Vec::with_capacity(len);
        let mut max = Vec::with_capacity(len);
        for _ in 0..len {
            min.push(decode_value(reader).await?);
        }
        for _ in 0..len {
            max.push(decode_value(reader).await?);
        }

        let gen = {
            let (result, _) = reader.read_exact(buf.as_mut_slice()).await;
            result?;
            FileId::from_bytes(buf)
        };
        let wal_ids = match u8::decode(reader).await? {
            0 => None,
            1 => {
                let len = u32::decode(reader).await? as usize;
                let mut ids = Vec::with_capacity(len);

                for _ in 0..len {
                    let (result, _) = reader.read_exact(buf.as_mut_slice()).await;
                    result?;
                    ids.push(FileId::from_bytes(buf));
                }
                Some(ids)
            }
            _ => unreachable!(),
        };

        Ok(Scope {
            min,
            max,
            gen,
            wal_ids,
        })
    }
}

#[cfg(test)]
mod test {
    use std::{
        io::{Cursor, SeekFrom},
        ops::Bound,
        sync::Arc,
    };

    use fusio_log::{Decode, Encode};
    use tokio::io::AsyncSeekExt;

    use super::Scope;
    use crate::fs::generate_file_id;

    #[tokio::test]
    async fn test_encode_decode_scope() {
        let scope = Scope {
            min: vec![Arc::new(100)],
            max: vec![Arc::new(200)],
            gen: generate_file_id(),
            wal_ids: None,
        };
        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);
        scope.encode(&mut cursor).await.unwrap();

        cursor.seek(SeekFrom::Start(0)).await.unwrap();
        let decoded = Scope::decode(&mut cursor).await.unwrap();
        assert_eq!(scope, decoded);
    }

    #[tokio::test]
    async fn test_meets_range() {
        let gen = ulid::Ulid::new();
        let scope = Scope {
            min: vec![Arc::new(100)],
            max: vec![Arc::new(200)],
            gen,
            wal_ids: None,
        };

        // test out of range
        {
            assert!(!scope.meets_range((Bound::Unbounded, Bound::Excluded(&vec![Arc::new(100)]))));
            assert!(!scope.meets_range((Bound::Unbounded, Bound::Included(&vec![Arc::new(99)]))));
            assert!(!scope.meets_range((Bound::Unbounded, Bound::Excluded(&vec![Arc::new(99)]))));

            assert!(!scope.meets_range((
                Bound::Included(&vec![Arc::new(100)]),
                Bound::Excluded(&vec![Arc::new(100)])
            )));
            assert!(!scope.meets_range((
                Bound::Excluded(&vec![Arc::new(100)]),
                Bound::Included(&vec![Arc::new(100)])
            )));
            assert!(!scope.meets_range((
                Bound::Excluded(&vec![Arc::new(100)]),
                Bound::Excluded(&vec![Arc::new(100)])
            )));

            assert!(!scope.meets_range((
                Bound::Excluded(&vec![Arc::new(150)]),
                Bound::Excluded(&vec![Arc::new(150)])
            )));
            assert!(!scope.meets_range((
                Bound::Included(&vec![Arc::new(150)]),
                Bound::Excluded(&vec![Arc::new(150)])
            )));
            assert!(!scope.meets_range((
                Bound::Excluded(&vec![Arc::new(150)]),
                Bound::Included(&vec![Arc::new(150)])
            )));

            assert!(!scope.meets_range((
                Bound::Excluded(&vec![Arc::new(200)]),
                Bound::Excluded(&vec![Arc::new(200)])
            )));
            assert!(!scope.meets_range((
                Bound::Included(&vec![Arc::new(200)]),
                Bound::Excluded(&vec![Arc::new(200)])
            )));
            assert!(!scope.meets_range((
                Bound::Excluded(&vec![Arc::new(200)]),
                Bound::Included(&vec![Arc::new(200)])
            )));

            assert!(!scope.meets_range((Bound::Excluded(&vec![Arc::new(200)]), Bound::Unbounded)));
            assert!(!scope.meets_range((Bound::Included(&vec![Arc::new(201)]), Bound::Unbounded)));
            assert!(!scope.meets_range((Bound::Excluded(&vec![Arc::new(201)]), Bound::Unbounded)));

            assert!(!scope.meets_range((
                Bound::Included(&vec![Arc::new(99)]),
                Bound::Excluded(&vec![Arc::new(100)])
            )));
            assert!(!scope.meets_range((
                Bound::Excluded(&vec![Arc::new(99)]),
                Bound::Excluded(&vec![Arc::new(100)])
            )));
        }
        // test in range
        {
            assert!(scope.meets_range((Bound::Unbounded, Bound::Unbounded)));
            assert!(scope.meets_range((Bound::Unbounded, Bound::Included(&vec![Arc::new(100)]))));
            assert!(scope.meets_range((Bound::Unbounded, Bound::Included(&vec![Arc::new(200)]))));
            assert!(scope.meets_range((Bound::Unbounded, Bound::Excluded(&vec![Arc::new(200)]))));
            assert!(scope.meets_range((Bound::Unbounded, Bound::Included(&vec![Arc::new(201)]))));
            assert!(scope.meets_range((Bound::Included(&vec![Arc::new(200)]), Bound::Unbounded)));
            assert!(scope.meets_range((Bound::Included(&vec![Arc::new(100)]), Bound::Unbounded)));
            assert!(scope.meets_range((Bound::Excluded(&vec![Arc::new(100)]), Bound::Unbounded)));
            assert!(scope.meets_range((Bound::Included(&vec![Arc::new(99)]), Bound::Unbounded)));
            assert!(scope.meets_range((Bound::Excluded(&vec![Arc::new(99)]), Bound::Unbounded)));

            assert!(scope.meets_range((
                Bound::Included(&vec![Arc::new(100)]),
                Bound::Included(&vec![Arc::new(100)])
            )));
            assert!(scope.meets_range((
                Bound::Included(&vec![Arc::new(200)]),
                Bound::Included(&vec![Arc::new(200)])
            )));
            assert!(scope.meets_range((
                Bound::Included(&vec![Arc::new(99)]),
                Bound::Included(&vec![Arc::new(100)])
            )));
            assert!(scope.meets_range((
                Bound::Excluded(&vec![Arc::new(99)]),
                Bound::Included(&vec![Arc::new(100)])
            )));
            assert!(scope.meets_range((
                Bound::Included(&vec![Arc::new(150)]),
                Bound::Included(&vec![Arc::new(150)])
            )));
            assert!(scope.meets_range((
                Bound::Included(&vec![Arc::new(100)]),
                Bound::Included(&vec![Arc::new(200)])
            )));
            assert!(scope.meets_range((
                Bound::Included(&vec![Arc::new(99)]),
                Bound::Included(&vec![Arc::new(150)])
            )));
            assert!(scope.meets_range((
                Bound::Included(&vec![Arc::new(99)]),
                Bound::Included(&vec![Arc::new(201)])
            )));
            assert!(scope.meets_range((
                Bound::Included(&vec![Arc::new(99)]),
                Bound::Excluded(&vec![Arc::new(201)])
            )));
            assert!(scope.meets_range((
                Bound::Excluded(&vec![Arc::new(99)]),
                Bound::Included(&vec![Arc::new(201)])
            )));
            assert!(scope.meets_range((
                Bound::Excluded(&vec![Arc::new(99)]),
                Bound::Excluded(&vec![Arc::new(201)])
            )));
            assert!(scope.meets_range((
                Bound::Excluded(&vec![Arc::new(100)]),
                Bound::Excluded(&vec![Arc::new(200)])
            )));
        }
    }
}
