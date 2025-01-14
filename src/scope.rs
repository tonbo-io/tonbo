use std::ops::Bound;

use fusio::{SeqRead, Write};
use fusio_log::{Decode, Encode};

use crate::fs::FileId;

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Scope<K> {
    pub(crate) min: K,
    pub(crate) max: K,
    pub(crate) gen: FileId,
    pub(crate) wal_ids: Option<Vec<FileId>>,
}

impl<K> Clone for Scope<K>
where
    K: Clone,
{
    fn clone(&self) -> Self {
        Scope {
            min: self.min.clone(),
            max: self.max.clone(),
            gen: self.gen,
            wal_ids: self.wal_ids.clone(),
        }
    }
}

impl<K> Scope<K>
where
    K: Ord,
{
    pub(crate) fn contains(&self, key: &K) -> bool {
        &self.min <= key && key <= &self.max
    }

    #[allow(unused)]
    pub(crate) fn meets(&self, target: &Self) -> bool {
        self.contains(&target.min) || self.contains(&target.max)
    }

    pub(crate) fn meets_range(&self, range: (Bound<&K>, Bound<&K>)) -> bool {
        let excluded_contains = |key| -> bool { &self.min < key && key < &self.max };
        let included_by = |min, max| -> bool { min <= &self.min && &self.max <= max };

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
            (Bound::Included(start), Bound::Unbounded) => start <= &self.max,
            (Bound::Excluded(start), Bound::Unbounded) => start < &self.max,
            (Bound::Unbounded, Bound::Included(end)) => end >= &self.min,
            (Bound::Unbounded, Bound::Excluded(end)) => end > &self.min,
            (Bound::Unbounded, Bound::Unbounded) => true,
        }
    }

    pub(crate) fn gen(&self) -> FileId {
        self.gen
    }
}

impl<K> Encode for Scope<K>
where
    K: Encode + Sync,
{
    type Error = <K as Encode>::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: Write,
    {
        self.min.encode(writer).await?;
        self.max.encode(writer).await?;

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
        self.min.size() + self.max.size() + 16
    }
}

impl<K> Decode for Scope<K>
where
    K: Decode,
{
    type Error = <K as Decode>::Error;

    async fn decode<R: SeqRead>(reader: &mut R) -> Result<Self, Self::Error> {
        let mut buf = [0u8; 16];
        let min = K::decode(reader).await?;
        let max = K::decode(reader).await?;

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
    use std::ops::Bound;

    use super::Scope;
    use crate::fs::generate_file_id;

    #[tokio::test]
    async fn test_meets_range() {
        let scope = Scope {
            min: 100,
            max: 200,
            gen: generate_file_id(),
            wal_ids: None,
        };

        // test out of range
        {
            assert!(!scope.meets_range((Bound::Unbounded, Bound::Excluded(&100))));
            assert!(!scope.meets_range((Bound::Unbounded, Bound::Included(&99))));
            assert!(!scope.meets_range((Bound::Unbounded, Bound::Excluded(&99))));

            assert!(!scope.meets_range((Bound::Included(&100), Bound::Excluded(&100))));
            assert!(!scope.meets_range((Bound::Excluded(&100), Bound::Included(&100))));
            assert!(!scope.meets_range((Bound::Excluded(&100), Bound::Excluded(&100))));

            assert!(!scope.meets_range((Bound::Excluded(&150), Bound::Excluded(&150))));
            assert!(!scope.meets_range((Bound::Included(&150), Bound::Excluded(&150))));
            assert!(!scope.meets_range((Bound::Excluded(&150), Bound::Included(&150))));

            assert!(!scope.meets_range((Bound::Excluded(&200), Bound::Excluded(&200))));
            assert!(!scope.meets_range((Bound::Included(&200), Bound::Excluded(&200))));
            assert!(!scope.meets_range((Bound::Excluded(&200), Bound::Included(&200))));

            assert!(!scope.meets_range((Bound::Excluded(&200), Bound::Unbounded)));
            assert!(!scope.meets_range((Bound::Included(&201), Bound::Unbounded)));
            assert!(!scope.meets_range((Bound::Excluded(&201), Bound::Unbounded)));

            assert!(!scope.meets_range((Bound::Included(&99), Bound::Excluded(&100))));
            assert!(!scope.meets_range((Bound::Excluded(&99), Bound::Excluded(&100))));
        }
        // test in range
        {
            assert!(scope.meets_range((Bound::Unbounded, Bound::Unbounded)));
            assert!(scope.meets_range((Bound::Unbounded, Bound::Included(&100))));
            assert!(scope.meets_range((Bound::Unbounded, Bound::Included(&200))));
            assert!(scope.meets_range((Bound::Unbounded, Bound::Excluded(&200))));
            assert!(scope.meets_range((Bound::Unbounded, Bound::Included(&201))));
            assert!(scope.meets_range((Bound::Included(&200), Bound::Unbounded)));
            assert!(scope.meets_range((Bound::Included(&100), Bound::Unbounded)));
            assert!(scope.meets_range((Bound::Excluded(&100), Bound::Unbounded)));
            assert!(scope.meets_range((Bound::Included(&99), Bound::Unbounded)));
            assert!(scope.meets_range((Bound::Excluded(&99), Bound::Unbounded)));

            assert!(scope.meets_range((Bound::Included(&100), Bound::Included(&100))));
            assert!(scope.meets_range((Bound::Included(&200), Bound::Included(&200))));
            assert!(scope.meets_range((Bound::Included(&99), Bound::Included(&100))));
            assert!(scope.meets_range((Bound::Excluded(&99), Bound::Included(&100))));
            assert!(scope.meets_range((Bound::Included(&150), Bound::Included(&150))));
            assert!(scope.meets_range((Bound::Included(&100), Bound::Included(&200))));
            assert!(scope.meets_range((Bound::Included(&99), Bound::Included(&150))));
            assert!(scope.meets_range((Bound::Included(&99), Bound::Included(&201))));
            assert!(scope.meets_range((Bound::Included(&99), Bound::Excluded(&201))));
            assert!(scope.meets_range((Bound::Excluded(&99), Bound::Included(&201))));
            assert!(scope.meets_range((Bound::Excluded(&99), Bound::Excluded(&201))));
            assert!(scope.meets_range((Bound::Excluded(&100), Bound::Excluded(&200))));
        }
    }
}
