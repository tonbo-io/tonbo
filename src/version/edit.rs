use std::mem::size_of;

use fusio::{SeqRead, Write};
use fusio_log::{Decode, Encode, FsOptions, Options, Path};
use futures_util::TryStreamExt;

use crate::{fs::FileId, scope::Scope, timestamp::Timestamp};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum VersionEdit<K> {
    Add { level: u8, scope: Scope<K> },
    Remove { level: u8, gen: FileId },
    LatestTimeStamp { ts: Timestamp },
    NewLogLength { len: u32 },
}

impl<K> VersionEdit<K>
where
    K: Decode + Send,
{
    pub(crate) async fn recover(path: Path, fs_option: FsOptions) -> Vec<VersionEdit<K>> {
        let mut edits = vec![];

        let mut edits_stream = Options::new(path)
            .disable_buf()
            .fs(fs_option)
            .recover::<VersionEdit<K>>()
            .await
            .unwrap();
        while let Ok(batch) = edits_stream.try_next().await {
            match batch {
                Some(mut batch) => edits.append(&mut batch),
                None => break,
            }
        }
        edits
    }
}

impl<K> Encode for VersionEdit<K>
where
    K: Encode + Sync,
{
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: Write,
    {
        match self {
            VersionEdit::Add { scope, level } => {
                0u8.encode(writer).await?;
                level.encode(writer).await?;
                scope.encode(writer).await?;
            }
            VersionEdit::Remove { gen, level } => {
                1u8.encode(writer).await?;
                level.encode(writer).await?;
                let (result, _) = writer.write_all(&gen.to_bytes()[..]).await;
                result?;
            }
            VersionEdit::LatestTimeStamp { ts } => {
                2u8.encode(writer).await?;
                ts.encode(writer).await?;
            }
            VersionEdit::NewLogLength { len } => {
                3u8.encode(writer).await?;
                len.encode(writer).await?;
            }
        }

        Ok(())
    }

    fn size(&self) -> usize {
        size_of::<u8>()
            + size_of::<u8>()
            + match self {
                VersionEdit::Add { scope, .. } => scope.size(),
                VersionEdit::Remove { .. } => 16,
                VersionEdit::LatestTimeStamp { ts } => ts.size(),
                VersionEdit::NewLogLength { .. } => size_of::<u32>(),
            }
    }
}

impl<K> Decode for VersionEdit<K>
where
    K: Decode + Send,
{
    async fn decode<R: SeqRead>(reader: &mut R) -> Result<Self, fusio::Error> {
        let edit_type = u8::decode(reader).await?;

        Ok(match edit_type {
            0 => {
                let level = u8::decode(reader).await?;
                let scope = Scope::<K>::decode(reader).await?;

                VersionEdit::Add { level, scope }
            }
            1 => {
                let level = u8::decode(reader).await?;
                let gen = {
                    let mut buf = [0u8; 16];
                    let (result, _) = reader.read_exact(&mut buf[..]).await;
                    result?;
                    FileId::from_bytes(buf)
                };
                VersionEdit::Remove { level, gen }
            }
            2 => {
                let ts = Timestamp::decode(reader).await?;
                VersionEdit::LatestTimeStamp { ts }
            }
            3 => {
                let len = u32::decode(reader).await?;
                VersionEdit::NewLogLength { len }
            }
            _ => unreachable!(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use fusio_log::{Decode, Encode};
    use tokio::io::AsyncSeekExt;

    use crate::{fs::generate_file_id, scope::Scope, version::edit::VersionEdit};

    #[tokio::test]
    async fn encode_and_decode() {
        let edits = vec![
            VersionEdit::Add {
                level: 0,
                scope: Scope {
                    min: "Min".to_string(),
                    max: "Max".to_string(),
                    gen: Default::default(),
                    wal_ids: Some(vec![generate_file_id(), generate_file_id()]),
                    file_size: 13,
                },
            },
            VersionEdit::Remove {
                level: 1,
                gen: Default::default(),
            },
            VersionEdit::LatestTimeStamp { ts: 10.into() },
            VersionEdit::NewLogLength { len: 233 },
        ];

        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);

        for edit in edits.clone() {
            edit.encode(&mut cursor).await.unwrap();
        }

        cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();

        let mut decode_edits = Vec::new();

        while let Ok(edit) = VersionEdit::decode(&mut cursor).await {
            decode_edits.push(edit);
        }

        assert_eq!(edits, decode_edits);
    }
}
