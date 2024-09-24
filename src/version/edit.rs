use std::mem::size_of;

use fusio::{IoBuf, Read, Write};

use crate::{
    fs::FileId,
    scope::Scope,
    serdes::{Decode, Encode},
    timestamp::Timestamp,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum VersionEdit<K> {
    Add { level: u8, scope: Scope<K> },
    Remove { level: u8, gen: FileId },
    LatestTimeStamp { ts: Timestamp },
    NewLogLength { len: u32 },
}

impl<K> VersionEdit<K>
where
    K: Decode,
{
    pub(crate) async fn recover<R: Read + Unpin>(reader: &mut R) -> Vec<VersionEdit<K>> {
        let mut edits = Vec::new();

        while let Ok(edit) = VersionEdit::decode(reader).await {
            edits.push(edit)
        }
        edits
    }
}

impl<K> Encode for VersionEdit<K>
where
    K: Encode + Sync,
{
    type Error = <K as Encode>::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: Write + Unpin + Send,
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
                let (result, _) = writer.write(&gen.to_bytes()[..]).await;
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
    K: Decode,
{
    type Error = <K as Decode>::Error;

    async fn decode<R: Read + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
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
                    let buf = reader.read(Some(16)).await?;
                    // SAFETY
                    FileId::from_bytes(buf.as_slice().try_into().unwrap())
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

    use fusio::Seek;

    use crate::{fs::FileId, scope::Scope, serdes::Encode, version::edit::VersionEdit};

    #[tokio::test]
    async fn encode_and_decode() {
        let edits = vec![
            VersionEdit::Add {
                level: 0,
                scope: Scope {
                    min: "Min".to_string(),
                    max: "Max".to_string(),
                    gen: Default::default(),
                    wal_ids: Some(vec![FileId::new(), FileId::new()]),
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

        let decode_edits = {
            cursor.seek(0).await.unwrap();

            VersionEdit::<String>::recover(&mut cursor).await
        };

        assert_eq!(edits, decode_edits);
    }
}
