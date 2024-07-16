use std::mem::size_of;

use futures_io::{AsyncRead, AsyncWrite};
use futures_util::{AsyncReadExt, AsyncWriteExt};

use crate::{
    fs::FileId,
    scope::Scope,
    serdes::{Decode, Encode},
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum VersionEdit<K>
where
    K: Encode + Decode + Ord + Clone,
{
    Add { level: u8, scope: Scope<K> },
    Remove { level: u8, gen: FileId },
}

impl<K> VersionEdit<K>
where
    K: Encode + Decode + Ord + Clone,
{
    pub(crate) async fn recover<R: AsyncRead + Unpin>(reader: &mut R) -> Vec<VersionEdit<K>> {
        let mut edits = Vec::new();

        while let Ok(edit) = VersionEdit::decode(reader).await {
            edits.push(edit)
        }
        edits
    }
}

impl<K> Encode for VersionEdit<K>
where
    K: Encode + Decode + Ord + Clone,
{
    type Error = <K as Encode>::Error;

    async fn encode<W: AsyncWrite + Unpin + Send + Sync>(
        &self,
        writer: &mut W,
    ) -> Result<(), Self::Error> {
        match self {
            VersionEdit::Add { scope, level } => {
                writer.write_all(&0u8.to_le_bytes()).await?;
                writer.write_all(&level.to_le_bytes()).await?;
                scope.encode(writer).await?;
            }
            VersionEdit::Remove { gen, level } => {
                writer.write_all(&1u8.to_le_bytes()).await?;
                writer.write_all(&level.to_le_bytes()).await?;
                writer.write_all(&gen.to_bytes()).await?;
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
            }
    }
}

impl<K> Decode for VersionEdit<K>
where
    K: Encode + Decode + Ord + Clone,
{
    type Error = <K as Decode>::Error;

    async fn decode<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
        let edit_type = {
            let mut len = [0; size_of::<u8>()];
            reader.read_exact(&mut len).await?;
            u8::from_le_bytes(len) as usize
        };
        let level = {
            let mut level = [0; size_of::<u8>()];
            reader.read_exact(&mut level).await?;
            u8::from_le_bytes(level)
        };

        Ok(match edit_type {
            0 => {
                let scope = Scope::<K>::decode(reader).await?;

                VersionEdit::Add { level, scope }
            }
            1 => {
                let gen = {
                    let mut slice = [0; 16];
                    reader.read_exact(&mut slice).await?;
                    FileId::from_bytes(slice)
                };
                VersionEdit::Remove { level, gen }
            }
            _ => todo!(),
        })
    }
}

#[cfg(test)]
mod tests {
    use futures_executor::block_on;
    use futures_util::io::Cursor;

    use crate::{fs::FileId, scope::Scope, serdes::Encode, version::edit::VersionEdit};

    #[test]
    fn encode_and_decode() {
        block_on(async {
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
            ];

            let bytes = {
                let mut cursor = Cursor::new(vec![]);

                for edit in edits.clone() {
                    edit.encode(&mut cursor).await.unwrap();
                }
                cursor.into_inner()
            };

            let decode_edits = {
                let mut cursor = Cursor::new(bytes);

                VersionEdit::<String>::recover(&mut cursor).await
            };

            assert_eq!(edits, decode_edits);
        })
    }
}
