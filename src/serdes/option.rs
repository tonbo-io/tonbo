use std::io;

use futures_io::{AsyncRead, AsyncWrite};
use futures_util::{AsyncReadExt, AsyncWriteExt};
use thiserror::Error;

use super::{Decode, Encode};

#[derive(Debug, Error)]
#[error("option encode error")]
pub enum EncodeError<E>
where
    E: std::error::Error,
{
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("inner error: {0}")]
    Inner(#[source] E),
}

#[derive(Debug, Error)]
#[error("option decode error")]
pub enum DecodeError<E>
where
    E: std::error::Error,
{
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("inner error: {0}")]
    Inner(#[source] E),
}

impl<V> Encode for Option<V>
where
    V: Encode,
{
    type Error = EncodeError<V::Error>;

    async fn encode<W: AsyncWrite + Unpin + Send + Sync>(
        &self,
        writer: &mut W,
    ) -> Result<(), Self::Error> {
        match self {
            None => writer.write_all(&[0]).await?,
            Some(v) => {
                writer.write_all(&[1]).await?;
                v.encode(writer).await.map_err(EncodeError::Inner)?;
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        1
    }
}

impl<V> Decode for Option<V>
where
    V: Decode,
{
    type Error = DecodeError<V::Error>;

    async fn decode<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
        let mut o = [0];
        reader.read_exact(&mut o).await?;
        match o[0] {
            0 => Ok(None),
            1 => Ok(Some(V::decode(reader).await.map_err(DecodeError::Inner)?)),
            _ => panic!("invalid option tag"),
        }
    }
}
