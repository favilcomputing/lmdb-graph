use ulid::DecodeError;

use std::io;
use std::{result, time::Duration};

use crate::graph::Id;

pub type Result<T> = result::Result<T, Error>;

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("not found {0:?}")]
    NotFound(Id),

    #[error("value not found")]
    ValueNotFound,

    #[error("invalid vertex")]
    VertexInvalid,

    #[error("empty traversal")]
    EmptyTraversal,

    #[error("error with serialization {0}")]
    Postcard(#[from] postcard::Error),

    #[error("ulid decode error {0}")]
    Ulid(DecodeError),

    #[error("io error {0}")]
    IoError(#[from] io::Error),

    #[error("ulid overflow error")]
    UlidOverflow,

    #[error("heed error {0}")]
    Heed(#[from] heed::Error),

    #[error("bad write")]
    BadWrite,

    #[error("timed out waiting for transaction {0:?}")]
    TimedOut(Duration),

    #[error("database is busy")]
    Busy,

    #[error("Invalid PValue {0}")]
    InvalidPValue(String)
}

impl From<ulid::MonotonicError> for Error {
    fn from(_: ulid::MonotonicError) -> Self {
        Self::UlidOverflow
    }
}
