use heed;
use ulid::DecodeError;

use std::io;
use std::result;

use crate::graph::LogId;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    NotFound(LogId),
    ValueNotFound,
    NodeInvalid,

    Postcard(postcard::Error),
    Ulid(DecodeError),
    Internal(InternalError),
    UsedArc,
}

#[derive(Debug)]
pub enum InternalError {
    IoError(io::Error),
    UlidOverflow,
    Heed(heed::Error),
    BadWrite,
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Internal(InternalError::IoError(e))
    }
}

impl From<DecodeError> for Error {
    fn from(e: DecodeError) -> Self {
        Self::Ulid(e)
    }
}

impl From<heed::Error> for Error {
    fn from(e: heed::Error) -> Self {
        Self::Internal(InternalError::Heed(e))
    }
}

impl From<ulid::MonotonicError> for Error {
    fn from(_: ulid::MonotonicError) -> Self {
        Self::Internal(InternalError::UlidOverflow)
    }
}

impl From<postcard::Error> for Error {
    fn from(e: postcard::Error) -> Self {
        Self::Postcard(e)
    }
}
