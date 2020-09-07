use lmdb_zero;
use rmp_serde::{decode, encode};
use ulid::DecodeError;

use std::io;
use std::result;

use crate::graph::LogId;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    NotFound(LogId),

    Encode(encode::Error),
    Decode(decode::Error),
    Ulid(DecodeError),
    Internal(InternalError),
    UsedArc,
}

#[derive(Debug)]
pub enum InternalError {
    IoError(io::Error),
    Lmdb(lmdb_zero::error::Error),
    BadWrite,
}

impl From<decode::Error> for Error {
    fn from(e: decode::Error) -> Self {
        Self::Decode(e)
    }
}

impl From<encode::Error> for Error {
    fn from(e: encode::Error) -> Self {
        Self::Encode(e)
    }
}

impl From<lmdb_zero::error::Error> for Error {
    fn from(e: lmdb_zero::error::Error) -> Self {
        Self::Internal(InternalError::Lmdb(e))
    }
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
