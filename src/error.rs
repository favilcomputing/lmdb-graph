use lmdb_zero;
use heed;
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
    UlidOverflow,
    Lmdb(lmdb_zero::error::Error),
    Heed(heed::Error),
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
