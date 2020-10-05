pub(crate) mod edge;
pub(crate) mod node;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use ulid::{Generator, Ulid};

pub use self::{edge::Edge, node::Node};
use crate::error::Result;
use heed::{BytesDecode, BytesEncode};
use std::{borrow::Cow, convert::TryInto, hash::Hash};

#[derive(Serialize, Deserialize, PartialEq, PartialOrd, Debug, Clone, Copy, Eq, Ord, Hash)]
pub struct LogId(Ulid);

impl LogId {
    pub fn new(gen: &mut Generator) -> Result<Self> {
        Ok(Self(gen.generate()?))
    }

    pub fn nil() -> Self {
        Self(Ulid::nil())
    }

    pub fn max() -> Self {
        Self(Ulid(u128::max_value()))
    }
}

impl<'a> BytesEncode<'a> for LogId {
    type EItem = LogId;
    fn bytes_encode(item: &'a Self::EItem) -> Option<std::borrow::Cow<'a, [u8]>> {
        Some(Cow::Owned((item.0).0.to_be_bytes().to_vec()))
    }
}

impl<'a> BytesDecode<'a> for LogId {
    type DItem = LogId;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        if bytes.len() != 16 {
            None
        } else {
            let bytes = &bytes[0..16];
            Some(LogId(Ulid(u128::from_be_bytes(bytes.try_into().unwrap()))))
        }
    }
}

pub trait FromDB<Value> {
    type Key: Serialize + DeserializeOwned;

    fn rev_from_db(data: &[u8]) -> Result<Self>
    where
        Self: Sized,
        Value: DeserializeOwned;

    fn key_from_db(key: &[u8]) -> Result<Self::Key>
    where
        Self: Sized,
        Value: DeserializeOwned;
}

pub trait ToDB {
    type Key: Serialize + DeserializeOwned;
    type Value: Serialize;

    fn rev_to_db(&self) -> Result<Vec<u8>>;
    fn value_to_db(value: &Self::Value) -> Result<Vec<u8>>;
    fn key(&self) -> Result<Vec<u8>>;
    fn key_to_db(key: &Self::Key) -> Result<Vec<u8>>;
}
