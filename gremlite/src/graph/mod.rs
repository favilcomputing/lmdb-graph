pub(crate) mod edge;
pub(crate) mod parameter;
pub(crate) mod vertex;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use ulid::{Generator, Ulid};

pub use self::{
    edge::Edge,
    parameter::{FromPValue, PValue, ToPValue},
    vertex::Vertex,
};
use crate::error::Result;
use heed::{BytesDecode, BytesEncode};
use postcard::{from_bytes, to_stdvec};
use std::{borrow::Cow, convert::TryInto, fmt::Debug, hash::Hash};

pub trait Writable: Serialize + DeserializeOwned + Clone + Hash + Debug + PartialEq {}

impl Writable for () {}
impl Writable for String {}
// impl<V: Writable, E: Writable, P: Writable + Eq> Writable for PValue<V, E, P> {}

impl<T: Writable> Writable for Vec<T> {}
impl<T: Writable, U: Writable> Writable for (T, U) {}

#[derive(Serialize, Deserialize, PartialEq, PartialOrd, Clone, Copy, Eq, Ord, Hash, Debug)]
pub enum Type {
    Vertex,
    Edge,
    Parameter,
}

#[derive(Serialize, Deserialize, PartialEq, PartialOrd, Clone, Copy, Eq, Ord, Hash)]
pub struct Id(Type, Ulid);

impl Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}:{}", self.0, self.1.to_string())
    }
}

impl Id {
    pub fn new(t: Type, gen: &mut Generator) -> Result<Self> {
        Ok(Self(t, gen.generate()?))
    }

    pub const fn nil(t: Type) -> Self {
        Self(t, Ulid(0))
    }

    pub const fn max(t: Type) -> Self {
        Self(t, Ulid(u128::max_value()))
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Ids(pub(crate) Vec<Id>);

impl From<()> for Ids {
    fn from(_: ()) -> Self {
        Self(vec![])
    }
}

impl<T: Into<Id>> From<T> for Ids {
    fn from(i: T) -> Self {
        Self(vec![i.into()])
    }
}

impl<T: Into<Id>> From<Vec<T>> for Ids {
    fn from(i: Vec<T>) -> Self {
        Self(i.into_iter().map(Into::into).collect())
    }
}

impl<'a> BytesEncode<'a> for Id {
    type EItem = Self;
    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        let ulid = item.1;
        let ulid_bytes = ulid.0.to_be_bytes();
        let vector = to_stdvec(&(item.0, ulid_bytes)).unwrap();
        Some(Cow::Owned(vector))
    }
}

impl<'a> BytesDecode<'a> for Id {
    type DItem = Self;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        if bytes.len() != 17 {
            None
        } else {
            let t: Type = from_bytes(&bytes[0..1]).unwrap();
            let bytes: &[u8] = &bytes[1..17];
            Some(Self(
                t,
                Ulid(u128::from_be_bytes(bytes.try_into().unwrap())),
            ))
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
    type Label: Serialize;

    fn rev_to_db(&self) -> Result<Vec<u8>>;
    fn label_to_db(label: &Self::Label) -> Result<Vec<u8>>;
    fn key(&self) -> Result<Vec<u8>>;
    fn key_to_db(key: &Self::Key) -> Result<Vec<u8>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vertex_decode_encode() {
        let nil = Id::nil(Type::Vertex);
        let encoded = Id::bytes_encode(&nil);
        assert!(encoded.is_some());
        let encoded = encoded.unwrap();
        let decoded = Id::bytes_decode(&encoded);
        assert!(decoded.is_some());
        assert_eq!(decoded.unwrap(), nil);
    }

    #[test]
    fn test_edge_decode_encode() {
        let nil = Id::nil(Type::Edge);
        let encoded = Id::bytes_encode(&nil);
        assert!(encoded.is_some());
        let encoded = encoded.unwrap();
        let decoded = Id::bytes_decode(&encoded);
        assert!(decoded.is_some());
        assert_eq!(decoded.unwrap(), nil);
    }
}
