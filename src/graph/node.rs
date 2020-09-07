use heed::{BytesDecode, BytesEncode};
use postcard::{from_bytes, to_stdvec};
use rmp_serde::{from_read_ref, Serializer};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::{FromDB, LogId, ToDB};
use crate::error::Result;
use std::{hash::Hash, borrow::Cow};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, Eq, Hash)]
pub struct Node<Value> {
    pub(crate) id: Option<LogId>,

    pub(crate) value: Value,
}

impl<Value> Node<Value>
where
    Value: Serialize + DeserializeOwned + Clone,
{
    pub fn new(value: Value) -> Result<Self> {
        Ok(Self { id: None, value })
    }

    pub fn get_value(&self) -> Value {
        self.value.clone()
    }

    pub fn get_id(&self) -> Option<LogId> {
        self.id
    }
}

impl<'a, Value: 'a + Serialize> BytesEncode<'a> for Node<Value> {
    type EItem = Node<Value>;

    fn bytes_encode(item: &'a Self::EItem) -> Option<std::borrow::Cow<'a, [u8]>> {
        match to_stdvec(item).ok() {
            Some(vec) => Some(Cow::Owned(vec)),
            None => None,
        }
    }
}

impl<'a, Value: 'a + DeserializeOwned> BytesDecode<'a> for Node<Value> {
    type DItem = Node<Value>;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        from_bytes(bytes).ok()
    }
}

impl<Value> FromDB<Value> for Node<Value>
where
    Value: DeserializeOwned,
{
    type Key = LogId;

    fn from_db(key: &Self::Key, data: &[u8]) -> Result<Self>
    where
        Self: Sized,
        Value: DeserializeOwned,
    {
        let node = from_read_ref::<[u8], Self>(data)?;
        Ok(Self {
            id: Some(key.clone()),
            ..node
        })
    }

    fn key_from_db(key: &[u8]) -> Result<Self::Key>
    where
        Self: Sized,
        Value: DeserializeOwned,
    {
        Ok(from_read_ref::<[u8], Self::Key>(key)?)
    }
}

impl<Value> ToDB for Node<Value>
where
    Value: Serialize,
{
    type Key = LogId;

    fn to_db(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.serialize(&mut Serializer::new(&mut buf))?;
        Ok(buf)
    }

    fn value_to_db(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        &self.value.serialize(&mut Serializer::new(&mut buf))?;
        Ok(buf)
    }

    fn key(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.id.serialize(&mut Serializer::new(&mut buf))?;
        Ok(buf)
    }

    fn key_to_db(key: &Self::Key) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        key.serialize(&mut Serializer::new(&mut buf))?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_serialize() {
        let value = "Testing".to_string();
        let node = Node::new(value.clone()).unwrap();
        assert_eq!(node.get_value(), value);
        assert_eq!(node.id, None);
    }
}
