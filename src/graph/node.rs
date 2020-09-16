use heed::{BytesDecode, BytesEncode};
use postcard::{from_bytes, to_stdvec};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::{FromDB, LogId, ToDB};
use crate::error::Result;
use std::{borrow::Cow, fmt::Debug, hash::Hash};

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

    fn rev_from_db(data: &[u8]) -> Result<Self>
    where
        Self: Sized,
        Value: DeserializeOwned,
    {
        let (value, id): (Value, LogId) = from_bytes(data)?;
        Ok(Self {
            id: Some(id),
            value,
        })
    }

    fn key_from_db(_key: &[u8]) -> Result<Self::Key>
    where
        Self: Sized,
        Value: DeserializeOwned,
    {
        // Ok(from_read_ref::<[u8], Self::Key>(key)?)
        todo!()
    }
}

impl<Value> ToDB for Node<Value>
where
    Value: Serialize,
{
    type Key = LogId;
    type Value = Value;

    fn rev_to_db(&self) -> Result<Vec<u8>> {
        Ok(to_stdvec(&(&self.value, &self.id.unwrap()))?)
    }

    fn value_to_db(value: &Value) -> Result<Vec<u8>> {
        Ok(to_stdvec(value)?)
    }
    fn key(&self) -> Result<Vec<u8>> {
        Ok(to_stdvec(&self.id)?)
    }

    fn key_to_db(key: &Self::Key) -> Result<Vec<u8>> {
        Ok(to_stdvec(key)?)
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
