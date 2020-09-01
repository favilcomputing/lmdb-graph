use rmp_serde::{from_read_ref, Serializer};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::{FromDB, LogId, ToDB};
use crate::error::Result;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Node<Type, Value> {
    pub(crate) id: Option<LogId>,

    pub(crate) _type: Type,
    pub(crate) value: Value,
}

impl<Type, Value> Node<Type, Value>
where
    Type: Serialize + DeserializeOwned + Clone,
    Value: Serialize + DeserializeOwned + Clone,
{
    pub fn new(_type: Type, value: Value) -> Result<Self> {
        Ok(Self {
            id: None,
            _type,
            value,
        })
    }

    pub fn get_value(&self) -> Value {
        self.value.clone()
    }

    pub fn get_id(&self) -> Option<LogId> {
        self.id
    }
}

impl<Type, Value> FromDB<Value> for Node<Type, Value>
where
    Type: DeserializeOwned,
    Value: DeserializeOwned,
{
    type Key = LogId;

    fn from_db(key: &Self::Key, data: &[u8]) -> Result<Self>
    where
        Self: Sized,
        Type: DeserializeOwned,
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
        Type: DeserializeOwned,
        Value: DeserializeOwned,
    {
        Ok(from_read_ref::<[u8], Self::Key>(key)?)
    }
}

impl<Type, Value> ToDB for Node<Type, Value>
where
    Type: Serialize,
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
        (&self._type, &self.value).serialize(&mut Serializer::new(&mut buf))?;
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
        let node = Node::new("Name".to_string(), value.clone()).unwrap();
        assert_eq!(node.get_value(), value);
        assert_eq!(node.id, None);
    }
}
