mod node;

use rmp_serde::{from_read_ref, Serializer};
use serde::{de::DeserializeOwned, Serialize};
use ulid::Ulid;

use crate::error::Result;
pub use self::node::*;

pub trait Graph {
    type ReadT: ReadTransaction;
    type WriteT: ReadTransaction + WriteTransaction;

    fn write_transaction(&mut self) -> Result<Self::WriteT>;
    fn read_transaction(&self) -> Result<Self::ReadT>;
}

pub type LogId = Ulid;

pub trait FromDB<Value> {
    type Key: Serialize + DeserializeOwned;

    fn from_db(key: &Self::Key, data: &[u8]) -> Result<Self>
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

    fn to_db(&self) -> Result<Vec<u8>>;
    fn key(&self) -> Result<Vec<u8>>;
    fn key_to_db(key: &Self::Key) -> Result<Vec<u8>>;
}


pub trait WriteTransaction {
    type Graph;

    fn put_node<Type, Value>(&mut self, n: Node<Type, Value>) -> Result<Node<Type, Value>>
    where
        Type: Clone + Serialize + DeserializeOwned,
        Value: Clone + Serialize + DeserializeOwned;

    fn commit(self) -> Result<()>;

    fn clear(&mut self) -> Result<()>;
}

pub trait ReadTransaction {
    type Graph;

    fn get_node<Type, Value>(&self, id: LogId) -> Result<Option<Node<Type, Value>>>
    where
        Type: Clone + DeserializeOwned + Serialize,
        Value: Clone + DeserializeOwned + Serialize;
    fn get_node_by_value<Type, Value>(
        &self,
        n: &Node<Type, Value>,
    ) -> Result<Option<Node<Type, Value>>>
    where
        Type: Clone + DeserializeOwned + Serialize,
        Value: Clone + DeserializeOwned + Serialize;
}

#[derive(Debug, PartialEq, Clone)]
pub struct Edge<Type, Value> {
    id: Option<LogId>,
    to: LogId,
    from: LogId,
    _type: Type,
    value: Value,
}

impl<Type, Value> Edge<Type, Value> {
    pub fn new(to: LogId, from: LogId, _type: Type, value: Value) -> Self {
        Self {
            id: None,
            to,
            from,
            _type,
            value,
        }
    }
}

impl<Type, Value> FromDB<Value> for Edge<Type, Value>
where
    Type: DeserializeOwned,
    Value: DeserializeOwned,
{
    type Key = (LogId, LogId, LogId);

    fn from_db((id, to, from): &Self::Key, data: &[u8]) -> Result<Self>
    where
        Self: Sized,
        Type: DeserializeOwned,
        Value: DeserializeOwned,
    {
        let (_type, value) = from_read_ref::<[u8], (Type, Value)>(data)?;
        Ok(Self {
            id: Some(id.clone()),
            to: to.clone(),
            from: from.clone(),
            _type,
            value,
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

impl<Type, Value> ToDB for Edge<Type, Value>
where
    Type: Serialize,
    Value: Serialize,
{
    type Key = (LogId, LogId, LogId);

    fn to_db(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        (&self._type, &self.value).serialize(&mut Serializer::new(&mut buf))?;
        Ok(buf)
    }

    fn key(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        (&self.id, &self.to, &self.from).serialize(&mut Serializer::new(&mut buf))?;
        Ok(buf)
    }

    fn key_to_db(key: &Self::Key) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        key.serialize(&mut Serializer::new(&mut buf))?;
        Ok(buf)
    }
}

