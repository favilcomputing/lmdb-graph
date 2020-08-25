use rmp_serde::{from_read_ref, Serializer};
use serde::{de::DeserializeOwned, Serialize};
use ulid::Ulid;

use crate::error::{Error, InternalError, Result};

pub trait Graph {
    type WriteTransaction: ReadTransaction;
    type ReadTransaction;

    fn write_transaction(&mut self) -> Result<Self::WriteTransaction>;
    fn read_transaction(&self) -> Result<Self::ReadTransaction>;
}

pub type LogId = Ulid;

pub trait FromDB<T> {
    fn from_db(key: impl Into<String>, data: &[u8]) -> Result<Self>
    where
        Self: Sized,
        T: DeserializeOwned;
}

pub trait ToDB {
    fn to_db(&self) -> Result<Vec<u8>>;
    fn key(&self) -> Result<String>;
}

#[derive(Debug, PartialEq, Clone)]
pub struct Node<T> {
    pub(crate) id: Option<LogId>,

    pub(crate) next_id: Option<LogId>,
    pub(crate) type_name: String,
    pub(crate) value: T,
}

impl<T: Serialize + DeserializeOwned + Clone> Node<T> {
    pub fn new(type_name: impl Into<String>, value: T) -> Result<Self> {
        Ok(Self {
            id: None,
            next_id: None,
            type_name: type_name.into(),
            value,
        })
    }

    pub fn get_value(&self) -> T {
        self.value.clone()
    }
}

impl<T> FromDB<T> for Node<T> {
    fn from_db(key: impl Into<String>, data: &[u8]) -> Result<Self>
    where
        Self: Sized,
        T: DeserializeOwned,
    {
        let (type_name, value) = from_read_ref::<[u8], (String, T)>(data)?;
        Ok(Self {
            id: Some(Ulid::from_string(&key.into())?),
            next_id: None,
            type_name,
            value,
        })
    }
}

impl<T> ToDB for Node<T>
where
    T: Serialize,
{
    fn to_db(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        (&self.type_name, &self.value).serialize(&mut Serializer::new(&mut buf))?;
        Ok(buf)
    }
    fn key(&self) -> Result<String> {
        self.id
            .map(|u| u.to_string())
            .ok_or(Error::Internal(InternalError::BadWrite))
    }
}

pub trait WriteTransaction {
    type Graph;

    fn put_node<T>(&mut self, n: Node<T>) -> Result<Node<T>>
    where
        T: Clone + Serialize + DeserializeOwned;
    fn commit(self) -> Result<()>;
}

pub trait ReadTransaction {
    type Graph;

    fn get_node<T>(&self, id: LogId) -> Result<Option<Node<T>>>
    where
        T: Clone + DeserializeOwned + Serialize;
    fn get_node_by_value<T>(&self, n: &Node<T>) -> Result<Option<Node<T>>>
    where
        T: Clone + DeserializeOwned + Serialize;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize() -> Result<()> {
        let value = "Testing".to_string();
        let node = Node::new("Name", value.clone())?;
        assert_eq!(node.get_value(), value);
        assert_eq!(node.id, None);
        assert_eq!(node.next_id, None);
        Ok(())
    }
}
