use serde::{de::DeserializeOwned, Serialize};
use ulid::Ulid;

use crate::error::Result;

pub trait Graph {
    type WriteTransaction: ReadTransaction;
    type ReadTransaction;

    fn write_transaction(&mut self) -> Result<Self::WriteTransaction>;
    fn read_transaction(&self) -> Result<Self::ReadTransaction>;
}

pub type LogId = Ulid;

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

pub trait WriteTransaction {
    type Graph;

    fn put_node<T: Clone + Serialize + DeserializeOwned>(&mut self, n: Node<T>) -> Result<Node<T>>;
    fn commit(self) -> Result<()>;
}

pub trait ReadTransaction {
    type Graph;

    fn get_node<T: Clone + DeserializeOwned + Serialize>(&self, id: LogId) -> Result<Node<T>>;
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
