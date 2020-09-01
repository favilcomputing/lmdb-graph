use serde::{de::DeserializeOwned, Serialize};

use super::{Edge, LogId, Node};
use crate::error::Result;
use std::{marker::PhantomData, sync::Arc};

pub trait WriteTransaction {
    type Graph;

    fn put_node<Type, Value>(&mut self, n: Node<Type, Value>) -> Result<Node<Type, Value>>
    where
        Type: Clone + Serialize + DeserializeOwned,
        Value: Clone + Serialize + DeserializeOwned;

    fn put_edge<Type, Value>(&mut self, e: Edge<Type, Value>) -> Result<Edge<Type, Value>>
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

pub(crate) trait NodeReader<Type, Value> {
    type Graph;
    type Iter;

    fn all_nodes(&self) -> Result<Self::Iter>;
}
