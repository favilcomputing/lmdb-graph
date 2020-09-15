use heed::{types::OwnedSlice, RoIter, RoRange};
use serde::de::DeserializeOwned;

use crate::graph::{FromDB, LogId, Node};
use std::marker::PhantomData;

pub struct NodeIter<'txn, Value> {
    pub(crate) iter: RoIter<'txn, LogId, Node<Value>>,
}

impl<'txn, Value: 'txn + DeserializeOwned> Iterator for NodeIter<'txn, Value> {
    type Item = Node<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.iter.next();
        match next {
            Some(Ok(node)) => Some(node.1),
            Some(Err(_)) => None,
            None => None,
        }
    }
}

pub struct NodeRange<'txn, Value> {
    pub(crate) iter: RoRange<'txn, OwnedSlice<u8>, LogId>,
    _marker: PhantomData<Value>,
}

impl<'txn, Value> NodeRange<'txn, Value> {
    pub fn new(iter: RoRange<'txn, OwnedSlice<u8>, LogId>) -> Self {
        Self {
            iter,
            _marker: PhantomData,
        }
    }
}

impl<'txn, Value: 'txn + DeserializeOwned> Iterator for NodeRange<'txn, Value> {
    type Item = Node<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        let next: Option<Result<(Vec<u8>, LogId), heed::Error>> = self.iter.next();
        match next {
            Some(Ok(next)) => Some(Node::rev_from_db(&next.0).unwrap()),
            Some(Err(_)) => None,
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::fixture;

    // use super::*;
    use crate::heed::Graph;
    use tempdir::TempDir;

    #[fixture]
    fn tmpdir() -> TempDir {
        TempDir::new("test").unwrap()
    }
    #[fixture]
    fn graph(tmpdir: TempDir) -> Graph<String> {
        Graph::new(tmpdir.path()).unwrap()
    }
}
