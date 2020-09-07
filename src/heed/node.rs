use heed::RoIter;
use serde::de::DeserializeOwned;

use crate::{
    graph::{LogId, Node},
    heed::Graph,
};

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

#[cfg(test)]
mod tests {
    use rstest::{fixture, rstest};

    use super::*;
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
