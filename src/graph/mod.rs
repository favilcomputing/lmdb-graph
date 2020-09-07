pub(crate) mod edge;
pub(crate) mod node;
pub(crate) mod trans;

use serde::{de::DeserializeOwned, Serialize};
use ulid::Ulid;

pub use self::{
    edge::Edge,
    node::Node,
    trans::{ReadTransaction, WriteTransaction},
};
use crate::error::Result;
use trans::NodeReader;

pub trait Graph {
    type ReadT: ReadTransaction + NodeReader;
    type WriteT: ReadTransaction + WriteTransaction + NodeReader;

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
    fn value_to_db(&self) -> Result<Vec<u8>>;
    fn key(&self) -> Result<Vec<u8>>;
    fn key_to_db(key: &Self::Key) -> Result<Vec<u8>>;
}
