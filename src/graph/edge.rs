use postcard::{from_bytes, to_stdvec};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::{FromDB, LogId, Node, ToDB};
use crate::error::{Error, Result};
use heed::{BytesDecode, BytesEncode};
use std::borrow::Cow;

#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub(crate) enum HexOrder {
    TFE,
    FTE,
    ETF,
    EFT,
    TEF,
    FET,
}

#[allow(dead_code)]
pub(crate) static ORDERS: [HexOrder; 6] = [
    HexOrder::TFE,
    HexOrder::FTE,
    HexOrder::ETF,
    HexOrder::EFT,
    HexOrder::TEF,
    HexOrder::FET,
];

impl HexOrder {
    #[allow(dead_code)]
    pub(crate) fn to_db(&self, id: LogId, to: LogId, from: LogId) -> Result<Vec<u8>> {
        let value = match self {
            HexOrder::TFE => (Self::TFE, to, from, id),
            HexOrder::FTE => (Self::FTE, from, to, id),
            HexOrder::ETF => (Self::ETF, id, to, from),
            HexOrder::EFT => (Self::EFT, id, from, to),
            HexOrder::TEF => (Self::TEF, to, id, from),
            HexOrder::FET => (Self::FET, from, id, to),
        };
        Ok(to_stdvec(&value)?)
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Edge<Value> {
    pub(crate) id: Option<LogId>,
    pub(crate) to: LogId,
    pub(crate) from: LogId,
    pub(crate) value: Value,
}

impl<Value> Edge<Value>
where
    Value: Serialize + DeserializeOwned + Clone,
{
    pub fn new<NodeT>(to: &Node<NodeT>, from: &Node<NodeT>, value: Value) -> Result<Self> {
        if to.id.is_none() || from.id.is_none() {
            Err(Error::NodeInvalid)
        } else {
            Ok(Self {
                id: None,
                to: to.id.unwrap(),
                from: from.id.unwrap(),
                value,
            })
        }
    }

    pub fn get_value(&self) -> Value {
        self.value.clone()
    }
}

impl<'a, Value: 'a + Serialize> BytesEncode<'a> for Edge<Value> {
    type EItem = Edge<Value>;

    fn bytes_encode(item: &'a Self::EItem) -> Option<std::borrow::Cow<'a, [u8]>> {
        match to_stdvec(item).ok() {
            Some(vec) => Some(Cow::Owned(vec)),
            None => None,
        }
    }
}

impl<'a, Value: 'a + DeserializeOwned> BytesDecode<'a> for Edge<Value> {
    type DItem = Edge<Value>;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        from_bytes(bytes).ok()
    }
}

impl<Value> FromDB<Value> for Edge<Value>
where
    Value: DeserializeOwned,
{
    type Key = LogId;

    fn rev_from_db(data: &[u8]) -> Result<Self>
    where
        Self: Sized,
        Value: DeserializeOwned,
    {
        let (value, to, from, id): (Value, LogId, LogId, LogId) = from_bytes(data)?;
        Ok(Self {
            id: Some(id),
            to,
            from,
            value,
        })
    }

    fn key_from_db(key: &[u8]) -> Result<Self::Key>
    where
        Self: Sized,
        Value: DeserializeOwned,
    {
        Ok(from_bytes(key)?)
    }
}

impl<Value> ToDB for Edge<Value>
where
    Value: Serialize,
{
    type Key = LogId;
    type Value = Value;

    fn rev_to_db(&self) -> Result<Vec<u8>> {
        Ok(to_stdvec(&(
            &self.value,
            &self.to,
            &self.from,
            &self.id.unwrap(),
        ))?)
    }

    fn value_to_db(value: &Value) -> Result<Vec<u8>> {
        Ok(to_stdvec(value)?)
    }

    fn key(&self) -> Result<Vec<u8>> {
        Ok(to_stdvec(&self.id)?)
    }

    fn key_to_db(key: &Self::Key) -> Result<Vec<u8>> {
        Ok(to_stdvec(&key)?)
    }
}

#[cfg(test)]
mod tests {
    // use rstest::rstest;

    // use super::*;

    // #[rstest]
    // fn test_serialize() -> Result<()> {
    //     let value = "Testing".to_string();

    //     let mut edge = Edge::new(LogId::nil(), LogId::nil(), value.clone())?;
    //     edge.id = Some(LogId::nil());
    //     assert_eq!(edge.get_value(), value);
    //     // assert_eq!(
    //     //     Edge::<String>::rev_from_db(edge.rev_to_db()?.as_slice())?,
    //     //     edge
    //     // );
    //     Ok(())
    // }
}
