use postcard::{from_bytes, to_stdvec};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::{
    parameter::{FromPValue, PValue, ToPValue},
    FromDB, Id, ToDB, Vertex, Writable,
};
use crate::error::{Error, Result};
use heed::{BytesDecode, BytesEncode};
use std::{borrow::Cow, clone::Clone, collections::HashMap};

#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum HexOrder {
    TFE,
    FTE,
    ETF,
    EFT,
    TEF,
    FET,
}

#[allow(dead_code)]
pub static ORDERS: [HexOrder; 6] = [
    HexOrder::TFE,
    HexOrder::FTE,
    HexOrder::ETF,
    HexOrder::EFT,
    HexOrder::TEF,
    HexOrder::FET,
];

impl HexOrder {
    #[allow(dead_code)]
    pub(crate) fn to_db(&self, id: Id, to: Id, from: Id) -> Result<Vec<u8>> {
        let order = match self {
            Self::TFE => (Self::TFE, to, from, id),
            Self::FTE => (Self::FTE, from, to, id),
            Self::ETF => (Self::ETF, id, to, from),
            Self::EFT => (Self::EFT, id, from, to),
            Self::TEF => (Self::TEF, to, id, from),
            Self::FET => (Self::FET, from, id, to),
        };
        Ok(to_stdvec(&order)?)
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Edge<V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable + Eq,
{
    pub(crate) id: Option<Id>,
    pub(crate) to: Id,
    pub(crate) from: Id,
    #[serde(bound(deserialize = "E: DeserializeOwned"))]
    pub(crate) label: E,

    #[serde(bound(deserialize = "P: DeserializeOwned"))]
    pub(crate) parameters: HashMap<P, PValue<V, E, P>>,
}

impl<V, E, P> Edge<V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable,
{
    pub fn new(to: &Vertex<V, E, P>, from: &Vertex<V, E, P>, label: E) -> Result<Self>
    where
        V: Writable,
        P: Writable,
    {
        if to.id.is_none() || from.id.is_none() {
            Err(Error::VertexInvalid)
        } else {
            Ok(Self {
                id: None,
                to: to.id.unwrap(),
                from: from.id.unwrap(),
                label,
                parameters: Default::default(),
            })
        }
    }

    pub fn get_label(&self) -> E {
        self.label.clone()
    }
}

impl<V, E, P> FromPValue<V, E, P> for Edge<V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable,
{
    fn from_pvalue(v: PValue<V, E, P>) -> Result<Self> {
        match v {
            PValue::Edge(e) => Ok(e),
            _ => Err(Error::InvalidPValue(format!("{:#?}", v))),
        }
    }
}

impl<V, E, P> ToPValue<V, E, P> for Edge<V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable,
{
    fn to_pvalue(&self) -> PValue<V, E, P> {
        PValue::Edge(self.clone())
    }
}

impl<'a, V, E, P> BytesEncode<'a> for Edge<V, E, P>
where
    V: 'a + Writable,
    E: 'a + Writable,
    P: 'a + Writable,
{
    type EItem = Self;

    fn bytes_encode(item: &'a Self::EItem) -> Option<std::borrow::Cow<'a, [u8]>> {
        match to_stdvec(item).ok() {
            Some(vec) => Some(Cow::Owned(vec)),
            None => None,
        }
    }
}

impl<'a, V, E, P> BytesDecode<'a> for Edge<V, E, P>
where
    V: 'a + Writable,
    E: 'a + Writable,
    P: 'a + Writable,
{
    type DItem = Self;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        from_bytes(bytes).ok()
    }
}

impl<V, E, P> FromDB<E> for Edge<V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable,
{
    type Key = Id;

    fn rev_from_db(data: &[u8]) -> Result<Self>
    where
        Self: Sized,
        E: DeserializeOwned,
    {
        let (label, to, from, id): (E, Id, Id, Id) = from_bytes(data)?;
        Ok(Self {
            id: Some(id),
            to,
            from,
            label,
            parameters: Default::default(),
        })
    }

    fn key_from_db(key: &[u8]) -> Result<Self::Key>
    where
        Self: Sized,
        E: DeserializeOwned,
    {
        Ok(from_bytes(key)?)
    }
}

impl<V, E, P> ToDB for Edge<V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable,
{
    type Key = Id;
    type Label = E;

    fn rev_to_db(&self) -> Result<Vec<u8>> {
        Ok(to_stdvec(&(
            &self.label,
            &self.to,
            &self.from,
            &self.id.unwrap(),
        ))?)
    }

    fn label_to_db(label: &E) -> Result<Vec<u8>> {
        Ok(to_stdvec(label)?)
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
