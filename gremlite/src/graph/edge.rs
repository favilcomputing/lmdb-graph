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

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::graph::Type;

    #[rstest]
    fn test_none_id() -> Result<()> {
        let v1 = Vertex::<String, String, String>::new("v".into());
        let v2 = Vertex::<String, String, String>::new("v".into());
        let e = Edge::<String, String, String>::new(&v1, &v2, "e".into());
        assert!(e.is_err());

        Ok(())
    }

    #[rstest]
    fn test_to_pvalue() -> Result<()> {
        let mut v1 = Vertex::<String, String, String>::new("v".into());
        v1.id = Some(Id::nil(Type::Vertex));
        let mut v2 = Vertex::<String, String, String>::new("v".into());
        v2.id = Some(Id::max(Type::Vertex));
        let e = Edge::<String, String, String>::new(&v1, &v2, "e".into())?;

        assert_eq!(e.get_label(), "e");

        let pvalue = e.to_pvalue();
        let decoded = Edge::from_pvalue(pvalue)?;

        assert_eq!(decoded, e);

        Ok(())
    }

    #[rstest]
    fn test_from_pvalue() -> Result<()> {
        let v1 = Vertex::<String, String, String>::new("v".into());
        let pvalue = v1.to_pvalue();

        let decoded = Edge::from_pvalue(pvalue);

        assert!(decoded.is_err());

        Ok(())
    }

    #[rstest]
    fn test_hex_order() -> Result<()> {
        let ho = HexOrder::EFT;
        let computed = ho.to_db(
            Id::nil(Type::Edge),
            Id::nil(Type::Vertex),
            Id::max(Type::Vertex),
        )?;
        let expected = to_stdvec(&(
            HexOrder::EFT,
            Id::nil(Type::Edge),
            Id::max(Type::Vertex),
            Id::nil(Type::Vertex),
        ))?;

        assert_eq!(computed, expected);

        Ok(())
    }
}
