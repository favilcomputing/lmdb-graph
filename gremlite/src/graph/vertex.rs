use heed::{BytesDecode, BytesEncode};
use postcard::{from_bytes, to_stdvec};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::{
    parameter::{FromPValue, PValue, ToPValue},
    Id, Writable,
};
use crate::error::{Error, Result};
use std::{borrow::Cow, clone::Clone, collections::HashMap, fmt::Debug};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Vertex<V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable + Eq,
{
    pub(crate) id: Option<Id>,

    #[serde(bound(deserialize = "V: DeserializeOwned"))]
    pub(crate) label: V,
    #[serde(bound(deserialize = "P: DeserializeOwned"))]
    pub(crate) parameters: HashMap<P, PValue<V, E, P>>,
}

impl<V, E, P> Vertex<V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable + Eq,
{
    pub fn new(label: V) -> Self {
        Self {
            id: None,
            label,
            parameters: Default::default(),
        }
    }

    pub fn get_label(&self) -> V {
        self.label.clone()
    }

    pub fn get_id(&self) -> Option<Id> {
        self.id
    }

    pub fn set_param(mut self, p: P, val: PValue<V, E, P>) -> Self
where {
        self.parameters.insert(p, val);
        self
    }
}

impl<'a, V, E, P> BytesEncode<'a> for Vertex<V, E, P>
where
    V: 'a + Writable,
    E: 'a + Writable,
    P: 'a + Writable + Eq,
{
    type EItem = Self;

    fn bytes_encode(item: &'a Self::EItem) -> Option<std::borrow::Cow<'a, [u8]>> {
        match to_stdvec(item).ok() {
            Some(vec) => Some(Cow::Owned(vec)),
            None => None,
        }
    }
}

impl<'a, V, E, P> BytesDecode<'a> for Vertex<V, E, P>
where
    V: 'a + Writable,
    E: 'a + Writable,
    P: 'a + Writable + Eq,
{
    type DItem = Self;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        from_bytes(bytes).ok()
    }
}

impl<V, E, P> FromPValue<V, E, P> for Vertex<V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable + Eq,
{
    fn from_pvalue(v: PValue<V, E, P>) -> Result<Self> {
        match v {
            PValue::Vertex(v) => Ok(v),
            _ => Err(Error::InvalidPValue(format!("{:#?}", v))),
        }
    }
}

impl<V, E, P> ToPValue<V, E, P> for Vertex<V, E, P>
where
    V: 'static + Writable,
    E: 'static + Writable,
    P: 'static + Writable + Eq,
{
    fn to_pvalue(&self) -> PValue<V, E, P> {
        PValue::Vertex(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::graph::Type;

    #[rstest]
    fn test_serialize() {
        let label = "Testing".to_string();
        let vertex = Vertex::<_, (), ()>::new(label.clone());
        assert_eq!(vertex.get_label(), label);
        assert_eq!(vertex.id, None);
    }

    #[rstest]
    fn test_to_pvalue() -> Result<()> {
        let mut v = Vertex::<String, String, String>::new("v".into());
        v.id = Some(Id::nil(Type::Vertex));

        let pvalue = v.to_pvalue();
        let decoded = Vertex::from_pvalue(pvalue)?;

        assert_eq!(decoded, v);

        Ok(())
    }

    #[rstest]
    fn test_from_pvalue() -> Result<()> {
        let pvalue = PValue::<String, String, String>::default();

        let decoded = Vertex::from_pvalue(pvalue);

        assert!(decoded.is_err());

        Ok(())
    }
}
