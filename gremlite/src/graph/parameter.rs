use super::{Edge, Id, Type, Vertex, Writable};
use crate::error::Result;

use chrono::{offset::Utc, serde::ts_nanoseconds, DateTime};
use heed::{BytesDecode, BytesEncode};
use postcard::{from_bytes, to_stdvec};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{borrow::Cow, collections::HashMap};
use ulid::Ulid;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum PValue<V = String, E = String, P = String>
where
    V: Writable,
    E: Writable,
    P: Writable + Eq,
{
    None,
    #[serde(bound(deserialize = "V: DeserializeOwned, E: DeserializeOwned, P: DeserializeOwned"))]
    Vertex(Vertex<V, E, P>),
    #[serde(bound(deserialize = "V: DeserializeOwned, E: DeserializeOwned, P: DeserializeOwned"))]
    Edge(Edge<V, E, P>),

    Id(Id),
    Ulid(Ulid),
    Type(Type),
    I32(i32),
    I64(i64),
    I128(i128),
    Float(f32),
    Double(f64),
    #[serde(with = "ts_nanoseconds")]
    Date(DateTime<Utc>),
    Token(String),
    String(String),
    Bool(bool),

    // From gremlin_client that I'm not ready to do yet
    // Path
    // Metrics
    // TraversalMetrics
    // TraversalExplanation
    // IntermediateRepr
    // Predicate
    // TextPredicate
    // T
    // Bytecode
    // Traverser
    // Scope
    // Order
    // Pop
    // Cardinality
    #[serde(bound(deserialize = "V: DeserializeOwned, E: DeserializeOwned, P: DeserializeOwned"))]
    List(Vec<PValue<V, E, P>>),
    // TODO: Is there a better type?
    #[serde(bound(deserialize = "V: DeserializeOwned, E: DeserializeOwned, P: DeserializeOwned"))]
    Set(Vec<PValue<V, E, P>>),
    #[serde(bound(deserialize = "V: DeserializeOwned, E: DeserializeOwned, P: DeserializeOwned"))]
    Map(HashMap<P, PValue<V, E, P>>),
}

impl<V, E, P> Default for PValue<V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable,
{
    fn default() -> Self {
        Self::None
    }
}

impl<'a, V, E, P> BytesEncode<'a> for PValue<V, E, P>
where
    V: 'a + Writable,
    E: 'a + Writable,
    P: 'a + Writable,
{
    type EItem = Self;
    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        to_stdvec(item).map(Cow::Owned).ok()
    }
}

impl<'a, V, E, P> BytesDecode<'a> for PValue<V, E, P>
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

pub trait FromPValue<V, E, P>: Sized
where
    V: Writable,
    E: Writable,
    P: Writable,
{
    fn from_pvalue(v: PValue<V, E, P>) -> Result<Self>;
}

impl<V, E, P> FromPValue<V, E, P> for PValue<V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable,
{
    fn from_pvalue(v: Self) -> Result<Self> {
        Ok(v)
    }
}

pub trait ToPValue<V, E, P>: Sized
where
    V: Writable,
    E: Writable,
    P: Writable,
{
    fn to_pvalue(&self) -> PValue<V, E, P>;
}

impl<V, E, P> ToPValue<V, E, P> for PValue<V, E, P>
where
    V: Writable,
    E: Writable,
    P: Writable,
{
    fn to_pvalue(&self) -> Self {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_none_id() -> Result<()> {
        let pv = PValue::<String, String, String>::default();
        assert_eq!(pv, PValue::None);

        assert_eq!(pv, pv.to_pvalue());

        Ok(())
    }
}
