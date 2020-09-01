use rmp_serde::{from_read_ref, Serializer};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::{FromDB, LogId, ToDB};
use crate::error::Result;

#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub(crate) enum HexOrder {
    TFE,
    FTE,
    ETF,
    EFT,
    TEF,
    FET,
}

pub(crate) static ORDERS: [HexOrder; 6] = [
    HexOrder::TFE,
    HexOrder::FTE,
    HexOrder::ETF,
    HexOrder::EFT,
    HexOrder::TEF,
    HexOrder::FET,
];

impl HexOrder {
    pub(crate) fn to_db(&self, id: LogId, to: LogId, from: LogId) -> Result<Vec<u8>> {
        let value = match self {
            HexOrder::TFE => (Self::TFE, to, from, id),
            HexOrder::FTE => (Self::FTE, from, to, id),
            HexOrder::ETF => (Self::ETF, id, to, from),
            HexOrder::EFT => (Self::EFT, id, from, to),
            HexOrder::TEF => (Self::TEF, to, id, from),
            HexOrder::FET => (Self::FET, from, id, to),
        };
        let mut buf = Vec::new();
        value.serialize(&mut Serializer::new(&mut buf))?;
        Ok(buf)
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Edge<Type, Value> {
    pub(crate) id: Option<LogId>,
    pub(crate) to: LogId,
    pub(crate) from: LogId,
    pub(crate) _type: Type,
    pub(crate) value: Value,
}

impl<Type, Value> Edge<Type, Value>
where
    Type: Serialize + DeserializeOwned + Clone,
    Value: Serialize + DeserializeOwned + Clone,
{
    pub fn new(to: LogId, from: LogId, _type: Type, value: Value) -> Result<Self> {
        Ok(Self {
            id: None,
            to,
            from,
            _type,
            value,
        })
    }

    pub fn get_value(&self) -> Value {
        self.value.clone()
    }
}

impl<Type, Value> FromDB<Value> for Edge<Type, Value>
where
    Type: DeserializeOwned,
    Value: DeserializeOwned,
{
    type Key = LogId;

    fn from_db(id: &Self::Key, data: &[u8]) -> Result<Self>
    where
        Self: Sized,
        Type: DeserializeOwned,
        Value: DeserializeOwned,
    {
        let edge = from_read_ref::<[u8], Self>(data)?;
        Ok(Self {
            id: Some(id.clone()),
            ..edge
        })
    }

    fn key_from_db(key: &[u8]) -> Result<Self::Key>
    where
        Self: Sized,
        Type: DeserializeOwned,
        Value: DeserializeOwned,
    {
        Ok(from_read_ref::<[u8], Self::Key>(key)?)
    }
}

impl<Type, Value> ToDB for Edge<Type, Value>
where
    Type: Serialize,
    Value: Serialize,
{
    type Key = LogId;

    fn to_db(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.serialize(&mut Serializer::new(&mut buf))?;
        Ok(buf)
    }

    fn value_to_db(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        (&self._type, &self.value).serialize(&mut Serializer::new(&mut buf))?;
        Ok(buf)
    }

    fn key(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        &self.id.serialize(&mut Serializer::new(&mut buf))?;
        Ok(buf)
    }

    fn key_to_db(key: &Self::Key) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        key.serialize(&mut Serializer::new(&mut buf))?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_serialize() -> Result<()> {
        let value = "Testing".to_string();

        let mut edge = Edge::new(
            LogId::nil(),
            LogId::nil(),
            "Name".to_string(),
            value.clone(),
        )?;
        edge.id = Some(LogId::nil());
        assert_eq!(edge.get_value(), value);
        assert_eq!(Edge::<String, String>::from_db(&LogId::nil(), edge.to_db()?.as_slice())?, edge);
        Ok(())
    }
}
