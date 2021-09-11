use crate::proto::google::spanner::v1::KeySet as SpannerKeySet;
use prost_types::value::Kind;
use prost_types::{ListValue, Value as SpannerValue};

// https://github.com/googleapis/googleapis/blob/master/google/spanner/v1/type.proto
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    // Bytes
    // Json
    // Numeric
    // Timestamp
    // Date
    // Array
    // Struct
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int64(v)
    }
}

#[allow(clippy::from_over_into)]
impl Into<SpannerValue> for Value {
    fn into(self) -> SpannerValue {
        let kind = match self {
            Value::Bool(b) => Kind::BoolValue(b),
            Value::Int64(i) => Kind::StringValue(i.to_string()),
            Value::Float64(f) => Kind::NumberValue(f),
            Value::String(s) => Kind::StringValue(s),
        };
        SpannerValue { kind: Some(kind) }
    }
}

pub struct Key(Vec<Value>);

impl From<Value> for Key {
    fn from(v: Value) -> Self {
        Key(vec![v])
    }
}
impl From<(Value, Value)> for Key {
    fn from(v: (Value, Value)) -> Self {
        let (a, b) = v;
        Key(vec![a, b])
    }
}

impl core::iter::FromIterator<Value> for ListValue {
    fn from_iter<T: IntoIterator<Item = Value>>(iter: T) -> Self {
        ListValue {
            values: iter.into_iter().map(|v| v.into()).collect(),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<ListValue> for Key {
    fn into(self) -> ListValue {
        self.0.into_iter().collect()
    }
}
pub struct KeySet {
    keys: Vec<Key>,
}

impl From<Vec<Key>> for KeySet {
    fn from(keys: Vec<Key>) -> Self {
        Self { keys }
    }
}

#[allow(clippy::from_over_into)]
impl Into<SpannerKeySet> for KeySet {
    fn into(self) -> SpannerKeySet {
        SpannerKeySet {
            all: false,
            keys: self.keys.into_iter().map(|v| v.into()).collect(),
            ranges: vec![],
        }
    }
}
