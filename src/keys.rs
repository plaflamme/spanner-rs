use prost_types::ListValue;

use crate::proto::google::spanner::v1::KeySet as SpannerKeySet;
use crate::Value;

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
