use crate::{StructType, Type};

use prost_types::value::Kind;
use prost_types::{ListValue, Value as SpannerValue};

#[derive(Debug, Clone, PartialEq)]
pub struct StructValue(pub Vec<(Option<String>, Value)>);

impl StructValue {
    pub fn try_from(tpe: &StructType, list_value: ListValue) -> Result<Self, crate::Error> {
        if tpe.0.len() != list_value.values.len() {
            Err(crate::Error::Codec(format!(
                "unmatched number of fields: expected {}, got {}",
                tpe.0.len(),
                list_value.values.len()
            )))
        } else {
            tpe.0
                .iter()
                .zip(list_value.values)
                .map(|((name, tpe), value)| {
                    Value::try_from(tpe, value).map(|value| (name.clone(), value))
                })
                .collect::<Result<Vec<(Option<String>, Value)>, crate::Error>>()
                .map(StructValue)
        }
    }
}

// https://github.com/googleapis/googleapis/blob/master/google/spanner/v1/type.proto
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null(Type),
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    // Bytes
    // Json
    // Numeric
    // Timestamp
    // Date
    Array(Vec<Value>),
    Struct(StructValue),
}

impl Value {
    pub fn try_from(tpe: &Type, value: SpannerValue) -> Result<Self, crate::Error> {
        if let Some(kind) = value.kind {
            match (tpe, kind) {
                (Type::Bool, Kind::BoolValue(b)) => Ok(Value::Bool(b)),
                (Type::Int64, Kind::StringValue(s)) => s
                    .parse::<i64>()
                    .map(Value::Int64)
                    .map_err(|_| crate::Error::Codec(format!("{} is not a valid Int64", s))),
                (Type::Float64, Kind::NumberValue(n)) => Ok(Value::Float64(n)),
                (Type::String, Kind::StringValue(s)) => Ok(Value::String(s)),
                (Type::Array(inner), Kind::ListValue(list_value)) => list_value
                    .values
                    .into_iter()
                    .map(|v| Value::try_from(inner, v))
                    .collect::<Result<Vec<Value>, crate::Error>>()
                    .map(Value::Array),
                (Type::Struct(row_type), Kind::ListValue(list_value)) => {
                    StructValue::try_from(row_type, list_value).map(Value::Struct)
                }
                _ => Err(crate::Error::Codec(format!(
                    "invalid value kind type {:?}",
                    tpe
                ))),
            }
        } else {
            Ok(Value::Null(tpe.clone()))
        }
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int64(v)
    }
}

impl From<Value> for SpannerValue {
    fn from(value: Value) -> Self {
        let kind = match value {
            Value::Null(tpe) => Kind::NullValue(tpe.code() as i32),
            Value::Bool(b) => Kind::BoolValue(b),
            Value::Int64(i) => Kind::StringValue(i.to_string()),
            Value::Float64(f) => Kind::NumberValue(f),
            Value::String(s) => Kind::StringValue(s),
            Value::Array(values) => Kind::ListValue(ListValue {
                values: values.into_iter().map(|v| v.into()).collect(),
            }),
            Value::Struct(StructValue(values)) => Kind::ListValue(ListValue {
                values: values.into_iter().map(|(_, value)| value.into()).collect(),
            }),
        };
        Self { kind: Some(kind) }
    }
}
