use crate::proto::google::spanner::v1 as proto;
use crate::{Error, StructType, Type};

use prost_types::value::Kind;
use prost_types::{ListValue, Value as SpannerValue};

#[derive(Debug, Clone, PartialEq)]
pub struct Struct(pub StructType, pub Vec<Value>);

impl Struct {
    pub(crate) fn try_from(tpe: &StructType, list_value: ListValue) -> Result<Self, crate::Error> {
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
                .map(|((_name, tpe), value)| Value::try_from(tpe, value))
                .collect::<Result<Vec<Value>, crate::Error>>()
                .map(|values| Struct(tpe.clone(), values))
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
    // Bytes,
    // Json,
    // Numeric,
    // Timestamp,
    // Date,
    Array(Vec<Value>),
    Struct(Struct),
}

fn name_of(kind: Kind) -> &'static str {
    match kind {
        Kind::BoolValue(_) => "BoolValue",
        Kind::ListValue(_) => "ListValue",
        Kind::NullValue(_) => "NullValue",
        Kind::NumberValue(_) => "NumberValue",
        Kind::StringValue(_) => "StringValue",
        Kind::StructValue(_) => "StructValue",
    }
}

impl Value {
    pub fn try_from(tpe: &Type, value: SpannerValue) -> Result<Self, crate::Error> {
        let kind = value
            .kind
            .ok_or_else(|| Error::Codec("unexpected missing value format".to_string()))?;

        if let Kind::NullValue(type_code) = kind {
            if let Some(type_code) = proto::TypeCode::from_i32(type_code) {
                if tpe.code() == type_code {
                    return Ok(Value::Null(tpe.clone()));
                }
            }
            return Err(Error::Codec(format!(
                "null value had unexpected type code {}, expected {}",
                type_code,
                tpe.code() as i32
            )));
        }

        match tpe {
            Type::Bool => {
                if let Kind::BoolValue(b) = kind {
                    return Ok(Value::Bool(b));
                }
            }
            Type::Int64 => {
                if let Kind::StringValue(s) = kind {
                    return s
                        .parse::<i64>()
                        .map(Value::Int64)
                        .map_err(|_| crate::Error::Codec(format!("{} is not a valid Int64", s)));
                }
            }
            Type::Float64 => {
                if let Kind::NumberValue(n) = kind {
                    return Ok(Value::Float64(n));
                }
            }
            Type::String => {
                if let Kind::StringValue(s) = kind {
                    return Ok(Value::String(s));
                }
            }
            Type::Array(inner) => {
                if let Kind::ListValue(list_value) = kind {
                    return list_value
                        .values
                        .into_iter()
                        .map(|v| Value::try_from(inner, v))
                        .collect::<Result<Vec<Value>, crate::Error>>()
                        .map(Value::Array);
                }
            }
            Type::Struct(struct_type) => {
                if let Kind::ListValue(list_value) = kind {
                    return Struct::try_from(struct_type, list_value).map(Value::Struct);
                }
            }
            Type::Bytes => todo!(),
            Type::Json => todo!(),
            Type::Numeric => todo!(),
            Type::Timestamp => todo!(),
            Type::Date => todo!(),
        }

        Err(Error::Codec(format!(
            "unexpected value kind {} for type {:?}",
            name_of(kind),
            tpe.code(),
        )))
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
            Value::Struct(Struct(_, values)) => Kind::ListValue(ListValue {
                values: values.into_iter().map(|value| value.into()).collect(),
            }),
        };
        Self { kind: Some(kind) }
    }
}
