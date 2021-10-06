use std::str::FromStr;

use crate::{Error, StructType, Type};

use bigdecimal::BigDecimal;
use prost::bytes::Bytes;
use prost_types::value::Kind;
use prost_types::{ListValue, Value as SpannerValue};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Struct(StructType, Vec<Value>);

impl Struct {
    pub fn new(struct_type: StructType, values: Vec<Value>) -> Self {
        Self(struct_type, values)
    }

    pub fn struct_type(&self) -> &StructType {
        &self.0
    }
    pub fn values(&self) -> &Vec<Value> {
        &self.1
    }
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
    Bytes(Bytes),
    Json(String), // TODO: serde-json feature
    Numeric(BigDecimal),
    // Timestamp,
    // Date,
    Array(Type, Vec<Value>),
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
    pub fn r#type(&self) -> Type {
        match self {
            Value::Bool(_) => Type::Bool,
            Value::Null(inner) => inner.clone(),
            Value::Int64(_) => Type::Int64,
            Value::Float64(_) => Type::Float64,
            Value::String(_) => Type::String,
            Value::Bytes(_) => Type::Bytes,
            Value::Json(_) => Type::Json,
            Value::Numeric(_) => Type::Numeric,
            Value::Array(inner, _) => inner.clone(),
            Value::Struct(Struct(struct_type, _)) => Type::Struct(struct_type.clone()),
        }
    }
    pub(crate) fn try_from(tpe: &Type, value: SpannerValue) -> Result<Self, crate::Error> {
        let kind = value
            .kind
            .ok_or_else(|| Error::Codec("unexpected missing value format".to_string()))?;

        if let Kind::NullValue(_) = kind {
            return Ok(Value::Null(tpe.clone()));
            // TODO: this doesn't seem to work. Null values seem to have 0 as their type code
            // if let Some(type_code) = proto::TypeCode::from_i32(type_code) {
            //     if tpe.code() == type_code {
            //         return Ok(Value::Null(tpe.clone()));
            //     }
            // }
            // return Err(Error::Codec(format!(
            //     "null value had unexpected type code {}, expected {} ({:?})",
            //     type_code,
            //     tpe.code() as i32,
            //     tpe.code(),
            // )));
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
            Type::Numeric => {
                if let Kind::StringValue(s) = kind {
                    return BigDecimal::from_str(&s)
                        .map(Value::Numeric)
                        .map_err(|_| crate::Error::Codec(format!("{} is not a valid Numeric", s)));
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
                        .map(|values| Value::Array(inner.as_ref().clone(), values));
                }
            }
            Type::Struct(struct_type) => {
                if let Kind::ListValue(list_value) = kind {
                    return Struct::try_from(struct_type, list_value).map(Value::Struct);
                }
            }
            Type::Bytes => {
                if let Kind::StringValue(base64) = kind {
                    return base64::decode(base64)
                        .map_err(|e| Error::Codec(format!("invalid bytes value: {}", e)))
                        .map(|bytes| Value::Bytes(Bytes::from(bytes)));
                }
            }
            Type::Json => {
                if let Kind::StringValue(json) = kind {
                    return Ok(Value::Json(json));
                }
            }
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
            Value::Array(_, values) => Kind::ListValue(ListValue {
                values: values.into_iter().map(|v| v.into()).collect(),
            }),
            Value::Bool(b) => Kind::BoolValue(b),
            Value::Bytes(b) => Kind::StringValue(base64::encode(b)),
            Value::Float64(f) => Kind::NumberValue(f),
            Value::Int64(i) => Kind::StringValue(i.to_string()),
            Value::Json(json) => Kind::StringValue(json),
            Value::Null(tpe) => Kind::NullValue(tpe.code() as i32),
            Value::Numeric(n) => Kind::StringValue(n.to_string()),
            Value::String(s) => Kind::StringValue(s),
            Value::Struct(Struct(_, values)) => Kind::ListValue(ListValue {
                values: values.into_iter().map(|value| value.into()).collect(),
            }),
        };
        Self { kind: Some(kind) }
    }
}

#[cfg(test)]
mod test {

    use super::*;

    fn spanner_value(kind: Kind) -> SpannerValue {
        SpannerValue { kind: Some(kind) }
    }

    fn assert_try_from(tpe: Type, kind: Kind, expected: Value) {
        let value = Value::try_from(&tpe, spanner_value(kind)).unwrap();
        assert_eq!(value, expected);
    }

    fn assert_nullable(tpe: Type) {
        assert_try_from(
            tpe.clone(),
            Kind::NullValue(tpe.code() as i32),
            Value::Null(tpe),
        );
    }

    fn assert_invalid(tpe: Type, kind: Kind) {
        let value = Value::try_from(&tpe, spanner_value(kind));
        assert!(value.is_err(), "unexpected Ok");
    }

    #[test]
    fn test_value_array() {
        assert_try_from(
            Type::Array(Box::new(Type::Bool)),
            Kind::ListValue(ListValue {
                values: vec![
                    spanner_value(Kind::BoolValue(true)),
                    spanner_value(Kind::BoolValue(false)),
                ],
            }),
            Value::Array(Type::Bool, vec![Value::Bool(true), Value::Bool(false)]),
        );
        assert_nullable(Type::Array(Box::new(Type::Bool)));
        assert_invalid(Type::Array(Box::new(Type::Bool)), Kind::BoolValue(true));
    }

    #[test]
    fn test_value_bool() {
        assert_try_from(Type::Bool, Kind::BoolValue(true), Value::Bool(true));
        assert_try_from(Type::Bool, Kind::BoolValue(false), Value::Bool(false));
        assert_nullable(Type::Bool);
        assert_invalid(Type::Bool, Kind::NumberValue(6.0));
    }

    #[test]
    fn test_value_bytes() {
        assert_try_from(
            Type::Bytes,
            Kind::StringValue(base64::encode(vec![1, 2, 3, 4])),
            Value::Bytes(Bytes::from(vec![1, 2, 3, 4])),
        );
        assert_try_from(
            Type::Bytes,
            Kind::StringValue(String::new()),
            Value::Bytes(Bytes::new()),
        );
        assert_nullable(Type::Bytes);
        assert_invalid(Type::Bytes, Kind::NumberValue(6.0));
    }

    #[test]
    fn test_value_float64() {
        assert_try_from(Type::Float64, Kind::NumberValue(42.0), Value::Float64(42.0));
        assert_try_from(
            Type::Float64,
            Kind::NumberValue(f64::MAX),
            Value::Float64(f64::MAX),
        );
        assert_try_from(
            Type::Float64,
            Kind::NumberValue(f64::MIN),
            Value::Float64(f64::MIN),
        );
        assert_try_from(
            Type::Float64,
            Kind::NumberValue(f64::NEG_INFINITY),
            Value::Float64(f64::NEG_INFINITY),
        );
        assert_try_from(
            Type::Float64,
            Kind::NumberValue(f64::INFINITY),
            Value::Float64(f64::INFINITY),
        );
        assert_nullable(Type::Float64);
        assert_invalid(Type::Float64, Kind::BoolValue(true));
        assert_invalid(
            Type::Float64,
            Kind::StringValue("this is not a number".to_string()),
        );
    }

    #[test]
    fn test_value_int64() {
        assert_try_from(
            Type::Int64,
            Kind::StringValue("42".to_string()),
            Value::Int64(42),
        );
        assert_try_from(
            Type::Int64,
            Kind::StringValue(i64::MAX.to_string()),
            Value::Int64(i64::MAX),
        );
        assert_try_from(
            Type::Int64,
            Kind::StringValue(i64::MIN.to_string()),
            Value::Int64(i64::MIN),
        );
        assert_nullable(Type::Int64);
        assert_invalid(Type::Int64, Kind::NumberValue(6.0));
        assert_invalid(Type::Int64, Kind::StringValue(f64::MAX.to_string()));
        assert_invalid(Type::Int64, Kind::StringValue(u64::MAX.to_string()));
        assert_invalid(
            Type::Int64,
            Kind::StringValue("this is not a number".to_string()),
        );
    }

    #[test]
    fn test_value_json() {
        assert_try_from(
            Type::Json,
            Kind::StringValue("this is json".to_string()),
            Value::Json("this is json".to_string()),
        );
        assert_nullable(Type::Json);
        assert_invalid(Type::Json, Kind::BoolValue(true));
    }

    #[test]
    fn test_value_numeric() {
        assert_try_from(
            Type::Numeric,
            Kind::StringValue(
                "987654321098765432109876543210.987654321098765432109876543210".to_string(),
            ),
            Value::Numeric(
                BigDecimal::parse_bytes(
                    "987654321098765432109876543210.987654321098765432109876543210".as_bytes(),
                    10,
                )
                .unwrap(),
            ),
        );
        assert_try_from(
            Type::Numeric,
            Kind::StringValue("1e-24".to_string()),
            Value::Numeric(BigDecimal::parse_bytes("1e-24".as_bytes(), 10).unwrap()),
        );
        assert_nullable(Type::Numeric);
        assert_invalid(Type::Numeric, Kind::NumberValue(6.0));
        assert_invalid(
            Type::Numeric,
            Kind::StringValue("this is not a number".to_string()),
        );
    }

    #[test]
    fn test_value_string() {
        assert_try_from(
            Type::String,
            Kind::StringValue("this is a string".to_string()),
            Value::String("this is a string".to_string()),
        );
        assert_nullable(Type::String);
        assert_invalid(Type::String, Kind::BoolValue(true));
    }

    #[test]
    fn test_value_struct() {
        let test_tpe = Type::strct(vec![
            ("bool", Type::Bool),
            ("int64", Type::Int64),
            ("string", Type::String),
            ("null", Type::Float64),
        ]);
        assert_try_from(
            test_tpe.clone(),
            Kind::ListValue(ListValue {
                values: vec![
                    spanner_value(Kind::BoolValue(true)),
                    spanner_value(Kind::StringValue("42".to_string())),
                    spanner_value(Kind::StringValue("this is a string".to_string())),
                    spanner_value(Kind::NullValue(Type::Float64.code() as i32)),
                ],
            }),
            Value::Struct(Struct(
                StructType(vec![
                    (Some("bool".to_string()), Type::Bool),
                    (Some("int64".to_string()), Type::Int64),
                    (Some("string".to_string()), Type::String),
                    (Some("null".to_string()), Type::Float64),
                ]),
                vec![
                    Value::Bool(true),
                    Value::Int64(42),
                    Value::String("this is a string".to_string()),
                    Value::Null(Type::Float64),
                ],
            )),
        );
        assert_nullable(test_tpe.clone());
        assert_invalid(test_tpe, Kind::BoolValue(true));
    }
}
