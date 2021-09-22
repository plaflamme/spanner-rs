use crate::proto::google::spanner::v1 as proto;

use std::{collections::HashMap, convert::TryFrom};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StructType(pub Vec<(Option<String>, Type)>);

impl StructType {
    pub fn fields(&self) -> &Vec<(Option<String>, Type)> {
        &self.0
    }

    pub(crate) fn type_by_name(&self) -> Result<HashMap<String, Type>, crate::Error> {
        self.fields()
            .iter()
            .cloned()
            .map(|(name, tpe)| match name {
                Some(name) => Ok((name, tpe)),
                None => Err(crate::Error::Client("unamed field in struct".to_string())),
            })
            .collect()
    }
}

impl TryFrom<proto::StructType> for StructType {
    type Error = crate::Error;

    fn try_from(value: proto::StructType) -> Result<Self, Self::Error> {
        StructType::try_from(&value)
    }
}

impl TryFrom<&proto::StructType> for StructType {
    type Error = crate::Error;

    fn try_from(value: &proto::StructType) -> Result<Self, Self::Error> {
        value
            .fields
            .iter()
            .map(|field| {
                field
                    .r#type
                    .as_ref()
                    .ok_or_else(|| {
                        Self::Error::Codec(format!("field '{}' is missing type", field.name))
                    })
                    .and_then(Type::try_from)
                    .map(|tpe| (Some(field.name.clone()), tpe))
            })
            .collect::<Result<Vec<(Option<String>, Type)>, Self::Error>>()
            .map(StructType)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Type {
    Bool,
    Int64,
    Float64,
    String,
    Bytes,
    Json,
    Numeric,
    Timestamp,
    Date,
    Array(Box<Type>),
    Struct(StructType),
}

impl Type {
    pub fn array(inner: Type) -> Self {
        Type::Array(Box::new(inner))
    }

    pub fn strct(fields: Vec<(&str, Type)>) -> Self {
        Type::Struct(StructType(
            fields
                .into_iter()
                .map(|(name, tpe)| (Some(name.to_string()), tpe))
                .collect(),
        ))
    }

    pub(crate) fn code(&self) -> proto::TypeCode {
        match self {
            Type::Bool => proto::TypeCode::Bool,
            Type::Int64 => proto::TypeCode::Int64,
            Type::Float64 => proto::TypeCode::Float64,
            Type::String => proto::TypeCode::String,
            Type::Bytes => proto::TypeCode::Bytes,
            Type::Json => proto::TypeCode::Json,
            Type::Numeric => proto::TypeCode::Numeric,
            Type::Timestamp => proto::TypeCode::Timestamp,
            Type::Date => proto::TypeCode::Date,
            Type::Array(_) => proto::TypeCode::Array,
            Type::Struct(_) => proto::TypeCode::Struct,
        }
    }
}

impl TryFrom<proto::Type> for Type {
    type Error = crate::Error;

    fn try_from(value: proto::Type) -> Result<Self, Self::Error> {
        Type::try_from(&value)
    }
}

impl TryFrom<&proto::Type> for Type {
    type Error = crate::Error;

    fn try_from(value: &proto::Type) -> Result<Self, Self::Error> {
        match proto::TypeCode::from_i32(value.code) {
            Some(proto::TypeCode::Bool) => Ok(Type::Bool),
            Some(proto::TypeCode::Int64) => Ok(Type::Int64),
            Some(proto::TypeCode::Float64) => Ok(Type::Float64),
            Some(proto::TypeCode::String) => Ok(Type::String),
            Some(proto::TypeCode::Bytes) => Ok(Type::Bytes),
            Some(proto::TypeCode::Json) => Ok(Type::Json),
            Some(proto::TypeCode::Numeric) => Ok(Type::Numeric),
            Some(proto::TypeCode::Timestamp) => Ok(Type::Timestamp),
            Some(proto::TypeCode::Date) => Ok(Type::Date),
            Some(proto::TypeCode::Array) => value
                .array_element_type
                .as_ref()
                .ok_or_else(|| Self::Error::Codec("missing array element type".to_string()))
                .and_then(|tpe| Type::try_from(tpe.as_ref()))
                .map(|tpe| Type::Array(Box::new(tpe))),

            Some(proto::TypeCode::Struct) => value
                .struct_type
                .as_ref()
                .ok_or_else(|| Self::Error::Codec("missing struct type definition".to_string()))
                .and_then(StructType::try_from)
                .map(Type::Struct),
            Some(proto::TypeCode::Unspecified) => {
                Err(Self::Error::Codec("unspecified type".to_string()))
            }
            None => Err(Self::Error::Codec(format!(
                "unknown type code {}",
                value.code
            ))),
        }
    }
}

impl From<&Type> for proto::Type {
    fn from(value: &Type) -> Self {
        match value {
            Type::Array(inner) => proto::Type {
                code: value.code() as i32,
                array_element_type: Some(Box::new((*inner).as_ref().into())),
                struct_type: None,
            },
            Type::Struct(StructType(fields)) => proto::Type {
                code: value.code() as i32,
                array_element_type: None,
                struct_type: Some(proto::StructType {
                    fields: fields
                        .into_iter()
                        .map(|(name, tpe)| proto::struct_type::Field {
                            name: name.clone().unwrap_or_default(),
                            r#type: Some(tpe.into()),
                        })
                        .collect(),
                }),
            },
            other => proto::Type {
                code: other.code() as i32,
                array_element_type: None,
                struct_type: None,
            },
        }
    }
}

impl From<Type> for proto::Type {
    fn from(value: Type) -> Self {
        From::from(&value)
    }
}

#[cfg(test)]
mod test {

    use crate::proto::google::spanner::v1 as proto;

    use super::*;

    fn scalar_type(code: proto::TypeCode) -> proto::Type {
        proto::Type {
            code: code as i32,
            array_element_type: None,
            struct_type: None,
        }
    }

    fn array_type(underlying: proto::Type) -> proto::Type {
        proto::Type {
            code: proto::TypeCode::Array as i32,
            array_element_type: Some(Box::new(underlying)),
            struct_type: None,
        }
    }

    fn struct_type(fields: Vec<(&str, proto::Type)>) -> proto::Type {
        proto::Type {
            code: proto::TypeCode::Struct as i32,
            array_element_type: None,
            struct_type: Some(proto::StructType {
                fields: fields
                    .iter()
                    .map(|(name, tpe)| proto::struct_type::Field {
                        name: name.to_string(),
                        r#type: Some(tpe.clone()),
                    })
                    .collect(),
            }),
        }
    }

    fn test_scalar(code: proto::TypeCode, expected: Type) {
        assert_eq!(Type::try_from(scalar_type(code)).unwrap(), expected);
        assert_eq!(proto::Type::from(expected).code, code as i32)
    }

    #[test]
    fn test_try_from_scalar() {
        test_scalar(proto::TypeCode::Bool, Type::Bool);
        test_scalar(proto::TypeCode::Int64, Type::Int64);
        test_scalar(proto::TypeCode::Float64, Type::Float64);
        test_scalar(proto::TypeCode::String, Type::String);
        test_scalar(proto::TypeCode::Bytes, Type::Bytes);
        test_scalar(proto::TypeCode::Json, Type::Json);
        test_scalar(proto::TypeCode::Numeric, Type::Numeric);
        test_scalar(proto::TypeCode::Timestamp, Type::Timestamp);
        test_scalar(proto::TypeCode::Date, Type::Date);
    }

    fn test_array_of_scalar(code: proto::TypeCode, inner: Type) {
        let expected = Type::Array(Box::new(inner.clone()));
        assert_eq!(
            Type::try_from(array_type(scalar_type(code))).unwrap(),
            expected.clone(),
        );
        assert_eq!(
            proto::Type::from(expected.clone()),
            proto::Type {
                code: proto::TypeCode::Array as i32,
                array_element_type: Some(Box::new(inner.into())),
                struct_type: None,
            }
        )
    }

    #[test]
    fn test_try_from_array() {
        test_array_of_scalar(proto::TypeCode::Bool, Type::Bool);
        test_array_of_scalar(proto::TypeCode::Int64, Type::Int64);
        test_array_of_scalar(proto::TypeCode::Float64, Type::Float64);
        test_array_of_scalar(proto::TypeCode::String, Type::String);
        test_array_of_scalar(proto::TypeCode::Bytes, Type::Bytes);
        test_array_of_scalar(proto::TypeCode::Json, Type::Json);
        test_array_of_scalar(proto::TypeCode::Numeric, Type::Numeric);
        test_array_of_scalar(proto::TypeCode::Timestamp, Type::Timestamp);
        test_array_of_scalar(proto::TypeCode::Date, Type::Date);

        assert_eq!(
            Type::try_from(array_type(array_type(scalar_type(proto::TypeCode::Bool)))).unwrap(),
            Type::array(Type::array(Type::Bool)),
        );

        let invalid = proto::Type {
            code: proto::TypeCode::Array as i32,
            array_element_type: None,
            struct_type: None,
        };

        assert!(Type::try_from(invalid).is_err());
    }

    #[test]
    fn test_try_from_struct() {
        assert_eq!(
            Type::try_from(struct_type(vec![])).unwrap(),
            Type::strct(vec![])
        );
        assert_eq!(
            Type::try_from(struct_type(vec![(
                "bool",
                scalar_type(proto::TypeCode::Bool)
            )]))
            .unwrap(),
            Type::strct(vec![("bool", Type::Bool)]),
        );
        assert_eq!(
            Type::try_from(struct_type(vec![(
                "array_of_bools",
                array_type(scalar_type(proto::TypeCode::Bool))
            )]))
            .unwrap(),
            Type::strct(vec![("array_of_bools", Type::array(Type::Bool))]),
        );
        assert_eq!(
            Type::try_from(struct_type(vec![
                ("bool", scalar_type(proto::TypeCode::Bool)),
                (
                    "struct",
                    struct_type(vec![("int64", scalar_type(proto::TypeCode::Int64))])
                ),
            ]))
            .unwrap(),
            Type::strct(vec![
                ("bool", Type::Bool),
                ("struct", Type::strct(vec![("int64", Type::Int64)]))
            ]),
        );

        assert_eq!(
            proto::Type::from(struct_type(vec![(
                "bool",
                scalar_type(proto::TypeCode::Bool)
            )])),
            proto::Type {
                code: proto::TypeCode::Struct as i32,
                array_element_type: None,
                struct_type: Some(proto::StructType {
                    fields: vec![proto::struct_type::Field {
                        name: "bool".to_string(),
                        r#type: Some(proto::Type {
                            code: proto::TypeCode::Bool as i32,
                            array_element_type: None,
                            struct_type: None,
                        })
                    }]
                })
            }
        );

        let invalid = proto::Type {
            code: proto::TypeCode::Struct as i32,
            array_element_type: None,
            struct_type: None,
        };

        assert!(Type::try_from(invalid).is_err());
    }
}
