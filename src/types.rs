use crate::proto::google::spanner::v1::StructType as SpannerStructType;
use crate::proto::google::spanner::v1::Type as SpannerType;
use crate::proto::google::spanner::v1::TypeCode;

use std::convert::TryFrom;

#[derive(Clone, Debug, PartialEq)]
pub struct StructType(pub Vec<(Option<String>, Type)>);

impl TryFrom<SpannerStructType> for StructType {
    type Error = crate::Error;

    fn try_from(value: SpannerStructType) -> Result<Self, Self::Error> {
        value
            .fields
            .iter()
            .map(|field| {
                field
                    .r#type
                    .as_ref()
                    .ok_or_else(|| Self::Error::Codec("missing type".to_string()))
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

    pub(crate) fn code(&self) -> TypeCode {
        match self {
            Type::Bool => TypeCode::Bool,
            Type::Int64 => TypeCode::Int64,
            Type::Float64 => TypeCode::Float64,
            Type::String => TypeCode::String,
            Type::Bytes => TypeCode::Bytes,
            Type::Json => TypeCode::Json,
            Type::Numeric => TypeCode::Numeric,
            Type::Timestamp => TypeCode::Timestamp,
            Type::Date => TypeCode::Date,
            Type::Array(_) => TypeCode::Array,
            Type::Struct(_) => TypeCode::Struct,
        }
    }
}

impl TryFrom<&SpannerType> for Type {
    type Error = crate::Error;

    fn try_from(value: &SpannerType) -> Result<Self, Self::Error> {
        match TypeCode::from_i32(value.code) {
            Some(TypeCode::Bool) => Ok(Type::Bool),
            Some(TypeCode::Int64) => Ok(Type::Int64),
            Some(TypeCode::Float64) => Ok(Type::Float64),
            Some(TypeCode::String) => Ok(Type::String),
            Some(TypeCode::Bytes) => Ok(Type::Bytes),
            Some(TypeCode::Json) => Ok(Type::Json),
            Some(TypeCode::Numeric) => Ok(Type::Numeric),
            Some(TypeCode::Timestamp) => Ok(Type::Timestamp),
            Some(TypeCode::Date) => Ok(Type::Date),
            Some(TypeCode::Array) => value
                .array_element_type
                .as_ref()
                .ok_or_else(|| Self::Error::Codec("missing array element type".to_string()))
                .and_then(|tpe| Type::try_from(tpe.as_ref()))
                .map(|tpe| Type::Array(Box::new(tpe))),

            Some(TypeCode::Struct) => value
                .struct_type
                .clone()
                .ok_or_else(|| Self::Error::Codec("missing struct type definition".to_string()))
                .and_then(StructType::try_from)
                .map(Type::Struct),
            Some(TypeCode::Unspecified) => Err(Self::Error::Codec("unspecified type".to_string())),
            None => Err(Self::Error::Codec("unknown type code".to_string())),
        }
    }
}

impl TryFrom<SpannerType> for Type {
    type Error = crate::Error;

    fn try_from(value: SpannerType) -> Result<Self, Self::Error> {
        Type::try_from(&value)
    }
}

#[cfg(test)]
mod test {

    use crate::proto::google::spanner::v1::struct_type::Field;
    use crate::proto::google::spanner::v1::StructType;

    use super::*;

    fn scalar_type(code: TypeCode) -> SpannerType {
        SpannerType {
            code: code as i32,
            array_element_type: None,
            struct_type: None,
        }
    }

    fn array_type(underlying: SpannerType) -> SpannerType {
        SpannerType {
            code: TypeCode::Array as i32,
            array_element_type: Some(Box::new(underlying)),
            struct_type: None,
        }
    }

    fn struct_type(fields: Vec<(&str, SpannerType)>) -> SpannerType {
        SpannerType {
            code: TypeCode::Struct as i32,
            array_element_type: None,
            struct_type: Some(StructType {
                fields: fields
                    .iter()
                    .map(|(name, tpe)| Field {
                        name: name.to_string(),
                        r#type: Some(tpe.clone()),
                    })
                    .collect(),
            }),
        }
    }

    fn test_scalar(code: TypeCode, expected: Type) {
        assert_eq!(Type::try_from(scalar_type(code)).unwrap(), expected);
    }

    #[test]
    fn test_try_from_scalar() {
        test_scalar(TypeCode::Bool, Type::Bool);
        test_scalar(TypeCode::Int64, Type::Int64);
        test_scalar(TypeCode::Float64, Type::Float64);
        test_scalar(TypeCode::String, Type::String);
        test_scalar(TypeCode::Bytes, Type::Bytes);
        test_scalar(TypeCode::Json, Type::Json);
        test_scalar(TypeCode::Numeric, Type::Numeric);
        test_scalar(TypeCode::Timestamp, Type::Timestamp);
        test_scalar(TypeCode::Date, Type::Date);
    }

    fn test_array_of_scalar(code: TypeCode, expected: Type) {
        assert_eq!(
            Type::try_from(array_type(scalar_type(code))).unwrap(),
            Type::Array(Box::new(expected))
        );
    }

    #[test]
    fn test_try_from_array() {
        test_array_of_scalar(TypeCode::Bool, Type::Bool);
        test_array_of_scalar(TypeCode::Int64, Type::Int64);
        test_array_of_scalar(TypeCode::Float64, Type::Float64);
        test_array_of_scalar(TypeCode::String, Type::String);
        test_array_of_scalar(TypeCode::Bytes, Type::Bytes);
        test_array_of_scalar(TypeCode::Json, Type::Json);
        test_array_of_scalar(TypeCode::Numeric, Type::Numeric);
        test_array_of_scalar(TypeCode::Timestamp, Type::Timestamp);
        test_array_of_scalar(TypeCode::Date, Type::Date);

        assert_eq!(
            Type::try_from(array_type(array_type(scalar_type(TypeCode::Bool)))).unwrap(),
            Type::array(Type::array(Type::Bool)),
        );

        let invalid = SpannerType {
            code: TypeCode::Array as i32,
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
            Type::try_from(struct_type(vec![("bool", scalar_type(TypeCode::Bool))])).unwrap(),
            Type::strct(vec![("bool", Type::Bool)]),
        );
        assert_eq!(
            Type::try_from(struct_type(vec![(
                "array_of_bools",
                array_type(scalar_type(TypeCode::Bool))
            )]))
            .unwrap(),
            Type::strct(vec![("array_of_bools", Type::array(Type::Bool))]),
        );
        assert_eq!(
            Type::try_from(struct_type(vec![
                ("bool", scalar_type(TypeCode::Bool)),
                (
                    "struct",
                    struct_type(vec![("int64", scalar_type(TypeCode::Int64))])
                ),
            ]))
            .unwrap(),
            Type::strct(vec![
                ("bool", Type::Bool),
                ("struct", Type::strct(vec![("int64", Type::Int64)]))
            ]),
        );

        let invalid = SpannerType {
            code: TypeCode::Struct as i32,
            array_element_type: None,
            struct_type: None,
        };

        assert!(Type::try_from(invalid).is_err());
    }
}
