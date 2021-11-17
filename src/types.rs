use googapis::google::spanner::v1 as proto;

use std::convert::TryFrom;

/// The Cloud Spanner [`Struct`](https://cloud.google.com/spanner/docs/data-types#struct_type) type which is composed of optionally named fields and their data type.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StructType(Vec<(Option<String>, Type)>);

impl StructType {
    /// Creates a new `StructType` with the provided fields.
    ///
    /// Note that Cloud Spanner allows "unnamed" fields. If a provided field name is the empty string,
    /// it will be converted to a `None` in the resulting `StructType`.
    pub fn new(fields: Vec<(&str, Type)>) -> Self {
        Self(
            fields
                .into_iter()
                .map(|(name, tpe)| {
                    let field_name = if !name.is_empty() {
                        Some(name.to_string())
                    } else {
                        None
                    };
                    (field_name, tpe)
                })
                .collect(),
        )
    }

    /// Returns a reference to this struct's fields.
    pub fn fields(&self) -> &Vec<(Option<String>, Type)> {
        &self.0
    }

    /// Returns an iterator over the names of this struct's fields.
    pub fn field_names(&self) -> impl Iterator<Item = &Option<String>> {
        self.0.iter().map(|(name, _)| name)
    }

    /// Returns an iterator over the types of this struct's fields.
    pub fn types(&self) -> impl Iterator<Item = &Type> {
        self.0.iter().map(|(_, tpe)| tpe)
    }

    /// Returns the index of the provided field name.
    /// Returns `None` if no field matches the provided name.
    /// Note that this function ignores unnamed fields.
    pub fn field_index(&self, field_name: &str) -> Option<usize> {
        self.0.iter().position(|(name, _)| match name {
            Some(col) => *col == field_name,
            None => false,
        })
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

/// An enumeration of all Cloud Spanner [data types](https://cloud.google.com/spanner/docs/data-types).
///
/// Refer to the Cloud Spanner documentation for detailed information about individual data types.
#[derive(Clone, Debug, PartialEq)]
pub enum Type {
    /// The [`BOOL`](https://cloud.google.com/spanner/docs/data-types#boolean_type) data type.
    ///
    /// * Storage size: 1 byte
    Bool,

    /// The [`INT64`](https://cloud.google.com/spanner/docs/data-types#integer_type) data type.
    ///
    /// * Storage size: 8 bytes
    /// * Range: `-9,223,372,036,854,775,808` to `9,223,372,036,854,775,807`
    Int64,

    /// The [`FLOAT64`](https://cloud.google.com/spanner/docs/data-types#floating_point_types) data type.
    ///
    /// Supports the special `NaN`, `+inf` and `-inf` values.
    ///
    /// * Storage size: 8 bytes
    Float64,

    /// The [`STRING`](https://cloud.google.com/spanner/docs/data-types#string_type) data type.
    ///
    /// Must be valid UTF-8.
    ///
    /// * Storage: the number of bytes in its UTF-8 encoding
    String,

    /// The [`BYTES`](https://cloud.google.com/spanner/docs/data-types#bytes_type) data type.
    ///
    /// * Storage: the number of bytes
    Bytes,

    /// The [`JSON`](https://cloud.google.com/spanner/docs/data-types#json_type) data type.
    ///
    /// Note that the JSON document will be canonicalized before storing. Refer to the Cloud Spanner for details.
    ///
    /// * Storage: The number of bytes in UTF-8 encoding of the JSON-formatted string equivalent after canonicalization.
    #[cfg(feature = "json")]
    Json,

    /// The [`NUMERIC`](https://cloud.google.com/spanner/docs/data-types#numeric_type) data type.
    ///
    /// * Storage: varies between 6 and 22 bytes, except for the value 0 which uses 1 byte.
    #[cfg(feature = "numeric")]
    Numeric,

    /// The [`TIMESTAMP`](https://cloud.google.com/spanner/docs/data-types#timestamp_type) data type.
    ///
    /// Refer to the Cloud Spanner documentation for details on timezones and format when used in SQL statements.
    ///
    /// * Storage: 12 bytes
    /// * Range: `0001-01-01 00:00:00` to `9999-12-31 23:59:59.999999999` UTC.
    #[cfg(feature = "temporal")]
    Timestamp,

    /// The [`DATE`](https://cloud.google.com/spanner/docs/data-types#date_type) data type.
    ///
    /// * Storage: 4 bytes
    /// * Range: `0001-01-01` to `9999-12-31`.
    /// * Canonical format: `YYYY-[M]M-[D]D`
    #[cfg(feature = "temporal")]
    Date,

    /// The [`ARRAY`](https://cloud.google.com/spanner/docs/data-types#array_type) data type.
    /// Can contain elements of any other type except `Array` (i.e.: arrays of arrays are not allowed).
    /// Can contain `NULL` elements.
    /// A `NULL` value of type array and an empty array are different values.
    ///
    /// * Storage: the sum of the size of its elements
    Array(
        /// The array's element type.
        Box<Type>,
    ),

    /// The [`STRUCT`](https://cloud.google.com/spanner/docs/data-types#struct_type) data type.
    Struct(StructType),
}

impl Type {
    /// Creates a new `Type::Array` with elements of the specified type.
    ///
    /// # Panics
    ///
    /// If the provided type is itself an `Type::Array`.
    pub fn array(inner: Type) -> Self {
        if let Type::Array(_) = &inner {
            panic!("array of array is not supported by Cloud Spanner");
        }
        Type::Array(Box::new(inner))
    }

    /// Creates a new `Type::Struct` with the provided field names and types.
    pub fn strct(fields: Vec<(&str, Type)>) -> Self {
        Type::Struct(StructType::new(fields))
    }

    pub(crate) fn code(&self) -> proto::TypeCode {
        match self {
            Type::Bool => proto::TypeCode::Bool,
            Type::Int64 => proto::TypeCode::Int64,
            Type::Float64 => proto::TypeCode::Float64,
            Type::String => proto::TypeCode::String,
            Type::Bytes => proto::TypeCode::Bytes,
            #[cfg(feature = "json")]
            Type::Json => proto::TypeCode::Json,
            #[cfg(feature = "numeric")]
            Type::Numeric => proto::TypeCode::Numeric,
            #[cfg(feature = "temporal")]
            Type::Timestamp => proto::TypeCode::Timestamp,
            #[cfg(feature = "temporal")]
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
            #[cfg(feature = "json")]
            Some(proto::TypeCode::Json) => Ok(Type::Json),
            #[cfg(not(feature = "json"))]
            Some(proto::TypeCode::Json) => {
                panic!("JSON type support is not enabled; use the 'json' feature to enable it")
            }
            #[cfg(feature = "numeric")]
            Some(proto::TypeCode::Numeric) => Ok(Type::Numeric),
            #[cfg(not(feature = "numeric"))]
            Some(proto::TypeCode::Numeric) => {
                panic!(
                    "NUMERIC type support is not enabled; use the 'numeric' feature to enable it"
                )
            }
            #[cfg(feature = "temporal")]
            Some(proto::TypeCode::Timestamp) => Ok(Type::Timestamp),
            #[cfg(not(feature = "temporal"))]
            Some(proto::TypeCode::Timestamp) => panic!(
                "TIMESTAMP type support is not enabled; use the 'temporal' feature to enable it"
            ),
            #[cfg(feature = "temporal")]
            Some(proto::TypeCode::Date) => Ok(Type::Date),
            #[cfg(not(feature = "temporal"))]
            Some(proto::TypeCode::Date) => {
                panic!("DATE type support is not enabled; use the 'temporal' feature to enable it")
            }
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
                        .iter()
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

    use googapis::google::spanner::v1 as proto;

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
        #[cfg(feature = "json")]
        test_scalar(proto::TypeCode::Json, Type::Json);
        #[cfg(feature = "numeric")]
        test_scalar(proto::TypeCode::Numeric, Type::Numeric);
        #[cfg(feature = "temporal")]
        test_scalar(proto::TypeCode::Timestamp, Type::Timestamp);
        #[cfg(feature = "temporal")]
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
        #[cfg(feature = "json")]
        test_array_of_scalar(proto::TypeCode::Json, Type::Json);
        #[cfg(feature = "numeric")]
        test_array_of_scalar(proto::TypeCode::Numeric, Type::Numeric);
        #[cfg(feature = "temporal")]
        test_array_of_scalar(proto::TypeCode::Timestamp, Type::Timestamp);
        #[cfg(feature = "temporal")]
        test_array_of_scalar(proto::TypeCode::Date, Type::Date);

        let invalid = proto::Type {
            code: proto::TypeCode::Array as i32,
            array_element_type: None,
            struct_type: None,
        };

        assert!(Type::try_from(invalid).is_err());
    }

    #[test]
    #[should_panic]
    fn _test_array_of_array_is_illegal() {
        Type::array(Type::array(Type::Bool));
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

    #[test]
    fn test_column_index() {
        let strct = StructType(vec![
            (Some("foo".into()), Type::Bool),
            (None, Type::Bool),
            (Some("bar".into()), Type::Bool),
        ]);
        assert_eq!(strct.field_index("foo"), Some(0));
        assert_eq!(strct.field_index("bar"), Some(2));
        assert_eq!(strct.field_index("not present"), None);
    }
}
