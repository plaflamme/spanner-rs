#[cfg(feature = "numeric")]
use bigdecimal::BigDecimal;
use prost::bytes::Bytes;

use crate::{Error, Type, Value};

/// A trait for Rust types that can be converted to Cloud Spanner values.
///
/// # Types
///
/// The crate provides the following mapping between Cloud Spanner types and Rust types.
///
/// | Rust Type | Spanner Type |
/// |---|---|
/// | `bool` | [`BOOL`](https://cloud.google.com/spanner/docs/data-types#boolean_type) |
/// | `u8`, `i8`, `u16`, `i16`, `u32`, `i32`, `i64` | [`INT64`](https://cloud.google.com/spanner/docs/data-types#integer_type) |
/// | `f64` | [`FLOAT64`](https://cloud.google.com/spanner/docs/data-types#floating_point_types) |
/// | `&str`, `String` | [`STRING`](https://cloud.google.com/spanner/docs/data-types#string_type) |
/// | `&[u8]`, `Bytes` | [`BYTES`](https://cloud.google.com/spanner/docs/data-types#bytes_type) |
///
/// The following are provided when the corresponding feature is enabled:
///
/// | Feature | Rust Type | Spanner Type |
/// |---|---|---|
/// | `json` | `serde_json::Value` | [`JSON`](https://cloud.google.com/spanner/docs/data-types#json_type) |
/// | `numeric` | `bigdecimal::BigDecimal` | [`NUMERIC`](https://cloud.google.com/spanner/docs/data-types#numeric_type) |
/// | `temporal` | `chrono::DateTime<Utc>` | [`TIMESTAMP`](https://cloud.google.com/spanner/docs/data-types#timestamp_type) |
/// | `temporal` | `chrono::NaiveDate` | [`DATE`](https://cloud.google.com/spanner/docs/data-types#date_type) |
///
/// # Nullability
///
/// `ToSpanner` is implemented for `Option<T>` when `T` implements `ToSpanner`.
/// `Option<T>` represents a nullable Spanner value.
///
/// # Arrays
///
/// `ToSpanner` is implemented for `Vec<T>` when `T` implements `ToSpanner`.
/// Such values map to Spanner's [`Array`](https://cloud.google.com/spanner/docs/data-types#array_type) type.
/// Arrays may contain `null` values (i.e.: `Vec<Option<T>>`). Note that `Vec<Vec<T>>` is not allowed.
pub trait ToSpanner {
    /// Creates a new Cloud Spanner value from this value.
    fn to_spanner(&self) -> Result<Value, Error>;

    /// Returns the Cloud Spanner [Type] that this implementation produces.
    fn spanner_type() -> Type
    where
        Self: Sized;
}

impl<T> ToSpanner for Option<T>
where
    T: ToSpanner,
{
    fn to_spanner(&self) -> Result<Value, Error> {
        match self.as_ref() {
            Some(v) => v.to_spanner(),
            None => Ok(Value::Null(<T as ToSpanner>::spanner_type())),
        }
    }
    fn spanner_type() -> Type {
        <T as ToSpanner>::spanner_type()
    }
}

impl<T> ToSpanner for Vec<T>
where
    T: ToSpanner,
{
    fn to_spanner(&self) -> Result<Value, Error> {
        let values = self
            .iter()
            .map(|v| v.to_spanner())
            .collect::<Result<Vec<Value>, Error>>()?;
        Ok(Value::Array(<T as ToSpanner>::spanner_type(), values))
    }
    fn spanner_type() -> Type {
        Type::Array(Box::new(<T as ToSpanner>::spanner_type()))
    }
}

impl<T> ToSpanner for &[T]
where
    T: ToSpanner,
{
    fn to_spanner(&self) -> Result<Value, Error> {
        let values = self
            .iter()
            .map(|v| v.to_spanner())
            .collect::<Result<Vec<Value>, Error>>()?;
        Ok(Value::Array(<T as ToSpanner>::spanner_type(), values))
    }
    fn spanner_type() -> Type {
        Type::Array(Box::new(<T as ToSpanner>::spanner_type()))
    }
}

macro_rules! simple {
    ($t:ty, $v:ident, $into:path $(, $deref:tt)?) => {
        impl ToSpanner for $t {
            fn to_spanner(&self) -> Result<Value, Error> {
                Ok(Value::$v($into($($deref)? self)))
            }

            fn spanner_type() -> Type {
                Type::$v
            }
        }
    };
}

simple!(i8, Int64, i64::from, *);
simple!(u8, Int64, i64::from, *);
simple!(i16, Int64, i64::from, *);
simple!(u16, Int64, i64::from, *);
simple!(i32, Int64, i64::from, *);
simple!(u32, Int64, i64::from, *);
simple!(i64, Int64, i64::from, *);
simple!(String, String, Clone::clone);
simple!(&str, String, ToString::to_string);
#[cfg(feature = "numeric")]
simple!(BigDecimal, Numeric, Clone::clone);
simple!(Bytes, Bytes, Clone::clone);
#[cfg(feature = "json")]
simple!(serde_json::Value, Json, Clone::clone);
#[cfg(feature = "temporal")]
simple!(chrono::DateTime<chrono::Utc>, Timestamp, Clone::clone);
#[cfg(feature = "temporal")]
simple!(chrono::NaiveDate, Date, Clone::clone);

#[cfg(test)]
mod test {
    use super::*;

    macro_rules! simple_test_int64 {
        ($t:ty) => {
            assert_eq!((0 as $t).to_spanner().ok(), Some(Value::Int64(0)));
        };
        ($($t:ty),+) => {
            $(
                simple_test_int64!($t);
            )+
        };
    }

    #[test]
    fn test_to_spanner_simple_int64() {
        simple_test_int64!(i8, u8, i16, u16, i32, u32, i64);
    }

    #[test]
    fn test_to_spanner_opt() {
        let some = Some(0 as u32);
        assert_eq!(some.to_spanner().ok(), Some(Value::Int64(0)));
        let none: Option<u32> = None;
        assert_eq!(none.to_spanner().ok(), Some(Value::Null(Type::Int64)));
    }

    #[test]
    fn test_to_spanner_array() {
        let array = vec![0, 1, 2, 3, 4];
        assert_eq!(
            array.to_spanner().ok(),
            Some(Value::Array(
                Type::Int64,
                vec![
                    Value::Int64(0),
                    Value::Int64(1),
                    Value::Int64(2),
                    Value::Int64(3),
                    Value::Int64(4)
                ]
            ))
        );
        let empty: Vec<u32> = vec![];
        assert_eq!(
            empty.to_spanner().ok(),
            Some(Value::Array(Type::Int64, vec![]))
        );
    }
}
