#[cfg(feature = "numeric")]
use bigdecimal::BigDecimal;
use prost::bytes::Bytes;

use crate::{Error, Type, Value};

/// A trait for Rust types that can be converted from Cloud Spanner values.
///
/// # Types
///
/// The crate provides the following mapping between Cloud Spanner types and Rust types.
///
/// | Rust Type | Spanner Type |
/// |---|---|
/// | `bool` | [`Bool`](https://cloud.google.com/spanner/docs/data-types#boolean_type) |
/// | `u8`, `i8`, `u16`, `i16`, `u32`, `i32`, `i64` | [`Int64`](https://cloud.google.com/spanner/docs/data-types#integer_type) |
/// | `f64` | [`Float64`](https://cloud.google.com/spanner/docs/data-types#floating_point_types) |
/// | `&str`, `String` | [`String`](https://cloud.google.com/spanner/docs/data-types#string_type) |
/// | `bigdecimal::BigDecimal` | [`Numeric`](https://cloud.google.com/spanner/docs/data-types#numeric_type) |
/// | `&[u8]`, `Bytes` | [`Bytes`](https://cloud.google.com/spanner/docs/data-types#bytes_type) |
///
/// # Nullability
///
/// `FromSpanner` is implemented for `Option<T>` when `T` implements `FromSpanner`.
/// `Option<T>` represents a nullable Spanner value.
///
/// # Arrays
///
/// `FromSpanner` is implemented for `Vec<T>` when `T` implements `FromSpanner`.
/// Such values map to Spanner's [`Array`](https://cloud.google.com/spanner/docs/data-types#array_type) type.
/// Arrays may contain `null` values (i.e.: `Vec<Option<T>>`). Note that `Vec<Vec<T>>` is not allowed.
pub trait FromSpanner<'a>: Sized {
    /// Creates a new value of this type from the provided Cloud Spanner value.
    /// Values passed to this method should not be `Value::Null`, if this is not known to be the case, use [FromSpanner::from_spanner_nullable] instead.
    fn from_spanner(value: &'a Value) -> Result<Self, Error>;

    /// Creates a new value of this type from the provided Cloud Spanner `NULL` value's type.
    #[allow(unused_variables)]
    fn from_spanner_null(tpe: &Type) -> Result<Self, Error> {
        Err(crate::Error::Codec("value was null".to_string()))
    }

    /// Creates a new value of this type from the provided Cloud Spanner value which may or may not be null.
    /// This method will dispatch to either [FromSpanner::from_spanner] or [FromSpanner::from_spanner_null] depending
    /// on whether the provided value is `NULL`.
    fn from_spanner_nullable(value: &'a Value) -> Result<Self, Error> {
        match value {
            Value::Null(tpe) => Self::from_spanner_null(tpe),
            not_null => Self::from_spanner(not_null),
        }
    }
}

impl<'a, T> FromSpanner<'a> for Option<T>
where
    T: FromSpanner<'a>,
{
    fn from_spanner(value: &'a Value) -> Result<Self, Error> {
        <T as FromSpanner>::from_spanner(value).map(Some)
    }

    fn from_spanner_null(_tpe: &Type) -> Result<Self, Error> {
        Ok(None)
    }
}

macro_rules! wrong_type {
    ($expect:ident, $tpe:expr) => {
        Err(Error::Codec(format!(
            "type {:?} is unsupported by FromSpanner impl, expected {:?}",
            $tpe,
            Type::$expect,
        )))
    };
}

impl<'a, T> FromSpanner<'a> for Vec<T>
where
    T: FromSpanner<'a>,
{
    fn from_spanner(value: &'a Value) -> Result<Self, Error> {
        match value {
            Value::Array(_, values) => values
                .iter()
                .map(|value| <T as FromSpanner>::from_spanner_nullable(value))
                .collect(),
            _ => wrong_type!(String, value.spanner_type()),
        }
    }
}

impl<'a> FromSpanner<'a> for String {
    fn from_spanner(value: &'a Value) -> Result<Self, Error> {
        match value {
            Value::String(v) => Ok(v.clone()),
            _ => wrong_type!(String, value.spanner_type()),
        }
    }
}

impl<'a> FromSpanner<'a> for &'a str {
    fn from_spanner(value: &'a Value) -> Result<Self, Error> {
        match value {
            Value::String(v) => Ok(v),
            _ => wrong_type!(String, value.spanner_type()),
        }
    }
}

macro_rules! simple {
    ($t:ty, $f:ident, TryFrom::try_from) => {
        impl<'a> FromSpanner<'a> for $t {
            fn from_spanner(value: &'a Value) -> Result<$t, Error> {
                match value {
                    Value::$f(v) => Ok(TryFrom::try_from(*v)?),
                    _ => wrong_type!($f, value.spanner_type()),
                }
            }
        }
    };
    ($t:ty, $f:ident, $from:path) => {
        impl<'a> FromSpanner<'a> for $t {
            fn from_spanner(value: &'a Value) -> Result<$t, Error> {
                match value {
                    Value::$f(v) => Ok($from(v)),
                    _ => wrong_type!($f, value.spanner_type()),
                }
            }
        }
    };
}

#[inline]
fn copy<T>(value: &T) -> T
where
    T: Copy,
{
    *value
}

simple!(i8, Int64, TryFrom::try_from);
simple!(u8, Int64, TryFrom::try_from);
simple!(i16, Int64, TryFrom::try_from);
simple!(u16, Int64, TryFrom::try_from);
simple!(i32, Int64, TryFrom::try_from);
simple!(u32, Int64, TryFrom::try_from);
simple!(i64, Int64, copy);
simple!(f64, Float64, copy);
simple!(bool, Bool, copy);
#[cfg(feature = "numeric")]
simple!(BigDecimal, Numeric, Clone::clone);
#[cfg(feature = "numeric")]
simple!(&'a BigDecimal, Numeric, std::convert::identity);
simple!(Bytes, Bytes, Clone::clone);
simple!(&'a Bytes, Bytes, std::convert::identity);
simple!(&'a [u8], Bytes, std::convert::identity);

#[cfg(test)]
mod test {
    use super::*;
    use crate::{Type, Value};
    #[cfg(feature = "numeric")]
    use bigdecimal::{BigDecimal, FromPrimitive};

    macro_rules! from_spanner_ok {
        ($t:ty, $ok_tpe:ident, $($ok_val:expr),+) => {
            $(
                let result = <$t as FromSpanner>::from_spanner_nullable(
                    &Value::$ok_tpe($ok_val.into()),
                );
                assert_eq!(result.ok(), Some($ok_val));
            )+
        };
    }
    macro_rules! from_spanner_err {
        ($t:ty, $err_tpe:ident, $($err_val:expr), +) => {
            $(
                let result = <$t as FromSpanner>::from_spanner_nullable(
                    &Value::$err_tpe($err_val),
                );
                assert!(
                    result.is_err(),
                    "value {:?} expected to fail",
                    &Value::$err_tpe($err_val)
                );
            )+
        };
    }
    macro_rules! from_spanner_non_nullable {
        ($t:ty, $tpe:ident) => {
            let result = <$t as FromSpanner>::from_spanner_nullable(&Value::Null(Type::$tpe));
            assert!(
                result.is_err(),
                "expected Err from null value, got {:?}",
                result
            );
        };
    }
    macro_rules! from_spanner_nullable {
        ($t:ty, $tpe:ident) => {
            let result =
                <Option<$t> as FromSpanner>::from_spanner_nullable(&Value::Null(Type::$tpe));
            assert_eq!(result.ok(), Some(None));
        };
    }

    macro_rules! from_spanner_int64 {
        ($t:ty) => {
            from_spanner_ok!($t, Int64, <$t>::MIN, <$t>::MAX, 0);
            from_spanner_err!($t, Float64, 0.0, 42.5);
            from_spanner_err!($t, Bool, true, false);
            from_spanner_err!($t, String, "this is not an int64".to_string());
            from_spanner_non_nullable!($t, Int64);
            from_spanner_nullable!($t, Int64);
        };
        ($($t:ty),+) => {
            $(
                from_spanner_int64!($t);
            )+
        };
    }

    #[test]
    fn test_from_spanner_int64() {
        from_spanner_int64!(i8, u8, i16, u16, i32, u32, i64);
    }

    #[test]
    fn test_from_spanner_bool() {
        from_spanner_ok!(bool, Bool, true, false);
        from_spanner_err!(bool, Float64, 0.0);
        from_spanner_err!(bool, Int64, 0);
        from_spanner_err!(bool, String, "this is not a bool".to_string());
        from_spanner_non_nullable!(bool, Bool);
        from_spanner_nullable!(bool, Bool);
    }

    #[test]
    fn test_from_spanner_bytes() {
        from_spanner_ok!(
            Bytes,
            Bytes,
            Bytes::from_static(&[1, 2, 3, 4]),
            Bytes::from_static(&[])
        );
        from_spanner_err!(Bytes, Float64, 0.0);
        from_spanner_err!(Bytes, Int64, 0);
        from_spanner_non_nullable!(Bytes, Bytes);
        from_spanner_nullable!(Bytes, Bytes);

        // assert FromSpanner for &[u8] from Bytes
        let data: &'static [u8] = &[1, 2, 3, 4];
        let bytes = Value::Bytes(Bytes::from_static(data));
        let slice = <&[u8] as FromSpanner>::from_spanner_nullable(&bytes);
        assert_eq!(slice.ok(), Some(data));
    }

    #[test]
    fn test_from_spanner_float64() {
        from_spanner_ok!(
            f64,
            Float64,
            f64::MIN,
            f64::MAX,
            // f64::NAN, Works, but assert_eq fails
            f64::NEG_INFINITY,
            0.0
        );
        from_spanner_err!(f64, Bool, true);
        from_spanner_err!(f64, Int64, 0);
        from_spanner_err!(f64, String, "this is not a bool".to_string());
        from_spanner_non_nullable!(f64, Float64);
        from_spanner_nullable!(f64, Float64);
    }

    #[cfg(feature = "numeric")]
    #[test]
    fn test_from_spanner_numeric() {
        from_spanner_ok!(
            BigDecimal,
            Numeric,
            BigDecimal::from_i128(0).unwrap(),
            BigDecimal::from_i128(42).unwrap()
        );
        from_spanner_err!(BigDecimal, Float64, 0.0);
        from_spanner_err!(BigDecimal, Int64, 0);
        from_spanner_err!(BigDecimal, String, "this is not a bool".to_string());
        from_spanner_non_nullable!(BigDecimal, Numeric);
        from_spanner_nullable!(BigDecimal, Numeric);
    }

    #[test]
    fn test_from_spanner_array() {
        let bool_array = Type::Array(Box::new(Type::Bool));
        let value = Value::Array(Type::Bool, vec![Value::Bool(true), Value::Bool(false)]);

        let result = <Vec<bool> as FromSpanner>::from_spanner_nullable(&value);
        assert_eq!(result.ok(), Some(vec![true, false]));
        let result =
            <Vec<bool> as FromSpanner>::from_spanner_nullable(&Value::Array(Type::Bool, vec![]));
        assert_eq!(result.ok(), Some(vec![]));

        let result =
            <Vec<bool> as FromSpanner>::from_spanner_nullable(&Value::Null(bool_array.clone()));
        assert!(result.is_err());
        let result = <Option<Vec<bool>> as FromSpanner>::from_spanner_nullable(&Value::Null(
            bool_array.clone(),
        ));
        assert_eq!(result.ok(), Some(None));

        let result = <Vec<Option<bool>> as FromSpanner>::from_spanner_nullable(&Value::Array(
            Type::Bool,
            vec![
                Value::Bool(true),
                Value::Null(bool_array),
                Value::Bool(false),
            ],
        ))
        .unwrap();
        assert_eq!(result, vec![Some(true), None, Some(false)]);
    }

    #[test]
    fn test_from_spanner_string() {
        from_spanner_ok!(
            String,
            String,
            "this is a string".to_string(),
            "".to_string()
        );
        from_spanner_err!(String, Float64, 0.0);
        from_spanner_err!(String, Int64, 0);
        from_spanner_non_nullable!(String, String);
        from_spanner_nullable!(String, String);
    }
}
