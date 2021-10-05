use bigdecimal::BigDecimal;
use prost::bytes::Bytes;

use crate::{Error, Type, Value};

pub trait FromSpanner<'a>: Sized {
    fn from_spanner(tpe: &'a Type, value: &'a Value) -> Result<Self, Error>;

    #[allow(unused_variables)]
    fn from_spanner_null(tpe: &Type) -> Result<Self, Error> {
        Err(crate::Error::Codec("value was null".to_string()))
    }

    fn from_spanner_nullable(tpe: &'a Type, value: &'a Value) -> Result<Self, Error> {
        match value {
            Value::Null(_) => Self::from_spanner_null(tpe),
            not_null => Self::from_spanner(tpe, not_null),
        }
    }
}

impl<'a, T> FromSpanner<'a> for Option<T>
where
    T: FromSpanner<'a>,
{
    fn from_spanner(tpe: &'a Type, value: &'a Value) -> Result<Self, Error> {
        <T as FromSpanner>::from_spanner(tpe, value).map(Some)
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
    fn from_spanner(tpe: &'a Type, value: &'a Value) -> Result<Self, Error> {
        match value {
            Value::Array(tpe, values) => values
                .iter()
                .map(|value| <T as FromSpanner>::from_spanner_nullable(tpe, value))
                .collect(),
            _ => wrong_type!(String, tpe),
        }
    }
}

impl<'a> FromSpanner<'a> for String {
    fn from_spanner(tpe: &'a Type, value: &'a Value) -> Result<Self, Error> {
        match value {
            Value::String(v) => Ok(v.clone()),
            _ => wrong_type!(String, tpe),
        }
    }
}

impl<'a> FromSpanner<'a> for &'a str {
    fn from_spanner(tpe: &'a Type, value: &'a Value) -> Result<Self, Error> {
        match value {
            Value::String(v) => Ok(v),
            _ => wrong_type!(String, tpe),
        }
    }
}

macro_rules! simple {
    ($t:ty, $f:ident, TryFrom::try_from) => {
        impl<'a> FromSpanner<'a> for $t {
            fn from_spanner(tpe: &'a Type, value: &'a Value) -> Result<$t, Error> {
                match value {
                    Value::$f(v) => Ok(TryFrom::try_from(*v)?),
                    _ => wrong_type!($f, tpe),
                }
            }
        }
    };
    ($t:ty, $f:ident, $from:path) => {
        impl<'a> FromSpanner<'a> for $t {
            fn from_spanner(tpe: &'a Type, value: &'a Value) -> Result<$t, Error> {
                match value {
                    Value::$f(v) => Ok($from(v)),
                    _ => wrong_type!($f, tpe),
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
simple!(BigDecimal, Numeric, Clone::clone);
simple!(&'a BigDecimal, Numeric, std::convert::identity);
simple!(Bytes, Bytes, Clone::clone);
simple!(&'a Bytes, Bytes, std::convert::identity);

#[cfg(test)]
mod test {
    use super::*;
    use crate::{Type, Value};
    use bigdecimal::{BigDecimal, FromPrimitive};

    macro_rules! from_spanner_ok {
        ($t:ty, $ok_tpe:ident, $($ok_val:expr),+) => {
            $(
                let result = <$t as FromSpanner>::from_spanner_nullable(
                    &Type::$ok_tpe,
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
                    &Type::$err_tpe,
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
            let result =
                <$t as FromSpanner>::from_spanner_nullable(&Type::$tpe, &Value::Null(Type::$tpe));
            assert!(
                result.is_err(),
                "expected Err from null value, got {:?}",
                result
            );
        };
    }
    macro_rules! from_spanner_nullable {
        ($t:ty, $tpe:ident) => {
            let result = <Option<$t> as FromSpanner>::from_spanner_nullable(
                &Type::$tpe,
                &Value::Null(Type::$tpe),
            );
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

        let result = <Vec<bool> as FromSpanner>::from_spanner_nullable(&bool_array, &value);
        assert_eq!(result.ok(), Some(vec![true, false]));
        let result = <Vec<bool> as FromSpanner>::from_spanner_nullable(
            &bool_array,
            &Value::Array(Type::Bool, vec![]),
        );
        assert_eq!(result.ok(), Some(vec![]));

        let result = <Vec<bool> as FromSpanner>::from_spanner_nullable(
            &Type::Array(Box::new(Type::Bool)),
            &Value::Null(bool_array.clone()),
        );
        assert!(result.is_err());
        let result = <Option<Vec<bool>> as FromSpanner>::from_spanner_nullable(
            &Type::Array(Box::new(Type::Bool)),
            &Value::Null(bool_array.clone()),
        );
        assert_eq!(result.ok(), Some(None));

        let result = <Vec<Option<bool>> as FromSpanner>::from_spanner_nullable(
            &Type::Array(Box::new(Type::Bool)),
            &Value::Array(
                Type::Bool,
                vec![
                    Value::Bool(true),
                    Value::Null(bool_array),
                    Value::Bool(false),
                ],
            ),
        )
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
