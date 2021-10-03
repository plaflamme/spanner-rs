use crate::{Error, Type, Value};

pub trait ToSpanner {
    fn to_spanner(&self) -> Result<Value, Error>;

    fn spanner_type() -> Type
    where
        Self: Sized;
}

macro_rules! simple_to {
    ($t:ty, $v:ident, $self:ident, $into:expr) => {
        impl ToSpanner for $t {
            fn to_spanner(&self) -> Result<Value, Error> {
                let $self = self;
                Ok(Value::$v($into))
            }

            fn spanner_type() -> Type {
                Type::$v
            }
        }
    };
    ($t:ty, i64_from) => {
        simple_to!($t, Int64, v, i64::from(*v));
    };
    ($t:ty, $v:ident, clone) => {
        simple_to!($t, $v, v, v.clone());
    };
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

simple_to!(i8, i64_from);
simple_to!(u8, i64_from);
simple_to!(i16, i64_from);
simple_to!(u16, i64_from);
simple_to!(i32, i64_from);
simple_to!(u32, i64_from);
simple_to!(i64, i64_from);
simple_to!(String, String, clone);
simple_to!(&str, String, v, v.to_string());

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
