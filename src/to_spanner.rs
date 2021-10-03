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

simple_to!(i8, i64_from);
simple_to!(u8, i64_from);
simple_to!(i16, i64_from);
simple_to!(u16, i64_from);
simple_to!(i32, i64_from);
simple_to!(u32, i64_from);
simple_to!(i64, i64_from);
simple_to!(String, String, clone);
simple_to!(&str, String, v, v.to_string());
