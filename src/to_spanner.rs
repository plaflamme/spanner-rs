use crate::{Error, Type, Value};

pub trait ToSpanner {
    fn to_spanner(&self, tpe: &Type) -> Result<Value, Error>;
}

macro_rules! simple_to {
    ($t:ty, $v:ident, $self:ident, $into:expr) => {
        impl ToSpanner for $t {
            fn to_spanner(&self, _: &Type) -> Result<Value, Error> {
                let $self = self;
                Ok(Value::$v($into))
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
    fn to_spanner(&self, tpe: &Type) -> Result<Value, Error> {
        match self.as_ref() {
            Some(v) => v.to_spanner(tpe),
            None => Ok(Value::Null(*tpe)),
        }
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
