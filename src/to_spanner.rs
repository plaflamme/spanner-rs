use crate::{Error, Value};

pub trait ToSpanner {
    fn to_spanner(&self) -> Result<Value, Error>;
}

impl ToSpanner for u8 {
    fn to_spanner(&self) -> Result<Value, Error> {
        Ok(Value::Int64(i64::from(*self)))
    }
}
impl ToSpanner for u32 {
    fn to_spanner(&self) -> Result<Value, Error> {
        Ok(Value::Int64(i64::from(*self)))
    }
}
impl ToSpanner for i32 {
    fn to_spanner(&self) -> Result<Value, Error> {
        Ok(Value::Int64(i64::from(*self)))
    }
}
impl ToSpanner for String {
    fn to_spanner(&self) -> Result<Value, Error> {
        Ok(Value::String(self.clone()))
    }
}
impl ToSpanner for &str {
    fn to_spanner(&self) -> Result<Value, Error> {
        Ok(Value::String(self.to_string()))
    }
}
