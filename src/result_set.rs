use std::convert::TryFrom;

use prost_types::ListValue;

use crate::proto::google::spanner::v1::ResultSet as SpannerResultSet;
use crate::Error;
use crate::StructType;
use crate::Value;

pub struct Row {
    row_type: StructType,
    columns: ListValue,
}

impl Row {
    pub fn try_get(&self, column: usize) -> Result<Value, Error> {
        match self.row_type.0.get(column) {
            None => Err(Error::Codec("fudge".to_string())),
            Some((_, tpe)) => Value::try_from(tpe, self.columns.values.get(column).unwrap()),
        }
    }

    pub fn try_get_by_name(&self, column_name: &str) -> Result<Value, Error> {
        self.row_type
            .0
            .iter()
            .position(|(name, _)| match name {
                Some(n) => *n == column_name,
                None => false,
            })
            .ok_or_else(|| Error::Codec("fudge".to_string()))
            .and_then(|idx| self.try_get(idx))
    }
}

pub struct ResultSet {
    row_type: StructType,
    rows: Vec<ListValue>,
}

impl ResultSet {
    pub fn iter(self) -> impl Iterator<Item = Row> {
        let row_type = self.row_type.clone();

        self.rows.into_iter().map(move |columns| Row {
            row_type: row_type.clone(),
            columns,
        })
    }
}

impl TryFrom<SpannerResultSet> for ResultSet {
    type Error = crate::Error;

    fn try_from(value: SpannerResultSet) -> Result<Self, Self::Error> {
        let row_type: StructType = value
            .metadata
            .ok_or_else(|| Self::Error::Codec("missing result set metadata".to_string()))
            .and_then(|rsm| {
                rsm.row_type
                    .ok_or_else(|| Self::Error::Codec("missing row type metadata".to_string()))
            })
            .and_then(StructType::try_from)?;

        Ok(Self {
            row_type,
            rows: value.rows,
        })
    }
}
