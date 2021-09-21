use std::convert::TryFrom;
use std::convert::TryInto;

use prost_types::ListValue;

use crate::proto::google::spanner::v1 as proto;
use crate::Error;
use crate::StructType;
use crate::Transaction;
use crate::Value;

pub struct Row {
    row_type: StructType,
    columns: ListValue,
}

impl Row {
    pub fn try_get(&self, column: usize) -> Result<Value, Error> {
        match self.row_type.0.get(column) {
            None => Err(Error::Codec("fudge".to_string())),
            Some((_, tpe)) => {
                Value::try_from(tpe, self.columns.values.get(column).unwrap().clone())
            }
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

#[derive(Debug)]
pub(crate) struct Stats {
    pub(crate) row_count: Option<i64>,
}

impl TryFrom<proto::ResultSetStats> for Stats {
    type Error = Error;

    fn try_from(value: proto::ResultSetStats) -> Result<Self, Self::Error> {
        let row_count = match value.row_count {
            Some(proto::result_set_stats::RowCount::RowCountExact(exact)) => Ok(Some(exact)),
            Some(proto::result_set_stats::RowCount::RowCountLowerBound(_)) => Err(Error::Client(
                "lower bound row count is unsupported".to_string(),
            )),
            None => Ok(None),
        }?;
        Ok(Self { row_count })
    }
}

#[derive(Debug)]
pub struct ResultSet {
    row_type: StructType,
    rows: Vec<ListValue>,
    pub(crate) transaction: Option<Transaction>,
    pub(crate) stats: Stats,
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

impl TryFrom<proto::ResultSet> for ResultSet {
    type Error = crate::Error;

    fn try_from(value: proto::ResultSet) -> Result<Self, Self::Error> {
        let metadata = value
            .metadata
            .ok_or_else(|| Self::Error::Codec("missing result set metadata".to_string()))?;

        let row_type = metadata
            .row_type
            .ok_or_else(|| Self::Error::Codec("missing row type metadata".to_string()))
            .and_then(StructType::try_from)?;

        Ok(Self {
            row_type,
            rows: value.rows,
            transaction: metadata.transaction.map(Transaction::from),
            stats: value.stats.unwrap_or_default().try_into()?,
        })
    }
}
