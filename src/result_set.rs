use std::convert::TryFrom;
use std::convert::TryInto;

use crate::proto::google::spanner::v1 as proto;
use crate::Error;
use crate::FromSpanner;
use crate::StructType;
use crate::Transaction;
use crate::Value;

pub struct Row<'a> {
    row_type: StructType,
    columns: &'a Vec<Value>,
}

impl<'a> Row<'a> {
    pub fn try_get<T>(&'a self, column: usize) -> Result<T, Error>
    where
        T: FromSpanner<'a>,
    {
        match self.row_type.0.get(column) {
            None => Err(Error::Codec(format!("no such column {}", column))),
            Some((_, tpe)) => {
                let value = self.columns.get(column).unwrap();
                <T as FromSpanner>::from_spanner_nullable(tpe, value)
            }
        }
    }

    pub fn try_get_by_name<T>(&'a self, column_name: &str) -> Result<T, Error>
    where
        T: FromSpanner<'a>,
    {
        self.row_type
            .field_index(column_name)
            .ok_or_else(|| Error::Codec(format!("no such column: {}", column_name)))
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
    rows: Vec<Vec<Value>>,
    pub(crate) transaction: Option<Transaction>,
    pub(crate) stats: Stats,
}

impl ResultSet {
    pub fn iter(&self) -> impl Iterator<Item = Row<'_>> {
        let row_type = self.row_type.clone();

        self.rows.iter().map(move |columns| Row {
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

        let rows = value
            .rows
            .iter()
            .map(|row| {
                row.values
                    .iter()
                    .zip(row_type.types())
                    .map(|(value, tpe)| Value::try_from(tpe, value.clone()))
                    .collect()
            })
            .collect::<Result<Vec<Vec<Value>>, Error>>()?;

        Ok(Self {
            row_type,
            rows,
            transaction: metadata.transaction.map(Transaction::from),
            stats: value.stats.unwrap_or_default().try_into()?,
        })
    }
}
