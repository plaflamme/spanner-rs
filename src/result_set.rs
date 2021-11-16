use std::convert::TryFrom;
use std::convert::TryInto;

use crate::Error;
use crate::FromSpanner;
use crate::StructType;
use crate::Transaction;
use crate::Value;
use googapis::google::spanner::v1 as proto;

/// A trait implemented by types that can index into a row.
///
/// Only the crate itself implements this.
pub trait RowIndex: private::Sealed {
    #[doc(hidden)]
    fn index(&self, struct_type: &StructType) -> Option<usize>;
}

/// Allows indexing into a row using a column index.
impl RowIndex for usize {
    fn index(&self, struct_type: &StructType) -> Option<usize> {
        if *self < struct_type.fields().len() {
            Some(*self)
        } else {
            None
        }
    }
}

/// Allows indexing into a row using a column name.
impl RowIndex for str {
    fn index(&self, struct_type: &StructType) -> Option<usize> {
        struct_type.field_index(self)
    }
}

impl<'a, T> RowIndex for &'a T
where
    T: RowIndex + ?Sized,
{
    fn index(&self, struct_type: &StructType) -> Option<usize> {
        <T as RowIndex>::index(self, struct_type)
    }
}

mod private {
    pub trait Sealed {}

    impl Sealed for usize {}
    impl Sealed for str {}
    impl<'a, T> Sealed for &'a T where T: ?Sized + Sealed {}
}

/// A row of a result set returned by Cloud Spanner.
///
/// Every row of a result set shares the same type.
pub struct Row<'a> {
    row_type: &'a StructType,
    columns: &'a [Value],
}

impl<'a> Row<'a> {
    /// Returns the structure of this row (field names and type).
    pub fn row_type(&'a self) -> &'a StructType {
        self.row_type
    }

    /// Returns true when this row has no fields.
    pub fn is_empty(&'a self) -> bool {
        self.row_type.fields().is_empty()
    }

    /// Returns the converted value of the specified column.
    ///
    /// An error is returned if the requested column does not exist or if the decoding of the value returns an error.
    pub fn get<T, R>(&'a self, row_index: R) -> Result<T, Error>
    where
        T: FromSpanner<'a>,
        R: RowIndex + std::fmt::Display,
    {
        self.get_impl(&row_index)
    }

    /// Returns the converted value of the specified column.
    ///
    /// # Panics
    ///
    /// Panics if the specified index does not exist or if the value cannot be converted to requested type.
    pub fn get_unchecked<T, R>(&'a self, row_index: R) -> T
    where
        T: FromSpanner<'a>,
        R: RowIndex + std::fmt::Display,
    {
        match self.get_impl(&row_index) {
            Ok(value) => value,
            Err(error) => panic!(
                "unexpected error while reading column {}: {}",
                row_index, error
            ),
        }
    }

    fn get_impl<T, R>(&'a self, row_index: &R) -> Result<T, Error>
    where
        T: FromSpanner<'a>,
        R: RowIndex + std::fmt::Display,
    {
        match row_index.index(self.row_type) {
            None => Err(Error::Codec(format!("no such column {}", row_index))),
            Some(index) => <T as FromSpanner>::from_spanner_nullable(&self.columns[index]),
        }
    }
}

/// Prints the row's type, but omits the values.
impl<'a> std::fmt::Debug for Row<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Row")
            .field("columns", &self.row_type)
            .finish()
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

/// A result set is returned by Cloud Spanner when executing SQL queries.
///
/// Contains the structure of each row as well as each row's values.
/// A result set is not lazy and will eagerly decode all rows in the result set.
#[derive(Debug)]
pub struct ResultSet {
    row_type: StructType,
    rows: Vec<Vec<Value>>,
    pub(crate) transaction: Option<Transaction>,
    pub(crate) stats: Stats,
}

impl ResultSet {
    /// Returns an iterator over the rows of this result set.
    pub fn iter(&self) -> impl Iterator<Item = Row<'_>> {
        self.rows.iter().map(move |columns| Row {
            row_type: &self.row_type,
            columns,
        })
    }
}

impl TryFrom<proto::ResultSet> for ResultSet {
    type Error = crate::Error;

    fn try_from(value: proto::ResultSet) -> Result<Self, Self::Error> {
        let stats = value.stats.unwrap_or_default().try_into()?;
        let metadata = value.metadata.unwrap_or_default();
        let row_type: StructType = metadata.row_type.unwrap_or_default().try_into()?;

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
            stats,
        })
    }
}
