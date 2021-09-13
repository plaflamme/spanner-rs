use std::collections::BTreeMap;
use std::convert::TryFrom;

use prost_types::ListValue;

use crate::proto::google::spanner::v1::ResultSet as SpannerResultSet;
use crate::StructType;
use crate::StructValue;
use crate::Value;

pub struct Row {
    _row_type: StructType,
    by_name: BTreeMap<String, usize>,
    columns: StructValue,
}

impl Row {
    pub fn get_value(&self, column: usize) -> Option<&Value> {
        self.columns.0.get(column).map(|(_, value)| value)
    }

    pub fn get_by_name(&self, column_name: String) -> Option<&Value> {
        self.by_name
            .get(&column_name)
            .and_then(|idx| self.get_value(*idx))
    }
}

pub struct ResultSet {
    row_type: StructType,
    rows: Vec<ListValue>,
}

impl ResultSet {
    pub fn iter(self) -> impl Iterator<Item = Result<Row, crate::Error>> {
        let row_type = self.row_type.clone();
        let by_name: BTreeMap<String, usize> = self
            .row_type
            .0
            .iter()
            .enumerate()
            .flat_map(|(idx, (name, _))| name.as_ref().map(|n| (n.clone(), idx)))
            .collect();

        self.rows.into_iter().map(move |row| {
            StructValue::try_from(&row_type, row).map(|columns| Row {
                _row_type: row_type.clone(),
                by_name: by_name.clone(),
                columns,
            })
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
