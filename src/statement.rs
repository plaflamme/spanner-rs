use crate::ToSpanner;
#[cfg(doc)]
use crate::TransactionContext;
use google_api_proto::google::spanner::v1 as proto;

/// A single DML statement that can be used in a batch of DML statements using [`TransactionContext::execute_updates`]
pub struct Statement<'a> {
    pub sql: &'a str,
    pub params: &'a [(&'a str, &'a (dyn ToSpanner + Sync))],
}

impl<'a> TryFrom<&Statement<'a>> for proto::execute_batch_dml_request::Statement {
    type Error = crate::Error;

    fn try_from(
        value: &Statement,
    ) -> Result<proto::execute_batch_dml_request::Statement, Self::Error> {
        let mut params = std::collections::BTreeMap::new();
        let mut param_types = std::collections::BTreeMap::new();
        for (name, value) in value.params {
            let value = value.to_spanner()?;
            param_types.insert(name.to_string(), value.spanner_type().into());
            params.insert(name.to_string(), value.try_into()?);
        }

        Ok(proto::execute_batch_dml_request::Statement {
            sql: value.sql.to_string(),
            params: Some(prost_types::Struct { fields: params }),
            param_types,
        })
    }
}
