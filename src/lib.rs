//! An asynchronous client for the Cloud Spanner database.
//!
//! # Example
//!
//! ```no_run
//! use spanner_rs::{Client, Error, ReadContext, TransactionContext};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     let mut client = Client::configure()
//!         .project("my-gcp-project")
//!         .instance("my-instance")
//!         .database("my-database")
//!         .connect()
//!         .await?;
//!
//!     // assuming the following table:
//!     //   person(id INT64, name STRING(MAX), data BYTES(MAX))
//!     client
//!         .read_write()
//!         .run(|tx| {
//!             tx.execute_update(
//!                 "INSERT INTO person(id, name, data) VALUES(@id, @name, NULL)",
//!                 &[("id", &42), ("name", &"ferris")],
//!             )
//!         })
//!         .await?;
//!
//!     let result_set = client
//!         .read_only()
//!         .execute_query("SELECT * FROM person", &[])
//!         .await?;
//!
//!     for row in result_set.iter() {
//!         let id: u32 = row.get("id")?;
//!         let name: &str = row.get("name")?;
//!         let data: Option<&[u8]> = row.get("data")?;
//!
//!         println!("found person: {} {} {:?}", id, name, data);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # Transactions
//!
//! Cloud Spanner [supports](https://cloud.google.com/spanner/docs/transactions) several transaction "modes":
//!
//! * read-only: provides guaranteed consistency between several reads, cannot write;
//! * read-write: the only way to write into Cloud Spanner they use a combination of locking and retries and are typically more expensive;
//! * partitioned DML: these are unsupported by this client at the moment.
//!
//! ## Read Only
//!
//! Reads are done within "single-use" transactions and can be bounded to determine what data is visible to them, see [`TimestampBound`].
//! Reading is done using [`ReadContext`] which can be obtained using [`Client::read_only()`] or [`Client::read_only_with_bound()`].
//!
//! Example:
//!
//! ```no_run
//! # use spanner_rs::*;
//! #[tokio::main]
//! # async fn main() -> Result<(), crate::Error> {
//! # let mut client = Client::configure().connect().await?;
//! let result_set = client
//!     .read_only()
//!     .execute_query("SELECT COUNT(*) AS people FROM person", &[])
//!     .await?;
//! let people: u32 = result_set.iter().next().unwrap().get("people")?;
//! # Ok(()) }
//! ```
//!
//! ## Read Write
//!
//! Read / write transactions are done through [`TransactionContext`] which extends [`ReadContext`] to allow writes.
//! When a transaction that conflicts with another tries to commit, Cloud Spanner will reject one of them let the client know it may retry.
//! This client encapsulates the necessary retry logic such that applications do not need to implement it themselves.
//!
//! Example:
//!
//! ```no_run
//! # use spanner_rs::*;
//! #[tokio::main]
//! # async fn main() -> Result<(), crate::Error> {
//! # let mut client = Client::configure().connect().await?;
//! client
//!     .read_write()
//!     .run(|tx| {
//!         // this closure may be invoked more than once
//!         Box::pin(async move {
//!             // read
//!             let rs = tx.execute_query("...", &[]).await?;
//!             // write
//!             tx.execute_update("...", &[]).await?;
//!             Ok(())
//!         })
//!     })
//!     .await?;
//! # Ok(()) }
//! ```
//!
//! ## Authentication
//!
//! Authentication uses the [`gcp_auth`] crate which supports several authentication methods.

pub use crate::client::*;
pub use crate::config::*;
pub(crate) use crate::connection::Connection;
pub use crate::error::Error;
pub use crate::from_spanner::*;
pub use crate::resource::*;
pub use crate::result_set::*;
pub(crate) use crate::session::*;
pub use crate::statement::*;
pub use crate::to_spanner::*;
pub use crate::transaction::*;
pub use crate::types::*;
pub use crate::value::*;

mod auth;
mod client;
mod config;
mod connection;
mod error;
mod from_spanner;
mod resource;
mod result_set;
mod session;
mod statement;
mod to_spanner;
mod transaction;
mod types;
mod value;
