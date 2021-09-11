pub use crate::client::Client;
pub use crate::config::Config;
pub use crate::error::Error;
pub use crate::resource::*;

mod client;
mod config;
mod error;
// TODO: drop pub
pub mod proto;
mod resource;
