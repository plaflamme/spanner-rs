# spanner-rs

An asynchronous Rust client for [Cloud Spanner](https://cloud.google.com/spanner/).

[![Build Status](https://github.com/plaflamme/spanner-rs/workflows/CI/badge.svg)](https://github.com/plaflamme/spanner-rs/actions)
[![Crates.io](https://img.shields.io/crates/v/spanner-rs)](https://crates.io/crates/spanner-rs)
[![Documentation](https://docs.rs/spanner-rs/badge.svg)](https://docs.rs/spanner-rs)
[![Crates.io](https://img.shields.io/crates/l/spanner-rs)](LICENSE)

# Implementation

The client uses gRPC to interface with Cloud Spanner. As such, this crate uses [`tonic`](https://crates.io/crates/tonic) and related crates (i.e.: [`prost`](https://crates.io/crates/prost), [`tower`](https://github.com/tower-rs/tower) and [`tokio`](https://crates.io/crates/tokio)).

The client also uses [`bb8`](https://crates.io/crates/bb8) to maintain a pool of Cloud Spanner sessions (conceptually similar to a connection pool in other databases).

The implementation is heavily inspired by the excellent [`postgres`](https://crates.io/crates/postgres) and related crates.

# Status

Spanner is a complex database and this client does not implement all available features.
The current focus is on getting idiomatic SQL support for both reads and writes.

It's not recommended to use this in production for any serious workload.

# Features

## Database Client

- [x] SQL read-only, single use, time-bounded transactions
- [x] SQL read-write transactions with retries
- [x] Type classes to convert Rust values to/from Cloud Spanner values
- [ ] Timestamp and Date type support (chrono feature?)
- [ ] Json type support (serde json feature)
- [ ] [Streaming result sets](https://cloud.google.com/spanner/docs/reference/rpc/google.spanner.v1#google.spanner.v1.Spanner.ExecuteStreamingSql)
- [ ] Derive `ToSpanner` and `FromSpanner` for `struct`s

## Admin Client

- [ ] DDL statements

# Example

```rust
use spanner_rs::{Client, Error};

#[tokio::main]
fn main() -> Result<(), Error> {
    let mut client = Client::configure()
        .project("my-gcp-project")
        .instance("my-instance")
        .database("my-database")
        .connect()
        .await?;

    // assuming the following table:
    //   person(id INT64, name STRING(MAX), data BYTES(MAX))
    client
        .read_write()
        .run(|tx| {
            tx.execute_update(
                "INSERT INTO person(id, name, data) VALUES(@id, @name, NULL)",
                &[("id", &42), ("name", &"ferris")],
            )
        })
        .await?;

    let result_set = client
        .read_only()
        .execute_sql("SELECT * FROM person", &[])
        .await?;

    for row in result_set.iter() {
        let id: u32 = row.get_by_name("id")?;
        let name: &str = row.get_by_name("name")?;
        let data: Option<&[u8]> = row.get_by_name("data")?;

        println!("found person: {} {} {:?}", id, name, data);
    }

    Ok(())
}
```