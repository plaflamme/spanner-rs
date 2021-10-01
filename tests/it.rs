#![feature(async_closure)]

use std::sync::atomic::{AtomicU16, Ordering};

use spanner_rs::{Error, ReadContext, ResultSet, TransactionContext, Value};

#[cfg(not(feature = "gcp"))]
mod spanner_emulator;
#[cfg(not(feature = "gcp"))]
use spanner_emulator::new_client;

#[cfg(feature = "gcp")]
mod gcp;
#[cfg(feature = "gcp")]
use gcp::new_client;

#[tokio::test]
async fn test_read_only() -> Result<(), Error> {
    let client = new_client().await?;
    let mut read_only = client.read_only();

    let result_set = read_only
        .execute_sql("SELECT * FROM my_table", vec![])
        .await?;
    let row = result_set.iter().next();
    assert!(row.is_none());

    let result_set = read_only
        .execute_sql("SELECT * FROM my_table", vec![])
        .await?;
    let row = result_set.iter().next();
    assert!(row.is_none());
    Ok(())
}

#[tokio::test]
async fn test_read_write() -> Result<(), Error> {
    let mut client = new_client().await?;
    let row_count = client
        .read_write()
        .run(|ctx| {
            Box::pin(async move {
                ctx.execute_update(
                    "INSERT INTO my_table(a,b) VALUES(@a, @b)",
                    vec![
                        ("a".to_string(), Value::Int64(1)),
                        ("b".to_string(), Value::String("one".to_string())),
                    ],
                )
                .await
            })
        })
        .await?;

    assert_eq!(row_count, 1);

    let result_set = client
        .read_only()
        .execute_sql("SELECT * FROM my_table", vec![])
        .await?;
    let row = result_set.iter().next();
    assert!(row.is_some());
    let row = row.unwrap();
    assert_eq!(row.try_get_by_name("a")?, Value::Int64(1));
    assert_eq!(row.try_get_by_name("b")?, Value::String("one".to_string()));

    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "gcp"), ignore)]
async fn test_read_write_abort() -> Result<(), Error> {
    async fn write(evaluations: &AtomicU16) -> Result<ResultSet, Error> {
        new_client()
            .await?
            .read_write()
            .run(|ctx| {
                evaluations.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    let rs = ctx.execute_sql("SELECT * FROM my_table", vec![]).await?;
                    let rows = rs.iter().count();
                    ctx.execute_update(
                        "INSERT INTO my_table(a,b) VALUES(@a, @b)",
                        vec![
                            ("a".to_string(), Value::Int64(rows as i64)),
                            ("b".to_string(), Value::String(rows.to_string())),
                        ],
                    )
                    .await?;
                    ctx.execute_sql("SELECT * FROM my_table", vec![]).await
                })
            })
            .await
    }

    let evaluations = AtomicU16::new(0);

    let (one, two) = tokio::join!(write(&evaluations), write(&evaluations));

    let diff = i32::abs(one?.iter().count() as i32 - two?.iter().count() as i32);
    assert_eq!(diff, 1);

    // we expect at least one retry
    assert!(evaluations.load(Ordering::SeqCst) > 2);

    Ok(())
}
