#![feature(async_closure)]

use std::sync::atomic::{AtomicU16, Ordering};

use spanner_rs::{Error, ReadContext, ResultSet, Statement};

#[cfg(not(feature = "gcp"))]
mod spanner_emulator;
#[cfg(not(feature = "gcp"))]
use spanner_emulator::new_client;

#[cfg(feature = "gcp")]
mod gcp;
#[cfg(feature = "gcp")]
use gcp::new_client;

#[tokio::test]
async fn test_lib_example() -> Result<(), Error> {
    let client = new_client().await?;

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
        .execute_query("SELECT * FROM person", &[])
        .await?;

    for row in result_set.iter() {
        let id: u32 = row.get("id")?;
        let name: &str = row.get("name")?;
        let data: Option<&[u8]> = row.get("data")?;

        println!("found person: {} {} {:?}", id, name, data);
    }

    Ok(())
}

#[tokio::test]
async fn test_read_only() -> Result<(), Error> {
    let client = new_client().await?;
    let mut read_only = client.read_only();

    let result_set = read_only
        .execute_query("SELECT * FROM my_table", &[])
        .await?;
    let row = result_set.iter().next();
    assert!(row.is_none());
    Ok(())
}

#[tokio::test]
async fn test_read_write() -> Result<(), Error> {
    let client = new_client().await?;
    let row_count = client
        .read_write()
        .run(|ctx| {
            Box::pin(async move {
                ctx.execute_update(
                    "INSERT INTO my_table(a,b) VALUES(@a, @b)",
                    &[("a", &1), ("b", &"one")],
                )
                .await
            })
        })
        .await?;

    assert_eq!(row_count, 1);

    let result_set = client
        .read_only()
        .execute_query("SELECT * FROM my_table", &[])
        .await?;
    let row = result_set.iter().next();
    assert!(row.is_some());
    let row = row.unwrap();
    assert_eq!(row.get_unchecked::<i32, _>("a"), 1);
    assert_eq!(row.get_unchecked::<&str, _>("b"), "one");

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
                    let rs = ctx.execute_query("SELECT * FROM my_table", &[]).await?;
                    let rows = rs.iter().count();
                    ctx.execute_update(
                        "INSERT INTO my_table(a,b) VALUES(@a, @b)",
                        &[("a", &(rows as u32)), ("b", &rows.to_string())],
                    )
                    .await?;
                    ctx.execute_query("SELECT * FROM my_table", &[]).await
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

#[tokio::test]
async fn test_read_write_rollback() -> Result<(), Error> {
    let rollback = new_client()
        .await?
        .read_write()
        .run(|tx| {
            Box::pin(async move {
                tx.execute_update(
                    "INSERT INTO my_table(a,b) VALUES (@a,@b)",
                    &[("a", &42), ("b", &"life, the universe and everything")],
                )
                .await?;

                let result: Result<(), Error> = Err(Error::Client("oops".to_string()));
                result
            })
        })
        .await;

    assert!(rollback.is_err());
    match rollback.err() {
        Some(Error::Client(err)) => assert_eq!(err, "oops".to_string()),
        err => panic!("unexpected error: {:?}", err),
    }

    let rs = new_client()
        .await?
        .read_only()
        .execute_query("SELECT * FROM my_table WHERE a = 42", &[])
        .await?;

    assert!(rs.iter().next().is_none());

    Ok(())
}

#[tokio::test]
async fn test_execute_updates() -> Result<(), Error> {
    let client = new_client().await?;
    let row_count = client
        .read_write()
        .run(|ctx| {
            Box::pin(async move {
                ctx.execute_updates(&[
                    &Statement {
                        sql: "INSERT INTO my_table(a,b) VALUES(@a, @b)",
                        params: &[("a", &1), ("b", &"one")],
                    },
                    &Statement {
                        sql: "INSERT INTO my_table(a,b) VALUES(@a, @b)",
                        params: &[("a", &2), ("b", &"two")],
                    },
                    &Statement {
                        sql: "UPDATE my_table SET b = @b WHERE a > 0",
                        params: &[("b", &"foo")],
                    },
                ])
                .await
            })
        })
        .await?;

    assert_eq!(row_count, vec![1, 1, 2]);

    let result_set = client
        .read_only()
        .execute_query("SELECT * FROM my_table ORDER BY a", &[])
        .await?;

    let mut rows = result_set.iter();

    let row = rows.next();
    assert!(row.is_some());
    let row = row.unwrap();
    assert_eq!(row.get_unchecked::<i32, _>("a"), 1);
    assert_eq!(row.get_unchecked::<&str, _>("b"), "foo");

    let row = rows.next();
    assert!(row.is_some());
    let row = row.unwrap();
    assert_eq!(row.get_unchecked::<i32, _>("a"), 2);
    assert_eq!(row.get_unchecked::<&str, _>("b"), "foo");

    Ok(())
}
