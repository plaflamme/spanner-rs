use std::sync::atomic::{AtomicU16, Ordering};

use spanner_rs::*;

async fn new_client() -> Client {
    let _ = env_logger::builder().is_test(true).try_init();
    dotenv::dotenv().ok();

    let project_id = std::env::var("SPANNER_RS_PROJECT_ID")
        .expect("missing SPANNER_RS_PROJECT_ID environment variable");
    let instance = std::env::var("SPANNER_RS_INSTANCE")
        .expect("missing SPANNER_RS_INSTANCE environment variable");
    let database = std::env::var("SPANNER_RS_DATABASE")
        .expect("missing SPANNER_RS_DATABASE environment variable");

    Client::config()
        .database(DatabaseId::new(
            InstanceId::new(&project_id, &instance),
            &database,
        ))
        .connect()
        .await
        .unwrap()
}

#[tokio::test]
#[cfg_attr(not(feature = "gcp"), ignore)]
async fn test_connect() {
    new_client()
        .await
        .read_only()
        .execute_sql("SELECT 1", vec![])
        .await
        .unwrap();
}

#[tokio::test]
#[cfg_attr(not(feature = "gcp"), ignore)]
async fn test_read_write_abort() -> Result<(), Error> {
    async fn write(evaluations: &AtomicU16) -> Result<ResultSet, Error> {
        new_client()
            .await
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
