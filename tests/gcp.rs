use spanner_rs::{Client, DatabaseId, InstanceId, ReadContext};

#[tokio::test]
#[cfg_attr(not(feature = "gcp"), ignore)]
async fn test_connect() {
    env_logger::init();
    dotenv::dotenv().ok();

    let project_id = std::env::var("SPANNER_RS_PROJECT_ID")
        .expect("missing SPANNER_RS_PROJECT_ID environment variable");
    let instance = std::env::var("SPANNER_RS_INSTANCE")
        .expect("missing SPANNER_RS_INSTANCE environment variable");
    let database = std::env::var("SPANNER_RS_DATABASE")
        .expect("missing SPANNER_RS_DATABASE environment variable");

    let client = Client::config()
        .database(DatabaseId::new(
            InstanceId::new(&project_id, &instance),
            &database,
        ))
        .connect()
        .await
        .unwrap();

    client
        .read_only()
        .execute_sql("SELECT 1", vec![])
        .await
        .unwrap();
}
