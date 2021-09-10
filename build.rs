use std::io::Result;
fn main() -> Result<()> {
    prost_build::Config::new().compile_protos(
        &[
            "commit_response.proto",
            "keys.proto",
            "mutation.proto",
            "query_plan.proto",
            "result_set.proto",
            "spanner.proto",
            "transaction.proto",
            "type.proto",
        ]
        .iter()
        .map(|&filename| format!("proto/googleapis/google/spanner/v1/{}", filename))
        .collect::<Vec<String>>(),
        &["proto/googleapis"],
    )?;
    Ok(())
}
