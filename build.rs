use std::io::Result;
fn main() -> Result<()> {
    tonic_build::configure()
        .build_server(false)
        .file_descriptor_set_path("/tmp/fd.bin")
        .compile(
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
            &["proto/googleapis".to_string()],
        )?;
    Ok(())
}
