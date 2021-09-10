pub mod google {
    pub mod rpc {
        tonic::include_proto!("google.rpc");
    }
    pub mod spanner {
        pub mod v1 {
            tonic::include_proto!("google.spanner.v1");
        }
    }
}
