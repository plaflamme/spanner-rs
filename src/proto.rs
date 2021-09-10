pub mod google {
    pub mod rpc {
        include!(concat!(env!("OUT_DIR"), "/google.rpc.rs"));
    }
    pub mod spanner {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/google.spanner.v1.rs"));
        }
    }
}
