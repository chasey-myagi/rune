pub mod rune {
    pub mod wire {
        pub mod v1 {
            tonic::include_proto!("rune.wire.v1");
        }
    }
}

pub use rune::wire::v1::*;
