fn main() {
    const PROTOC_ENVAR: &str = "PROTOC";
    if std::env::var(PROTOC_ENVAR).is_err() {
        unsafe {
            #[cfg(not(windows))]
            std::env::set_var(PROTOC_ENVAR, protobuf_src::protoc());
        }
    }

    tonic_prost_build::configure()
        .compile_protos(&["protos/shredstream.proto"], &["protos"])
        .unwrap();
}
