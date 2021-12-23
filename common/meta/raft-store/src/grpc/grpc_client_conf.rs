use common_flight_rpc::GrpcClientConf;

#[derive(Clone, Debug, Default)]
pub struct MetaGrpcClientConf {
    pub meta_service_config: GrpcClientConf,
    pub kv_service_config: GrpcClientConf,
    pub client_timeout_in_second: u64,
}
