use databend_common_grpc::TcpKeepAliveConfig;
use databend_common_settings::FlightKeepAliveParams;

pub fn build_keep_alive_config(params: FlightKeepAliveParams) -> Option<TcpKeepAliveConfig> {
    if params.is_disabled() {
        None
    } else {
        Some(TcpKeepAliveConfig {
            time: params.time,
            interval: params.interval,
            retries: params.retries,
        })
    }
}
