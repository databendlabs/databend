#[cfg(feature = "io_json_integration")]
mod ipc;

#[cfg(feature = "io_parquet")]
mod parquet;

#[cfg(feature = "io_flight")]
mod flight;
