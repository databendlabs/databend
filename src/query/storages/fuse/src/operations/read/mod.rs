pub mod fuse_source_new;
mod native_data_source;
mod native_data_source_deserializer;
mod native_data_source_reader;
mod parquet_data_source;
mod parquet_data_source_deserializer;
mod parquet_data_source_reader;

pub use fuse_source_new::build_fuse_parquet_source_pipeline;
