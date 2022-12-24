pub mod fuse_parquet_source_new;
mod parquet_data_source_reader;
mod parquet_data_source;
mod parquet_data_source_deserializer;

pub use fuse_parquet_source_new::build_fuse_parquet_source_pipeline;
