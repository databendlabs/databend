use common_arrow::parquet::compression::Compression as CompressionArrow2;
use parquet::basic::BrotliLevel;
use parquet::basic::Compression;
use parquet::basic::GzipLevel;
use parquet::basic::ZstdLevel;
use parquet::file::metadata::ColumnChunkMetaData;
use parquet::schema::types::ColumnDescPtr;

use crate::parquet_part::ColumnMeta;

pub(super) fn convert_compression_from_arrow2(c: CompressionArrow2) -> Compression {
    match c {
        CompressionArrow2::Uncompressed => Compression::UNCOMPRESSED,
        CompressionArrow2::Snappy => Compression::SNAPPY,
        CompressionArrow2::Gzip => Compression::GZIP(GzipLevel::default()),
        CompressionArrow2::Lzo => Compression::LZO,
        CompressionArrow2::Brotli => Compression::BROTLI(BrotliLevel::default()),
        CompressionArrow2::Lz4 => Compression::LZ4,
        CompressionArrow2::Zstd => Compression::ZSTD(ZstdLevel::default()),
        CompressionArrow2::Lz4Raw => Compression::LZ4_RAW,
    }
}

pub(super) fn convert_compression_to_arrow2(c: Compression) -> CompressionArrow2 {
    match c {
        Compression::UNCOMPRESSED => CompressionArrow2::Uncompressed,
        Compression::SNAPPY => CompressionArrow2::Snappy,
        Compression::GZIP(_) => CompressionArrow2::Gzip,
        Compression::LZO => CompressionArrow2::Lzo,
        Compression::BROTLI(_) => CompressionArrow2::Brotli,
        Compression::LZ4 => CompressionArrow2::Lz4,
        Compression::ZSTD(_) => CompressionArrow2::Zstd,
        Compression::LZ4_RAW => CompressionArrow2::Lz4Raw,
    }
}

pub(super) fn convert_column_meta(meta: &ColumnMeta, desc: ColumnDescPtr) -> ColumnChunkMetaData {
    ColumnChunkMetaData::builder(desc)
        .set_compression(convert_compression_from_arrow2(meta.compression))
        .set_total_compressed_size(meta.length as i64)
        .set_data_page_offset(0)
        .build()
        .unwrap()
}
