use std::any::Any;
use std::fmt::{Debug, Display};
use std::fmt::Formatter;
use std::sync::Arc;

use opendal::io_util::CompressAlgorithm;

pub trait DynData: Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;
}

#[derive(Debug)]
pub struct FileInfo {
    pub path: String,
    pub size: usize,
    pub num_splits: usize,
    pub compress_alg: Option<CompressAlgorithm>,
}

pub struct SplitInfo {
    pub file: Arc<FileInfo>,
    pub seq_in_file: usize,
    pub offset: usize,
    pub size: usize,
    pub format_info: Option<Arc<dyn DynData>>,
}

impl Debug for SplitInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileInfo")
            .field("seq_in_file", &self.seq_in_file)
            .field("offset", &self.offset)
            .field("size", &self.size)
            .finish()
    }
}

impl Display for SplitInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}[{}]", self.file.path, self.seq_in_file)
    }
}

pub fn split_by_size(size: usize, split_size: usize) -> Vec<(usize, usize)> {
    let mut splits = vec![];
    let n = (size + split_size - 1) / split_size;
    for i in 0..n - 1 {
        splits.push((i * split_size, std::cmp::min((i + 1) * split_size, size)))
    }
    splits
}

impl SplitInfo {
    pub fn from_stream_split(path: String, compress_alg: Option<CompressAlgorithm>) -> Self {
        SplitInfo {
            file: Arc::new(FileInfo {
                path,
                size: 0,
                num_splits: 1,
                compress_alg
            }),
            seq_in_file: 0,
            offset: 0,
            size: 0,
            format_info: None
        }
    }
}
