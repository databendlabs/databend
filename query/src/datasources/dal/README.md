# DAL

A data access layer (DAL) in Datafuse is a layer which provides methods for accessing to data stored in shared storage, such as AWS S3, Azure Blob, DFS or Local disk.

The API like:
```rust
    async fn get_input_stream(
    &self,
    path: &str,
    stream_len: Option<u64>,
) -> Result<Self::InputStream>;

async fn get(&self, path: &str) -> Result<Bytes>;

async fn put(&self, path: &str, content: Vec<u8>) -> common_exception::Result<()>;
```

