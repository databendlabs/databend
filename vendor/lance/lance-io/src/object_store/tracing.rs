// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Wrappers around object_store that apply tracing

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use lance_core::utils::tracing::StreamTracingExt;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, Result as OSResult, UploadPart,
};
use tracing::{debug_span, instrument, Instrument, Span};

#[derive(Debug)]
pub struct TracedMultipartUpload {
    write_span: Span,
    target: Box<dyn MultipartUpload>,
}

#[async_trait::async_trait]
impl MultipartUpload for TracedMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let write_span = self.write_span.clone();
        let fut = self.target.put_part(data);
        Box::pin(fut.instrument(write_span))
    }

    #[instrument(level = "debug", skip_all)]
    async fn complete(&mut self) -> OSResult<PutResult> {
        self.target.complete().await
    }

    #[instrument(level = "debug", skip_all)]
    async fn abort(&mut self) -> OSResult<()> {
        self.target.abort().await
    }
}

#[derive(Debug)]
pub struct TracedObjectStore {
    target: Arc<dyn object_store::ObjectStore>,
}

impl std::fmt::Display for TracedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("TracedObjectStore({})", self.target))
    }
}

#[async_trait::async_trait]
#[deny(clippy::missing_trait_methods)]
impl object_store::ObjectStore for TracedObjectStore {
    #[instrument(level = "debug", skip(self, bytes))]
    async fn put(&self, location: &Path, bytes: PutPayload) -> OSResult<PutResult> {
        self.target.put(location, bytes).await
    }

    #[instrument(level = "debug", skip(self, bytes))]
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> OSResult<PutResult> {
        self.target.put_opts(location, bytes, opts).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> OSResult<Box<dyn object_store::MultipartUpload>> {
        let upload = self.target.put_multipart(location).await?;
        Ok(Box::new(TracedMultipartUpload {
            target: upload,
            write_span: debug_span!("put_multipart"),
        }))
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OSResult<Box<dyn object_store::MultipartUpload>> {
        let upload = self.target.put_multipart_opts(location, opts).await?;
        Ok(Box::new(TracedMultipartUpload {
            target: upload,
            write_span: debug_span!("put_multipart_opts"),
        }))
    }

    #[instrument(level = "debug", skip(self, location))]
    async fn get(&self, location: &Path) -> OSResult<GetResult> {
        self.target.get(location).await
    }

    #[instrument(level = "debug", skip(self, options))]
    async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
        self.target.get_opts(location, options).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_range(&self, location: &Path, range: Range<u64>) -> OSResult<Bytes> {
        self.target.get_range(location, range).await
    }

    #[instrument(level = "debug", skip(self, ranges))]
    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OSResult<Vec<Bytes>> {
        self.target.get_ranges(location, ranges).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn head(&self, location: &Path) -> OSResult<ObjectMeta> {
        self.target.head(location).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete(&self, location: &Path) -> OSResult<()> {
        self.target.delete(location).await
    }

    #[instrument(level = "debug", skip_all)]
    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, OSResult<Path>>,
    ) -> BoxStream<'a, OSResult<Path>> {
        self.target
            .delete_stream(locations)
            .stream_in_current_span()
            .boxed()
    }

    #[instrument(level = "debug", skip(self))]
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.target.list(prefix).stream_in_current_span().boxed()
    }

    #[instrument(level = "debug", skip(self))]
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.target
            .list_with_offset(prefix, offset)
            .stream_in_current_span()
            .boxed()
    }

    #[instrument(level = "debug", skip(self))]
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
        self.target.list_with_delimiter(prefix).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn copy(&self, from: &Path, to: &Path) -> OSResult<()> {
        self.target.copy(from, to).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn rename(&self, from: &Path, to: &Path) -> OSResult<()> {
        self.target.rename(from, to).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> OSResult<()> {
        self.target.rename_if_not_exists(from, to).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OSResult<()> {
        self.target.copy_if_not_exists(from, to).await
    }
}

pub trait ObjectStoreTracingExt {
    fn traced(self) -> Arc<dyn object_store::ObjectStore>;
}

impl ObjectStoreTracingExt for Arc<dyn object_store::ObjectStore> {
    fn traced(self) -> Arc<dyn object_store::ObjectStore> {
        Arc::new(TracedObjectStore { target: self })
    }
}

impl<T: object_store::ObjectStore> ObjectStoreTracingExt for Arc<T> {
    fn traced(self) -> Arc<dyn object_store::ObjectStore> {
        Arc::new(TracedObjectStore { target: self })
    }
}
