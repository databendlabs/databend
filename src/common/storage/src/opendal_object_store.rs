// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! ObjectStore adapter over OpenDAL 0.58 for object_store 0.12.
//!
//! `object_store_opendal` 0.58 requires object_store 0.13, which is not yet
//! aligned with deltalake/lance in this workspace. This local adapter keeps
//! those integrations on object_store 0.12 while using OpenDAL 0.58.

use std::future::IntoFuture;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::SystemTime;

use chrono::DateTime;
use chrono::Utc;
use futures::Future;
use futures::Stream;
use object_store::ObjectMeta;
use opendal::Metadata;
use opendal::raw::Timestamp as OpendalTimestamp;
use pin_project::pin_project;

/// Format `opendal::Error` to `object_store::Error`.
pub fn format_object_store_error(err: opendal::Error, path: &str) -> object_store::Error {
    use opendal::ErrorKind;
    match err.kind() {
        ErrorKind::NotFound => object_store::Error::NotFound {
            path: path.to_string(),
            source: Box::new(err),
        },
        ErrorKind::Unsupported => object_store::Error::NotSupported {
            source: Box::new(err),
        },
        ErrorKind::AlreadyExists => object_store::Error::AlreadyExists {
            path: path.to_string(),
            source: Box::new(err),
        },
        ErrorKind::ConditionNotMatch => object_store::Error::Precondition {
            path: path.to_string(),
            source: Box::new(err),
        },
        kind => object_store::Error::Generic {
            store: kind.into_static(),
            source: Box::new(err),
        },
    }
}

fn timestamp_to_datetime(ts: OpendalTimestamp) -> Option<DateTime<Utc>> {
    Some(DateTime::<Utc>::from(SystemTime::from(ts)))
}

fn datetime_to_timestamp(dt: DateTime<Utc>) -> Option<OpendalTimestamp> {
    OpendalTimestamp::from_millisecond(dt.timestamp_millis()).ok()
}

/// Format `opendal::Metadata` to `object_store::ObjectMeta`.
pub fn format_object_meta(path: &str, meta: &Metadata) -> ObjectMeta {
    ObjectMeta {
        location: path.into(),
        last_modified: meta
            .last_modified()
            .and_then(timestamp_to_datetime)
            .unwrap_or_default(),
        size: meta.content_length(),
        e_tag: meta.etag().map(|x| x.to_string()),
        version: meta.version().map(|x| x.to_string()),
    }
}

/// Make given future `Send` (no-op wrapper for non-wasm).
pub trait IntoSendFuture {
    type Output;
    fn into_send(self) -> Self::Output;
}

impl<T> IntoSendFuture for T
where T: IntoFuture
{
    type Output = NoopWrapper<T::IntoFuture>;
    fn into_send(self) -> Self::Output {
        NoopWrapper::new(self.into_future())
    }
}

/// Make given Stream `Send` (no-op wrapper for non-wasm).
pub trait IntoSendStream {
    type Output;
    fn into_send(self) -> Self::Output;
}

impl<T> IntoSendStream for T
where T: Stream
{
    type Output = NoopWrapper<T>;
    fn into_send(self) -> Self::Output {
        NoopWrapper::new(self)
    }
}

#[pin_project]
pub struct NoopWrapper<T> {
    #[pin]
    item: T,
}

impl<T> NoopWrapper<T> {
    pub fn new(item: T) -> Self {
        Self { item }
    }
}

impl<T> Future for NoopWrapper<T>
where T: Future
{
    type Output = T::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().item.poll(cx)
    }
}

impl<T> Stream for NoopWrapper<T>
where T: Stream
{
    type Item = T::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().item.poll_next(cx)
    }
}

use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use object_store::GetOptions;
use object_store::GetRange;
use object_store::GetResult;
use object_store::GetResultPayload;
use object_store::ListResult;
use object_store::MultipartUpload;
use object_store::ObjectStore;
use object_store::PutMode;
use object_store::PutMultipartOptions;
use object_store::PutOptions;
use object_store::PutPayload;
use object_store::PutResult;
use object_store::UploadPart;
use object_store::path::Path;
use opendal::Buffer;
use opendal::Operator;
use opendal::OperatorInfo;
use opendal::Writer;
use opendal::options::CopyOptions;
use opendal::raw::percent_decode_path;
use tokio::sync::Mutex;
use tokio::sync::Notify;

/// OpendalStore implements ObjectStore trait by using opendal.
///
/// This allows users to use opendal as an object store without extra cost.
///
/// Visit [`opendal::services`] for more information about supported services.
///
/// ```no_run
/// use std::sync::Arc;
///
/// use bytes::Bytes;
/// use databend_common_storage::OpendalStore;
/// use object_store::ObjectStore;
/// use object_store::path::Path;
/// use opendal::Builder;
/// use opendal::Operator;
/// use opendal::services::S3;
///
/// #[tokio::main]
/// async fn main() {
///     let builder = S3::default()
///         .access_key_id("my_access_key")
///         .secret_access_key("my_secret_key")
///         .endpoint("my_endpoint")
///         .region("my_region");
///
///     // Create a new operator
///     let operator = Operator::new(builder).unwrap();
///
///     // Create a new object store
///     let object_store = Arc::new(OpendalStore::new(operator));
///
///     let path = Path::from("data/nested/test.txt");
///     let bytes = Bytes::from_static(b"hello, world! I am nested.");
///
///     object_store.put(&path, bytes.clone().into()).await.unwrap();
///
///     let content = object_store
///         .get(&path)
///         .await
///         .unwrap()
///         .bytes()
///         .await
///         .unwrap();
///
///     assert_eq!(content, bytes);
/// }
/// ```
#[derive(Clone)]
pub struct OpendalStore {
    info: Arc<OperatorInfo>,
    inner: Operator,
}

impl OpendalStore {
    /// Create OpendalStore by given Operator.
    pub fn new(op: Operator) -> Self {
        Self {
            info: Arc::new(op.info()),
            inner: op,
        }
    }

    /// Get the Operator info.
    pub fn info(&self) -> &OperatorInfo {
        self.info.as_ref()
    }

    /// Copy a file from one location to another
    async fn copy_request(
        &self,
        from: &Path,
        to: &Path,
        if_not_exists: bool,
    ) -> object_store::Result<()> {
        let mut copy_options = CopyOptions::default();
        if if_not_exists {
            copy_options.if_not_exists = true;
        }

        // Perform the copy operation
        self.inner
            .copy_options(
                &percent_decode_path(from.as_ref()),
                &percent_decode_path(to.as_ref()),
                copy_options,
            )
            .into_send()
            .await
            .map_err(|err| {
                if if_not_exists && err.kind() == opendal::ErrorKind::AlreadyExists {
                    object_store::Error::AlreadyExists {
                        path: to.to_string(),
                        source: Box::new(err),
                    }
                } else {
                    format_object_store_error(err, from.as_ref())
                }
            })?;

        Ok(())
    }
}

impl Debug for OpendalStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpendalStore")
            .field("scheme", &self.info.scheme())
            .field("name", &self.info.name())
            .field("root", &self.info.root())
            .field("capability", &self.info.capability())
            .finish()
    }
}

impl Display for OpendalStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let info = self.inner.info();
        write!(
            f,
            "Opendal({}, bucket={}, root={})",
            info.scheme(),
            info.name(),
            info.root()
        )
    }
}

impl From<Operator> for OpendalStore {
    fn from(value: Operator) -> Self {
        Self::new(value)
    }
}

#[async_trait]
impl ObjectStore for OpendalStore {
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let decoded_location = percent_decode_path(location.as_ref());
        let mut future_write = self
            .inner
            .write_with(&decoded_location, Buffer::from_iter(bytes));
        let opts_mode = opts.mode.clone();
        match opts.mode {
            PutMode::Overwrite => {}
            PutMode::Create => {
                future_write = future_write.if_not_exists(true);
            }
            PutMode::Update(update_version) => {
                let Some(etag) = update_version.e_tag else {
                    Err(object_store::Error::NotSupported {
                        source: Box::new(opendal::Error::new(
                            opendal::ErrorKind::Unsupported,
                            "etag is required for conditional put",
                        )),
                    })?
                };
                future_write = future_write.if_match(etag.as_str());
            }
        }
        let rp = future_write.into_send().await.map_err(|err| {
            match format_object_store_error(err, location.as_ref()) {
                object_store::Error::Precondition { path, source }
                    if opts_mode == PutMode::Create =>
                {
                    object_store::Error::AlreadyExists { path, source }
                }
                e => e,
            }
        })?;

        let e_tag = rp.etag().map(|s| s.to_string());
        let version = rp.version().map(|s| s.to_string());

        Ok(PutResult { e_tag, version })
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let decoded_location = percent_decode_path(location.as_ref());
        let writer = self
            .inner
            .writer_with(&decoded_location)
            .concurrent(8)
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;
        let upload = OpendalMultipartUpload::new(writer, location.clone());

        Ok(Box::new(upload))
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        const DEFAULT_CONCURRENT: usize = 8;

        let mut options = opendal::options::WriteOptions {
            concurrent: DEFAULT_CONCURRENT,
            ..Default::default()
        };

        // Collect user metadata separately to handle multiple entries
        let mut user_metadata = HashMap::new();

        // Handle attributes if provided
        for (key, value) in opts.attributes.iter() {
            match key {
                object_store::Attribute::CacheControl => {
                    options.cache_control = Some(value.to_string());
                }
                object_store::Attribute::ContentDisposition => {
                    options.content_disposition = Some(value.to_string());
                }
                object_store::Attribute::ContentEncoding => {
                    options.content_encoding = Some(value.to_string());
                }
                object_store::Attribute::ContentLanguage => {
                    // no support
                    continue;
                }
                object_store::Attribute::ContentType => {
                    options.content_type = Some(value.to_string());
                }
                object_store::Attribute::Metadata(k) => {
                    user_metadata.insert(k.to_string(), value.to_string());
                }
                _ => {}
            }
        }

        // Apply user metadata if any entries were collected
        if !user_metadata.is_empty() {
            options.user_metadata = Some(user_metadata);
        }

        let decoded_location = percent_decode_path(location.as_ref());
        let writer = self
            .inner
            .writer_options(&decoded_location, options)
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;
        let upload = OpendalMultipartUpload::new(writer, location.clone());

        Ok(Box::new(upload))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let raw_location = percent_decode_path(location.as_ref());
        let meta = {
            let mut s = self.inner.stat_with(&raw_location);
            if let Some(version) = &options.version {
                s = s.version(version.as_str())
            }
            if let Some(if_match) = &options.if_match {
                s = s.if_match(if_match.as_str());
            }
            if let Some(if_none_match) = &options.if_none_match {
                s = s.if_none_match(if_none_match.as_str());
            }
            if let Some(if_modified_since) =
                options.if_modified_since.and_then(datetime_to_timestamp)
            {
                s = s.if_modified_since(if_modified_since);
            }
            if let Some(if_unmodified_since) =
                options.if_unmodified_since.and_then(datetime_to_timestamp)
            {
                s = s.if_unmodified_since(if_unmodified_since);
            }
            s.into_send()
                .await
                .map_err(|err| format_object_store_error(err, location.as_ref()))?
        };

        // Convert user defined metadata from OpenDAL to object_store attributes
        let mut attributes = object_store::Attributes::new();
        if let Some(user_meta) = meta.user_metadata() {
            for (key, value) in user_meta {
                attributes.insert(
                    object_store::Attribute::Metadata(key.clone().into()),
                    value.clone().into(),
                );
            }
        }

        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: meta
                .last_modified()
                .and_then(timestamp_to_datetime)
                .unwrap_or_default(),
            size: meta.content_length(),
            e_tag: meta.etag().map(|x| x.to_string()),
            version: meta.version().map(|x| x.to_string()),
        };

        if options.head {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(futures::stream::empty())),
                range: 0..0,
                meta,
                attributes,
            });
        }

        let reader = {
            let mut r = self.inner.reader_with(raw_location.as_ref());
            if let Some(version) = options.version {
                r = r.version(version.as_str());
            }
            if let Some(if_match) = options.if_match {
                r = r.if_match(if_match.as_str());
            }
            if let Some(if_none_match) = options.if_none_match {
                r = r.if_none_match(if_none_match.as_str());
            }
            if let Some(if_modified_since) =
                options.if_modified_since.and_then(datetime_to_timestamp)
            {
                r = r.if_modified_since(if_modified_since);
            }
            if let Some(if_unmodified_since) =
                options.if_unmodified_since.and_then(datetime_to_timestamp)
            {
                r = r.if_unmodified_since(if_unmodified_since);
            }
            r.into_send()
                .await
                .map_err(|err| format_object_store_error(err, location.as_ref()))?
        };

        let read_range = match options.range {
            Some(GetRange::Bounded(r)) => {
                if r.start >= r.end || r.start >= meta.size {
                    0..0
                } else {
                    let end = r.end.min(meta.size);
                    r.start..end
                }
            }
            Some(GetRange::Offset(r)) => {
                if r < meta.size {
                    r..meta.size
                } else {
                    0..0
                }
            }
            Some(GetRange::Suffix(r)) if r < meta.size => (meta.size - r)..meta.size,
            _ => 0..meta.size,
        };

        let stream = reader
            .into_bytes_stream(read_range.start..read_range.end)
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?
            .into_send()
            .map_err(|err: io::Error| object_store::Error::Generic {
                store: "IoError",
                source: Box::new(err),
            });

        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(stream)),
            range: read_range.start..read_range.end,
            meta,
            attributes,
        })
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let decoded_location = percent_decode_path(location.as_ref());
        self.inner
            .delete(&decoded_location)
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        // object_store `Path` always removes trailing slash
        // need to add it back
        let path = prefix.map_or("".into(), |x| {
            format!("{}/", percent_decode_path(x.as_ref()))
        });

        let this = self.clone();
        let fut = async move {
            let stream = this
                .inner
                .lister_with(&path)
                .recursive(true)
                .await
                .map_err(|err| format_object_store_error(err, &path))?;

            let stream = stream.then(|res| async {
                let entry = res.map_err(|err| format_object_store_error(err, ""))?;
                let meta = entry.metadata();

                Ok(format_object_meta(entry.path(), meta))
            });
            Ok::<_, object_store::Error>(stream)
        };

        fut.into_stream().try_flatten().into_send().boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let path = prefix.map_or("".into(), |x| {
            format!("{}/", percent_decode_path(x.as_ref()))
        });
        let offset = offset.clone();

        // clone self for 'static lifetime
        // clone self is cheap
        let this = self.clone();

        let fut = async move {
            let list_with_start_after = this.inner.info().capability().list_with_start_after;
            let mut fut = this.inner.lister_with(&path).recursive(true);

            // Use native start_after support if possible.
            if list_with_start_after {
                fut = fut.start_after(offset.as_ref());
            }

            let lister = fut
                .await
                .map_err(|err| format_object_store_error(err, &path))?
                .then(move |entry| {
                    let path = path.clone();
                    let this = this.clone();
                    async move {
                        let entry = entry.map_err(|err| format_object_store_error(err, &path))?;
                        let (path, metadata) = entry.into_parts();

                        // If it's a dir or last_modified is present, we can use it directly.
                        if metadata.is_dir() || metadata.last_modified().is_some() {
                            let object_meta = format_object_meta(&path, &metadata);
                            return Ok(object_meta);
                        }

                        let metadata = this
                            .inner
                            .stat(&path)
                            .await
                            .map_err(|err| format_object_store_error(err, &path))?;
                        let object_meta = format_object_meta(&path, &metadata);
                        Ok::<_, object_store::Error>(object_meta)
                    }
                })
                .into_send()
                .boxed();

            let stream = if list_with_start_after {
                lister
            } else {
                lister
                    .try_filter(move |entry| futures::future::ready(entry.location > offset))
                    .into_send()
                    .boxed()
            };

            Ok::<_, object_store::Error>(stream)
        };

        fut.into_stream().into_send().try_flatten().boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let path = prefix.map_or("".into(), |x| {
            format!("{}/", percent_decode_path(x.as_ref()))
        });
        let mut stream = self
            .inner
            .lister_with(&path)
            .into_future()
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, &path))?
            .into_send();

        let mut common_prefixes = Vec::new();
        let mut objects = Vec::new();

        while let Some(res) = stream.next().into_send().await {
            let entry = res.map_err(|err| format_object_store_error(err, ""))?;
            let meta = entry.metadata();

            if meta.is_dir() {
                common_prefixes.push(entry.path().into());
            } else if meta.last_modified().is_some() {
                objects.push(format_object_meta(entry.path(), meta));
            } else {
                let meta = self
                    .inner
                    .stat(entry.path())
                    .into_send()
                    .await
                    .map_err(|err| format_object_store_error(err, entry.path()))?;
                objects.push(format_object_meta(entry.path(), &meta));
            }
        }

        Ok(ListResult {
            common_prefixes,
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.copy_request(from, to, false).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.copy_request(from, to, true).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner
            .rename(
                &percent_decode_path(from.as_ref()),
                &percent_decode_path(to.as_ref()),
            )
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, from.as_ref()))?;

        Ok(())
    }
}

/// `MultipartUpload`'s impl based on `Writer` in opendal
///
/// # Notes
///
/// OpenDAL writer can handle concurrent internally we don't generate real `UploadPart` like existing
/// implementation do. Instead, we just write the part and notify the next task to be written.
///
/// The lock here doesn't really involve the write process, it's just for the notify mechanism.
struct OpendalMultipartUpload {
    writer: Arc<Mutex<Writer>>,
    location: Path,
    next_notify: Option<Arc<Notify>>,
}

impl OpendalMultipartUpload {
    fn new(writer: Writer, location: Path) -> Self {
        Self {
            writer: Arc::new(Mutex::new(writer)),
            location,
            next_notify: None,
        }
    }
}

#[async_trait]
impl MultipartUpload for OpendalMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let writer = self.writer.clone();
        let location = self.location.clone();

        // Generate next notify which will be notified after the current part is written.
        let next_notify = Arc::new(Notify::new());
        // Fetch the notify for current part to wait for it to be written.
        let current_notify = self.next_notify.replace(next_notify.clone());

        async move {
            // current_notify == None means that it's the first part, we don't need to wait.
            if let Some(notify) = current_notify {
                // Wait for the previous part to be written
                notify.notified().await;
            }

            let mut writer = writer.lock().await;
            let result = writer
                .write(Buffer::from_iter(data.into_iter()))
                .await
                .map_err(|err| format_object_store_error(err, location.as_ref()));

            // Notify the next part to be written
            next_notify.notify_one();

            result
        }
        .into_send()
        .boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let mut writer = self.writer.lock().await;
        let metadata = writer
            .close()
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, self.location.as_ref()))?;

        let e_tag = metadata.etag().map(|s| s.to_string());
        let version = metadata.version().map(|s| s.to_string());

        Ok(PutResult { e_tag, version })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let mut writer = self.writer.lock().await;
        writer
            .abort()
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, self.location.as_ref()))
    }
}

impl Debug for OpendalMultipartUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpendalMultipartUpload")
            .field("location", &self.location)
            .finish()
    }
}
