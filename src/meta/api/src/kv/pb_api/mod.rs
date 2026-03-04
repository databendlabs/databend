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

//! Kv API with `kvapi::Key` type key and protobuf encoded value.

pub mod compress;
pub mod errors;

mod codec;
mod upsert_pb;

use std::future::Future;

use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::data_id::DataId;
use databend_common_meta_app::tenant_key::resource::TenantResource;
use databend_common_proto_conv::FromToProto;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::DirName;
use databend_meta_kvapi::kvapi::KVApi;
use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_kvapi::kvapi::NonEmptyItem;
use databend_meta_types::Change;
use databend_meta_types::SeqV;
use databend_meta_types::UpsertKV;
use databend_meta_types::protobuf::StreamItem;
use futures::TryStreamExt;
use futures::future::FutureExt;
use futures::future::TryFutureExt;
use futures::stream;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use itertools::Itertools;
use seq_marked::SeqValue;

pub(crate) use self::codec::decode_change;
pub(crate) use self::codec::decode_non_empty_item;
pub use self::codec::decode_pb;
pub use self::codec::decode_seqv;
pub(crate) use self::codec::encode_operation;
pub use self::codec::encode_pb;
pub use self::upsert_pb::UpsertPB;
use crate::kv_pb_api::errors::PbApiReadError;
use crate::kv_pb_api::errors::PbApiWriteError;
use crate::kv_pb_api::errors::StreamReadEof;

/// This trait provides a way to access a kv store with `kvapi::Key` type key and protobuf encoded value.
pub trait KVPbApi: KVApi {
    /// The number of keys in one batch get.
    const CHUNK_SIZE: usize = 256;

    /// Update or insert a protobuf encoded value by kvapi::Key.
    ///
    /// The key will be converted to string and the value is encoded by `FromToProto`.
    /// It returns the transition before and after executing the operation.
    /// The state before and after will be the same if the seq does not match.
    fn upsert_pb<K>(
        &self,
        req: &UpsertPB<K>,
    ) -> impl Future<Output = Result<Change<K::ValueType>, Self::Error>> + Send
    where
        K: kvapi::Key + Send,
        K::ValueType: FromToProto,
        Self::Error: From<PbApiWriteError<Self::Error>>,
    {
        self.upsert_pb_low(req).map_err(Self::Error::from)
    }

    /// Same as `upsert_pb` but returns [`PbApiWriteError`]. No require of `From<PbApiWriteError>` for `Self::Error`.
    fn upsert_pb_low<K>(
        &self,
        req: &UpsertPB<K>,
    ) -> impl Future<Output = Result<Change<K::ValueType>, PbApiWriteError<Self::Error>>> + Send
    where
        K: kvapi::Key,
        K::ValueType: FromToProto,
    {
        // leave it out of async move block to avoid requiring Send
        let k = req.key.to_string_key();
        let v = encode_operation(&req.value);
        let seq = req.seq;
        let value_meta = req.value_meta.clone();

        async move {
            let v = v?;
            let req = UpsertKV::new(k, seq, v, value_meta);
            let reply = self
                .upsert_kv(req)
                .await
                .map_err(PbApiWriteError::KvApiError)?;
            let transition = decode_change(reply)?;
            Ok(transition)
        }
    }

    /// Query kvapi for a 2 level mapping: `name -> id -> value`.
    ///
    /// `K` is the key type for `name -> id`.
    /// `R2` is the level 2 resource type and the level 2 key type is `DataId<R2>`.
    fn get_id_and_value<K, R2>(
        &self,
        key: &K,
    ) -> impl Future<Output = Result<Option<(SeqV<DataId<R2>>, SeqV<R2::ValueType>)>, Self::Error>> + Send
    where
        K: kvapi::Key<ValueType = DataId<R2>> + KeyWithTenant + Sync,
        R2: TenantResource + Send + Sync,
        R2::ValueType: FromToProto,
        Self::Error: From<PbApiReadError<Self::Error>>,
    {
        async move {
            let Some(seq_id) = self.get_pb(key).await? else {
                return Ok(None);
            };

            let id_ident = seq_id.data.into_t_ident(key.tenant());

            let Some(seq_v) = self.get_pb(&id_ident).await? else {
                return Ok(None);
            };

            Ok(Some((seq_id, seq_v)))
        }
    }

    /// Same as [`get_pb`](Self::get_pb)` but returns seq and value separately.
    fn get_pb_seq_and_value<K>(
        &self,
        key: &K,
    ) -> impl Future<Output = Result<(u64, Option<K::ValueType>), Self::Error>> + Send
    where
        K: kvapi::Key + Send + Sync,
        K::ValueType: FromToProto,
        Self::Error: From<PbApiReadError<Self::Error>>,
    {
        async move {
            let seq_v = self.get_pb(key).await?;
            Ok((seq_v.seq(), seq_v.into_value()))
        }
    }

    /// Get protobuf encoded value by kvapi::Key.
    ///
    /// The key will be converted to string and the returned value is decoded by `FromToProto`.
    /// It returns the same error as `KVApi::Error`,
    /// thus it requires KVApi::Error can describe a decoding error, i.e., `impl From<PbApiReadError>`.
    fn get_pb<K>(
        &self,
        key: &K,
    ) -> impl Future<Output = Result<Option<SeqV<K::ValueType>>, Self::Error>> + Send
    where
        K: kvapi::Key,
        K::ValueType: FromToProto,
        Self::Error: From<PbApiReadError<Self::Error>>,
    {
        self.get_pb_low(key).map(|r| r.map_err(Self::Error::from))
    }

    /// Same as `get_pb` but returns [`PbApiReadError`]. No require of `From<PbApiReadError>` for `Self::Error`.
    fn get_pb_low<K>(
        &self,
        key: &K,
    ) -> impl Future<Output = Result<Option<SeqV<K::ValueType>>, PbApiReadError<Self::Error>>> + Send
    where
        K: kvapi::Key,
        K::ValueType: FromToProto,
    {
        let key = key.to_string_key();
        async move {
            let raw_seqv = self
                .get_kv(&key)
                .await
                .map_err(PbApiReadError::KvApiError)?;
            let v = raw_seqv
                .map(|seqv| {
                    decode_seqv::<K::ValueType>(seqv, || format!("decode value of {}", key))
                })
                .transpose()?;
            Ok(v)
        }
    }

    /// Get seq by [`kvapi::Key`].
    fn get_seq<K>(&self, key: &K) -> impl Future<Output = Result<u64, Self::Error>> + Send
    where K: kvapi::Key {
        let key = key.to_string_key();
        async move {
            let raw_seqv = self.get_kv(&key).await?;
            Ok(raw_seqv.seq())
        }
    }

    /// Same as [`get_pb_values`](Self::get_pb_values) but collect the result in a `Vec` instead of a stream.
    ///
    /// If the number of keys is larger than [`Self::CHUNK_SIZE`], it will be split into multiple requests.
    fn get_pb_values_vec<K, I>(
        &self,
        keys: I,
    ) -> impl Future<Output = Result<Vec<Option<SeqV<K::ValueType>>>, Self::Error>> + Send
    where
        K: kvapi::Key + Send + 'static,
        K::ValueType: FromToProto + Send + 'static,
        I: IntoIterator<Item = K> + Send,
        I::IntoIter: Send,
        Self::Error: From<PbApiReadError<Self::Error>>,
    {
        async move {
            let pairs = self.get_pb_vec(keys).await?;
            Ok(pairs.into_iter().map(|(_k, v)| v).collect())
        }
    }

    /// Same as `get_pb_stream` but does not return keys, only values.
    ///
    /// It guaranteed to return the same number of results as the input keys.
    /// If the backend stream closed before all keys are processed, the following items is filled with `StreamReadEof` Error.
    fn get_pb_values<K, I>(
        &self,
        keys: I,
    ) -> impl Future<
        Output = Result<
            BoxStream<
                'static, //
                Result<Option<SeqV<K::ValueType>>, Self::Error>,
            >,
            Self::Error,
        >,
    > + Send
    where
        K: kvapi::Key + 'static,
        K::ValueType: FromToProto + Send + 'static,
        I: IntoIterator<Item = K>,
        Self::Error: From<PbApiReadError<Self::Error>>,
    {
        self.get_pb_stream(keys)
            .map_ok(|strm| strm.map_ok(|(_k, v)| v).boxed())
    }

    /// Same as [`get_pb_stream`](Self::get_pb_stream) but collect the result in a `Vec` instead of a stream.
    ///
    /// If the number of keys is larger than [`Self::CHUNK_SIZE`], it will be split into multiple requests.
    fn get_pb_vec<K, I>(
        &self,
        keys: I,
    ) -> impl Future<Output = Result<Vec<(K, Option<SeqV<K::ValueType>>)>, Self::Error>> + Send
    where
        K: kvapi::Key + Send + 'static,
        K::ValueType: FromToProto + Send + 'static,
        I: IntoIterator<Item = K> + Send,
        I::IntoIter: Send,
        Self::Error: From<PbApiReadError<Self::Error>>,
    {
        let it = keys.into_iter();
        let key_chunks = it
            .chunks(Self::CHUNK_SIZE)
            .into_iter()
            .map(|x| x.collect::<Vec<_>>())
            .collect::<Vec<_>>();

        async move {
            let mut res = vec![];
            for chunk in key_chunks {
                let strm = self.get_pb_stream(chunk).await?;

                let vec = strm.try_collect::<Vec<_>>().await?;
                res.extend(vec);
            }
            Ok(res)
        }
    }

    /// Get protobuf encoded values by a series of kvapi::Key.
    ///
    /// The key will be converted to string and the returned value is decoded by `FromToProto`.
    /// It returns the same error as `KVApi::Error`,
    /// thus it requires KVApi::Error can describe a decoding error, i.e., `impl From<PbApiReadError>`.
    ///
    /// It guaranteed to return the same number of results as the input keys.
    /// If the backend stream closed before all keys are processed, the following items is filled with `StreamReadEof` Error.
    fn get_pb_stream<K, I>(
        &self,
        keys: I,
    ) -> impl Future<
        Output = Result<
            BoxStream<
                'static, //
                Result<(K, Option<SeqV<K::ValueType>>), Self::Error>,
            >,
            Self::Error,
        >,
    > + Send
    where
        K: kvapi::Key + 'static,
        K::ValueType: FromToProto + Send + 'static,
        I: IntoIterator<Item = K>,
        Self::Error: From<PbApiReadError<Self::Error>>,
    {
        self.get_pb_stream_low(keys)
            .map_ok(|strm| strm.map_err(Self::Error::from).boxed())
    }

    /// Same as `get_pb_stream` but returns [`PbApiReadError`]. No require of `From<PbApiReadError>` for `Self::Error`.
    fn get_pb_stream_low<K, I>(
        &self,
        keys: I,
    ) -> impl Future<
        Output = Result<
            BoxStream<
                'static, //
                Result<(K, Option<SeqV<K::ValueType>>), PbApiReadError<Self::Error>>,
            >,
            Self::Error,
        >,
    > + Send
    where
        K: kvapi::Key + 'static,
        K::ValueType: FromToProto + Send + 'static,
        I: IntoIterator<Item = K>,
    {
        let keys = keys
            .into_iter()
            .map(|k| kvapi::Key::to_string_key(&k))
            .collect::<Vec<_>>();

        async move {
            let sent = keys.len();

            let strm = self.get_kv_stream(&keys).await?;

            let strm = strm.map(|r: Result<StreamItem, Self::Error>| {
                let item = r.map_err(PbApiReadError::KvApiError)?;

                let k = K::from_str_key(&item.key).map_err(PbApiReadError::KeyError)?;

                let v = item
                    .value
                    .map(|pb_seqv| {
                        decode_seqv::<K::ValueType>(SeqV::from(pb_seqv), || {
                            format!("decode value of {}", k.to_string_key())
                        })
                    })
                    .transpose()?;

                Ok((k, v))
            });

            // If the backend stream is closed, fill it with `StreamReadEof` error.

            let strm = strm
                // chain with a stream of `StreamReadEof` error but without received count set.
                .chain(stream::once(async move {
                    Err(PbApiReadError::StreamReadEof(StreamReadEof::new(
                        sent as u64,
                        0,
                    )))
                }))
                .take(sent)
                // set received count for `StreamReadEof` error after `sent`
                .enumerate()
                .map(move |(i, mut r)| {
                    if let Err(PbApiReadError::StreamReadEof(e)) = &mut r {
                        e.set_received(i as u64)
                    }
                    r
                });

            Ok(strm.boxed())
        }
    }

    /// Same as [`list_pb`](Self::list_pb)` but collect the result in a `Vec` instead of a stream.
    fn list_pb_vec<K>(
        &self,
        opts: ListOptions<'_, DirName<K>>,
    ) -> impl Future<Output = Result<Vec<(K, SeqV<K::ValueType>)>, Self::Error>> + Send
    where
        K: kvapi::Key + Send + Sync + 'static,
        K::ValueType: FromToProto + Send,
        Self::Error: From<PbApiReadError<Self::Error>>,
    {
        async move {
            let strm = self.list_pb(opts).await?;
            let kvs = strm
                .map_ok(|itm| (itm.key, itm.seqv))
                .try_collect::<Vec<_>>()
                .await?;
            Ok(kvs)
        }
    }

    /// Same as `list_pb` but does not return values, only keys.
    fn list_pb_keys<K>(
        &self,
        opts: ListOptions<'_, DirName<K>>,
    ) -> impl Future<Output = Result<BoxStream<'static, Result<K, Self::Error>>, Self::Error>> + Send
    where
        K: kvapi::Key + 'static,
        Self::Error: From<PbApiReadError<Self::Error>>,
    {
        let prefix = opts.prefix.dir_name_with_slash();
        let limit = opts.limit;
        async move {
            let strm = self.list_kv(ListOptions::new(&prefix, limit)).await?;

            let strm = strm.map(|r: Result<StreamItem, Self::Error>| {
                //
                let item = r?;
                let k = K::from_str_key(&item.key).map_err(PbApiReadError::KeyError)?;
                Ok(k)
            });

            Ok(strm.boxed())
        }
    }

    /// Same as `list_pb` but does not return key, only values.
    fn list_pb_values<K>(
        &self,
        opts: ListOptions<'_, DirName<K>>,
    ) -> impl Future<
        Output = Result<BoxStream<'static, Result<K::ValueType, Self::Error>>, Self::Error>,
    > + Send
    where
        K: kvapi::Key + 'static,
        K::ValueType: FromToProto,
        Self::Error: From<PbApiReadError<Self::Error>>,
    {
        self.list_pb(opts)
            .map_ok(|strm| strm.map_ok(|x| x.seqv.data).boxed())
    }

    /// List protobuf encoded values by prefix and returns a stream.
    ///
    /// The returned value is decoded by `FromToProto`.
    /// It returns the same error as `KVApi::Error`,
    /// thus it requires KVApi::Error can describe a decoding error, i.e., `impl From<PbApiReadError>`.
    fn list_pb<K>(
        &self,
        opts: ListOptions<'_, DirName<K>>,
    ) -> impl Future<
        Output = Result<BoxStream<'static, Result<NonEmptyItem<K>, Self::Error>>, Self::Error>,
    > + Send
    where
        K: kvapi::Key + 'static,
        K::ValueType: FromToProto,
        Self::Error: From<PbApiReadError<Self::Error>>,
    {
        self.list_pb_low(opts).map(|r| match r {
            Ok(strm) => Ok(strm.map_err(Self::Error::from).boxed()),
            Err(e) => Err(Self::Error::from(e)),
        })
    }

    /// Same as `list_pb` but returns [`PbApiReadError`]. No require of `From<PbApiReadError>` for `Self::Error`.
    fn list_pb_low<K>(
        &self,
        opts: ListOptions<'_, DirName<K>>,
    ) -> impl Future<
        Output = Result<
            BoxStream<'static, Result<NonEmptyItem<K>, PbApiReadError<Self::Error>>>,
            PbApiReadError<Self::Error>,
        >,
    > + Send
    where
        K: kvapi::Key + 'static,
        K::ValueType: FromToProto,
    {
        let prefix = opts.prefix.dir_name_with_slash();
        let limit = opts.limit;
        async move {
            let strm = self
                .list_kv(ListOptions::new(&prefix, limit))
                .await
                .map_err(PbApiReadError::KvApiError)?;
            let strm = strm.map(decode_non_empty_item::<K, Self::Error>);
            Ok(strm.boxed())
        }
    }
}

impl<T> KVPbApi for T where T: KVApi + ?Sized {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use async_trait::async_trait;
    use chrono::DateTime;
    use chrono::Utc;
    use databend_common_meta_app::schema::CatalogIdIdent;
    use databend_common_meta_app::schema::CatalogMeta;
    use databend_common_meta_app::schema::CatalogOption;
    use databend_common_meta_app::schema::HiveCatalogOption;
    use databend_common_meta_app::storage::StorageS3Config;
    use databend_common_meta_app::tenant::Tenant;
    use databend_common_proto_conv::FromToProto;
    use databend_meta_kvapi::kvapi::DirName;
    use databend_meta_kvapi::kvapi::KVApi;
    use databend_meta_kvapi::kvapi::KVStream;
    use databend_meta_kvapi::kvapi::ListOptions;
    use databend_meta_kvapi::kvapi::UpsertKVReply;
    use databend_meta_kvapi::kvapi::limit_stream;
    use databend_meta_types::MetaError;
    use databend_meta_types::SeqV;
    use databend_meta_types::TxnReply;
    use databend_meta_types::TxnRequest;
    use databend_meta_types::UpsertKV;
    use databend_meta_types::protobuf::StreamItem;
    use futures::StreamExt;
    use futures::TryStreamExt;
    use futures::stream::BoxStream;
    use prost::Message;
    use seq_marked::SeqValue;

    use crate::kv_pb_api::KVPbApi;

    //
    struct FooKV {
        /// Whether to return without exhausting the input for `get_many_kv`.
        early_return: Option<usize>,
        kvs: BTreeMap<String, SeqV>,
    }

    #[async_trait]
    impl KVApi for FooKV {
        type Error = MetaError;

        async fn upsert_kv(&self, _req: UpsertKV) -> Result<UpsertKVReply, Self::Error> {
            unimplemented!()
        }

        async fn get_many_kv(
            &self,
            keys: BoxStream<'static, Result<String, Self::Error>>,
        ) -> Result<KVStream<Self::Error>, Self::Error> {
            use databend_meta_kvapi::kvapi::fail_fast;
            use futures::TryStreamExt;

            let kvs = self.kvs.clone();
            let early_return = self.early_return;

            // early_return limits total items for testing
            let strm = fail_fast(keys)
                .take(early_return.unwrap_or(usize::MAX))
                .map_ok(move |key| {
                    let v = kvs.get(&key).cloned();
                    StreamItem::new(key, v.map(|v| v.into()))
                });
            Ok(strm.boxed())
        }

        async fn list_kv(
            &self,
            opts: ListOptions<'_, str>,
        ) -> Result<KVStream<Self::Error>, Self::Error> {
            let items = self
                .kvs
                .iter()
                .filter(|(k, _)| k.starts_with(opts.prefix))
                .map(|(k, v)| Ok(StreamItem::new(k.clone(), Some(v.clone().into()))))
                .collect::<Vec<_>>();

            let strm = futures::stream::iter(items);
            Ok(limit_stream(strm, opts.limit))
        }

        async fn transaction(&self, _txn: TxnRequest) -> Result<TxnReply, Self::Error> {
            unimplemented!()
        }
    }

    // TODO: test upsert_kv
    // TODO: test list_kv

    /// If the backend stream returns early, the returned stream should be filled with error item at the end.
    #[tokio::test]
    async fn test_mget_early_return() -> anyhow::Result<()> {
        let catalog_meta = CatalogMeta {
            catalog_option: CatalogOption::Hive(HiveCatalogOption {
                address: "127.0.0.1:10000".to_string(),
                storage_params: None,
            }),
            created_on: DateTime::<Utc>::MIN_UTC,
        };
        let v = catalog_meta.to_pb()?.encode_to_vec();

        let foo = FooKV {
            early_return: Some(2),
            kvs: vec![
                (s("__fd_catalog_by_id/1"), SeqV::new(1, v.clone())),
                (s("__fd_catalog_by_id/2"), SeqV::new(2, v.clone())),
                (s("__fd_catalog_by_id/3"), SeqV::new(3, v.clone())),
            ]
            .into_iter()
            .collect(),
        };

        let tenant = Tenant::new_literal("dummy");

        // Get key value pairs
        {
            let strm = foo
                .get_pb_stream([
                    CatalogIdIdent::new(&tenant, 1),
                    CatalogIdIdent::new(&tenant, 2),
                    CatalogIdIdent::new(&tenant, 4),
                ])
                .await?;

            let got = strm.try_collect::<Vec<_>>().await;
            assert_eq!(
                got.unwrap_err().to_string(),
                r#"InvalidReply: StreamReadEOF: expected 3 items but only received 2 items; source:()"#
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_mget() -> anyhow::Result<()> {
        let catalog_meta = CatalogMeta {
            catalog_option: CatalogOption::Hive(HiveCatalogOption {
                address: "127.0.0.1:10000".to_string(),
                storage_params: Some(Box::new(
                    databend_common_meta_app::storage::StorageParams::S3(StorageS3Config {
                        endpoint_url: "http://127.0.0.1:9900".to_string(),
                        region: "hello".to_string(),
                        bucket: "world".to_string(),
                        access_key_id: "databend_has_super_power".to_string(),
                        secret_access_key: "databend_has_super_power".to_string(),
                        ..Default::default()
                    }),
                )),
            }),
            created_on: DateTime::<Utc>::MIN_UTC,
        };
        let v = catalog_meta.to_pb()?.encode_to_vec();

        let foo = FooKV {
            early_return: None,
            kvs: vec![
                (s("__fd_catalog_by_id/1"), SeqV::new(1, v.clone())),
                (s("__fd_catalog_by_id/2"), SeqV::new(2, v.clone())),
                (s("__fd_catalog_by_id/3"), SeqV::new(3, v.clone())),
            ]
            .into_iter()
            .collect(),
        };

        let tenant = Tenant::new_literal("dummy");

        // Get key value pairs
        {
            let strm = foo
                .get_pb_stream([
                    CatalogIdIdent::new(&tenant, 1),
                    CatalogIdIdent::new(&tenant, 2),
                    CatalogIdIdent::new(&tenant, 4),
                ])
                .await?;

            let got = strm.try_collect::<Vec<_>>().await?;

            assert_eq!(CatalogIdIdent::new(&tenant, 1), got[0].0);
            assert_eq!(CatalogIdIdent::new(&tenant, 2), got[1].0);
            assert_eq!(CatalogIdIdent::new(&tenant, 4), got[2].0);

            assert_eq!(1, got[0].1.seq());
            assert_eq!(2, got[1].1.seq());
            assert_eq!(0, got[2].1.seq());

            assert_eq!(Some(&catalog_meta), got[0].1.value());
            assert_eq!(Some(&catalog_meta), got[1].1.value());
            assert_eq!(None, got[2].1.value());
        }

        // Get values
        {
            let strm = foo
                .get_pb_values([
                    CatalogIdIdent::new(&tenant, 1),
                    CatalogIdIdent::new(&tenant, 2),
                    CatalogIdIdent::new(&tenant, 4),
                ])
                .await?;

            let got = strm.try_collect::<Vec<_>>().await?;

            assert_eq!(1, got[0].seq());
            assert_eq!(2, got[1].seq());
            assert_eq!(0, got[2].seq());

            assert_eq!(Some(&catalog_meta), got[0].value());
            assert_eq!(Some(&catalog_meta), got[1].value());
            assert_eq!(None, got[2].value());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_get_pb_vec_span_chunk() -> anyhow::Result<()> {
        let catalog_meta = CatalogMeta {
            catalog_option: CatalogOption::Hive(HiveCatalogOption {
                address: "127.0.0.1:10000".to_string(),
                storage_params: None,
            }),
            created_on: DateTime::<Utc>::MIN_UTC,
        };
        let catalog_bytes = catalog_meta.to_pb()?.encode_to_vec();

        let n = 1024;
        let mut kvs = vec![];
        for i in 1..=n {
            let key = s(format!("__fd_catalog_by_id/{}", i));
            let value = SeqV::new(i, catalog_bytes.clone());
            kvs.push((key, value));
        }

        let foo = FooKV {
            early_return: None,
            kvs: kvs.into_iter().collect(),
        };

        assert!(FooKV::CHUNK_SIZE < n as usize);

        let tenant = Tenant::new_literal("dummy");

        {
            let got = foo
                .get_pb_vec((1..=n).map(|i| CatalogIdIdent::new(&tenant, i)))
                .await?;

            for i in 1..=n {
                let key = CatalogIdIdent::new(&tenant, i);
                assert_eq!(key, got[i as usize - 1].0.clone());
                let value = got[i as usize - 1].1.clone().unwrap();
                assert_eq!(i, value.seq());
            }
        }

        {
            let got = foo
                .get_pb_values_vec((1..=n).map(|i| CatalogIdIdent::new(&tenant, i)))
                .await?;

            for i in 1..=n {
                let value = got[i as usize - 1].clone().unwrap();
                assert_eq!(i, value.seq());
            }
        }

        Ok(())
    }

    /// A decoding error should include the key.
    #[tokio::test]
    async fn test_list_pb_values_report_error_with_key() -> anyhow::Result<()> {
        let n = 3;
        let mut kvs = vec![];
        for i in 1..=n {
            let key = s(format!("__fd_catalog_by_id/{}", i));
            let value = SeqV::new(i, b"b".to_vec());
            kvs.push((key, value));
        }

        let foo = FooKV {
            early_return: None,
            kvs: kvs.into_iter().collect(),
        };

        let tenant = Tenant::new_literal("dummy");

        let dir = DirName::new(CatalogIdIdent::new(&tenant, 0));
        let mut strm = foo.list_pb_values(ListOptions::unlimited(&dir)).await?;
        let mut errors = vec![];
        while let Some(r) = strm.next().await {
            if let Err(e) = r {
                errors.push(e.to_string());
            }
        }

        let want = vec![
            "InvalidReply: source:(PbDecodeError: failed to decode Protobuf message: invalid varint; when:(decode value of __fd_catalog_by_id/1))",
            "InvalidReply: source:(PbDecodeError: failed to decode Protobuf message: invalid varint; when:(decode value of __fd_catalog_by_id/2))",
            "InvalidReply: source:(PbDecodeError: failed to decode Protobuf message: invalid varint; when:(decode value of __fd_catalog_by_id/3))",
        ];

        assert_eq!(errors, want);

        Ok(())
    }

    /// Create a FooKV with 5 catalog entries for testing limit functionality.
    fn new_foo_kv_for_limit_test() -> anyhow::Result<(FooKV, CatalogMeta)> {
        let catalog_meta = CatalogMeta {
            catalog_option: CatalogOption::Hive(HiveCatalogOption {
                address: "127.0.0.1:10000".to_string(),
                storage_params: None,
            }),
            created_on: DateTime::<Utc>::MIN_UTC,
        };
        let v = catalog_meta.to_pb()?.encode_to_vec();

        let kvs: BTreeMap<_, _> = (1..=5)
            .map(|i| {
                (
                    s(format!("__fd_catalog_by_id/{}", i)),
                    SeqV::new(i, v.clone()),
                )
            })
            .collect();

        Ok((
            FooKV {
                early_return: None,
                kvs,
            },
            catalog_meta,
        ))
    }

    /// Test `list_pb_values` with limit parameter
    #[tokio::test]
    async fn test_list_pb_values_with_limit() -> anyhow::Result<()> {
        let (foo, catalog_meta) = new_foo_kv_for_limit_test()?;
        let tenant = Tenant::new_literal("dummy");
        let dir = DirName::new(CatalogIdIdent::new(&tenant, 0));

        // List all with no limit
        let strm = foo.list_pb_values(ListOptions::unlimited(&dir)).await?;
        let res: Vec<_> = strm.try_collect().await?;
        assert_eq!(res.len(), 5);
        assert_eq!(res[0].catalog_option, catalog_meta.catalog_option);

        // List with limit 3
        let strm = foo.list_pb_values(ListOptions::limited(&dir, 3)).await?;
        let res: Vec<_> = strm.try_collect().await?;
        assert_eq!(res.len(), 3);

        // List with limit 0
        let strm = foo.list_pb_values(ListOptions::limited(&dir, 0)).await?;
        let res: Vec<_> = strm.try_collect().await?;
        assert!(res.is_empty());

        Ok(())
    }

    /// Test `list_pb_vec` with limit parameter
    #[tokio::test]
    async fn test_list_pb_vec_with_limit() -> anyhow::Result<()> {
        let (foo, _) = new_foo_kv_for_limit_test()?;
        let tenant = Tenant::new_literal("dummy");
        let dir = DirName::new(CatalogIdIdent::new(&tenant, 0));

        // List all with no limit
        let res = foo.list_pb_vec(ListOptions::unlimited(&dir)).await?;
        let ids: Vec<_> = res.iter().map(|(k, _)| *k.catalog_id()).collect();
        assert_eq!(ids, vec![1u64, 2, 3, 4, 5]);

        // List with limit 2
        let res = foo.list_pb_vec(ListOptions::limited(&dir, 2)).await?;
        let ids: Vec<_> = res.iter().map(|(k, _)| *k.catalog_id()).collect();
        assert_eq!(ids, vec![1u64, 2]);

        // List with limit 0
        let res = foo.list_pb_vec(ListOptions::limited(&dir, 0)).await?;
        assert!(res.is_empty());

        Ok(())
    }

    fn s(x: impl ToString) -> String {
        x.to_string()
    }
}
