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

pub mod errors;

mod codec;
mod upsert_pb;

use std::future::Future;

use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_kvapi::kvapi::NonEmptyItem;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::seq_value::SeqV;
use databend_common_meta_types::Change;
use databend_common_meta_types::UpsertKV;
use databend_common_proto_conv::FromToProto;
use futures::future::FutureExt;
use futures::future::TryFutureExt;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use futures::TryStreamExt;

pub(crate) use self::codec::decode_non_empty_item;
pub(crate) use self::codec::decode_seqv;
pub(crate) use self::codec::decode_transition;
pub(crate) use self::codec::encode_operation;
pub use self::upsert_pb::UpsertPB;
use crate::kv_pb_api::errors::PbApiReadError;
use crate::kv_pb_api::errors::PbApiWriteError;

/// This trait provides a way to access a kv store with `kvapi::Key` type key and protobuf encoded value.
pub trait KVPbApi: KVApi {
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
            let transition = decode_transition(reply)?;
            Ok(transition)
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
            let v = raw_seqv.map(decode_seqv::<K::ValueType>).transpose()?;
            Ok(v)
        }
    }

    /// Same as `get_pb_stream` but does not return keys, only values.
    #[deprecated(note = "stream may be closed. The caller must check it")]
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
        self.get_pb_stream_low(keys)
            // This `map()` handles Future result
            .map(|r| match r {
                Ok(strm) => {
                    // These two `map_xx()` handles Stream result
                    Ok(strm.map_ok(|(_k, v)| v).map_err(Self::Error::from).boxed())
                }
                Err(e) => Err(e),
            })
    }

    /// Get protobuf encoded values by a series of kvapi::Key.
    ///
    /// The key will be converted to string and the returned value is decoded by `FromToProto`.
    /// It returns the same error as `KVApi::Error`,
    /// thus it requires KVApi::Error can describe a decoding error, i.e., `impl From<PbApiReadError>`.
    #[deprecated(note = "stream may be closed. The caller must check it")]
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
        self.get_pb_stream_low(keys).map(|r| match r {
            Ok(strm) => Ok(strm.map_err(Self::Error::from).boxed()),
            Err(e) => Err(e),
        })
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
            let strm = self.get_kv_stream(&keys).await?;

            let strm = strm.map(|r: Result<StreamItem, Self::Error>| {
                let item = r.map_err(PbApiReadError::KvApiError)?;

                let k = K::from_str_key(&item.key).map_err(PbApiReadError::KeyError)?;

                let v = if let Some(pb_seqv) = item.value {
                    let seqv = decode_seqv::<K::ValueType>(SeqV::from(pb_seqv))?;
                    Some(seqv)
                } else {
                    None
                };

                Ok((k, v))
            });

            Ok(strm.boxed())
        }
    }

    /// Same as `list_pb` but does not return values, only keys.
    fn list_pb_keys<K>(
        &self,
        prefix: &DirName<K>,
    ) -> impl Future<Output = Result<BoxStream<'static, Result<K, Self::Error>>, Self::Error>> + Send
    where
        K: kvapi::Key + 'static,
        Self::Error: From<PbApiReadError<Self::Error>>,
    {
        let prefix = prefix.dir_name_with_slash();
        async move {
            let strm = self.list_kv(&prefix).await?;

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
        prefix: &DirName<K>,
    ) -> impl Future<
        Output = Result<BoxStream<'static, Result<K::ValueType, Self::Error>>, Self::Error>,
    > + Send
    where
        K: kvapi::Key + 'static,
        K::ValueType: FromToProto,
        Self::Error: From<PbApiReadError<Self::Error>>,
    {
        self.list_pb(prefix)
            .map_ok(|strm| strm.map_ok(|x| x.seqv.data).boxed())
    }

    /// List protobuf encoded values by prefix and returns a stream.
    ///
    /// The returned value is decoded by `FromToProto`.
    /// It returns the same error as `KVApi::Error`,
    /// thus it requires KVApi::Error can describe a decoding error, i.e., `impl From<PbApiReadError>`.
    fn list_pb<K>(
        &self,
        prefix: &DirName<K>,
    ) -> impl Future<
        Output = Result<BoxStream<'static, Result<NonEmptyItem<K>, Self::Error>>, Self::Error>,
    > + Send
    where
        K: kvapi::Key + 'static,
        K::ValueType: FromToProto,
        Self::Error: From<PbApiReadError<Self::Error>>,
    {
        self.list_pb_low(prefix).map(|r| match r {
            Ok(strm) => Ok(strm.map_err(Self::Error::from).boxed()),
            Err(e) => Err(Self::Error::from(e)),
        })
    }

    /// Same as `list_pb` but returns [`PbApiReadError`]. No require of `From<PbApiReadError>` for `Self::Error`.
    fn list_pb_low<K>(
        &self,
        prefix: &DirName<K>,
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
        let prefix = prefix.dir_name_with_slash();
        async move {
            let strm = self
                .list_kv(&prefix)
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
    use databend_common_meta_kvapi::kvapi::KVApi;
    use databend_common_meta_kvapi::kvapi::KVStream;
    use databend_common_meta_kvapi::kvapi::UpsertKVReply;
    use databend_common_meta_kvapi::kvapi::UpsertKVReq;
    use databend_common_meta_types::protobuf::StreamItem;
    use databend_common_meta_types::seq_value::SeqV;
    use databend_common_meta_types::seq_value::SeqValue;
    use databend_common_meta_types::MetaError;
    use databend_common_meta_types::TxnReply;
    use databend_common_meta_types::TxnRequest;
    use databend_common_proto_conv::FromToProto;
    use futures::StreamExt;
    use futures::TryStreamExt;
    use prost::Message;

    use crate::kv_pb_api::KVPbApi;

    //
    struct Foo {
        kvs: BTreeMap<String, SeqV>,
    }

    #[async_trait]
    impl KVApi for Foo {
        type Error = MetaError;

        async fn upsert_kv(&self, _req: UpsertKVReq) -> Result<UpsertKVReply, Self::Error> {
            unimplemented!()
        }

        async fn get_kv_stream(
            &self,
            keys: &[String],
        ) -> Result<KVStream<Self::Error>, Self::Error> {
            let mut res = Vec::with_capacity(keys.len());
            for key in keys {
                let k = key.clone();
                let v = self.kvs.get(key).cloned();

                let item = StreamItem::new(k, v.map(|v| v.into()));
                res.push(Ok(item));
            }

            let strm = futures::stream::iter(res);
            Ok(strm.boxed())
        }

        async fn list_kv(&self, _prefix: &str) -> Result<KVStream<Self::Error>, Self::Error> {
            unimplemented!()
        }

        async fn transaction(&self, _txn: TxnRequest) -> Result<TxnReply, Self::Error> {
            unimplemented!()
        }
    }

    // TODO: test upsert_kv
    // TODO: test upsert_kv
    // TODO: test list_kv

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

        let foo = Foo {
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
            #[allow(deprecated)]
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
            #[allow(deprecated)]
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

    fn s(x: impl ToString) -> String {
        x.to_string()
    }
}
