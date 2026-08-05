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

use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::PayloadFlushState;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_storages_parquet::deserialize_row_group_meta_from_bytes;
use databend_common_storages_parquet::serialize_row_group_meta_to_bytes;

use super::AggregateMeta;
use super::AggregateSerdeMeta;
use super::AggregatorParams;
use super::BUCKET_TYPE;
use super::PARTITIONED_AGGREGATE_TYPE;
use super::PartitionedData;
use super::SPILLED_TYPE;
use super::SerializedPayload;
use super::SpilledPayload;
use crate::servers::flight::v1::network::ExchangeDataCodec;

/// The aggregate adapter at the unified Exchange channel seam.
///
/// Aggregate processors use process-local `AggregateMeta` payloads. Remote
/// channels call this adapter to materialize those payloads into regular
/// columns plus `AggregateSerdeMeta`; inbound remote channels call it again to
/// restore the process-local representation. Local channels bypass the adapter.
pub struct AggregateExchangeDataCodec {
    params: Arc<AggregatorParams>,
}

impl AggregateExchangeDataCodec {
    pub fn create(params: Arc<AggregatorParams>) -> Arc<dyn ExchangeDataCodec> {
        Arc::new(Self { params })
    }

    fn validate_state_schema(&self, block: &DataBlock) -> Result<()> {
        let expected = self.params.spill_schema();
        if block.num_columns() != expected.num_fields() {
            return Err(ErrorCode::BadBytes(format!(
                "Aggregate transport schema mismatch: expected {} columns, got {}",
                expected.num_fields(),
                block.num_columns()
            )));
        }

        for (index, (entry, field)) in block
            .columns()
            .iter()
            .zip(expected.fields().iter())
            .enumerate()
        {
            let actual = entry.data_type();
            if &actual != field.data_type() {
                return Err(ErrorCode::BadBytes(format!(
                    "Aggregate transport schema mismatch at column {index}: expected {:?}, got {actual:?}",
                    field.data_type()
                )));
            }
        }
        Ok(())
    }

    fn encode_payload(
        &self,
        payload: databend_common_expression::AggregatePayload,
    ) -> Result<DataBlock> {
        let databend_common_expression::AggregatePayload { bucket, payload } = payload;
        let mut flush_state = PayloadFlushState::default();
        let mut blocks = Vec::new();
        while let Some(block) = payload.aggregate_flush(&mut flush_state)? {
            blocks
                .push(block.add_meta(Some(AggregateSerdeMeta::create_agg_payload(bucket, false)))?);
        }

        // Preserve the bucket even when the payload has no rows. Arrow Flight
        // carries the explicit zero row count and the serde metadata.
        if blocks.is_empty() {
            blocks.push(
                payload
                    .empty_block(1)
                    .add_meta(Some(AggregateSerdeMeta::create_agg_payload(bucket, true)))?,
            );
        }

        let meta = blocks[0].take_meta().ok_or_else(|| {
            ErrorCode::Internal("Aggregate payload transport block has no metadata")
        })?;
        let mut block = DataBlock::concat(&blocks)?;
        block.replace_meta(meta);
        Ok(block)
    }

    fn encode_partitioned(&self, data: PartitionedData) -> Result<Option<DataBlock>> {
        match data {
            PartitionedData::Serialized(data) if data.is_empty() => Ok(None),
            PartitionedData::AggregatePayload(data) if data.is_empty() => Ok(None),
            PartitionedData::BucketSpilled(data) if data.is_empty() => Ok(None),
            PartitionedData::AggregatePayload(data) => {
                let mut buckets = Vec::with_capacity(data.len());
                let mut payload_row_counts = Vec::with_capacity(data.len());
                let mut payload_blocks = Vec::with_capacity(data.len());

                for payload in data {
                    let block = payload.payload.aggregate_flush_all()?;
                    if block.num_rows() == 0 {
                        continue;
                    }
                    buckets.push(payload.bucket);
                    payload_row_counts.push(block.num_rows());
                    payload_blocks.push(block);
                }

                if payload_blocks.is_empty() {
                    return Ok(None);
                }

                let block = DataBlock::concat(&payload_blocks)?.add_meta(Some(
                    AggregateSerdeMeta::create_partitioned_payload(buckets, payload_row_counts),
                ))?;
                Ok(Some(block))
            }
            PartitionedData::BucketSpilled(data) => {
                let rows = data.len();
                let mut buckets = Vec::with_capacity(rows);
                let mut locations = Vec::with_capacity(rows);
                let mut row_groups = Vec::with_capacity(rows);

                for payload in data {
                    buckets.push(payload.bucket as i64);
                    locations.push(payload.location);
                    row_groups.push(serialize_row_group_meta_to_bytes(&payload.row_group)?);
                }

                let block = DataBlock::new_from_columns(vec![
                    Int64Type::from_data(buckets),
                    StringType::from_data(locations),
                    BinaryType::from_data(row_groups),
                ])
                .add_meta(Some(AggregateSerdeMeta::create_spilled()))?;
                Ok(Some(block))
            }
            PartitionedData::Serialized(_) => Err(ErrorCode::Internal(
                "Serialized partitioned aggregate data is not transportable",
            )),
        }
    }

    fn decode_partitioned_payload(
        &self,
        meta: &AggregateSerdeMeta,
        block: DataBlock,
    ) -> Result<Option<DataBlock>> {
        if meta.is_empty {
            return Ok(None);
        }
        self.validate_state_schema(&block)?;
        if meta.buckets.len() != meta.payload_row_counts.len() {
            return Err(ErrorCode::BadBytes(
                "Invalid partitioned aggregate transport metadata",
            ));
        }

        let mut offset = 0;
        let mut payloads = Vec::with_capacity(meta.buckets.len());
        for (bucket, rows) in meta.buckets.iter().zip(meta.payload_row_counts.iter()) {
            let start = offset;
            offset += *rows;
            if offset > block.num_rows() {
                return Err(ErrorCode::BadBytes(
                    "Partitioned aggregate payload rows exceed block rows",
                ));
            }

            payloads.push(SerializedPayload {
                bucket: *bucket,
                data_block: block.slice(start..offset),
            });
        }

        if offset != block.num_rows() {
            return Err(ErrorCode::BadBytes(
                "Partitioned aggregate payload rows do not match block rows",
            ));
        }

        Ok(Some(DataBlock::empty_with_meta(
            AggregateMeta::create_partitioned(None, PartitionedData::Serialized(payloads)),
        )))
    }

    fn decode_spilled(&self, block: DataBlock) -> Result<Option<DataBlock>> {
        if block.num_columns() != 3 {
            return Err(ErrorCode::BadBytes(
                "Spilled aggregate transport block must contain three columns",
            ));
        }

        let columns = block
            .columns()
            .iter()
            .map(|entry| {
                entry
                    .as_column()
                    .cloned()
                    .ok_or_else(|| ErrorCode::BadBytes("Spilled aggregate column is scalar"))
            })
            .collect::<Result<Vec<_>>>()?;
        let buckets = NumberType::<i64>::try_downcast_column(&columns[0])
            .map_err(|_| ErrorCode::BadBytes("Invalid spilled aggregate bucket column"))?;
        let locations = StringType::try_downcast_column(&columns[1])
            .map_err(|_| ErrorCode::BadBytes("Invalid spilled aggregate location column"))?;
        let row_groups = BinaryType::try_downcast_column(&columns[2])
            .map_err(|_| ErrorCode::BadBytes("Invalid spilled aggregate row-group column"))?;

        let mut payloads = Vec::with_capacity(block.num_rows());
        for index in 0..block.num_rows() {
            let bucket = *buckets
                .get(index)
                .ok_or_else(|| ErrorCode::BadBytes("Missing spilled aggregate bucket value"))?
                as isize;
            let location = locations
                .index(index)
                .ok_or_else(|| ErrorCode::BadBytes("Missing spilled aggregate location"))?
                .to_string();
            let row_group_bytes = row_groups
                .index(index)
                .ok_or_else(|| ErrorCode::BadBytes("Missing spilled aggregate row group"))?;
            let row_group = deserialize_row_group_meta_from_bytes(row_group_bytes)?;
            payloads.push(SpilledPayload {
                bucket,
                location,
                row_group,
            });
        }

        Ok(Some(DataBlock::empty_with_meta(
            AggregateMeta::create_partitioned(None, PartitionedData::BucketSpilled(payloads)),
        )))
    }
}

impl ExchangeDataCodec for AggregateExchangeDataCodec {
    fn encode(&self, mut block: DataBlock) -> Result<Option<DataBlock>> {
        if block.is_empty() && block.get_meta().is_none() {
            return Ok(None);
        }

        let meta = block
            .take_meta()
            .and_then(AggregateMeta::downcast_from)
            .ok_or_else(|| {
                ErrorCode::Internal("Aggregate exchange expected AggregateMeta input")
            })?;

        match meta {
            AggregateMeta::AggregatePayload(payload) => self.encode_payload(payload).map(Some),
            AggregateMeta::Partitioned { data, .. } => self.encode_partitioned(data),
            other => Err(ErrorCode::Internal(format!(
                "Aggregate exchange cannot encode metadata: {other:?}"
            ))),
        }
    }

    fn decode(&self, mut block: DataBlock) -> Result<Option<DataBlock>> {
        let meta = block
            .take_meta()
            .and_then(AggregateSerdeMeta::downcast_from)
            .ok_or_else(|| ErrorCode::BadBytes("Aggregate exchange expected AggregateSerdeMeta"))?;

        match meta.typ {
            BUCKET_TYPE => {
                if meta.is_empty {
                    block = block.slice(0..0);
                } else {
                    self.validate_state_schema(&block)?;
                }
                Ok(Some(DataBlock::empty_with_meta(
                    AggregateMeta::create_serialized(meta.bucket, block),
                )))
            }
            PARTITIONED_AGGREGATE_TYPE => self.decode_partitioned_payload(&meta, block),
            SPILLED_TYPE => self.decode_spilled(block),
            other => Err(ErrorCode::BadBytes(format!(
                "Unknown aggregate transport metadata type {other}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_ipc::writer::IpcWriteOptions;
    use arrow_schema::Schema as ArrowSchema;
    use bumpalo::Bump;
    use databend_common_expression::BlockMetaInfoDowncast;
    use databend_common_expression::DataField;
    use databend_common_expression::DataSchemaRefExt;
    use databend_common_expression::FromData;
    use databend_common_expression::aggregate::AggregatePayload;
    use databend_common_expression::aggregate::SerializedPayload as ExpressionSerializedPayload;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::NumberDataType;
    use parquet::basic::Repetition;
    use parquet::file::metadata::RowGroupMetaData;
    use parquet::schema::types::SchemaDescriptor;
    use parquet::schema::types::Type;

    use super::*;
    use crate::servers::flight::v1::network::inbound_channel::deserialize_flight_data;
    use crate::servers::flight::v1::network::outbound_channel::serialize_block;

    fn params() -> Arc<AggregatorParams> {
        let data_type = DataType::Number(NumberDataType::Int64);
        AggregatorParams::try_create(
            DataSchemaRefExt::create(vec![DataField::new("group", data_type.clone())]),
            vec![data_type],
            &[0],
            &[],
            &[],
            true,
            1024,
            1024 * 1024,
        )
        .unwrap()
    }

    fn nullable_params() -> Arc<AggregatorParams> {
        let data_type = DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64)));
        AggregatorParams::try_create(
            DataSchemaRefExt::create(vec![DataField::new("group", data_type.clone())]),
            vec![data_type],
            &[0],
            &[],
            &[],
            true,
            1024,
            1024 * 1024,
        )
        .unwrap()
    }

    fn payload_block(values: Vec<i64>) -> DataBlock {
        let params = params();
        let serialized = ExpressionSerializedPayload {
            bucket: 7,
            data_block: DataBlock::new_from_columns(vec![Int64Type::from_data(values)]),
        };
        let payload = serialized
            .convert_to_single_payload(
                params.group_data_types.clone(),
                vec![],
                0,
                Arc::new(Bump::new()),
            )
            .unwrap();

        DataBlock::empty_with_meta(Box::new(AggregateMeta::AggregatePayload(
            AggregatePayload { bucket: 7, payload },
        )))
    }

    fn flight_round_trip(
        block: DataBlock,
        schema: &databend_common_expression::DataSchemaRef,
    ) -> DataBlock {
        let mut packets = serialize_block(block, &IpcWriteOptions::default(), None).unwrap();
        assert_eq!(packets.len(), 1);
        deserialize_flight_data(
            packets.pop().unwrap(),
            schema,
            &Arc::new(ArrowSchema::from(schema.as_ref())),
        )
        .unwrap()
    }

    #[test]
    fn test_payload_remote_round_trip() {
        let params = params();
        let codec = AggregateExchangeDataCodec::create(params.clone());

        let transport = codec
            .encode(payload_block(vec![11, 22, 33]))
            .unwrap()
            .unwrap();
        let serde_meta = transport
            .get_meta()
            .and_then(AggregateSerdeMeta::downcast_ref_from)
            .unwrap();
        assert_eq!(serde_meta.typ, BUCKET_TYPE);
        assert_eq!(serde_meta.bucket, 7);
        assert!(!serde_meta.is_empty);

        let transport = flight_round_trip(transport, &params.spill_schema());
        let mut restored = codec.decode(transport).unwrap().unwrap();
        let restored = restored
            .take_meta()
            .and_then(AggregateMeta::downcast_from)
            .unwrap();
        let AggregateMeta::Serialized(restored) = restored else {
            panic!("expected serialized aggregate payload")
        };
        assert_eq!(restored.bucket, 7);
        assert_eq!(restored.data_block.num_rows(), 3);
        let column = restored.data_block.get_by_offset(0).to_column();
        let values = Int64Type::try_downcast_column(&column).unwrap();
        assert_eq!(values.as_slice(), &[11, 22, 33]);
    }

    #[test]
    fn test_empty_payload_remote_round_trip() {
        let params = params();
        let codec = AggregateExchangeDataCodec::create(params.clone());
        let payload = databend_common_expression::Payload::new(
            Arc::new(Bump::new()),
            params.group_data_types.clone(),
            vec![],
            None,
        );
        let block = DataBlock::empty_with_meta(Box::new(AggregateMeta::AggregatePayload(
            AggregatePayload { bucket: 3, payload },
        )));

        let transport = codec.encode(block).unwrap().unwrap();
        let serde_meta = transport
            .get_meta()
            .and_then(AggregateSerdeMeta::downcast_ref_from)
            .unwrap();
        assert!(serde_meta.is_empty);

        let transport = flight_round_trip(transport, &params.spill_schema());
        let mut restored = codec.decode(transport).unwrap().unwrap();
        let restored = restored
            .take_meta()
            .and_then(AggregateMeta::downcast_from)
            .unwrap();
        let AggregateMeta::Serialized(restored) = restored else {
            panic!("expected serialized aggregate payload")
        };
        assert_eq!(restored.bucket, 3);
        assert_eq!(restored.data_block.num_rows(), 0);
    }

    #[test]
    fn test_nullable_group_key_remote_round_trip() {
        let params = nullable_params();
        let codec = AggregateExchangeDataCodec::create(params.clone());
        let group_column = Int64Type::from_opt_data(vec![Some(5), None, Some(-2)]);
        let serialized = ExpressionSerializedPayload {
            bucket: 2,
            data_block: DataBlock::new_from_columns(vec![group_column.clone()]),
        };
        let payload = serialized
            .convert_to_single_payload(
                params.group_data_types.clone(),
                vec![],
                0,
                Arc::new(Bump::new()),
            )
            .unwrap();
        let block = DataBlock::empty_with_meta(Box::new(AggregateMeta::AggregatePayload(
            AggregatePayload { bucket: 2, payload },
        )));

        let transport = codec.encode(block).unwrap().unwrap();
        let transport = flight_round_trip(transport, &params.spill_schema());
        let mut restored = codec.decode(transport).unwrap().unwrap();
        let restored = restored
            .take_meta()
            .and_then(AggregateMeta::downcast_from)
            .unwrap();
        let AggregateMeta::Serialized(restored) = restored else {
            panic!("expected serialized aggregate payload")
        };
        assert_eq!(
            restored.data_block.get_by_offset(0).to_column(),
            group_column
        );
    }

    #[tokio::test]
    async fn test_local_channel_preserves_aggregate_payload() {
        let params = params();
        let codec = AggregateExchangeDataCodec::create(params.clone());
        let channel_set = crate::servers::flight::v1::network::NetworkInboundChannelSet::new(1);
        let channel = crate::servers::flight::v1::network::create_local_channels(&channel_set)
            .pop()
            .unwrap();
        let receiver = channel_set.create_receiver_with_codec(0, &params.spill_schema(), codec);

        channel
            .add_block(payload_block(vec![11, 22, 33]))
            .await
            .unwrap();
        let mut received = receiver.recv().await.unwrap().unwrap();
        let received = received
            .take_meta()
            .and_then(AggregateMeta::downcast_from)
            .unwrap();
        let AggregateMeta::AggregatePayload(received) = received else {
            panic!("local channel materialized aggregate payload")
        };
        assert_eq!(received.bucket, 7);
        assert_eq!(received.payload.len(), 3);
    }

    #[test]
    fn test_partitioned_payload_remote_round_trip() {
        let params = params();
        let codec = AggregateExchangeDataCodec::create(params.clone());
        let transport = DataBlock::new_from_columns(vec![Int64Type::from_data(vec![10, 20, 30])])
            .add_meta(Some(AggregateSerdeMeta::create_partitioned_payload(
                vec![4, 9],
                vec![2, 1],
            )))
            .unwrap();

        let transport = flight_round_trip(transport, &params.spill_schema());
        let mut restored = codec.decode(transport).unwrap().unwrap();
        let restored = restored
            .take_meta()
            .and_then(AggregateMeta::downcast_from)
            .unwrap();
        let AggregateMeta::Partitioned {
            data: PartitionedData::Serialized(payloads),
            ..
        } = restored
        else {
            panic!("expected partitioned serialized payload")
        };
        assert_eq!(payloads.len(), 2);
        assert_eq!(payloads[0].bucket, 4);
        assert_eq!(payloads[0].data_block.num_rows(), 2);
        assert_eq!(payloads[1].bucket, 9);
        assert_eq!(payloads[1].data_block.num_rows(), 1);
    }

    #[test]
    fn test_spilled_payload_remote_round_trip() {
        let params = params();
        let codec = AggregateExchangeDataCodec::create(params.clone());
        let schema = Type::group_type_builder("schema")
            .with_repetition(Repetition::REPEATED)
            .build()
            .unwrap();
        let descriptor = SchemaDescriptor::new(Arc::new(schema));
        let row_group = RowGroupMetaData::builder(descriptor.into())
            .set_num_rows(17)
            .build()
            .unwrap();
        let block = DataBlock::empty_with_meta(AggregateMeta::create_partitioned(
            None,
            PartitionedData::BucketSpilled(vec![SpilledPayload {
                bucket: 6,
                location: "memory://aggregate-spill".to_string(),
                row_group,
            }]),
        ));

        let transport = codec.encode(block).unwrap().unwrap();
        let serde_meta = transport
            .get_meta()
            .and_then(AggregateSerdeMeta::downcast_ref_from)
            .unwrap();
        assert_eq!(serde_meta.typ, SPILLED_TYPE);

        // The spilled metadata overrides the aggregate state schema during
        // generic Arrow Flight decoding.
        let transport = flight_round_trip(transport, &params.spill_schema());
        let mut restored = codec.decode(transport).unwrap().unwrap();
        let restored = restored
            .take_meta()
            .and_then(AggregateMeta::downcast_from)
            .unwrap();
        let AggregateMeta::Partitioned {
            data: PartitionedData::BucketSpilled(payloads),
            ..
        } = restored
        else {
            panic!("expected spilled aggregate payload")
        };
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].bucket, 6);
        assert_eq!(payloads[0].location, "memory://aggregate-spill");
        assert_eq!(payloads[0].row_group.num_rows(), 17);
    }

    #[test]
    fn test_rejects_malformed_partitioned_payload() {
        let codec = AggregateExchangeDataCodec::create(params());
        let block = DataBlock::new_from_columns(vec![Int64Type::from_data(vec![10, 20])])
            .add_meta(Some(AggregateSerdeMeta::create_partitioned_payload(
                vec![1],
                vec![3],
            )))
            .unwrap();
        let error = codec.decode(block).unwrap_err();
        assert!(error.message().contains("exceed block rows"));
    }

    #[test]
    fn test_rejects_transport_schema_mismatch() {
        let codec = AggregateExchangeDataCodec::create(params());
        let block = DataBlock::new_from_columns(vec![StringType::from_data(vec!["wrong"])])
            .add_meta(Some(AggregateSerdeMeta::create_agg_payload(0, false)))
            .unwrap();
        let error = codec.decode(block).unwrap_err();
        assert!(error.message().contains("schema mismatch"));
    }

    #[test]
    fn test_rejects_non_aggregate_metadata() {
        let codec = AggregateExchangeDataCodec::create(params());
        let error = codec
            .encode(DataBlock::new_from_columns(vec![Int64Type::from_data(
                vec![1],
            )]))
            .unwrap_err();
        assert!(error.message().contains("expected AggregateMeta"));
    }
}
