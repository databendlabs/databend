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

use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;

use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Scalar;
use databend_common_expression::types::ArrayColumn;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::NumberColumnBuilder;
use databend_common_expression::types::array::ArrayColumnBuilder;

/// Represents a runtime filter that can be transmitted and merged.
///
/// # Fields
///
/// * `id` - Unique identifier for each runtime filter, corresponds one-to-one with `(build key, probe key)` pair
/// * `inlist` - Deduplicated list of build key column
/// * `min_max` - The min and max values of the build column
/// * `bloom` - The deduplicated hashes of the build column
#[derive(serde::Serialize, serde::Deserialize, Clone, Default, PartialEq)]
pub struct RuntimeFilterPacket {
    pub id: usize,
    pub inlist: Option<Column>,
    pub min_max: Option<SerializableDomain>,
    pub bloom: Option<Vec<u64>>,
}

impl Debug for RuntimeFilterPacket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RuntimeFilterPacket {{ id: {}, inlist: {:?}, min_max: {:?}, bloom: {:?} }}",
            self.id,
            self.inlist,
            self.min_max,
            self.bloom.is_some()
        )
    }
}

/// Represents a collection of runtime filter packets that correspond to a join operator.
///
/// # Fields
///
/// * `packets` - A map of runtime filter packets, keyed by their unique identifier `RuntimeFilterPacket::id`.
/// * `build_rows` - Total number of rows used when building the runtime filters.
/// * `disable_all_due_to_spill` - Indicates if this packet comes from a spilled build and should disable all runtime filters globally.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, PartialEq)]
pub struct JoinRuntimeFilterPacket {
    #[serde(default)]
    pub packets: Option<HashMap<usize, RuntimeFilterPacket>>,
    #[serde(default)]
    pub build_rows: usize,
    #[serde(default)]
    pub disable_all_due_to_spill: bool,
}

impl JoinRuntimeFilterPacket {
    pub fn complete_without_filters(build_rows: usize) -> Self {
        Self {
            packets: None,
            build_rows,
            disable_all_due_to_spill: false,
        }
    }

    pub fn complete(packets: HashMap<usize, RuntimeFilterPacket>, build_rows: usize) -> Self {
        Self {
            packets: Some(packets),
            build_rows,
            disable_all_due_to_spill: false,
        }
    }

    pub fn disable_all(build_rows: usize) -> Self {
        Self {
            packets: None,
            build_rows,
            disable_all_due_to_spill: true,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, PartialEq)]
struct FlightRuntimeFilterPacket {
    pub id: usize,
    pub bloom: Option<usize>,
    pub inlist: Option<usize>,
    pub min_max: Option<SerializableDomain>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, PartialEq)]
struct FlightJoinRuntimeFilterPacket {
    #[serde(default)]
    pub build_rows: usize,
    #[serde(default)]
    pub packets: Option<HashMap<usize, FlightRuntimeFilterPacket>>,
    #[serde(default)]
    pub disable_all_due_to_spill: bool,

    pub schema: DataSchemaRef,
}

impl TryInto<DataBlock> for JoinRuntimeFilterPacket {
    type Error = ErrorCode;

    fn try_into(mut self) -> Result<DataBlock> {
        let mut entities = vec![];
        let mut join_flight_packets = None;

        if let Some(packets) = self.packets.take() {
            let mut flight_packets = HashMap::with_capacity(packets.len());

            for (id, packet) in packets {
                let mut inlist_pos = None;
                if let Some(in_list) = packet.inlist {
                    let len = in_list.len() as u64;
                    inlist_pos = Some(entities.len());
                    entities.push(Column::Array(Box::new(ArrayColumn::new(
                        in_list,
                        Buffer::from(vec![0, len]),
                    ))));
                }

                let mut bloom_pos = None;
                if let Some(bloom_filter) = packet.bloom {
                    let len = bloom_filter.len() as u64;
                    bloom_pos = Some(entities.len());

                    let builder = ArrayColumnBuilder {
                        builder: ColumnBuilder::Number(NumberColumnBuilder::UInt64(bloom_filter)),
                        offsets: vec![0, len],
                    };
                    entities.push(Column::Array(Box::new(builder.build())));
                }

                flight_packets.insert(id, FlightRuntimeFilterPacket {
                    id,
                    bloom: bloom_pos,
                    inlist: inlist_pos,
                    min_max: packet.min_max,
                });
            }

            join_flight_packets = Some(flight_packets);
        }

        let data_block = match entities.is_empty() {
            true => DataBlock::empty(),
            false => DataBlock::new_from_columns(entities),
        };

        let schema = DataSchemaRef::new(data_block.infer_schema());

        data_block.add_meta(Some(Box::new(FlightJoinRuntimeFilterPacket {
            build_rows: self.build_rows,
            packets: join_flight_packets,
            disable_all_due_to_spill: self.disable_all_due_to_spill,
            schema,
        })))
    }
}

impl TryFrom<DataBlock> for JoinRuntimeFilterPacket {
    type Error = ErrorCode;

    fn try_from(mut block: DataBlock) -> Result<Self> {
        if let Some(meta) = block.take_meta() {
            let flight_join_rf = FlightJoinRuntimeFilterPacket::downcast_from(meta)
                .ok_or_else(|| ErrorCode::Internal("It's a bug"))?;

            let Some(packet) = flight_join_rf.packets else {
                return Ok(JoinRuntimeFilterPacket {
                    packets: None,
                    build_rows: flight_join_rf.build_rows,
                    disable_all_due_to_spill: flight_join_rf.disable_all_due_to_spill,
                });
            };

            let mut flight_packets = HashMap::with_capacity(packet.len());
            for (id, flight_packet) in packet {
                let mut inlist = None;
                if let Some(column_idx) = flight_packet.inlist {
                    let column = block.get_by_offset(column_idx).clone();
                    let column = column.into_column().unwrap();
                    let array_column = column.into_array().expect("it's a bug");
                    inlist = Some(array_column.index(0).expect("It's a bug"));
                }

                let mut bloom = None;
                if let Some(column_idx) = flight_packet.bloom {
                    let column = block.get_by_offset(column_idx).clone();
                    let column = column.into_column().unwrap();
                    let array_column = column.into_array().expect("it's a bug");
                    let bloom_value_column = array_column.index(0).expect("It's a bug");
                    bloom = Some(match bloom_value_column {
                        Column::Number(NumberColumn::UInt64(v)) => v.to_vec(),
                        _ => unreachable!("Unexpected runtime bloom filter column type"),
                    })
                }

                flight_packets.insert(id, RuntimeFilterPacket {
                    bloom,
                    inlist,
                    id: flight_packet.id,
                    min_max: flight_packet.min_max,
                });
            }

            return Ok(JoinRuntimeFilterPacket {
                packets: Some(flight_packets),
                build_rows: flight_join_rf.build_rows,
                disable_all_due_to_spill: flight_join_rf.disable_all_due_to_spill,
            });
        }

        Err(ErrorCode::Internal(
            "Unexpected runtime filter packet meta type. It's a bug",
        ))
    }
}

#[typetag::serde(name = "join_runtime_filter_packet")]
impl BlockMetaInfo for FlightJoinRuntimeFilterPacket {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        FlightJoinRuntimeFilterPacket::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }

    fn override_block_schema(&self) -> Option<DataSchemaRef> {
        Some(self.schema.clone())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct SerializableDomain {
    pub min: Scalar,
    pub max: Scalar,
}
