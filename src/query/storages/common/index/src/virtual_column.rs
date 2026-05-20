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

use arrow_schema::Schema;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::types::DataType;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use parquet::format::FileMetaData;
use serde::ser::SerializeMap;

pub const VIRTUAL_COLUMN_STRING_TABLE_KEY: &str = "string_table";
pub const VIRTUAL_COLUMN_STRING_TABLE_JSON_KEY: &str = "string_table_json";
pub const VIRTUAL_COLUMN_NODES_KEY: &str = "nodes";
pub const VIRTUAL_COLUMN_SHARED_COLUMN_IDS_KEY: &str = "shared_ids";

const LEGACY_VIRTUAL_COLUMN_STRING_TABLE_KEY: &str = "virtual_column_string_table";
const LEGACY_VIRTUAL_COLUMN_NODES_KEY: &str = "virtual_column_nodes";
const LEGACY_VIRTUAL_COLUMN_SHARED_COLUMN_IDS_KEY: &str = "virtual_column_shared_column_ids";

#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub enum VirtualColumnSharedDataType {
    Boolean,
    UInt64,
    Int64,
    Float64,
    String,
    Jsonb,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum VirtualColumnNameIndex {
    // column_metas index
    Column(u32),
    // Legacy JSONB shared column index, index is shared map key index
    Shared(u32),
    // Typed shared column index, index is shared map key index within the typed bucket.
    TypedShared {
        data_type: VirtualColumnSharedDataType,
        index: u32,
    },
}

pub type VirtualColumnSharedColumnIds = HashMap<VirtualColumnSharedDataType, (u32, u32)>;
pub type VirtualColumnSharedColumnIdMap = HashMap<u32, VirtualColumnSharedColumnIds>;
pub type VirtualColumnSharedColumnMetas =
    HashMap<VirtualColumnSharedDataType, (VirtualColumnIdWithMeta, VirtualColumnIdWithMeta)>;
pub type VirtualColumnSharedColumnMetaMap = HashMap<u32, VirtualColumnSharedColumnMetas>;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct VirtualColumnNode {
    // children: segment_id -> child node, where segment_id references string_table.
    pub children: HashMap<u32, VirtualColumnNode>,
    // leaf: terminal node that maps to a virtual column or shared map entry.
    pub leaf: Option<VirtualColumnNameIndex>,
}

// Compact metadata encoding for virtual column trie nodes:
//
// | Logical field             | Compact key | Meaning                    |
// |---------------------------|-------------|----------------------------|
// | children                  | c           | child nodes                |
// | leaf Column               | l           | direct column leaf         |
// | leaf Shared               | j           | jsonb shared leaf          |
// | leaf TypedShared Boolean  | b           | boolean shared leaf        |
// | leaf TypedShared UInt64   | u           | uint64 shared leaf         |
// | leaf TypedShared Int64    | i           | int64 shared leaf          |
// | leaf TypedShared Float64  | f           | float64 shared leaf        |
// | leaf TypedShared String   | s           | string shared leaf         |
//
// Empty children and absent leaves are omitted to reduce parquet metadata size.
#[derive(Clone, Debug)]
struct CompactVirtualColumnNode {
    children: HashMap<u32, CompactVirtualColumnNode>,
    leaf: Option<CompactVirtualColumnLeaf>,
}

#[derive(Clone, Debug)]
enum CompactVirtualColumnLeaf {
    Column(u32),
    JsonbShared(u32),
    BooleanShared(u32),
    UInt64Shared(u32),
    Int64Shared(u32),
    Float64Shared(u32),
    StringShared(u32),
}

impl CompactVirtualColumnLeaf {
    fn key(&self) -> &'static str {
        match self {
            CompactVirtualColumnLeaf::Column(_) => "l",
            CompactVirtualColumnLeaf::JsonbShared(_) => "j",
            CompactVirtualColumnLeaf::BooleanShared(_) => "b",
            CompactVirtualColumnLeaf::UInt64Shared(_) => "u",
            CompactVirtualColumnLeaf::Int64Shared(_) => "i",
            CompactVirtualColumnLeaf::Float64Shared(_) => "f",
            CompactVirtualColumnLeaf::StringShared(_) => "s",
        }
    }

    fn index(&self) -> u32 {
        match self {
            CompactVirtualColumnLeaf::Column(index)
            | CompactVirtualColumnLeaf::JsonbShared(index)
            | CompactVirtualColumnLeaf::BooleanShared(index)
            | CompactVirtualColumnLeaf::UInt64Shared(index)
            | CompactVirtualColumnLeaf::Int64Shared(index)
            | CompactVirtualColumnLeaf::Float64Shared(index)
            | CompactVirtualColumnLeaf::StringShared(index) => *index,
        }
    }

    fn from_key(key: &str, index: u32) -> Option<Self> {
        match key {
            "l" => Some(CompactVirtualColumnLeaf::Column(index)),
            "j" => Some(CompactVirtualColumnLeaf::JsonbShared(index)),
            "b" => Some(CompactVirtualColumnLeaf::BooleanShared(index)),
            "u" => Some(CompactVirtualColumnLeaf::UInt64Shared(index)),
            "i" => Some(CompactVirtualColumnLeaf::Int64Shared(index)),
            "f" => Some(CompactVirtualColumnLeaf::Float64Shared(index)),
            "s" => Some(CompactVirtualColumnLeaf::StringShared(index)),
            _ => None,
        }
    }
}

impl serde::Serialize for CompactVirtualColumnNode {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let len =
            if self.children.is_empty() { 0 } else { 1 } + if self.leaf.is_some() { 1 } else { 0 };
        let mut map = serializer.serialize_map(Some(len))?;
        if !self.children.is_empty() {
            map.serialize_entry("c", &self.children)?;
        }
        if let Some(leaf) = &self.leaf {
            map.serialize_entry(leaf.key(), &leaf.index())?;
        }
        map.end()
    }
}

impl<'de> serde::Deserialize<'de> for CompactVirtualColumnNode {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        let mut fields = HashMap::<String, serde_json::Value>::deserialize(deserializer)?;
        let children = match fields.remove("c") {
            Some(value) => serde_json::from_value(value).map_err(serde::de::Error::custom)?,
            None => HashMap::new(),
        };

        let mut leaf = None;
        for key in ["l", "j", "b", "u", "i", "f", "s"] {
            let Some(value) = fields.remove(key) else {
                continue;
            };
            if leaf.is_some() {
                return Err(serde::de::Error::custom(
                    "compact virtual column node contains multiple leaves",
                ));
            }
            let index = serde_json::from_value(value).map_err(serde::de::Error::custom)?;
            leaf = CompactVirtualColumnLeaf::from_key(key, index);
        }
        if !fields.is_empty() {
            return Err(serde::de::Error::custom(format!(
                "compact virtual column node contains unknown keys {:?}",
                fields.keys().collect::<Vec<_>>()
            )));
        }

        Ok(Self { children, leaf })
    }
}

impl From<&VirtualColumnNode> for CompactVirtualColumnNode {
    fn from(node: &VirtualColumnNode) -> Self {
        let children = node
            .children
            .iter()
            .map(|(id, child)| (*id, CompactVirtualColumnNode::from(child)))
            .collect();
        let leaf = match node.leaf.as_ref() {
            Some(VirtualColumnNameIndex::Column(index)) => {
                Some(CompactVirtualColumnLeaf::Column(*index))
            }
            Some(VirtualColumnNameIndex::Shared(index)) => {
                Some(CompactVirtualColumnLeaf::JsonbShared(*index))
            }
            Some(VirtualColumnNameIndex::TypedShared {
                data_type: VirtualColumnSharedDataType::Boolean,
                index,
            }) => Some(CompactVirtualColumnLeaf::BooleanShared(*index)),
            Some(VirtualColumnNameIndex::TypedShared {
                data_type: VirtualColumnSharedDataType::UInt64,
                index,
            }) => Some(CompactVirtualColumnLeaf::UInt64Shared(*index)),
            Some(VirtualColumnNameIndex::TypedShared {
                data_type: VirtualColumnSharedDataType::Int64,
                index,
            }) => Some(CompactVirtualColumnLeaf::Int64Shared(*index)),
            Some(VirtualColumnNameIndex::TypedShared {
                data_type: VirtualColumnSharedDataType::Float64,
                index,
            }) => Some(CompactVirtualColumnLeaf::Float64Shared(*index)),
            Some(VirtualColumnNameIndex::TypedShared {
                data_type: VirtualColumnSharedDataType::String,
                index,
            }) => Some(CompactVirtualColumnLeaf::StringShared(*index)),
            Some(VirtualColumnNameIndex::TypedShared {
                data_type: VirtualColumnSharedDataType::Jsonb,
                index,
            }) => Some(CompactVirtualColumnLeaf::JsonbShared(*index)),
            None => None,
        };
        CompactVirtualColumnNode { children, leaf }
    }
}

impl CompactVirtualColumnNode {
    fn into_virtual_column_node(self) -> Result<VirtualColumnNode> {
        let children = self
            .children
            .into_iter()
            .map(|(id, child)| child.into_virtual_column_node().map(|child| (id, child)))
            .collect::<Result<HashMap<_, _>>>()?;

        let leaf = self.leaf.map(|leaf| match leaf {
            CompactVirtualColumnLeaf::Column(index) => VirtualColumnNameIndex::Column(index),
            CompactVirtualColumnLeaf::JsonbShared(index) => VirtualColumnNameIndex::Shared(index),
            CompactVirtualColumnLeaf::BooleanShared(index) => VirtualColumnNameIndex::TypedShared {
                data_type: VirtualColumnSharedDataType::Boolean,
                index,
            },
            CompactVirtualColumnLeaf::UInt64Shared(index) => VirtualColumnNameIndex::TypedShared {
                data_type: VirtualColumnSharedDataType::UInt64,
                index,
            },
            CompactVirtualColumnLeaf::Int64Shared(index) => VirtualColumnNameIndex::TypedShared {
                data_type: VirtualColumnSharedDataType::Int64,
                index,
            },
            CompactVirtualColumnLeaf::Float64Shared(index) => VirtualColumnNameIndex::TypedShared {
                data_type: VirtualColumnSharedDataType::Float64,
                index,
            },
            CompactVirtualColumnLeaf::StringShared(index) => VirtualColumnNameIndex::TypedShared {
                data_type: VirtualColumnSharedDataType::String,
                index,
            },
        });
        Ok(VirtualColumnNode { children, leaf })
    }
}

pub fn encode_compact_virtual_column_nodes(
    nodes: &HashMap<u32, VirtualColumnNode>,
) -> Result<String> {
    let compact_nodes = nodes
        .iter()
        .map(|(id, node)| (*id, CompactVirtualColumnNode::from(node)))
        .collect::<HashMap<_, _>>();
    serde_json::to_string(&compact_nodes).map_err(|e| {
        ErrorCode::StorageOther(format!(
            "failed to encode compact virtual column nodes {:?}",
            e
        ))
    })
}

// Compact metadata encoding for shared column id mappings:
//
// | Logical field       | Compact key | Meaning                |
// |---------------------|-------------|------------------------|
// | Shared Jsonb        | j           | jsonb shared columns   |
// | TypedShared Boolean | b           | boolean shared columns |
// | TypedShared UInt64  | u           | uint64 shared columns  |
// | TypedShared Int64   | i           | int64 shared columns   |
// | TypedShared Float64 | f           | float64 shared columns |
// | TypedShared String  | s           | string shared columns  |
//
// Each value is `(key_column_id, value_column_id)`. Jsonb typed shared is encoded as `j`,
// so it does not need a separate typed key.
#[derive(Clone, Debug, Default)]
struct CompactVirtualColumnSharedIds {
    ids: Vec<CompactVirtualColumnSharedId>,
}

impl CompactVirtualColumnSharedIds {
    fn insert(&mut self, id: CompactVirtualColumnSharedId) {
        let key = id.key();
        if let Some(existing) = self.ids.iter_mut().find(|existing| existing.key() == key) {
            *existing = id;
        } else {
            self.ids.push(id);
        }
    }
}

#[derive(Clone, Debug)]
enum CompactVirtualColumnSharedId {
    Jsonb((u32, u32)),
    Boolean((u32, u32)),
    UInt64((u32, u32)),
    Int64((u32, u32)),
    Float64((u32, u32)),
    String((u32, u32)),
}

impl CompactVirtualColumnSharedId {
    fn key(&self) -> &'static str {
        match self {
            CompactVirtualColumnSharedId::Jsonb(_) => "j",
            CompactVirtualColumnSharedId::Boolean(_) => "b",
            CompactVirtualColumnSharedId::UInt64(_) => "u",
            CompactVirtualColumnSharedId::Int64(_) => "i",
            CompactVirtualColumnSharedId::Float64(_) => "f",
            CompactVirtualColumnSharedId::String(_) => "s",
        }
    }

    fn ids(&self) -> (u32, u32) {
        match self {
            CompactVirtualColumnSharedId::Jsonb(ids)
            | CompactVirtualColumnSharedId::Boolean(ids)
            | CompactVirtualColumnSharedId::UInt64(ids)
            | CompactVirtualColumnSharedId::Int64(ids)
            | CompactVirtualColumnSharedId::Float64(ids)
            | CompactVirtualColumnSharedId::String(ids) => *ids,
        }
    }

    fn from_key(key: &str, ids: (u32, u32)) -> Option<Self> {
        match key {
            "j" => Some(CompactVirtualColumnSharedId::Jsonb(ids)),
            "b" => Some(CompactVirtualColumnSharedId::Boolean(ids)),
            "u" => Some(CompactVirtualColumnSharedId::UInt64(ids)),
            "i" => Some(CompactVirtualColumnSharedId::Int64(ids)),
            "f" => Some(CompactVirtualColumnSharedId::Float64(ids)),
            "s" => Some(CompactVirtualColumnSharedId::String(ids)),
            _ => None,
        }
    }
}

impl serde::Serialize for CompactVirtualColumnSharedIds {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let mut map = serializer.serialize_map(Some(self.ids.len()))?;
        for id in &self.ids {
            map.serialize_entry(id.key(), &id.ids())?;
        }
        map.end()
    }
}

impl<'de> serde::Deserialize<'de> for CompactVirtualColumnSharedIds {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        let mut fields = HashMap::<String, serde_json::Value>::deserialize(deserializer)?;
        let mut ids = Vec::with_capacity(fields.len());
        for key in ["j", "b", "u", "i", "f", "s"] {
            let Some(value) = fields.remove(key) else {
                continue;
            };
            let column_ids = serde_json::from_value(value).map_err(serde::de::Error::custom)?;
            ids.push(
                CompactVirtualColumnSharedId::from_key(key, column_ids).ok_or_else(|| {
                    serde::de::Error::custom(format!("invalid compact shared ids key {key}"))
                })?,
            );
        }
        if !fields.is_empty() {
            return Err(serde::de::Error::custom(format!(
                "compact virtual column shared ids contains unknown keys {:?}",
                fields.keys().collect::<Vec<_>>()
            )));
        }

        Ok(Self { ids })
    }
}

pub fn encode_compact_virtual_column_string_table(
    string_table: &[String],
) -> Result<(String, String)> {
    let needs_json = compact_string_table_needs_json(string_table);
    if needs_json {
        // JSON object keys may legally contain NUL bytes. Virtual columns are an
        // optimization, so use a JSON-array metadata format for these rare paths
        // instead of rejecting the write.
        let encoded = serde_json::to_string(string_table).map_err(|e| {
            ErrorCode::StorageOther(format!(
                "failed to encode virtual column string table {:?}",
                e
            ))
        })?;
        return Ok((VIRTUAL_COLUMN_STRING_TABLE_JSON_KEY.to_string(), encoded));
    }

    Ok((
        VIRTUAL_COLUMN_STRING_TABLE_KEY.to_string(),
        string_table.join("\0"),
    ))
}

fn compact_string_table_needs_json(string_table: &[String]) -> bool {
    string_table.iter().any(|segment| segment.contains('\0'))
        || matches!(string_table, [segment] if segment.is_empty())
}

pub fn encode_compact_virtual_column_shared_ids(
    typed_shared_column_ids: &VirtualColumnSharedColumnIdMap,
) -> Result<String> {
    let mut compact_ids: HashMap<u32, CompactVirtualColumnSharedIds> = HashMap::new();

    for (source_id, typed_ids) in typed_shared_column_ids {
        let compact = compact_ids.entry(*source_id).or_default();
        for (data_type, ids) in typed_ids {
            let id = match data_type {
                VirtualColumnSharedDataType::Boolean => CompactVirtualColumnSharedId::Boolean(*ids),
                VirtualColumnSharedDataType::UInt64 => CompactVirtualColumnSharedId::UInt64(*ids),
                VirtualColumnSharedDataType::Int64 => CompactVirtualColumnSharedId::Int64(*ids),
                VirtualColumnSharedDataType::Float64 => CompactVirtualColumnSharedId::Float64(*ids),
                VirtualColumnSharedDataType::String => CompactVirtualColumnSharedId::String(*ids),
                VirtualColumnSharedDataType::Jsonb => CompactVirtualColumnSharedId::Jsonb(*ids),
            };
            compact.insert(id);
        }
    }

    serde_json::to_string(&compact_ids).map_err(|e| {
        ErrorCode::StorageOther(format!(
            "failed to encode compact virtual column shared ids {:?}",
            e
        ))
    })
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct VirtualColumnIdWithMeta {
    pub column_id: u32,
    pub meta: SingleColumnMeta,
    pub data_type: DataType,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct VirtualColumnFileMeta {
    // All unique path segment strings in this file.
    pub string_table: Vec<String>,
    // Column metadata for all virtual columns (offset/len/num_values + type + column id).
    pub column_metas: Vec<VirtualColumnIdWithMeta>,
    // Sparse paths are stored in shared map columns grouped by value type:
    // source_id -> data_type -> (key column, value column).
    #[serde(default)]
    pub typed_shared_column_metas: VirtualColumnSharedColumnMetaMap,
    // Per source column trie: source_column_id -> root node.
    pub virtual_column_nodes: HashMap<u32, VirtualColumnNode>,
}

impl TryFrom<&VirtualColumnFileMeta> for Vec<u8> {
    type Error = ErrorCode;

    fn try_from(value: &VirtualColumnFileMeta) -> std::result::Result<Self, Self::Error> {
        bincode::serde::encode_to_vec(value, bincode::config::standard()).map_err(|e| {
            ErrorCode::StorageOther(format!("failed to encode vector index meta {:?}", e))
        })
    }
}

impl TryFrom<Bytes> for VirtualColumnFileMeta {
    type Error = ErrorCode;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::serde::decode_from_slice(value.as_ref(), bincode::config::standard())
            .map(|(v, len)| {
                assert_eq!(len, value.len());
                v
            })
            .map_err(|e| {
                ErrorCode::StorageOther(format!("failed to decode vector index meta {:?}", e))
            })
    }
}

// Virtual columns are extracted from Variant/JSON values into independent parquet columns.
// Each source column builds a trie keyed by path segments; leaf nodes point to either a
// dedicated virtual column or a shared map entry for sparse paths. The parquet file stores:
// - ARROW:schema: column names/types for all virtual columns.
// - VIRTUAL_COLUMN_STRING_TABLE_KEY: NUL-delimited string table of unique path segments.
// - VIRTUAL_COLUMN_STRING_TABLE_JSON_KEY: JSON-array string table for segments that cannot
//   be represented by NUL-delimited metadata.
// - VIRTUAL_COLUMN_NODES_KEY: compact trie encoded by string table ids and leaf indices.
// - VIRTUAL_COLUMN_SHARED_COLUMN_IDS_KEY: compact shared/typed-shared column id mapping.
// Legacy metadata keys are still accepted as a fallback for old virtual column files.
// Column offsets/lengths/num_values and shared map columns are derived from parquet metadata.
impl TryFrom<FileMetaData> for VirtualColumnFileMeta {
    type Error = ErrorCode;

    fn try_from(mut meta: FileMetaData) -> std::result::Result<Self, Self::Error> {
        let mut arrow_schema = None;
        let mut string_table = None;
        let mut json_string_table = None;
        let mut legacy_string_table = None;
        let mut virtual_column_nodes = None;
        let mut legacy_virtual_column_nodes = None;
        let mut typed_shared_column_ids = None;
        let mut legacy_typed_shared_column_ids = None;

        let key_value_metadata = meta.key_value_metadata.unwrap_or_default();
        for key_value in &key_value_metadata {
            if key_value.key == "ARROW:schema" {
                let encoded_meta = key_value.value.as_ref().unwrap();
                arrow_schema = Some(get_arrow_schema_from_metadata(encoded_meta)?);
            } else if key_value.key == VIRTUAL_COLUMN_STRING_TABLE_KEY {
                let encoded_meta = key_value.value.as_ref().unwrap();
                string_table = Some(get_compact_virtual_column_string_table(encoded_meta)?);
            } else if key_value.key == VIRTUAL_COLUMN_STRING_TABLE_JSON_KEY {
                let encoded_meta = key_value.value.as_ref().unwrap();
                json_string_table = Some(get_virtual_column_string_table(encoded_meta)?);
            } else if key_value.key == LEGACY_VIRTUAL_COLUMN_STRING_TABLE_KEY {
                let encoded_meta = key_value.value.as_ref().unwrap();
                legacy_string_table = Some(get_virtual_column_string_table(encoded_meta)?);
            } else if key_value.key == VIRTUAL_COLUMN_NODES_KEY {
                let encoded_meta = key_value.value.as_ref().unwrap();
                virtual_column_nodes = Some(get_compact_virtual_column_nodes(encoded_meta)?);
            } else if key_value.key == LEGACY_VIRTUAL_COLUMN_NODES_KEY {
                let encoded_meta = key_value.value.as_ref().unwrap();
                legacy_virtual_column_nodes = Some(get_virtual_column_nodes(encoded_meta)?);
            } else if key_value.key == VIRTUAL_COLUMN_SHARED_COLUMN_IDS_KEY {
                let encoded_meta = key_value.value.as_ref().unwrap();
                typed_shared_column_ids =
                    Some(get_compact_virtual_column_shared_ids(encoded_meta)?);
            } else if key_value.key == LEGACY_VIRTUAL_COLUMN_SHARED_COLUMN_IDS_KEY {
                let encoded_meta = key_value.value.as_ref().unwrap();
                legacy_typed_shared_column_ids =
                    Some(get_legacy_virtual_column_shared_ids(encoded_meta)?);
            }
        }

        let arrow_schema = arrow_schema.ok_or_else(|| {
            ErrorCode::Internal("virtual column file missing ARROW:schema metadata")
        })?;
        let data_schema = DataSchema::try_from(&arrow_schema)?;
        let string_table = string_table
            .or(json_string_table)
            .or(legacy_string_table)
            .ok_or_else(|| {
                ErrorCode::Internal(
                    "virtual column file missing virtual column string table metadata",
                )
            })?;
        let virtual_column_nodes = virtual_column_nodes
            .or(legacy_virtual_column_nodes)
            .ok_or_else(|| {
                ErrorCode::Internal("virtual column file missing virtual column nodes metadata")
            })?;
        let typed_shared_column_ids = typed_shared_column_ids
            .or(legacy_typed_shared_column_ids)
            .unwrap_or_default();

        let rg = meta.row_groups.remove(0);
        let mut column_metas = Vec::with_capacity(rg.columns.len());
        for (column_idx, column) in rg.columns.iter().enumerate() {
            let chunk_meta = column.meta_data.as_ref().ok_or_else(|| {
                ErrorCode::Internal(
                    "expecting chunk meta data while converting ThriftFileMetaData to VirtualColumnFileMeta",
                )
            })?;
            let col_start = if let Some(dict_page_offset) = chunk_meta.dictionary_page_offset {
                dict_page_offset
            } else {
                chunk_meta.data_page_offset
            };
            let col_len = chunk_meta.total_compressed_size;
            assert!(
                col_start >= 0 && col_len >= 0,
                "column start and length should not be negative"
            );
            let num_values = chunk_meta.num_values as u64;
            let res = SingleColumnMeta {
                offset: col_start as u64,
                len: col_len as u64,
                num_values,
            };
            let data_type = data_type_from_path(&data_schema, &chunk_meta.path_in_schema)?;
            let column_id = column_idx as u32;
            column_metas.push(VirtualColumnIdWithMeta {
                column_id,
                meta: res,
                data_type,
            });
        }

        let mut typed_shared_column_metas = HashMap::new();
        for (source_id, typed_ids) in typed_shared_column_ids {
            for (data_type, (key_idx, value_idx)) in typed_ids {
                let Some(key_meta) = column_metas.get(key_idx as usize) else {
                    continue;
                };
                let Some(value_meta) = column_metas.get(value_idx as usize) else {
                    continue;
                };
                typed_shared_column_metas
                    .entry(source_id)
                    .or_insert_with(HashMap::new)
                    .insert(data_type, (key_meta.clone(), value_meta.clone()));
            }
        }

        Ok(Self {
            string_table,
            column_metas,
            typed_shared_column_metas,
            virtual_column_nodes,
        })
    }
}

fn get_arrow_schema_from_metadata(encoded_meta: &str) -> Result<Schema> {
    let decoded = BASE64_STANDARD.decode(encoded_meta);
    match decoded {
        Ok(bytes) => {
            let slice = if bytes.len() > 8 && bytes[0..4] == [255u8; 4] {
                &bytes[8..]
            } else {
                bytes.as_slice()
            };
            match arrow_ipc::root_as_message(slice) {
                Ok(message) => message
                    .header_as_schema()
                    .map(arrow_ipc::convert::fb_to_schema)
                    .ok_or_else(|| ErrorCode::Internal("the message is not Arrow Schema")),
                Err(err) => {
                    // The flatbuffers implementation returns an error on verification error.
                    Err(ErrorCode::Internal(format!(
                        "Unable to get root as message stored in virtual column ARROW:schema: {:?}",
                        err
                    )))
                }
            }
        }
        Err(err) => Err(ErrorCode::Internal(format!(
            "Unable to decode the encoded schema stored in virtual column ARROW:schema: {:?}",
            err
        ))),
    }
}

fn get_virtual_column_string_table(encoded_meta: &str) -> Result<Vec<String>> {
    serde_json::from_str(encoded_meta).map_err(|e| {
        ErrorCode::StorageOther(format!(
            "failed to decode virtual column string table {:?}",
            e
        ))
    })
}

fn get_compact_virtual_column_string_table(encoded_meta: &str) -> Result<Vec<String>> {
    if encoded_meta.is_empty() {
        return Ok(Vec::new());
    }
    Ok(encoded_meta.split('\0').map(str::to_string).collect())
}

fn get_virtual_column_nodes(encoded_meta: &str) -> Result<HashMap<u32, VirtualColumnNode>> {
    serde_json::from_str(encoded_meta).map_err(|e| {
        ErrorCode::StorageOther(format!("failed to decode virtual column nodes {:?}", e))
    })
}

fn get_compact_virtual_column_nodes(encoded_meta: &str) -> Result<HashMap<u32, VirtualColumnNode>> {
    let compact_nodes: HashMap<u32, CompactVirtualColumnNode> = serde_json::from_str(encoded_meta)
        .map_err(|e| {
            ErrorCode::StorageOther(format!(
                "failed to decode compact virtual column nodes {:?}",
                e
            ))
        })?;
    compact_nodes
        .into_iter()
        .map(|(id, node)| node.into_virtual_column_node().map(|node| (id, node)))
        .collect()
}

fn get_legacy_virtual_column_shared_ids(
    encoded_meta: &str,
) -> Result<VirtualColumnSharedColumnIdMap> {
    let shared_ids: HashMap<u32, (u32, u32)> = serde_json::from_str(encoded_meta).map_err(|e| {
        ErrorCode::StorageOther(format!(
            "failed to decode virtual column shared ids {:?}",
            e
        ))
    })?;
    Ok(shared_ids
        .into_iter()
        .map(|(source_id, ids)| {
            (
                source_id,
                HashMap::from([(VirtualColumnSharedDataType::Jsonb, ids)]),
            )
        })
        .collect())
}

fn get_compact_virtual_column_shared_ids(
    encoded_meta: &str,
) -> Result<VirtualColumnSharedColumnIdMap> {
    let compact_ids: HashMap<u32, CompactVirtualColumnSharedIds> =
        serde_json::from_str(encoded_meta).map_err(|e| {
            ErrorCode::StorageOther(format!(
                "failed to decode compact virtual column shared ids {:?}",
                e
            ))
        })?;

    let mut typed_shared_column_ids = HashMap::new();
    for (source_id, ids) in compact_ids {
        let mut typed_ids = HashMap::new();
        for id in ids.ids {
            match id {
                CompactVirtualColumnSharedId::Jsonb(jsonb) => {
                    typed_ids.insert(VirtualColumnSharedDataType::Jsonb, jsonb);
                }
                CompactVirtualColumnSharedId::Boolean(boolean) => {
                    typed_ids.insert(VirtualColumnSharedDataType::Boolean, boolean);
                }
                CompactVirtualColumnSharedId::UInt64(uint64) => {
                    typed_ids.insert(VirtualColumnSharedDataType::UInt64, uint64);
                }
                CompactVirtualColumnSharedId::Int64(int64) => {
                    typed_ids.insert(VirtualColumnSharedDataType::Int64, int64);
                }
                CompactVirtualColumnSharedId::Float64(float64) => {
                    typed_ids.insert(VirtualColumnSharedDataType::Float64, float64);
                }
                CompactVirtualColumnSharedId::String(string) => {
                    typed_ids.insert(VirtualColumnSharedDataType::String, string);
                }
            }
        }
        if !typed_ids.is_empty() {
            typed_shared_column_ids.insert(source_id, typed_ids);
        }
    }

    Ok(typed_shared_column_ids)
}

fn data_type_from_path(schema: &DataSchema, path: &[String]) -> Result<DataType> {
    if path.is_empty() {
        return Err(ErrorCode::Internal(
            "empty path in parquet schema".to_string(),
        ));
    }

    let field = schema.field_with_name(&path[0])?;
    let data_type = field.data_type().clone();
    if path.len() == 1 {
        return Ok(data_type);
    }

    if let DataType::Map(inner) = data_type.remove_nullable() {
        if let DataType::Tuple(inner_types) = inner.as_ref() {
            if inner_types.len() == 2 {
                let last = path.last().map(String::as_str).unwrap_or_default();
                if last == "key" {
                    return Ok(inner_types[0].clone());
                }
                if last == "value" {
                    return Ok(inner_types[1].clone());
                }
            }
        }
    }

    Err(ErrorCode::Internal(format!(
        "failed to find data type for path {:?}",
        path
    )))
}

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use serde_json::json;

    use super::*;

    #[test]
    fn test_compact_virtual_column_metadata_key_names() {
        assert_eq!(VIRTUAL_COLUMN_STRING_TABLE_KEY, "string_table");
        assert_eq!(VIRTUAL_COLUMN_STRING_TABLE_JSON_KEY, "string_table_json");
        assert_eq!(VIRTUAL_COLUMN_NODES_KEY, "nodes");
        assert_eq!(VIRTUAL_COLUMN_SHARED_COLUMN_IDS_KEY, "shared_ids");
    }

    #[test]
    fn test_compact_virtual_column_string_table_uses_nul_delimiter() -> Result<()> {
        let string_table = vec![
            "message".to_string(),
            "attribute".to_string(),
            "user_id".to_string(),
        ];

        let (key, encoded) = encode_compact_virtual_column_string_table(&string_table)?;

        assert_eq!(key, VIRTUAL_COLUMN_STRING_TABLE_KEY);
        assert_eq!(encoded, "message\0attribute\0user_id");
        assert_eq!(
            get_compact_virtual_column_string_table(&encoded)?,
            string_table
        );
        assert!(get_compact_virtual_column_string_table("")?.is_empty());

        Ok(())
    }

    #[test]
    fn test_virtual_column_string_table_metadata_uses_json_for_nul_segment() -> Result<()> {
        let string_table = vec![
            "message\0attribute".to_string(),
            "".to_string(),
            "user_id".to_string(),
        ];

        let (key, encoded) = encode_compact_virtual_column_string_table(&string_table)?;

        assert_eq!(key, VIRTUAL_COLUMN_STRING_TABLE_JSON_KEY);
        assert_eq!(get_virtual_column_string_table(&encoded)?, string_table);

        Ok(())
    }

    #[test]
    fn test_virtual_column_string_table_metadata_uses_json_for_single_empty_segment() -> Result<()>
    {
        let string_table = vec!["".to_string()];

        let (key, encoded) = encode_compact_virtual_column_string_table(&string_table)?;

        assert_eq!(key, VIRTUAL_COLUMN_STRING_TABLE_JSON_KEY);
        assert_eq!(get_virtual_column_string_table(&encoded)?, string_table);

        Ok(())
    }

    #[test]
    fn test_decode_legacy_virtual_column_metadata_keys() -> Result<()> {
        let string_table = vec![
            "message\0attribute".to_string(),
            "".to_string(),
            "user_id".to_string(),
        ];
        let encoded_string_table = serde_json::to_string(&string_table).unwrap();
        assert_eq!(
            get_virtual_column_string_table(&encoded_string_table)?,
            string_table
        );

        let encoded_nodes = r#"{
            "5": {
                "children": {
                    "4": {"children": {}, "leaf": {"Column": 4}},
                    "6": {"children": {}, "leaf": {"Shared": 25}},
                    "7": {
                        "children": {},
                        "leaf": {
                            "TypedShared": {
                                "data_type": "Boolean",
                                "index": 325
                            }
                        }
                    }
                },
                "leaf": null
            }
        }"#;
        let nodes = get_virtual_column_nodes(encoded_nodes)?;
        let root = nodes.get(&5).unwrap();
        assert_column_leaf(root, 4, 4);
        assert_shared_leaf(root, 6, 25);
        assert_typed_shared_leaf(root, 7, VirtualColumnSharedDataType::Boolean, 325);

        let shared_ids = get_legacy_virtual_column_shared_ids(r#"{"5":[125,126]}"#)?;
        assert_eq!(
            shared_ids
                .get(&5)
                .and_then(|ids| ids.get(&VirtualColumnSharedDataType::Jsonb)),
            Some(&(125, 126))
        );

        Ok(())
    }

    #[test]
    fn test_encode_compact_virtual_column_nodes_omits_empty_children_and_shortens_leaf()
    -> Result<()> {
        let mut child = VirtualColumnNode {
            children: HashMap::new(),
            leaf: Some(VirtualColumnNameIndex::Column(4)),
        };
        child.children.insert(8, VirtualColumnNode {
            children: HashMap::new(),
            leaf: Some(VirtualColumnNameIndex::Shared(25)),
        });
        child.children.insert(9, VirtualColumnNode {
            children: HashMap::new(),
            leaf: Some(VirtualColumnNameIndex::TypedShared {
                data_type: VirtualColumnSharedDataType::Jsonb,
                index: 26,
            }),
        });

        let mut root = VirtualColumnNode {
            children: HashMap::new(),
            leaf: None,
        };
        root.children.insert(4, child);

        let mut nodes = HashMap::new();
        nodes.insert(5, root);

        let encoded = encode_compact_virtual_column_nodes(&nodes)?;
        let value: Value = serde_json::from_str(&encoded).unwrap();

        assert_eq!(value["5"]["c"]["4"]["l"], json!(4));
        assert_eq!(value["5"]["c"]["4"]["c"]["8"]["j"], json!(25));
        assert_eq!(value["5"]["c"]["4"]["c"]["9"]["j"], json!(26));
        assert!(value["5"]["c"]["4"]["c"]["8"].get("c").is_none());
        assert!(value["5"]["c"]["4"]["c"]["8"].get("children").is_none());
        assert!(value["5"]["c"]["4"]["c"]["8"].get("leaf").is_none());
        assert!(value["5"]["c"]["4"]["c"]["9"].get("t").is_none());

        Ok(())
    }

    #[test]
    fn test_decode_compact_virtual_column_nodes_supports_all_leaf_types() -> Result<()> {
        let encoded = r#"{
            "1": {
                "c": {
                    "10": {"l": 1},
                    "11": {"j": 2},
                    "12": {"b": 3},
                    "13": {"u": 4},
                    "14": {"i": 5},
                    "15": {"f": 6},
                    "16": {"s": 7}
                }
            }
        }"#;

        let nodes = get_compact_virtual_column_nodes(encoded)?;
        let root = nodes.get(&1).unwrap();
        assert!(root.leaf.is_none());

        assert_column_leaf(root, 10, 1);
        assert_shared_leaf(root, 11, 2);
        assert_typed_shared_leaf(root, 12, VirtualColumnSharedDataType::Boolean, 3);
        assert_typed_shared_leaf(root, 13, VirtualColumnSharedDataType::UInt64, 4);
        assert_typed_shared_leaf(root, 14, VirtualColumnSharedDataType::Int64, 5);
        assert_typed_shared_leaf(root, 15, VirtualColumnSharedDataType::Float64, 6);
        assert_typed_shared_leaf(root, 16, VirtualColumnSharedDataType::String, 7);

        Ok(())
    }

    #[test]
    fn test_decode_compact_virtual_column_node_rejects_multiple_leaves() {
        let encoded = r#"{"1":{"l":1,"j":2}}"#;
        assert!(get_compact_virtual_column_nodes(encoded).is_err());
    }

    #[test]
    fn test_encode_decode_compact_virtual_column_shared_ids() -> Result<()> {
        let mut typed_ids = HashMap::new();
        typed_ids.insert(VirtualColumnSharedDataType::Jsonb, (10, 11));
        typed_ids.insert(VirtualColumnSharedDataType::Boolean, (12, 13));
        typed_ids.insert(VirtualColumnSharedDataType::UInt64, (14, 15));
        typed_ids.insert(VirtualColumnSharedDataType::Int64, (16, 17));
        typed_ids.insert(VirtualColumnSharedDataType::Float64, (18, 19));
        typed_ids.insert(VirtualColumnSharedDataType::String, (20, 21));
        let mut typed_shared_column_ids = HashMap::new();
        typed_shared_column_ids.insert(1, typed_ids);

        let encoded = encode_compact_virtual_column_shared_ids(&typed_shared_column_ids)?;
        let value: Value = serde_json::from_str(&encoded).unwrap();

        assert_eq!(value["1"]["j"], json!([10, 11]));
        assert_eq!(value["1"]["b"], json!([12, 13]));
        assert_eq!(value["1"]["u"], json!([14, 15]));
        assert_eq!(value["1"]["i"], json!([16, 17]));
        assert_eq!(value["1"]["f"], json!([18, 19]));
        assert_eq!(value["1"]["s"], json!([20, 21]));

        let decoded_typed_shared_ids = get_compact_virtual_column_shared_ids(&encoded)?;

        assert_eq!(decoded_typed_shared_ids, typed_shared_column_ids);

        Ok(())
    }

    fn assert_column_leaf(root: &VirtualColumnNode, child_id: u32, expected_index: u32) {
        let child = root.children.get(&child_id).unwrap();
        assert!(child.children.is_empty());
        assert!(matches!(
            child.leaf,
            Some(VirtualColumnNameIndex::Column(index)) if index == expected_index
        ));
    }

    fn assert_shared_leaf(root: &VirtualColumnNode, child_id: u32, expected_index: u32) {
        let child = root.children.get(&child_id).unwrap();
        assert!(child.children.is_empty());
        assert!(matches!(
            child.leaf,
            Some(VirtualColumnNameIndex::Shared(index)) if index == expected_index
        ));
    }

    fn assert_typed_shared_leaf(
        root: &VirtualColumnNode,
        child_id: u32,
        expected_data_type: VirtualColumnSharedDataType,
        expected_index: u32,
    ) {
        let child = root.children.get(&child_id).unwrap();
        assert!(child.children.is_empty());
        assert!(matches!(
            child.leaf,
            Some(VirtualColumnNameIndex::TypedShared { data_type, index })
                if data_type == expected_data_type && index == expected_index
        ));
    }
}
