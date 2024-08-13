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

use std::fs;
use std::path::Path;
use std::sync::Arc;

use osmpbf::Element;
use osmpbf::ElementReader;
use parquet::basic::Compression;
use parquet::basic::Encoding;
use parquet::file::properties::WriterProperties;
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::*;
use serde::Serialize;
use serde::Serializer;

#[derive(Serialize)]
struct ElementSer<'a> {
    kind: u8,
    id: i64,
    tags: Map<'a>,
    nano_lon: Option<i64>,
    nano_lat: Option<i64>,
    refs: Vec<i64>,
    ref_types: Vec<i32>,
    ref_roles: Vec<i32>,
}

struct Map<'a>(Vec<(&'a str, &'a str)>);

impl<'a> Serialize for Map<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.collect_map(self.0.iter().cloned())
    }
}

fn main() {
    let message_type = "
    message osm {
      REQUIRED INT32 kind (UINT_8);
      REQUIRED INT64 id;
      OPTIONAL group tags (MAP) {
        REPEATED group map (MAP_KEY_VALUE) {
          REQUIRED BYTE_ARRAY key (UTF8);
          OPTIONAL BYTE_ARRAY value (UTF8);
        }
      }
      OPTIONAL INT64 nano_lon;
      OPTIONAL INT64 nano_lat;
      REPEATED INT64 refs;
      REPEATED INT32 ref_types (UINT_8);
      REPEATED INT32 ref_roles;
    ";

    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let schema = SchemaDescriptor::new(schema);
    let arrow_schema = Arc::new(parquet::arrow::parquet_to_arrow_schema(&schema, None).unwrap());

    let mut decoder = arrow_json::reader::ReaderBuilder::new(arrow_schema.clone())
        .build_decoder()
        .unwrap();

    let path = Path::new("hong-kong.parquet");
    let file = fs::File::create(&path).unwrap();

    let props = WriterProperties::builder()
        .set_column_encoding(ColumnPath::from("refs"), Encoding::DELTA_BINARY_PACKED)
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = parquet::arrow::ArrowWriter::try_new(file, arrow_schema, Some(props)).unwrap();

    let mut nodes = 0u64;
    let mut ways = 0u64;
    let mut relations = 0u64;
    let mut row = 0u64;

    let reader = ElementReader::from_path("hong-kong-latest.osm.pbf").unwrap();

    let _ = reader.for_each(|element| {
        match element {
            Element::Node(node) => {
                nodes += 1;
                let rows = [ElementSer {
                    kind: 0,
                    id: node.id(),
                    tags: Map(node.tags().collect()),
                    nano_lon: Some(node.nano_lon()),
                    nano_lat: Some(node.nano_lat()),
                    refs: vec![],
                    ref_types: vec![],
                    ref_roles: vec![],
                }];
                decoder.serialize(&rows).unwrap();
            }
            Element::DenseNode(node) => {
                nodes += 1;
                let rows = [ElementSer {
                    kind: 0,
                    id: node.id(),
                    tags: Map(node.tags().collect()),
                    nano_lon: Some(node.nano_lon()),
                    nano_lat: Some(node.nano_lat()),
                    refs: vec![],
                    ref_types: vec![],
                    ref_roles: vec![],
                }];
                decoder.serialize(&rows).unwrap();
            }
            Element::Way(way) => {
                ways += 1;
                assert!(way.node_locations().len() == 0);
                let rows = [ElementSer {
                    kind: 1,
                    id: way.id(),
                    tags: Map(way.tags().collect()),
                    nano_lon: None,
                    nano_lat: None,
                    refs: way.refs().collect(),
                    ref_types: vec![],
                    ref_roles: vec![],
                }];
                decoder.serialize(&rows).unwrap();
            }
            Element::Relation(relation) => {
                relations += 1;
                let members = relation.members();
                let mut refs = Vec::with_capacity(members.len());
                let mut ref_types = Vec::with_capacity(members.len());
                let mut ref_roles = Vec::with_capacity(members.len());
                for r in relation.members() {
                    refs.push(r.member_id);
                    ref_types.push(r.member_type as i32);
                    ref_roles.push(r.role_sid);
                }
                let rows = [ElementSer {
                    kind: 2,
                    id: relation.id(),
                    tags: Map(relation.tags().collect()),
                    nano_lon: None,
                    nano_lat: None,
                    refs,
                    ref_types,
                    ref_roles,
                }];
                decoder.serialize(&rows).unwrap();
            }
        };
        if row > 0 && row % 1000 == 0 {
            match decoder.flush().unwrap() {
                Some(batch) => writer.write(&batch).unwrap(),
                _ => {}
            }
        };
        row += 1;
    });

    match decoder.flush().unwrap() {
        Some(batch) => writer.write(&batch).unwrap(),
        _ => {}
    }

    let meta = writer.finish().unwrap();

    println!("Number of nodes: {nodes}");
    println!("Number of ways: {ways}");
    println!("Number of relations: {relations}");
    println!(
        "Writed {} rows, {} row_groups",
        meta.num_rows,
        meta.row_groups.len()
    )
}
