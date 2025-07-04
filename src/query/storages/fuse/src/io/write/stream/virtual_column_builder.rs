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

use std::collections::BTreeMap;

use databend_common_exception::Result;
use databend_common_expression::infer_schema_type;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::VIRTUAL_COLUMNS_LIMIT;
use databend_common_io::constants::DEFAULT_BLOCK_INDEX_BUFFER_SIZE;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_table_meta::meta::DraftVirtualBlockMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::table::TableCompression;

use crate::io::write::virtual_column_builder::VirtualColumnState;
use crate::io::TableMetaLocationGenerator;
use crate::io::VirtualColumnBuilder;
use crate::statistics::gen_columns_statistics;

#[derive(Clone)]
pub struct VirtualColumnWriter {
    table_compression: TableCompression,
    variant_fields: Vec<(usize, TableField)>,
    virtual_columns: Vec<Vec<BlockEntry>>,
    input_rows: Vec<usize>,
}

impl VirtualColumnWriter {
    pub fn create(builder: VirtualColumnBuilder, table_compression: TableCompression) -> Self {
        let filed_num = builder.variant_fields.len();
        let virtual_columns = (0..filed_num).map(|_| Vec::new()).collect();
        Self {
            table_compression,
            variant_fields: builder.variant_fields,
            virtual_columns,
            input_rows: vec![],
        }
    }

    pub fn add_block(&mut self, input: &DataBlock) -> Result<()> {
        let num_rows = input.num_rows();
        self.input_rows.push(num_rows);
        for ((offset, _), virtual_columns) in self
            .variant_fields
            .iter()
            .zip(self.virtual_columns.iter_mut())
        {
            let column = input.get_by_offset(*offset).clone();
            virtual_columns.push(column);
        }
        Ok(())
    }

    pub fn finalize(self, location: &Location) -> Result<VirtualColumnState> {
        let total_rows: usize = self.input_rows.iter().sum();
        // use a tmp column id to generate statistics for virtual columns.
        let mut tmp_column_id = 0;
        let mut virtual_column_names = Vec::new();
        let mut virtual_fields = Vec::new();
        let mut virtual_columns = Vec::new();

        'FOR: for ((_, source_field), columns) in self
            .variant_fields
            .into_iter()
            .zip(self.virtual_columns.into_iter())
        {
            let source_column_id = source_field.column_id;
            let virtual_field_num = virtual_fields.len();
            let mut virtual_values = BTreeMap::new();
            let mut sample_done = false;

            for (column, &num_rows) in columns.iter().zip(&self.input_rows) {
                let mut start_pos = 0;
                if !sample_done {
                    // use first 10 rows as sample to check whether the block is suitable for generating virtual columns
                    let sample_rows = num_rows.min(10);
                    for row in 0..sample_rows {
                        VirtualColumnBuilder::extract_virtual_values(
                            column,
                            row,
                            virtual_field_num,
                            &mut virtual_values,
                        );
                    }

                    if VirtualColumnBuilder::check_sample_virtual_values(
                        sample_rows,
                        &mut virtual_values,
                    ) {
                        continue 'FOR;
                    }
                    start_pos = sample_rows;
                    sample_done = true;
                }
                for row in start_pos..num_rows {
                    VirtualColumnBuilder::extract_virtual_values(
                        column,
                        row,
                        virtual_field_num,
                        &mut virtual_values,
                    );
                }
            }
            VirtualColumnBuilder::discard_virtual_values(
                total_rows,
                virtual_field_num,
                &mut virtual_values,
            );
            if virtual_values.is_empty() {
                continue;
            }

            let value_types = VirtualColumnBuilder::inference_data_type(&virtual_values);
            for ((key_paths, vals), val_type) in
                virtual_values.into_iter().zip(value_types.into_iter())
            {
                let (virtual_type, column) = VirtualColumnBuilder::build_column(&val_type, vals);
                let virtual_table_type = infer_schema_type(&virtual_type).unwrap();
                virtual_columns.push(column.into());

                let key_name = VirtualColumnBuilder::key_path_to_string(key_paths);
                let virtual_name = format!("{}{}", source_field.name, key_name);

                let virtual_field = TableField::new_from_column_id(
                    &virtual_name,
                    virtual_table_type,
                    tmp_column_id,
                );
                virtual_fields.push(virtual_field);
                tmp_column_id += 1;

                virtual_column_names.push((source_column_id, key_name, val_type));
            }
            if virtual_fields.len() >= VIRTUAL_COLUMNS_LIMIT {
                break;
            }
        }

        // There are no suitable virtual columns, returning empty data.
        if virtual_fields.is_empty() {
            let draft_virtual_block_meta = DraftVirtualBlockMeta {
                virtual_column_metas: vec![],
                virtual_column_size: 0,
                virtual_location: ("".to_string(), 0),
            };

            return Ok(VirtualColumnState {
                data: vec![],
                draft_virtual_block_meta,
            });
        }

        let virtual_block_schema = TableSchemaRefExt::create(virtual_fields);
        let virtual_block = DataBlock::new(virtual_columns, total_rows);

        let columns_statistics =
            gen_columns_statistics(&virtual_block, None, &virtual_block_schema)?;

        let mut data = Vec::with_capacity(DEFAULT_BLOCK_INDEX_BUFFER_SIZE);
        let file_meta = blocks_to_parquet(
            virtual_block_schema.as_ref(),
            vec![virtual_block],
            &mut data,
            self.table_compression,
        )?;
        let draft_virtual_column_metas = VirtualColumnBuilder::file_meta_to_virtual_column_metas(
            file_meta,
            virtual_column_names,
            columns_statistics,
        )?;

        let data_size = data.len() as u64;
        let virtual_column_location =
            TableMetaLocationGenerator::gen_virtual_block_location(&location.0);

        let draft_virtual_block_meta = DraftVirtualBlockMeta {
            virtual_column_metas: draft_virtual_column_metas,
            virtual_column_size: data_size,
            virtual_location: (virtual_column_location, 0),
        };

        Ok(VirtualColumnState {
            data,
            draft_virtual_block_meta,
        })
    }
}
