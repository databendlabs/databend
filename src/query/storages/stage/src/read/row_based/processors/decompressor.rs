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

use databend_common_compress::CompressAlgorithm;
use databend_common_compress::DecompressDecoder;
use databend_common_compress::DecompressState;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;

use crate::read::load_context::LoadContext;
use crate::read::row_based::batch::BytesBatch;

pub struct Decompressor {
    #[allow(dead_code)]
    ctx: Arc<LoadContext>,
    algo: Option<CompressAlgorithm>,
    decompressor: Option<(DecompressDecoder, usize)>,
    path: Option<String>,
}

impl Decompressor {
    pub fn try_create(ctx: Arc<LoadContext>, algo: Option<CompressAlgorithm>) -> Result<Self> {
        Ok(Decompressor {
            ctx,
            algo,
            path: None,
            decompressor: None,
        })
    }

    fn new_file(&mut self, path: String) {
        assert!(self.decompressor.is_none());
        let algo = if let Some(algo) = &self.algo {
            Some(algo.to_owned())
        } else {
            CompressAlgorithm::from_path(&path)
        };
        self.path = Some(path);

        if let Some(algo) = algo {
            let decompressor = DecompressDecoder::new(algo);
            self.decompressor = Some((decompressor, 0));
        } else {
            self.decompressor = None;
        }
    }
}

impl AccumulatingTransform for Decompressor {
    const NAME: &'static str = "Decompressor";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let batch = data
            .get_owned_meta()
            .and_then(BytesBatch::downcast_from)
            .unwrap();
        match &self.path {
            None => self.new_file(batch.path.clone()),
            Some(path) => {
                if path != &batch.path {
                    self.new_file(batch.path.clone())
                }
            }
        }

        if let Some((de, offset)) = &mut self.decompressor {
            let mut data = de.decompress_batch(&batch.data)?;
            if batch.is_eof {
                let mut end = de.decompress_batch(&[])?;
                data.append(&mut end);
                let state = de.state();
                if !matches!(state, DecompressState::Done) {
                    return Err(ErrorCode::BadBytes(format!(
                        "decompressor state is {:?} after decompressing all data",
                        state
                    )));
                }
            }
            let new_batch = Box::new(BytesBatch {
                data,
                path: batch.path.clone(),
                offset: *offset,
                is_eof: batch.is_eof,
            });
            *offset += batch.data.len();
            if batch.is_eof {
                self.decompressor = None;
            }
            Ok(vec![DataBlock::empty_with_meta(new_batch)])
        } else {
            Ok(vec![DataBlock::empty_with_meta(Box::new(batch))])
        }
    }
}
