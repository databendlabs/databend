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

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_compress::CompressAlgorithm;
use databend_common_compress::DecompressDecoder;
use databend_common_compress::DecompressState;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;

use crate::read::load_context::LoadContext;
use crate::read::row_based::batch::BytesBatch;

// Bound decompressed BytesBatch output. The upstream BytesReader is controlled by
// input_read_buffer_size; this size controls the row-based pipeline batch after
// inflation so separators and block builders can start earlier.
const DECOMPRESS_BUF_SIZE: usize = 1024 * 1024;

pub struct Decompressor {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,
    core: DecompressorCore,
}

impl Decompressor {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        _ctx: Arc<LoadContext>,
        algo: Option<CompressAlgorithm>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(Decompressor {
            input,
            output,
            input_data: None,
            output_data: None,
            core: DecompressorCore::create(algo, DECOMPRESS_BUF_SIZE),
        })))
    }

    #[cfg(test)]
    fn create_for_test(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        algo: Option<CompressAlgorithm>,
        output_batch_size: usize,
    ) -> Self {
        Self {
            input,
            output,
            input_data: None,
            output_data: None,
            core: DecompressorCore::create(algo, output_batch_size),
        }
    }
}

#[async_trait::async_trait]
impl Processor for Decompressor {
    fn name(&self) -> String {
        "Decompressor".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.core.has_output() || self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_data.take() {
            let batch = data_block
                .get_owned_meta()
                .and_then(BytesBatch::downcast_from)
                .unwrap();
            self.core.accept_input(batch)?;
        }

        if self.output_data.is_none() {
            self.output_data = self.core.pop_output()?;
        }

        Ok(())
    }
}

struct DecompressorCore {
    algo: Option<CompressAlgorithm>,
    current_algo: Option<CompressAlgorithm>,
    decoder: Option<DecompressDecoder>,
    path: Option<String>,
    output_batch_size: usize,
    output_offset: usize,
    pending_outputs: VecDeque<DataBlock>,
    zip_buf: Vec<u8>,
    eof_pending: bool,
    xz_padding_len_modulo: usize,
    file_content_key: Option<String>,
    file_last_modified: Option<DateTime<Utc>>,
}

impl DecompressorCore {
    fn create(algo: Option<CompressAlgorithm>, output_batch_size: usize) -> Self {
        Self {
            algo,
            current_algo: None,
            decoder: None,
            path: None,
            output_batch_size: output_batch_size.max(1),
            output_offset: 0,
            pending_outputs: VecDeque::new(),
            zip_buf: Vec::new(),
            eof_pending: false,
            xz_padding_len_modulo: 0,
            file_content_key: None,
            file_last_modified: None,
        }
    }

    fn accept_input(&mut self, batch: BytesBatch) -> Result<()> {
        if self.path.as_deref() != Some(batch.path.as_str()) {
            self.new_file(&batch)?;
        }

        match self.current_algo {
            None => {
                let is_eof = batch.is_eof;
                self.pending_outputs
                    .push_back(DataBlock::empty_with_meta(Box::new(batch)));
                if is_eof {
                    self.reset_file();
                }
            }
            Some(CompressAlgorithm::Zip) => self.accept_zip_input(batch)?,
            Some(_) => self.accept_compressed_input(batch)?,
        }

        Ok(())
    }

    fn has_output(&self) -> bool {
        if !self.pending_outputs.is_empty() {
            return true;
        }

        let Some(decoder) = &self.decoder else {
            return false;
        };

        matches!(
            decoder.state(),
            DecompressState::Decoding | DecompressState::Flushing
        ) || self.eof_pending
    }

    fn pop_output(&mut self) -> Result<Option<DataBlock>> {
        if let Some(block) = self.pending_outputs.pop_front() {
            return Ok(Some(block));
        }

        if self.decoder.is_none() {
            return Ok(None);
        }

        self.drain_decoder_output()
    }

    fn new_file(&mut self, batch: &BytesBatch) -> Result<()> {
        if self.decoder.is_some()
            || self.eof_pending
            || !self.zip_buf.is_empty()
            || !self.pending_outputs.is_empty()
        {
            return Err(ErrorCode::Internal(format!(
                "decompressor starts file {} before finishing previous file {:?}",
                batch.path, self.path
            )));
        }

        let algo = self
            .algo
            .or_else(|| CompressAlgorithm::from_path(&batch.path));
        self.path = Some(batch.path.clone());
        self.current_algo = algo;
        self.output_offset = 0;
        self.file_content_key = batch.content_key.clone();
        self.file_last_modified = batch.last_modified;

        if let Some(algo) = algo {
            if matches!(algo, CompressAlgorithm::Zip) {
                self.zip_buf.clear();
            } else {
                self.decoder = Some(DecompressDecoder::new(algo));
            }
        }

        Ok(())
    }

    fn accept_zip_input(&mut self, batch: BytesBatch) -> Result<()> {
        let memory_limit = GLOBAL_MEM_STAT.get_limit() as usize;
        if memory_limit > 0 && self.zip_buf.len() + batch.data.len() > memory_limit / 3 {
            return Err(ErrorCode::BadBytes(format!(
                "zip file {} is larger than memory_limit/3 ({})",
                batch.path,
                memory_limit / 3
            )));
        }

        self.zip_buf.extend_from_slice(&batch.data);
        if batch.is_eof {
            let bytes =
                DecompressDecoder::decompress_all_zip(&self.zip_buf, &batch.path, memory_limit)?;
            self.enqueue_bytes(batch.path, bytes, true);
            self.zip_buf.clear();
            self.reset_file();
        }

        Ok(())
    }

    fn accept_compressed_input(&mut self, batch: BytesBatch) -> Result<()> {
        let Some(decoder_state) = self.decoder.as_ref().map(|decoder| decoder.state()) else {
            return Err(ErrorCode::Internal(format!(
                "missing decompressor for compressed file {}",
                batch.path
            )));
        };

        // Self-describing streams (gzip/zstd/bz2/...) end at their own trailer, so the
        // decoder can reach `Done` on a data batch that is not yet marked as EOF. This
        // is the normal shape for streaming load, which appends a separate empty batch
        // to signal EOF. XZ may additionally have zero-byte Stream Padding after Done.
        if matches!(decoder_state, DecompressState::Done) {
            self.accept_stream_tail(&batch.data, &batch.path)?;
            self.eof_pending |= batch.is_eof;
            if self.eof_pending {
                self.validate_stream_tail(&batch.path)?;
            }
            return Ok(());
        }

        if !matches!(decoder_state, DecompressState::Reading) {
            return Err(ErrorCode::Internal(format!(
                "decompressor expects output to be drained before accepting more data for {}",
                batch.path
            )));
        }

        self.decoder.as_mut().unwrap().fill(&batch.data);
        self.eof_pending |= batch.is_eof;
        Ok(())
    }

    fn drain_decoder_output(&mut self) -> Result<Option<DataBlock>> {
        let path = self.path.clone();
        let mut data = Vec::with_capacity(self.output_batch_size);

        loop {
            if data.len() >= self.output_batch_size {
                break;
            }

            let state = self.decoder.as_ref().unwrap().state();
            match state {
                DecompressState::Reading => {
                    if self.eof_pending {
                        self.decoder.as_mut().unwrap().fill(&[]);
                    } else {
                        break;
                    }
                }
                DecompressState::Decoding => {
                    let remaining = self.output_batch_size - data.len();
                    let mut buf = vec![0u8; remaining.min(DECOMPRESS_BUF_SIZE)];
                    let before = self.decoder.as_ref().unwrap().state();
                    let (consumed, written) = self
                        .decoder
                        .as_mut()
                        .unwrap()
                        .decode_with_consumed(&mut buf)
                        .map_err(|e| Self::compression_error(path.as_ref(), e))?;
                    let after = self.decoder.as_ref().unwrap().state();
                    if consumed == 0 && written == 0 && before == after {
                        return Err(ErrorCode::Internal(format!(
                            "decompressor made no progress while decoding file {}",
                            path.as_deref().unwrap_or("")
                        )));
                    }
                    data.extend_from_slice(&buf[..written]);
                }
                DecompressState::Flushing => {
                    let remaining = self.output_batch_size - data.len();
                    let mut buf = vec![0u8; remaining.min(DECOMPRESS_BUF_SIZE)];
                    let written = self
                        .decoder
                        .as_mut()
                        .unwrap()
                        .finish(&mut buf)
                        .map_err(|e| Self::compression_error(path.as_ref(), e))?;
                    data.extend_from_slice(&buf[..written]);
                }
                DecompressState::Done => break,
            }
        }

        let is_done = matches!(
            self.decoder.as_ref().map(|decoder| decoder.state()),
            Some(DecompressState::Done)
        );
        if is_done {
            let buffered = self.decoder.as_mut().unwrap().take_buffered_input();
            self.accept_stream_tail(&buffered, path.as_deref().unwrap_or(""))?;
            if self.eof_pending {
                self.validate_stream_tail(path.as_deref().unwrap_or(""))?;
            }
        }
        // A self-describing stream can be fully decoded before the input side signals
        // EOF, so the file is only complete once both happened. Until then the file
        // stays open and the EOF marker is forwarded by a later output batch.
        let is_eof = is_done && self.eof_pending;
        if data.is_empty() && !is_eof {
            return Ok(None);
        }

        let block = self.output_batch(path.unwrap_or_default(), data, is_eof);
        if is_eof {
            self.reset_file();
        }

        Ok(Some(block))
    }

    fn accept_stream_tail(&mut self, data: &[u8], path: &str) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        if matches!(self.current_algo, Some(CompressAlgorithm::Xz)) {
            if data.iter().any(|byte| *byte != 0) {
                return Err(ErrorCode::BadBytes(format!(
                    "invalid XZ Stream Padding after compressed stream of file {path}: padding contains non-zero bytes"
                )));
            }
            self.xz_padding_len_modulo = (self.xz_padding_len_modulo + data.len() % 4) % 4;
            return Ok(());
        }

        Err(ErrorCode::BadBytes(format!(
            "got {} more bytes after the compressed stream of file {path} already ended",
            data.len()
        )))
    }

    fn validate_stream_tail(&self, path: &str) -> Result<()> {
        if matches!(self.current_algo, Some(CompressAlgorithm::Xz))
            && self.xz_padding_len_modulo != 0
        {
            return Err(ErrorCode::BadBytes(format!(
                "invalid XZ Stream Padding after compressed stream of file {path}: padding length must be a multiple of 4 bytes"
            )));
        }
        Ok(())
    }

    fn enqueue_bytes(&mut self, path: String, bytes: Vec<u8>, is_eof: bool) {
        if bytes.is_empty() {
            let block = self.output_batch(path, Vec::new(), is_eof);
            self.pending_outputs.push_back(block);
            return;
        }

        let mut chunks = bytes.chunks(self.output_batch_size).peekable();
        while let Some(chunk) = chunks.next() {
            let chunk_is_eof = is_eof && chunks.peek().is_none();
            let block = self.output_batch(path.clone(), chunk.to_vec(), chunk_is_eof);
            self.pending_outputs.push_back(block);
        }
    }

    fn output_batch(&mut self, path: String, data: Vec<u8>, is_eof: bool) -> DataBlock {
        let offset = self.output_offset;
        self.output_offset += data.len();
        DataBlock::empty_with_meta(Box::new(BytesBatch {
            data,
            path,
            offset,
            is_eof,
            content_key: self.file_content_key.clone(),
            last_modified: self.file_last_modified,
        }))
    }

    fn reset_file(&mut self) {
        self.current_algo = None;
        self.decoder = None;
        self.path = None;
        self.output_offset = 0;
        self.eof_pending = false;
        self.xz_padding_len_modulo = 0;
        self.file_content_key = None;
        self.file_last_modified = None;
    }

    fn compression_error(path: Option<&String>, err: std::io::Error) -> ErrorCode {
        let err = ErrorCode::InvalidCompressionData(format!("compression data invalid: {err}"));
        if let Some(path) = path {
            err.add_detail_back(format!("file path: {path}"))
        } else {
            err
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use databend_common_compress::CompressCodec;
    use databend_common_pipeline::core::port::connect;

    use super::*;

    const PROCESSOR_OUTPUT_SIZE: usize = 128 * 1024;
    const PROCESSOR_TEST_PATH: &str = "processor-test.data";

    fn bytes_batch(data: Vec<u8>, is_eof: bool) -> BytesBatch {
        bytes_batch_with_path(data, is_eof, "test.gz")
    }

    fn bytes_batch_with_path(data: Vec<u8>, is_eof: bool, path: &str) -> BytesBatch {
        BytesBatch {
            data,
            path: path.to_string(),
            offset: 0,
            is_eof,
            content_key: Some("etag".to_string()),
            last_modified: Some(Utc.with_ymd_and_hms(2026, 7, 27, 1, 2, 3).unwrap()),
        }
    }

    fn compress(algo: CompressAlgorithm, data: &[u8]) -> Result<Vec<u8>> {
        let mut encoder = CompressCodec::from(algo);
        encoder.compress_all(data)
    }

    fn collect_outputs(core: &mut DecompressorCore, batch: BytesBatch) -> Result<Vec<BytesBatch>> {
        core.accept_input(batch)?;
        let mut outputs = Vec::new();
        while core.has_output() {
            let block = core.pop_output()?.unwrap();
            let batch = block
                .get_owned_meta()
                .and_then(BytesBatch::downcast_from)
                .unwrap();
            outputs.push(batch);
        }
        Ok(outputs)
    }

    fn original_bytes(len: usize) -> Vec<u8> {
        // Deterministic non-trivial payload: stable across runs, but less codec-special
        // than all-zero or single-byte repeated data.
        (0..len)
            .map(|i| ((i * 131 + i / 7 + 17) % 251) as u8)
            .collect()
    }

    fn block_from_batch(batch: BytesBatch) -> DataBlock {
        DataBlock::empty_with_meta(Box::new(batch))
    }

    fn batch_from_block(block: DataBlock) -> BytesBatch {
        block
            .get_owned_meta()
            .and_then(BytesBatch::downcast_from)
            .unwrap()
    }

    fn run_decompressor_processor(
        algo: CompressAlgorithm,
        output_batch_size: usize,
        inputs: Vec<BytesBatch>,
    ) -> Result<(Vec<BytesBatch>, Vec<usize>)> {
        let input = InputPort::create();
        let output = OutputPort::create();
        let mut processor = Decompressor::create_for_test(
            input.clone(),
            output.clone(),
            Some(algo),
            output_batch_size,
        );

        let upstream = OutputPort::create();
        let downstream = InputPort::create();
        unsafe {
            connect(&input, &upstream);
            connect(&downstream, &output);
        }
        downstream.set_need_data();

        let mut inputs = VecDeque::from(inputs);
        let mut outputs = Vec::new();
        let mut output_counts_before_input_push = Vec::new();
        let mut finished_upstream = false;
        let mut iterations = 0;

        loop {
            iterations += 1;
            assert!(
                iterations < 10_000,
                "decompressor processor event loop did not finish"
            );

            match processor.event()? {
                Event::Sync => processor.process()?,
                Event::NeedConsume => {
                    if downstream.has_data() {
                        let block = downstream.pull_data().unwrap()?;
                        outputs.push(batch_from_block(block));
                    }
                    downstream.set_need_data();
                }
                Event::NeedData => {
                    if let Some(batch) = inputs.pop_front() {
                        assert!(input.is_need_data());
                        output_counts_before_input_push.push(outputs.len());
                        upstream.push_data(Ok(block_from_batch(batch)));
                    } else if !finished_upstream {
                        upstream.finish();
                        finished_upstream = true;
                    }
                }
                Event::Finished => break,
                Event::Async => unreachable!("Decompressor is a synchronous processor"),
            }
        }

        Ok((outputs, output_counts_before_input_push))
    }

    fn assert_processor_outputs(
        outputs: &[BytesBatch],
        original: &[u8],
        output_batch_size: usize,
        expect_multiple_outputs: bool,
    ) {
        if expect_multiple_outputs {
            assert!(
                outputs.len() > 1,
                "expected multiple output batches, got {}",
                outputs.len()
            );
        } else {
            assert_eq!(outputs.len(), 1);
        }

        let mut offset = 0;
        for (index, batch) in outputs.iter().enumerate() {
            let is_last = index + 1 == outputs.len();
            assert_eq!(batch.path, PROCESSOR_TEST_PATH);
            assert_eq!(batch.content_key.as_deref(), Some("etag"));
            assert!(batch.last_modified.is_some());
            assert_eq!(batch.offset, offset);
            assert_eq!(batch.is_eof, is_last);
            if !is_last {
                assert!(
                    batch.data.len() <= output_batch_size,
                    "non-final output length {} exceeds bound {}",
                    batch.data.len(),
                    output_batch_size
                );
            }
            offset += batch.data.len();
        }

        let decompressed = outputs
            .iter()
            .flat_map(|batch| batch.data.iter().copied())
            .collect::<Vec<_>>();
        assert_eq!(decompressed, original);
    }

    fn run_processor_single_input_case(
        algo: CompressAlgorithm,
        original: Vec<u8>,
        output_batch_size: usize,
        expect_multiple_outputs: bool,
    ) -> Result<()> {
        let compressed = compress(algo, &original)?;
        let inputs = vec![bytes_batch_with_path(compressed, true, PROCESSOR_TEST_PATH)];
        let (outputs, output_counts_before_input_push) =
            run_decompressor_processor(algo, output_batch_size, inputs)?;
        assert_eq!(output_counts_before_input_push, vec![0]);
        assert_processor_outputs(
            &outputs,
            &original,
            output_batch_size,
            expect_multiple_outputs,
        );
        Ok(())
    }

    #[test]
    fn test_decompressor_processor_single_input_boundaries() -> Result<()> {
        // These cases exercise the real Processor event loop while keeping the
        // input shape simple: normal split with tail, exact chunk boundary, and
        // an empty compressed stream that should still emit one EOF batch.
        let cases = [
            (
                "non_empty_tail",
                PROCESSOR_OUTPUT_SIZE * 3 + PROCESSOR_OUTPUT_SIZE / 3,
                true,
            ),
            ("exact_boundary", PROCESSOR_OUTPUT_SIZE * 3, true),
            ("empty", 0, false),
        ];

        for algo in [CompressAlgorithm::Gzip, CompressAlgorithm::Zstd] {
            for (name, original_size, expect_multiple_outputs) in cases {
                let original = original_bytes(original_size);
                run_processor_single_input_case(
                    algo,
                    original,
                    PROCESSOR_OUTPUT_SIZE,
                    expect_multiple_outputs,
                )
                .map_err(|e| {
                    e.add_detail_back(format!(
                        "processor boundary case {name} failed for {algo:?}"
                    ))
                })?;
            }
        }
        Ok(())
    }

    #[test]
    fn test_decompressor_processor_separate_empty_eof_batch() -> Result<()> {
        // Shape produced by streaming load: the whole compressed stream arrives in a
        // batch that is not marked as EOF, followed by an empty batch that only carries
        // the EOF marker. Self-describing streams reach `Done` on the first batch, so
        // the decompressor must keep the file open until that marker shows up.
        for algo in [
            CompressAlgorithm::Gzip,
            CompressAlgorithm::Zstd,
            CompressAlgorithm::Bz2,
        ] {
            for original_size in [0, PROCESSOR_OUTPUT_SIZE * 2 + 7] {
                let original = original_bytes(original_size);
                let compressed = compress(algo, &original)?;
                let inputs = vec![
                    bytes_batch_with_path(compressed, false, PROCESSOR_TEST_PATH),
                    bytes_batch_with_path(Vec::new(), true, PROCESSOR_TEST_PATH),
                ];

                let (outputs, _) = run_decompressor_processor(algo, PROCESSOR_OUTPUT_SIZE, inputs)
                    .map_err(|e| {
                        e.add_detail_back(format!(
                            "separate eof batch failed for {algo:?} with {original_size} bytes"
                        ))
                    })?;

                let decompressed = outputs
                    .iter()
                    .flat_map(|batch| batch.data.iter().copied())
                    .collect::<Vec<_>>();
                assert_eq!(
                    decompressed, original,
                    "payload mismatch for {algo:?} with {original_size} bytes"
                );
                assert!(
                    outputs.last().unwrap().is_eof,
                    "last output must carry EOF for {algo:?} with {original_size} bytes"
                );
                assert_eq!(
                    outputs.iter().filter(|batch| batch.is_eof).count(),
                    1,
                    "exactly one EOF output expected for {algo:?} with {original_size} bytes"
                );

                let mut offset = 0;
                for batch in &outputs {
                    assert_eq!(batch.path, PROCESSOR_TEST_PATH);
                    assert_eq!(batch.offset, offset);
                    offset += batch.data.len();
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_decompressor_rejects_data_after_stream_end() -> Result<()> {
        // Trailing bytes are rejected regardless of whether the input reader puts
        // them in the stream's final batch or in a following batch.
        let original = original_bytes(64);
        let compressed = compress(CompressAlgorithm::Gzip, &original)?;

        let mut compressed_with_trailing = compressed.clone();
        compressed_with_trailing.extend_from_slice(b"trailing");
        let mut core =
            DecompressorCore::create(Some(CompressAlgorithm::Gzip), PROCESSOR_OUTPUT_SIZE);
        let err = collect_outputs(
            &mut core,
            bytes_batch_with_path(compressed_with_trailing, true, PROCESSOR_TEST_PATH),
        )
        .expect_err("trailing bytes in the final input batch must fail");
        assert!(
            err.message().contains("more bytes"),
            "unexpected error: {err}"
        );

        let mut core =
            DecompressorCore::create(Some(CompressAlgorithm::Gzip), PROCESSOR_OUTPUT_SIZE);

        core.accept_input(bytes_batch_with_path(
            compressed,
            false,
            PROCESSOR_TEST_PATH,
        ))?;
        while core.has_output() {
            assert!(core.pop_output()?.is_some());
        }

        let err = core
            .accept_input(bytes_batch_with_path(
                b"trailing".to_vec(),
                true,
                PROCESSOR_TEST_PATH,
            ))
            .expect_err("trailing bytes after stream end must fail");
        assert!(
            err.message().contains("already ended"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    fn test_decompressor_accepts_valid_xz_stream_padding() -> Result<()> {
        let original = original_bytes(PROCESSOR_OUTPUT_SIZE * 2 + 7);
        let compressed = compress(CompressAlgorithm::Xz, &original)?;

        let mut padded = compressed.clone();
        padded.extend_from_slice(&[0; 4]);
        let input_cases = [
            vec![bytes_batch_with_path(padded, true, PROCESSOR_TEST_PATH)],
            vec![
                bytes_batch_with_path(compressed.clone(), false, PROCESSOR_TEST_PATH),
                bytes_batch_with_path(vec![0; 2], false, PROCESSOR_TEST_PATH),
                bytes_batch_with_path(vec![0; 6], true, PROCESSOR_TEST_PATH),
            ],
        ];

        for inputs in input_cases {
            let (outputs, _) =
                run_decompressor_processor(CompressAlgorithm::Xz, PROCESSOR_OUTPUT_SIZE, inputs)?;
            assert_processor_outputs(&outputs, &original, PROCESSOR_OUTPUT_SIZE, true);
        }

        for invalid_padding in [vec![0; 2], vec![0, 0, 0, 1]] {
            let mut invalid = compressed.clone();
            invalid.extend_from_slice(&invalid_padding);
            let mut core =
                DecompressorCore::create(Some(CompressAlgorithm::Xz), PROCESSOR_OUTPUT_SIZE);
            let err = collect_outputs(
                &mut core,
                bytes_batch_with_path(invalid, true, PROCESSOR_TEST_PATH),
            )
            .expect_err("invalid XZ Stream Padding must fail");
            assert!(
                err.message().contains("XZ Stream Padding"),
                "unexpected error: {err}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_decompressor_processor_drains_before_next_input() -> Result<()> {
        // Feed a file as two compressed input batches. The processor should
        // drain all available decompressed chunks from the first input before it
        // requests and accepts the second input batch.
        for algo in [CompressAlgorithm::Gzip, CompressAlgorithm::Zstd] {
            let original = original_bytes(PROCESSOR_OUTPUT_SIZE * 4 + PROCESSOR_OUTPUT_SIZE / 5);
            let compressed = compress(algo, &original)?;
            let split_at = compressed.len().saturating_sub(32).max(1);
            let first = compressed[..split_at].to_vec();
            let second = compressed[split_at..].to_vec();
            let inputs = vec![
                bytes_batch_with_path(first, false, PROCESSOR_TEST_PATH),
                bytes_batch_with_path(second, true, PROCESSOR_TEST_PATH),
            ];

            let (outputs, output_counts_before_input_push) =
                run_decompressor_processor(algo, PROCESSOR_OUTPUT_SIZE, inputs)?;
            assert_eq!(output_counts_before_input_push.len(), 2);
            assert_eq!(output_counts_before_input_push[0], 0);
            assert!(
                output_counts_before_input_push[1] > 1,
                "expected first compressed input to be drained into multiple outputs before the second input is requested, got {:?}",
                output_counts_before_input_push
            );
            assert_processor_outputs(&outputs, &original, PROCESSOR_OUTPUT_SIZE, true);
        }
        Ok(())
    }

    #[test]
    fn test_decompressor_preserves_metadata_and_offsets() -> Result<()> {
        let original = b"{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n".to_vec();
        let compressed = compress(CompressAlgorithm::Gzip, &original)?;
        let mut core = DecompressorCore::create(Some(CompressAlgorithm::Gzip), 8);

        let outputs = collect_outputs(&mut core, bytes_batch(compressed, true))?;
        assert!(outputs.len() > 1);
        let mut offset = 0;
        for batch in &outputs {
            assert_eq!(batch.path, "test.gz");
            assert_eq!(batch.content_key.as_deref(), Some("etag"));
            assert!(batch.last_modified.is_some());
            assert_eq!(batch.offset, offset);
            offset += batch.data.len();
        }

        assert_eq!(offset, original.len());
        assert!(outputs.last().unwrap().is_eof);
        assert!(
            outputs[..outputs.len() - 1]
                .iter()
                .all(|batch| batch.data.len() <= 8 && !batch.is_eof)
        );
        Ok(())
    }

    #[test]
    fn test_decompressor_output_can_split_ndjson_rows() -> Result<()> {
        let original = b"{\"long\":\"first row\"}\n{\"long\":\"second row\"}\n".to_vec();
        let compressed = compress(CompressAlgorithm::Gzip, &original)?;
        let mut core = DecompressorCore::create(Some(CompressAlgorithm::Gzip), 5);

        let outputs = collect_outputs(&mut core, bytes_batch(compressed, true))?;
        assert!(
            outputs
                .iter()
                .take(outputs.len() - 1)
                .any(|batch| batch.data.last() != Some(&b'\n'))
        );

        let decompressed = outputs
            .iter()
            .flat_map(|batch| batch.data.iter().copied())
            .collect::<Vec<_>>();
        assert_eq!(decompressed, original);
        Ok(())
    }

    #[test]
    fn test_decompressor_splits_zip_output_after_full_inflate() -> Result<()> {
        let original = b"first\nsecond\nthird\nfourth\n".to_vec();
        let compressed = CompressCodec::compress_all_zip(&original, "data.ndjson")?;
        let mut core = DecompressorCore::create(Some(CompressAlgorithm::Zip), 7);

        let outputs = collect_outputs(&mut core, bytes_batch(compressed, true))?;
        assert!(outputs.len() > 1);
        assert!(
            outputs[..outputs.len() - 1]
                .iter()
                .all(|batch| batch.data.len() <= 7 && !batch.is_eof)
        );
        assert!(outputs.last().unwrap().is_eof);

        let decompressed = outputs
            .iter()
            .flat_map(|batch| batch.data.iter().copied())
            .collect::<Vec<_>>();
        assert_eq!(decompressed, original);
        Ok(())
    }
}
