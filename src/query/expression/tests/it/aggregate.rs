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

#![allow(clippy::arc_with_non_send_sync)]

use std::alloc::Layout;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicIsize;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use bumpalo::Bump;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateType;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PayloadFlushState;
use databend_common_expression::ProbeState;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::SerializedPayload;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::aggregate::aggregate_function::*;
use databend_common_expression::block_debug::assert_block_value_sort_eq;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::UInt64Type;

const ROWS: usize = 4096;
const GROUPS: usize = 4;
const BYTES_PER_ROW: usize = 64;
const FAIL_ON_ACCUMULATE: u64 = u64::MAX - 1;
const FAIL_ON_BATCH_MERGE: u64 = u64::MAX - 2;
const FAIL_ON_MERGE: u64 = u64::MAX;
const FAIL_ON_MERGE_RESULT: u64 = u64::MAX - 3;
const FAIL_ON_SERIALIZE: u64 = u64::MAX - 4;

struct TrackedHeapAggregateFunction {
    live_bytes: Arc<AtomicIsize>,
    drop_count: Arc<AtomicUsize>,
    bytes_per_row: usize,
}

struct TrackedHeapState {
    bytes: Vec<u8>,
    failures: TrackedHeapFailures,
    live_bytes: Arc<AtomicIsize>,
    drop_count: Arc<AtomicUsize>,
}

#[derive(Clone, Copy, Default)]
struct TrackedHeapFailures {
    merge: bool,
    merge_result: bool,
    serialize: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TrackedHeapFailureTrigger {
    None,
    Accumulate,
    BatchMerge,
    Merge,
    MergeResult,
    Serialize,
}

impl TrackedHeapFailures {
    fn record(&mut self, trigger: TrackedHeapFailureTrigger) {
        match trigger {
            TrackedHeapFailureTrigger::None
            | TrackedHeapFailureTrigger::Accumulate
            | TrackedHeapFailureTrigger::BatchMerge => {}
            TrackedHeapFailureTrigger::Merge => self.merge = true,
            TrackedHeapFailureTrigger::MergeResult => self.merge_result = true,
            TrackedHeapFailureTrigger::Serialize => self.serialize = true,
        }
    }

    fn merge_from(&mut self, rhs: TrackedHeapFailures) {
        self.merge |= rhs.merge;
        self.merge_result |= rhs.merge_result;
        self.serialize |= rhs.serialize;
    }
}

impl Drop for TrackedHeapState {
    fn drop(&mut self) {
        self.drop_count.fetch_add(1, Ordering::SeqCst);
        self.live_bytes
            .fetch_sub(self.bytes.len() as isize, Ordering::SeqCst);
    }
}

impl TrackedHeapState {
    fn append(&mut self, bytes: usize) {
        self.bytes.extend(std::iter::repeat_n(0xDB, bytes));
        self.live_bytes.fetch_add(bytes as isize, Ordering::SeqCst);
    }

    fn append_row(&mut self, bytes: usize, trigger: TrackedHeapFailureTrigger) -> Result<()> {
        self.append(bytes);
        self.failures.record(trigger);

        if trigger == TrackedHeapFailureTrigger::Accumulate {
            return Err(ErrorCode::Internal(
                "injected tracked_heap accumulate failure",
            ));
        }

        Ok(())
    }
}

impl fmt::Display for TrackedHeapAggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tracked_heap")
    }
}

impl FunctionInstance for TrackedHeapAggregateFunction {
    fn signature(&self) -> &AggregateFunctionSignature {
        static SIGNATURE: std::sync::LazyLock<AggregateFunctionSignature> =
            std::sync::LazyLock::new(|| AggregateFunctionSignature {
                name: "tracked_heap".to_string(),
                params: vec![],
                args_type: vec![],
                distinct: false,
                order_by: vec![],
                return_type: UInt64Type::data_type(),
            });
        &SIGNATURE
    }

    fn features(&self) -> &FunctionFeatures {
        static FEATURES: FunctionFeatures = FunctionFeatures {
            is_decomposable: false,
            sort_policy: databend_common_expression::aggregate::aggregate_function::SortPolicy::Unsupported,
            distinct_policy: databend_common_expression::aggregate::aggregate_function::DistinctPolicy::Unsupported,
            category: "",
            description: "",
            definition: "",
            example: "",
        };
        &FEATURES
    }

    fn state(&self) -> &AggregateStateDescription {
        static STATE: std::sync::LazyLock<AggregateStateDescription> =
            std::sync::LazyLock::new(|| {
                AggregateStateDescription::new(
                    vec![AggrStateType::Custom(Layout::new::<TrackedHeapState>())],
                    vec![StateSerdeItem::DataType(UInt64Type::data_type())],
                )
                .with_manual_drop(true)
            });
        &STATE
    }

    fn init_state(&self, state: AggrState) {
        let live_bytes = self.live_bytes.clone();
        let drop_count = self.drop_count.clone();
        state.write(|| TrackedHeapState {
            bytes: Vec::new(),
            failures: TrackedHeapFailures::default(),
            live_bytes,
            drop_count,
        });
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let state = input.state.get::<TrackedHeapState>();
        for row in 0..input.columns.num_rows() {
            state.append_row(
                self.bytes_per_row,
                tracked_heap_row_trigger(input.columns, row),
            )?;
        }
        Ok(())
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            state.get::<TrackedHeapState>().append_row(
                self.bytes_per_row,
                tracked_heap_row_trigger(input.columns, row),
            )?;
        }
        Ok(())
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        input.state.get::<TrackedHeapState>().append_row(
            self.bytes_per_row,
            tracked_heap_row_trigger(input.columns, input.row),
        )
    }

    fn accumulate_row_count(&self, _input: AccumulateRowCountInput<'_>) -> Result<()> {
        Ok(())
    }

    fn accumulate_row_count_keys(&self, _input: AccumulateRowCountKeysInput<'_>) -> Result<()> {
        Ok(())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        let builder = &mut input.builders[0];
        for state in input.states.iter() {
            let state = state.get::<TrackedHeapState>();
            if state.failures.serialize {
                return Err(ErrorCode::Internal(
                    "injected tracked_heap serialize failure",
                ));
            }
            builder.push(ScalarRef::Number(NumberScalar::UInt64(
                state.bytes.len() as u64
            )));
        }
        Ok(())
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            let target = state.get::<TrackedHeapState>();
            let trigger = tracked_heap_trigger_from_entry(input.state, row);
            let bytes = match trigger {
                TrackedHeapFailureTrigger::BatchMerge => self.bytes_per_row,
                _ => tracked_heap_bytes_from_scalar(unsafe { input.state.index_unchecked(row) })
                    .unwrap_or(0),
            };
            target.append(bytes);

            if trigger == TrackedHeapFailureTrigger::BatchMerge {
                return Err(ErrorCode::Internal(
                    "injected tracked_heap batch_merge failure",
                ));
            }
        }
        Ok(())
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        let state = input.state.get::<TrackedHeapState>();
        let rhs = input.rhs.get::<TrackedHeapState>();
        state.bytes.extend_from_slice(&rhs.bytes);
        state
            .live_bytes
            .fetch_add(rhs.bytes.len() as isize, Ordering::SeqCst);
        state.failures.merge_from(rhs.failures);

        if rhs.failures.merge {
            return Err(ErrorCode::Internal("injected tracked_heap merge failure"));
        }

        Ok(())
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        let state = input.state.get::<TrackedHeapState>();
        if state.failures.merge_result {
            return Err(ErrorCode::Internal(
                "injected tracked_heap merge_result failure",
            ));
        }
        input.builder.push(ScalarRef::Number(NumberScalar::UInt64(
            state.bytes.len() as u64
        )));
        Ok(())
    }

    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        self.merge_result(input)
    }

    unsafe fn drop_state(&self, state: AggrState) {
        let state = state.get::<TrackedHeapState>();
        unsafe { std::ptr::drop_in_place(state) };
    }
}

fn tracked_heap_row_trigger(columns: ProjectedBlock, row: usize) -> TrackedHeapFailureTrigger {
    if columns.is_empty() {
        return TrackedHeapFailureTrigger::None;
    }

    tracked_heap_trigger_from_scalar(unsafe { columns[0].index_unchecked(row) })
}

fn tracked_heap_trigger_from_entry(entry: &BlockEntry, row: usize) -> TrackedHeapFailureTrigger {
    tracked_heap_trigger_from_scalar(unsafe { entry.index_unchecked(row) })
}

fn tracked_heap_trigger_from_scalar(scalar: ScalarRef) -> TrackedHeapFailureTrigger {
    match scalar {
        ScalarRef::Number(NumberScalar::UInt64(FAIL_ON_ACCUMULATE)) => {
            TrackedHeapFailureTrigger::Accumulate
        }
        ScalarRef::Number(NumberScalar::UInt64(FAIL_ON_BATCH_MERGE)) => {
            TrackedHeapFailureTrigger::BatchMerge
        }
        ScalarRef::Number(NumberScalar::UInt64(FAIL_ON_MERGE)) => TrackedHeapFailureTrigger::Merge,
        ScalarRef::Number(NumberScalar::UInt64(FAIL_ON_MERGE_RESULT)) => {
            TrackedHeapFailureTrigger::MergeResult
        }
        ScalarRef::Number(NumberScalar::UInt64(FAIL_ON_SERIALIZE)) => {
            TrackedHeapFailureTrigger::Serialize
        }
        ScalarRef::Tuple(fields) => fields
            .into_iter()
            .next()
            .map(tracked_heap_trigger_from_scalar)
            .unwrap_or(TrackedHeapFailureTrigger::None),
        _ => TrackedHeapFailureTrigger::None,
    }
}

fn tracked_heap_bytes_from_scalar(scalar: ScalarRef) -> Option<usize> {
    match scalar {
        ScalarRef::Number(NumberScalar::UInt64(bytes)) => Some(bytes as usize),
        ScalarRef::Tuple(fields) => fields
            .into_iter()
            .next()
            .and_then(tracked_heap_bytes_from_scalar),
        _ => None,
    }
}

struct TrackedHeapFixture {
    live_bytes: Arc<AtomicIsize>,
    drop_count: Arc<AtomicUsize>,
}

impl TrackedHeapFixture {
    fn new() -> Self {
        Self {
            live_bytes: Arc::new(AtomicIsize::new(0)),
            drop_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn aggrs(&self) -> Vec<AggregateFunctionRef> {
        vec![Arc::new(TrackedHeapAggregateFunction {
            live_bytes: self.live_bytes.clone(),
            drop_count: self.drop_count.clone(),
            bytes_per_row: BYTES_PER_ROW,
        })]
    }

    fn empty_table(&self, config: HashTableConfig) -> AggregateHashTable {
        AggregateHashTable::new(
            vec![Int64Type::data_type()],
            self.aggrs(),
            config,
            Arc::new(Bump::new()),
        )
    }

    fn build_from_rows(&self, marker: u64, config: HashTableConfig) -> AggregateHashTable {
        self.try_build_from_rows(marker, config).unwrap()
    }

    fn try_build_from_rows(
        &self,
        marker: u64,
        config: HashTableConfig,
    ) -> Result<AggregateHashTable> {
        let mut hashtable = self.empty_table(config);
        let mut state = ProbeState::default();
        let params = [vec![Self::heap_markers(marker)]];
        let params: Vec<ProjectedBlock> = params.iter().map(|v| v.into()).collect();
        let group_columns = Self::group_columns();

        hashtable.add_groups(
            &mut state,
            (&group_columns).into(),
            &params,
            (&[]).into(),
            ROWS,
        )?;

        Ok(hashtable)
    }

    fn try_build_from_serialized_states(
        &self,
        marker: u64,
        config: HashTableConfig,
    ) -> Result<AggregateHashTable> {
        let mut hashtable = self.empty_table(config);
        let mut state = ProbeState::default();
        let params: Vec<ProjectedBlock> = vec![];
        let agg_states = [Self::heap_markers(marker)];
        let group_columns = Self::group_columns();

        hashtable.add_groups(
            &mut state,
            (&group_columns).into(),
            &params,
            (&agg_states).into(),
            ROWS,
        )?;

        Ok(hashtable)
    }

    fn assert_result(&self, hashtable: &mut AggregateHashTable) {
        assert_block_value_sort_eq(
            &self.merge_result_block(hashtable),
            &self.expected_result_block(),
        );
    }

    fn merge_result_block(&self, hashtable: &mut AggregateHashTable) -> DataBlock {
        let mut merge_state = PayloadFlushState::default();
        let mut blocks = Vec::new();

        while hashtable.merge_result(&mut merge_state).unwrap() {
            let mut entries = merge_state.take_group_columns();
            entries.extend(merge_state.take_aggregate_results());

            let num_rows = entries[0].len();
            blocks.push(DataBlock::new(entries, num_rows));
        }

        DataBlock::concat(&blocks).unwrap()
    }

    fn expected_result_block(&self) -> DataBlock {
        let count_per_group = ROWS / GROUPS;
        let entries = vec![
            Int64Type::from_data((0..GROUPS).map(|x| x as i64).collect::<Vec<_>>()).into(),
            UInt64Type::from_data(vec![(count_per_group * BYTES_PER_ROW) as u64; GROUPS]).into(),
        ];
        DataBlock::new(entries, GROUPS)
    }

    fn live_bytes(&self) -> isize {
        self.live_bytes.load(Ordering::SeqCst)
    }

    fn drop_count(&self) -> usize {
        self.drop_count.load(Ordering::SeqCst)
    }

    fn assert_all_states_dropped(&self) {
        assert_eq!(self.live_bytes(), 0);
        assert!(self.drop_count() > 0);
    }

    fn group_columns() -> Vec<BlockEntry> {
        vec![
            Int64Type::from_data((0..ROWS).map(|x| (x % GROUPS) as i64).collect::<Vec<_>>()).into(),
        ]
    }

    fn heap_markers(marker: u64) -> BlockEntry {
        UInt64Type::from_data(
            (0..ROWS)
                .map(|row| if row == 0 { marker } else { 0_u64 })
                .collect::<Vec<_>>(),
        )
        .into()
    }
}

#[test]
fn test_partitioned_payload_public_bucket_iterators() {
    let fixture = TrackedHeapFixture::new();

    {
        let hashtable = fixture.build_from_rows(0, HashTableConfig::default());
        let rows = hashtable.len();
        let buckets = hashtable.payload.into_bucket_payloads().collect::<Vec<_>>();
        assert_eq!(buckets.len(), 8);
        assert_eq!(
            buckets
                .iter()
                .map(|(_, payload)| payload.len())
                .sum::<usize>(),
            rows
        );

        let hashtable = fixture.build_from_rows(0, HashTableConfig::default());
        let rows = hashtable.len();
        let non_empty = hashtable
            .payload
            .into_non_empty_bucket_payloads()
            .collect::<Vec<_>>();
        assert!(!non_empty.is_empty());
        assert!(non_empty.len() <= 8);
        assert_eq!(
            non_empty
                .iter()
                .map(|(_, payload)| payload.len())
                .sum::<usize>(),
            rows
        );
    }

    fixture.assert_all_states_dropped();
}

#[test]
fn test_partitioned_payload_scatter_preserves_results() {
    let fixture = TrackedHeapFixture::new();

    {
        let hashtable = fixture.build_from_rows(0, HashTableConfig::default());
        let rows = hashtable.len();
        let scattered = hashtable.payload.scatter_into_buckets(3);
        assert_eq!(scattered.len(), 3);
        assert_eq!(
            scattered.iter().map(|payload| payload.len()).sum::<usize>(),
            rows
        );

        let mut merged = fixture.empty_table(HashTableConfig::default());
        let mut flush_state = PayloadFlushState::default();
        for payload in &scattered {
            merged.combine_payloads(payload, &mut flush_state).unwrap();
        }
        fixture.assert_result(&mut merged);
    }

    fixture.assert_all_states_dropped();
}

#[test]
fn test_serialized_payload_conversions_preserve_results_and_drop_states() {
    let fixture = TrackedHeapFixture::new();

    {
        let hashtable =
            fixture.build_from_rows(0, HashTableConfig::default().with_initial_radix_bits(0));
        let serialized_block = hashtable.payload.aggregate_flush_all().unwrap();
        let serialized = SerializedPayload {
            bucket: 0,
            data_block: serialized_block.clone(),
            max_partition_count: 1,
        };

        let mut restored = serialized
            .convert_to_aggregate_table(
                vec![Int64Type::data_type()],
                fixture.aggrs(),
                1,
                0,
                Arc::new(Bump::new()),
                true,
            )
            .unwrap();
        fixture.assert_result(&mut restored);

        let partitioned = SerializedPayload {
            bucket: 0,
            data_block: serialized_block.clone(),
            max_partition_count: 1,
        }
        .convert_to_partitioned_payload(
            vec![Int64Type::data_type()],
            fixture.aggrs(),
            1,
            0,
            Arc::new(Bump::new()),
        )
        .unwrap();
        let mut merged_partitioned = fixture.empty_table(HashTableConfig::default());
        merged_partitioned
            .combine_payloads(&partitioned, &mut PayloadFlushState::default())
            .unwrap();
        fixture.assert_result(&mut merged_partitioned);

        let single_payload = SerializedPayload {
            bucket: 0,
            data_block: serialized_block.clone(),
            max_partition_count: 1,
        }
        .convert_to_single_payload(
            vec![Int64Type::data_type()],
            fixture.aggrs(),
            1,
            Arc::new(Bump::new()),
        )
        .unwrap();
        let mut merged_single = fixture.empty_table(HashTableConfig::default());
        merged_single
            .combine_payload(&single_payload, &mut PayloadFlushState::default())
            .unwrap();
        fixture.assert_result(&mut merged_single);

        assert!(fixture.live_bytes() > 0);
    }

    fixture.assert_all_states_dropped();
}

#[test]
fn test_tracked_heap_accumulate_failure_drops_states() {
    let fixture = TrackedHeapFixture::new();

    {
        let result = fixture.try_build_from_rows(FAIL_ON_ACCUMULATE, HashTableConfig::default());

        assert!(result.is_err());
    }

    fixture.assert_all_states_dropped();
}

#[test]
fn test_tracked_heap_batch_merge_failure_drops_states() {
    let fixture = TrackedHeapFixture::new();

    {
        let result = fixture
            .try_build_from_serialized_states(FAIL_ON_BATCH_MERGE, HashTableConfig::default());

        assert!(result.is_err());
    }

    fixture.assert_all_states_dropped();
}

#[test]
fn test_tracked_heap_merge_failure_drops_states() {
    let fixture = TrackedHeapFixture::new();
    let rows_before;

    {
        let hashtable = fixture.build_from_rows(FAIL_ON_MERGE, HashTableConfig::default());
        rows_before = hashtable.len();
        let scattered = hashtable.payload.scatter_into_buckets(3);

        let mut merged = fixture.empty_table(HashTableConfig::default());
        let mut flush_state = PayloadFlushState::default();
        let result = scattered
            .iter()
            .try_for_each(|payload| merged.combine_payloads(payload, &mut flush_state));

        assert!(result.is_err());
        assert!(fixture.live_bytes() > (ROWS * BYTES_PER_ROW) as isize);
    }

    assert_eq!(fixture.live_bytes(), 0);
    assert!(fixture.drop_count() > rows_before);
}

#[test]
fn test_tracked_heap_merge_result_failure_drops_states() {
    let fixture = TrackedHeapFixture::new();
    let rows_before;

    {
        let hashtable = fixture.build_from_rows(FAIL_ON_MERGE_RESULT, HashTableConfig::default());
        rows_before = hashtable.len();
        let scattered = hashtable.payload.scatter_into_buckets(3);

        let mut merged = fixture.empty_table(HashTableConfig::default());
        let mut flush_state = PayloadFlushState::default();
        for payload in &scattered {
            merged.combine_payloads(payload, &mut flush_state).unwrap();
        }

        let result = merged.merge_result(&mut PayloadFlushState::default());
        assert!(result.is_err());
        assert!(fixture.live_bytes() > (ROWS * BYTES_PER_ROW) as isize);
    }

    assert_eq!(fixture.live_bytes(), 0);
    assert!(fixture.drop_count() > rows_before);
}

#[test]
fn test_tracked_heap_payload_scatter_failure_drops_states() {
    let fixture = TrackedHeapFixture::new();

    {
        let config = HashTableConfig::default().with_initial_radix_bits(0);
        let hashtable = fixture.build_from_rows(FAIL_ON_SERIALIZE, config);

        let source = hashtable
            .payload
            .into_bucket_payloads()
            .next()
            .map(|(_, payload)| payload)
            .unwrap();
        let scattered = source.scatter_into_buckets(3);

        assert!(
            scattered
                .iter()
                .any(|payload| payload.aggregate_flush_all().is_err())
        );
        assert!(fixture.live_bytes() > 0);
    }

    fixture.assert_all_states_dropped();
}

#[test]
fn test_partitioned_payload_repartition_failure_drops_states() {
    let fixture = TrackedHeapFixture::new();

    {
        let hashtable = fixture.build_from_rows(FAIL_ON_SERIALIZE, HashTableConfig::default());
        let repartitioned = hashtable
            .payload
            .repartition(16, &mut PayloadFlushState::default());
        assert!(repartitioned.aggregate_flush_all().is_err());
        assert!(fixture.live_bytes() > 0);
    }

    fixture.assert_all_states_dropped();
}

#[test]
fn test_partitioned_payload_combine_failure_drops_states() {
    let fixture = TrackedHeapFixture::new();

    {
        let source = fixture.build_from_rows(FAIL_ON_SERIALIZE, HashTableConfig::default());
        let mut target = fixture.empty_table(HashTableConfig::default());

        target
            .combine_payloads(&source.payload, &mut PayloadFlushState::default())
            .unwrap();

        assert!(target.payload.aggregate_flush_all().is_err());
        assert!(fixture.live_bytes() > 0);
    }

    fixture.assert_all_states_dropped();
}

#[test]
fn test_partitioned_payload_cross_partition_combine_failure_drops_states() {
    let fixture = TrackedHeapFixture::new();

    {
        let source = fixture.build_from_rows(FAIL_ON_SERIALIZE, HashTableConfig::default());
        let mut target_config = HashTableConfig::default();
        target_config.initial_radix_bits += 1;
        let mut target = fixture.empty_table(target_config);

        target
            .combine_payloads(&source.payload, &mut PayloadFlushState::default())
            .unwrap();

        assert!(target.payload.aggregate_flush_all().is_err());
        assert!(fixture.live_bytes() > 0);
    }

    fixture.assert_all_states_dropped();
}

#[test]
fn test_combined_payload_bucket_extraction_keeps_states_valid() {
    let fixture = TrackedHeapFixture::new();

    {
        let source = fixture.build_from_rows(0, HashTableConfig::default());
        let mut target = fixture.empty_table(HashTableConfig::default());
        target
            .combine_payloads(&source.payload, &mut PayloadFlushState::default())
            .unwrap();
        assert!(fixture.live_bytes() > 0);

        let extracted = target
            .payload
            .into_non_empty_bucket_payloads()
            .collect::<Vec<_>>();
        assert!(!extracted.is_empty());
        let mut flushed_rows = 0;
        for (_, payload) in extracted {
            flushed_rows += payload.aggregate_flush_all().unwrap().num_rows();
        }
        assert_eq!(flushed_rows, GROUPS);
    }

    fixture.assert_all_states_dropped();
}

/// `accepts_arity` is a pruning aid, so it must never reject an argument list
/// that `matches_types` would accept. Both are derived from the same pattern,
/// and this pins the two together for the shapes used by real functions.
#[test]
fn test_argument_pattern_accepts_arity_agrees_with_matches_types() {
    let any = AggregateArgumentPattern::any;
    let boolean = || AggregateArgumentPattern::exact(DataType::Boolean);

    // sum: exactly one numeric-or-interval argument.
    let sum = AggregateArgumentsPattern::one_of(vec![
        AggregateArgumentsPattern::fixed(vec![AggregateArgumentPattern::any_numeric()]),
        AggregateArgumentsPattern::fixed(vec![AggregateArgumentPattern::exact(DataType::Interval)]),
    ]);
    assert!(!sum.accepts_arity(0));
    assert!(sum.accepts_arity(1));
    assert!(!sum.accepts_arity(2));
    assert!(!sum.accepts_arity(3));

    // count: zero or one argument.
    let count = AggregateArgumentsPattern::one_of(vec![
        AggregateArgumentsPattern::fixed(vec![]),
        AggregateArgumentsPattern::fixed(vec![any()]),
    ]);
    assert!(count.accepts_arity(0));
    assert!(count.accepts_arity(1));
    assert!(!count.accepts_arity(2));

    // arg_min: exactly two arguments.
    let arg_min = AggregateArgumentsPattern::fixed(vec![any(), any()]);
    assert!(!arg_min.accepts_arity(1));
    assert!(arg_min.accepts_arity(2));
    assert!(!arg_min.accepts_arity(3));

    // retention: 1..=32 boolean arguments.
    let retention = AggregateArgumentsPattern::variadic(vec![], boolean(), 1, Some(32));
    assert!(!retention.accepts_arity(0));
    assert!(retention.accepts_arity(1));
    assert!(retention.accepts_arity(32));
    assert!(!retention.accepts_arity(33));

    // `_if` wraps a nested pattern and appends a boolean condition.
    let sum_if = AggregateArgumentsPattern::if_condition(sum.clone());
    assert!(!sum_if.accepts_arity(0));
    assert!(!sum_if.accepts_arity(1));
    assert!(sum_if.accepts_arity(2));
    assert!(!sum_if.accepts_arity(3));

    // Cross-check: whenever matches_types accepts a list, so must accepts_arity.
    let types = [
        DataType::Boolean,
        UInt64Type::data_type(),
        DataType::Interval,
        DataType::String,
    ];
    let patterns = [sum, count, arg_min, retention, sum_if];
    for pattern in &patterns {
        for len in 0..=3usize {
            for combo in candidate_lists(&types, len) {
                if pattern.matches_types(&combo) {
                    assert!(
                        pattern.accepts_arity(combo.len()),
                        "accepts_arity rejected an arity that matches_types accepted: \
                         pattern={pattern:?} args={combo:?}"
                    );
                }
            }
        }
    }
}

fn candidate_lists(types: &[DataType], len: usize) -> Vec<Vec<DataType>> {
    if len == 0 {
        return vec![vec![]];
    }
    let mut out = Vec::new();
    for prefix in candidate_lists(types, len - 1) {
        for data_type in types {
            let mut next = prefix.clone();
            next.push(data_type.clone());
            out.push(next);
        }
    }
    out
}
