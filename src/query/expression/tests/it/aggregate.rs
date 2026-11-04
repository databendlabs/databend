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
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DistinctSampler;
use databend_common_expression::EmitReason;
use databend_common_expression::FromData;
use databend_common_expression::HashTableConfig;
use databend_common_expression::MAX_GROW_CAPACITY;
use databend_common_expression::MAX_PARTIAL_TABLE_BYTES;
use databend_common_expression::MIN_PARTIAL_TABLE_BYTES;
use databend_common_expression::PartialAddStats;
use databend_common_expression::PartialAggregateMode;
use databend_common_expression::PartialObservation;
use databend_common_expression::PartialState;
use databend_common_expression::Payload;
use databend_common_expression::PayloadFlushState;
use databend_common_expression::ProbeState;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::aggregate::aggregate_function::*;
use databend_common_expression::aggregate_function::EagerAggregation;
use databend_common_expression::block_debug::assert_block_value_sort_eq;
use databend_common_expression::group_hash_entries;
use databend_common_expression::partial_table_bytes;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::StringType;
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

impl AggregateCall for TrackedHeapAggregateFunction {
    fn signature(&self) -> &AggregateSignature {
        static SIGNATURE: std::sync::LazyLock<AggregateSignature> =
            std::sync::LazyLock::new(|| AggregateSignature {
                name: "tracked_heap".to_string(),
                params: vec![],
                args_type: vec![],
                distinct: false,
                order_by: vec![],
                return_type: UInt64Type::data_type(),
            });
        &SIGNATURE
    }

    fn features(&self) -> &AggregateFeatures {
        static FEATURES: AggregateFeatures = AggregateFeatures {
            eager_aggregation: EagerAggregation::Unsupported,
            supports_filter: false,
            supports_state: false,
            sort_policy: databend_common_expression::aggregate::aggregate_function::SortPolicy::Unsupported,
            distinct_policy: databend_common_expression::aggregate::aggregate_function::DistinctPolicy::Unsupported,
            hide_doc: false,
            category: "",
            description: "",
            definition: "",
            example: "",
        };
        &FEATURES
    }

    fn input_layout(&self) -> &FunctionInputLayout {
        &FunctionInputLayout::Identity
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

    fn accumulate(&self, state: AggrState<'_>, columns: ProjectedBlock<'_>) -> Result<()> {
        let state = state.get::<TrackedHeapState>();
        for row in 0..columns.num_rows() {
            state.append_row(self.bytes_per_row, tracked_heap_row_trigger(columns, row))?;
        }
        Ok(())
    }

    fn accumulate_keys(
        &self,
        states: AggregateStateSet<'_>,
        columns: ProjectedBlock<'_>,
    ) -> Result<()> {
        for (row, state) in states.iter().enumerate() {
            state
                .get::<TrackedHeapState>()
                .append_row(self.bytes_per_row, tracked_heap_row_trigger(columns, row))?;
        }
        Ok(())
    }

    fn accumulate_row(
        &self,
        state: AggrState<'_>,
        columns: ProjectedBlock<'_>,
        row: usize,
    ) -> Result<()> {
        state
            .get::<TrackedHeapState>()
            .append_row(self.bytes_per_row, tracked_heap_row_trigger(columns, row))
    }

    fn accumulate_row_count(&self, _state: AggrState<'_>, _rows: usize) -> Result<()> {
        Ok(())
    }

    fn serialize(
        &self,
        states: AggregateStateSet<'_>,
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        let builder = &mut builders[0];
        for state in states.iter() {
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

    fn merge_serialized(
        &self,
        states: AggregateStateSet<'_>,
        serialized: &BlockEntry,
    ) -> Result<()> {
        for (row, state) in states.iter().enumerate() {
            let target = state.get::<TrackedHeapState>();
            let trigger = tracked_heap_trigger_from_entry(serialized, row);
            let bytes = match trigger {
                TrackedHeapFailureTrigger::BatchMerge => self.bytes_per_row,
                _ => tracked_heap_bytes_from_scalar(unsafe { serialized.index_unchecked(row) })
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

    fn merge_states(&self, state: AggrState<'_>, rhs: AggrState<'_>) -> Result<()> {
        let state = state.get::<TrackedHeapState>();
        let rhs = rhs.get::<TrackedHeapState>();
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

    fn merge_result(&self, state: AggrState<'_>, builder: &mut ColumnBuilder) -> Result<()> {
        let state = state.get::<TrackedHeapState>();
        if state.failures.merge_result {
            return Err(ErrorCode::Internal(
                "injected tracked_heap merge_result failure",
            ));
        }
        builder.push(ScalarRef::Number(NumberScalar::UInt64(
            state.bytes.len() as u64
        )));
        Ok(())
    }

    fn merge_result_read_only(
        &self,
        state: AggrState<'_>,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        self.merge_result(state, builder)
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

    fn aggrs(&self) -> Vec<AggregateCallRef> {
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
fn test_singleton_group_keys_merge_across_blocks() -> Result<()> {
    for (data_type, scalar) in [
        (DataType::Null, Scalar::Null),
        (DataType::EmptyArray, Scalar::EmptyArray),
        (DataType::EmptyMap, Scalar::EmptyMap),
    ] {
        for materialize in [false, true] {
            let mut hashtable = AggregateHashTable::new(
                vec![data_type.clone(), Int64Type::data_type()],
                vec![],
                HashTableConfig::default(),
                Arc::new(Bump::new()),
            );
            let mut state = ProbeState::default();
            let block = DataBlock::new(
                vec![
                    BlockEntry::new_const_column(data_type.clone(), scalar.clone(), 1),
                    BlockEntry::new_const_column_arg::<Int64Type>(7, 1),
                ],
                1,
            );
            let block = if materialize {
                block.convert_to_full()
            } else {
                block
            };

            for _ in 0..3 {
                hashtable.add_groups(
                    &mut state,
                    ProjectedBlock::from(block.columns()),
                    &[],
                    (&[]).into(),
                    1,
                )?;
                assert_eq!(hashtable.payload.len(), 1, "group key: {data_type:?}");
            }
        }
    }
    Ok(())
}

#[test]
fn test_partitioned_payload_public_bucket_iterators() {
    let fixture = TrackedHeapFixture::new();

    {
        let hashtable = fixture.build_from_rows(0, HashTableConfig::default());
        let rows = hashtable.payload.len();
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
        let rows = hashtable.payload.len();
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

/// Scatter every partition of `hashtable` into `buckets` payloads, as the partial aggregate
/// routes its output to the final aggregates.
fn scatter_into_buckets(hashtable: AggregateHashTable, buckets: usize) -> Vec<Payload> {
    hashtable
        .into_payloads()
        .into_iter()
        .flat_map(|payload| payload.scatter_into_buckets(buckets))
        .collect()
}

#[test]
fn test_payload_scatter_preserves_results() {
    let fixture = TrackedHeapFixture::new();

    {
        let hashtable = fixture.build_from_rows(0, HashTableConfig::default());
        let rows = hashtable.payload.len();
        let scattered = scatter_into_buckets(hashtable, 3);
        assert_eq!(
            scattered.iter().map(|payload| payload.len()).sum::<usize>(),
            rows
        );

        let mut merged = fixture.empty_table(HashTableConfig::default());
        let mut flush_state = PayloadFlushState::default();
        for payload in &scattered {
            merged.combine_payload(payload, &mut flush_state).unwrap();
        }
        fixture.assert_result(&mut merged);
    }

    fixture.assert_all_states_dropped();
}

#[test]
fn test_add_serialized_groups_matches_combine_and_drops_states() {
    let fixture = TrackedHeapFixture::new();

    {
        let hashtable =
            fixture.build_from_rows(0, HashTableConfig::default().with_initial_radix_bits(0));
        let serialized = hashtable.payload.aggregate_flush_all().unwrap();
        let rows = serialized.num_rows();
        let states = ProjectedBlock::project(&[0], &serialized);
        let groups = ProjectedBlock::project(&[1], &serialized);

        let mut direct = fixture.empty_table(HashTableConfig::default());
        let mut flush_state = PayloadFlushState::default();
        direct
            .add_serialized_groups(&mut flush_state, groups, states, rows)
            .unwrap();
        fixture.assert_result(&mut direct);

        // Merged again into existing groups, like through a temporary table.
        direct
            .add_serialized_groups(&mut flush_state, groups, states, rows)
            .unwrap();
        let mut combined = fixture.empty_table(HashTableConfig::default());
        for _ in 0..2 {
            combined
                .combine_payloads(&hashtable.payload, &mut PayloadFlushState::default())
                .unwrap();
        }
        assert_block_value_sort_eq(
            &fixture.merge_result_block(&mut direct),
            &fixture.merge_result_block(&mut combined),
        );
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
        rows_before = hashtable.payload.len();
        let scattered = scatter_into_buckets(hashtable, 3);

        let mut merged = fixture.empty_table(HashTableConfig::default());
        let mut flush_state = PayloadFlushState::default();
        let result = scattered
            .iter()
            .try_for_each(|payload| merged.combine_payload(payload, &mut flush_state));

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
        rows_before = hashtable.payload.len();
        let scattered = scatter_into_buckets(hashtable, 3);

        let mut merged = fixture.empty_table(HashTableConfig::default());
        let mut flush_state = PayloadFlushState::default();
        for payload in &scattered {
            merged.combine_payload(payload, &mut flush_state).unwrap();
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

#[test]
fn test_payload_split_pages_keeps_rows_and_states() -> Result<()> {
    // Distinct keys in one partition, so the payload spans several pages.
    const KEYS: usize = 40_000;
    let fixture = TrackedHeapFixture::new();

    {
        let config = HashTableConfig::default().with_initial_radix_bits(0);
        let mut source = fixture.empty_table(config.clone());
        let group_columns: Vec<BlockEntry> =
            vec![Int64Type::from_data((0..KEYS as i64).collect::<Vec<_>>()).into()];
        let params = [vec![UInt64Type::from_data(vec![0_u64; KEYS]).into()]];
        let params: Vec<ProjectedBlock> = params.iter().map(|v| v.into()).collect();
        source.add_groups(
            &mut ProbeState::default(),
            (&group_columns).into(),
            &params,
            (&[]).into(),
            KEYS,
        )?;

        let (_, payload) = source
            .payload
            .into_non_empty_bucket_payloads()
            .next()
            .unwrap();
        let bytes = payload.memory_size();

        // Within the limits: kept whole.
        let mut whole = payload.split_pages(KEYS, bytes);
        assert_eq!(whole.len(), 1);

        // Byte limit: whole pages up to half of the payload.
        let halves = whole.pop().unwrap().split_pages(usize::MAX, bytes / 2);
        assert!(halves.len() >= 2);
        assert!(halves.iter().all(|p| p.memory_size() <= bytes / 2));

        // A page over the row limit makes a payload of its own.
        let mut pieces = vec![];
        for half in halves {
            pieces.extend(half.split_pages(1, usize::MAX));
        }
        assert!(pieces.len() >= 2);
        assert_eq!(pieces.iter().map(|p| p.len()).sum::<usize>(), KEYS);

        // Merge some pieces and drop the others unmerged: the states of each must be dropped
        // exactly once, although the pieces share one arena.
        let unmerged = pieces.split_off(pieces.len() / 2);
        let mut merged = fixture.empty_table(config);
        let mut flush_state = PayloadFlushState::default();
        for piece in &pieces {
            merged.combine_payload(piece, &mut flush_state)?;
        }
        assert_eq!(
            merged.payload.len(),
            pieces.iter().map(|p| p.len()).sum::<usize>()
        );
        drop(unmerged);
        drop(pieces);
        assert!(fixture.live_bytes() > 0);
    }

    fixture.assert_all_states_dropped();
    Ok(())
}

fn group_hashes(entry: BlockEntry) -> Vec<u64> {
    let entries = vec![entry];
    let mut hashes = vec![0; entries[0].len()];
    group_hash_entries((&entries).into(), &mut hashes);
    hashes
}

/// The distinct estimate of the partial aggregate is accurate for short string keys, whose
/// group hashes are poorly mixed in their low bits.
#[test]
fn test_distinct_sampler_estimates_string_keys() {
    for keys in [20_000, 50_000, 200_000] {
        for (name, format) in [("n", "".to_string()), ("vn", "v".to_string())] {
            let strings = (0..keys).map(|key| format!("{format}{key}"));
            let hashes = group_hashes(StringType::from_data(strings.collect::<Vec<_>>()).into());
            let mut sampler = DistinctSampler::default();
            sampler.start(0);
            sampler.observe(&hashes);
            let estimate = sampler.distinct().unwrap() as f64;
            let error = (estimate / keys as f64 - 1.0).abs();
            assert!(error < 0.05, "{keys} keys '{name}': estimated {estimate}");
        }
    }
}

/// The distinct estimate counts the groups of the first window too: unique keys are estimated
/// near their number, not near the keys after the first window.
#[test]
fn test_distinct_sampler_counts_the_first_window() -> Result<()> {
    const KEYS: u64 = 50_000;
    const BLOCK_ROWS: u64 = 8192;
    let mut table = AggregateHashTable::new(
        vec![DataType::Number(NumberDataType::UInt64)],
        vec![],
        HashTableConfig::partial_aggregate(),
        Arc::new(Bump::new()),
    );
    let mut partial = PartialState::new(PartialAggregateMode::Auto);
    let mut probe = ProbeState::default();
    for start in (0..KEYS).step_by(BLOCK_ROWS as usize) {
        let end = (start + BLOCK_ROWS).min(KEYS);
        let entries = vec![UInt64Type::from_data((start..end).collect::<Vec<_>>()).into()];
        let capacity = partial.capacity();
        table.add_groups_partial(
            &mut probe,
            (&entries).into(),
            &[],
            (end - start) as usize,
            capacity,
            partial.sampler(),
        )?;
    }
    assert!(table.partial_restarted());
    let estimate = partial.sampler().unwrap().distinct().unwrap() as f64;
    let error = (estimate / KEYS as f64 - 1.0).abs();
    assert!(error < 0.05, "estimated {estimate}");
    Ok(())
}

/// Memory pressure is only checked for a grown table.
fn unexpected_pressure() -> bool {
    panic!("memory pressure checked outside the grown state")
}

/// An observation of `stats` and of groups of `group_bytes` each.
fn observed(stats: PartialAddStats, group_bytes: usize) -> PartialObservation {
    PartialObservation {
        stats,
        group_bytes,
        ..Default::default()
    }
}

/// The grown index holds the estimated distinct groups even with an estimate a little low.
#[test]
fn test_partial_grows_with_margin_over_estimate() {
    // Just under what an index of 65536 slots holds, so an estimate a little low would size it
    // that small.
    const KEYS: u64 = 48_000;
    let (partial, reason) = learn_keys(KEYS, 256 * 1024, 256 * 1024, 0, MAX_PARTIAL_TABLE_BYTES);
    assert_eq!(reason, Some(EmitReason::Grow), "{partial:?}");
    let keys = KEYS as usize;
    assert!(partial.capacity() >= AggregateHashTable::capacity_for_groups(keys + keys / 20));
}

/// Learn `keys` distinct groups of `group_bytes` each among many more materialized ones, which
/// asks to grow.
fn learn_recurring_keys(keys: u64, group_bytes: usize) -> (PartialState, Option<EmitReason>) {
    learn_recurring_keys_within(keys, group_bytes, MAX_PARTIAL_TABLE_BYTES)
}

/// `learn_recurring_keys` with a table of at most `max_table_bytes`.
fn learn_recurring_keys_within(
    keys: u64,
    group_bytes: usize,
    max_table_bytes: usize,
) -> (PartialState, Option<EmitReason>) {
    let rows = 16 * keys as usize;
    learn_keys(keys, rows, rows, group_bytes, max_table_bytes)
}

/// Learn `keys` distinct groups in `rows` rows, of which `new_groups` were materialized.
fn learn_keys(
    keys: u64,
    rows: usize,
    new_groups: usize,
    group_bytes: usize,
    max_table_bytes: usize,
) -> (PartialState, Option<EmitReason>) {
    let mut partial = PartialState::new(PartialAggregateMode::Auto);
    let sampler = partial.sampler().unwrap();
    sampler.start(0);
    sampler.observe(&group_hashes(
        UInt64Type::from_data((0..keys).collect::<Vec<_>>()).into(),
    ));
    let stats = PartialAddStats {
        rows,
        new_groups,
        windows: rows / 24_000,
    };
    let reason = partial.step(
        observed(stats, group_bytes),
        max_table_bytes,
        unexpected_pressure,
    );
    (partial, reason)
}

/// The largest index a partial table grows to: the index rounds its slots to whole chunks, a
/// power of two of them, which makes 3M slots of the 4M of `MAX_GROW_CAPACITY`.
fn largest_grown_capacity() -> usize {
    let capacity = AggregateHashTable::capacity_for_groups(2_200_000);
    assert!(capacity <= MAX_GROW_CAPACITY && capacity * 2 > MAX_GROW_CAPACITY);
    capacity
}

/// The partial aggregate grows up to `MAX_GROW_CAPACITY`, and does not grow at all when the
/// distinct groups would not fit: a smaller table would absorb little of them. With a window
/// that absorbs nothing either, it bypasses.
#[test]
fn test_partial_grows_up_to_max_capacity() {
    let (partial, reason) = learn_recurring_keys(2_000_000, 0);
    assert_eq!(reason, Some(EmitReason::Grow));
    assert_eq!(partial.capacity(), largest_grown_capacity());

    let (partial, reason) = learn_recurring_keys(3_000_000, 0);
    assert_eq!(reason, Some(EmitReason::Bypass));
    assert!(matches!(partial, PartialState::Bypass), "{partial:?}");
}

/// The grown table must also fit in `MAX_PARTIAL_TABLE_BYTES` with the bytes a group takes:
/// a table cut short by its memory would absorb little of the distinct groups.
#[test]
fn test_partial_grows_within_table_bytes() {
    // 2M groups of 50 bytes: 16 MiB of index and 105 MiB of groups.
    let (partial, reason) = learn_recurring_keys(2_000_000, 50);
    assert_eq!(reason, Some(EmitReason::Grow));
    assert_eq!(partial.capacity(), largest_grown_capacity());

    // 2M groups of 150 bytes: 16 MiB of index and 315 MiB of groups.
    let (partial, reason) = learn_recurring_keys(2_000_000, 150);
    assert_eq!(reason, Some(EmitReason::Bypass));
    assert!(matches!(partial, PartialState::Bypass), "{partial:?}");

    // Few groups that take much memory each, such as large `array_agg` states.
    let (_, reason) = learn_recurring_keys(100_000, 4096);
    assert_eq!(reason, Some(EmitReason::Bypass));
    let (_, reason) = learn_recurring_keys(100_000, 1024);
    assert_eq!(reason, Some(EmitReason::Grow));

    let index = AggregateHashTable::index_bytes_for(MAX_GROW_CAPACITY);
    assert!(index < MAX_PARTIAL_TABLE_BYTES / 4);

    // The same 2M groups of 50 bytes do not fit in a table that leaves them 100 MiB.
    let table_bytes = AggregateHashTable::index_bytes_for(largest_grown_capacity()) + (100 << 20);
    let (partial, reason) = learn_recurring_keys_within(2_000_000, 50, table_bytes);
    assert_eq!(reason, Some(EmitReason::Bypass));
    assert!(matches!(partial, PartialState::Bypass), "{partial:?}");
}

/// Only a table that can be built counts: when no grown table holds the distinct groups, the
/// window decides, so the partial aggregate bypasses only if the window absorbs nothing.
#[test]
fn test_partial_bypasses_only_what_no_table_absorbs() {
    // Keys repeat far apart (D / R = 0.5), but 3M of them do not fit a grown table and the
    // window absorbs nothing: bypass.
    let (partial, reason) = learn_keys(3_000_000, 6_000_000, 5_900_000, 0, MAX_PARTIAL_TABLE_BYTES);
    assert_eq!(reason, Some(EmitReason::Bypass));
    assert!(matches!(partial, PartialState::Bypass), "{partial:?}");

    // The same keys with a window that absorbs half the rows: keep learning.
    let (partial, reason) = learn_keys(3_000_000, 6_000_000, 3_000_000, 0, MAX_PARTIAL_TABLE_BYTES);
    assert_eq!(reason, None);
    assert!(matches!(partial, PartialState::Learning(_)), "{partial:?}");

    // 1M keys fit a grown table, which would absorb 1 / 6 of 1.2M rows: neither bypass nor grow
    // pays yet, keep learning.
    let (partial, reason) = learn_keys(1_000_000, 1_200_000, 1_200_000, 0, MAX_PARTIAL_TABLE_BYTES);
    assert_eq!(reason, None);
    assert!(matches!(partial, PartialState::Learning(_)), "{partial:?}");
}

/// The memory of a partial table is a share of `max_memory_usage` per thread, within bounds.
#[test]
fn test_partial_table_bytes() {
    const GIB: usize = 1 << 30;
    const MIB: usize = 1 << 20;
    // 16 threads, 128 GiB of which 80% may be used, 2% for the partial tables: about 128 MiB.
    let bytes = partial_table_bytes(128 * GIB / 10 * 8, 16);
    assert!((120 * MIB..140 * MIB).contains(&bytes), "{bytes}");
    // Little memory per thread.
    assert_eq!(partial_table_bytes(16 * GIB, 16), MIN_PARTIAL_TABLE_BYTES);
    // Few threads.
    assert_eq!(partial_table_bytes(200 * GIB, 2), MAX_PARTIAL_TABLE_BYTES);
    assert_eq!(partial_table_bytes(0, 16), MIN_PARTIAL_TABLE_BYTES);
}

/// Without a change of state, the table is emitted once it holds `MAX_PARTIAL_TABLE_BYTES`, or
/// once its payload outgrew the index; in bypass there is no table to emit.
#[test]
fn test_partial_emits_held_memory_and_full_payload() {
    let quiet = PartialObservation::default();
    let mut partial = PartialState::new(PartialAggregateMode::Auto);
    assert_eq!(
        partial.step(quiet, MAX_PARTIAL_TABLE_BYTES, unexpected_pressure),
        None
    );
    let held = PartialObservation {
        held_bytes: MAX_PARTIAL_TABLE_BYTES,
        ..quiet
    };
    assert_eq!(
        partial.step(held, MAX_PARTIAL_TABLE_BYTES, unexpected_pressure),
        Some(EmitReason::MemoryFull)
    );
    let full = PartialObservation {
        payload_full: true,
        ..quiet
    };
    assert_eq!(
        partial.step(full, MAX_PARTIAL_TABLE_BYTES, unexpected_pressure),
        Some(EmitReason::PayloadFull)
    );
    assert!(matches!(partial, PartialState::Learning(_)), "{partial:?}");

    let mut bypass = PartialState::new(PartialAggregateMode::Bypass);
    let both = PartialObservation {
        held_bytes: MAX_PARTIAL_TABLE_BYTES,
        payload_full: true,
        ..quiet
    };
    assert_eq!(
        bypass.step(both, MAX_PARTIAL_TABLE_BYTES, unexpected_pressure),
        None
    );
}

/// A grown table shrinks back to the small index under memory pressure, for good: no further
/// decisions, and memory pressure is no longer checked.
#[test]
fn test_partial_shrinks_grown_table() {
    let quiet = PartialObservation::default();
    let (mut partial, reason) = learn_recurring_keys(100_000, 0);
    assert_eq!(reason, Some(EmitReason::Grow));
    assert_eq!(partial.step(quiet, MAX_PARTIAL_TABLE_BYTES, || false), None);

    assert_eq!(
        partial.step(quiet, MAX_PARTIAL_TABLE_BYTES, || true),
        Some(EmitReason::Shrink)
    );
    assert!(matches!(partial, PartialState::Shrunk), "{partial:?}");
    assert_eq!(
        partial.capacity(),
        AggregateHashTable::capacity_for_groups(0)
    );
    assert!(partial.sampler().is_none());
    let stats = PartialAddStats {
        rows: 1 << 24,
        new_groups: 1 << 24,
        windows: 1 << 10,
    };
    assert_eq!(
        partial.step(
            observed(stats, 0),
            MAX_PARTIAL_TABLE_BYTES,
            unexpected_pressure
        ),
        None
    );
    let held = PartialObservation {
        held_bytes: MAX_PARTIAL_TABLE_BYTES,
        ..quiet
    };
    assert_eq!(
        partial.step(held, MAX_PARTIAL_TABLE_BYTES, unexpected_pressure),
        Some(EmitReason::MemoryFull)
    );
}

/// String keys in a cyclic order, more than a 65536-slot index holds: the partial aggregate
/// grows to hold them all and absorbs every repeat afterwards.
#[test]
fn test_partial_absorbs_cyclic_string_keys() -> Result<()> {
    const KEYS: usize = 50_000;
    const BLOCK_ROWS: usize = 8192;
    let mut table = AggregateHashTable::new(
        vec![DataType::String],
        vec![],
        HashTableConfig::partial_aggregate(),
        Arc::new(Bump::new()),
    );
    let mut partial = PartialState::new(PartialAggregateMode::Auto);
    let mut probe = ProbeState::default();
    let (mut grown_rows, mut grown_new_groups) = (0, 0);
    for start in (0..40 * KEYS).step_by(BLOCK_ROWS) {
        let keys = (start..start + BLOCK_ROWS).map(|row| (row % KEYS).to_string());
        let entries = vec![StringType::from_data(keys.collect::<Vec<_>>()).into()];
        let grown = matches!(partial, PartialState::Grown { .. });
        let capacity = partial.capacity();
        let stats = table.add_groups_partial(
            &mut probe,
            (&entries).into(),
            &[],
            BLOCK_ROWS,
            capacity,
            partial.sampler(),
        )?;
        if grown {
            grown_rows += stats.rows;
            grown_new_groups += stats.new_groups;
        }
        if partial
            .step(observed(stats, 0), MAX_PARTIAL_TABLE_BYTES, || false)
            .is_some()
        {
            table.take_partial_payload();
            assert!(!table.partial_restarted());
        }
    }

    let PartialState::Grown { capacity } = partial else {
        panic!("expected to grow, got {partial:?}");
    };
    assert!(capacity >= AggregateHashTable::capacity_for_groups(KEYS));
    assert!(grown_rows > 30 * KEYS);
    assert!(grown_new_groups <= KEYS, "{grown_new_groups} new groups");
    Ok(())
}

/// `accepts_arity` is a pruning aid, so it must never reject an argument list
/// that `matches_types` would accept. Both are derived from the same pattern,
/// and this pins the two together for the shapes used by real functions.
#[test]
fn test_argument_pattern_accepts_arity_agrees_with_matches_types() {
    let any = ArgumentPattern::any;
    let boolean = || ArgumentPattern::exact(DataType::Boolean);

    // sum: exactly one numeric-or-interval argument.
    let sum = ArgumentsPattern::one_of(vec![
        ArgumentsPattern::fixed(vec![ArgumentPattern::any_numeric()]),
        ArgumentsPattern::fixed(vec![ArgumentPattern::exact(DataType::Interval)]),
    ]);
    assert!(!sum.accepts_arity(0));
    assert!(sum.accepts_arity(1));
    assert!(!sum.accepts_arity(2));
    assert!(!sum.accepts_arity(3));

    // count: zero or one argument.
    let count = ArgumentsPattern::one_of(vec![
        ArgumentsPattern::fixed(vec![]),
        ArgumentsPattern::fixed(vec![any()]),
    ]);
    assert!(count.accepts_arity(0));
    assert!(count.accepts_arity(1));
    assert!(!count.accepts_arity(2));

    // arg_min: exactly two arguments.
    let arg_min = ArgumentsPattern::fixed(vec![any(), any()]);
    assert!(!arg_min.accepts_arity(1));
    assert!(arg_min.accepts_arity(2));
    assert!(!arg_min.accepts_arity(3));

    // retention: 1..=32 boolean arguments.
    let retention = ArgumentsPattern::variadic(vec![], boolean(), 1, Some(32));
    assert!(!retention.accepts_arity(0));
    assert!(retention.accepts_arity(1));
    assert!(retention.accepts_arity(32));
    assert!(!retention.accepts_arity(33));

    // `_if` wraps a nested pattern and appends a boolean condition.
    let sum_if = ArgumentsPattern::if_condition(sum.clone());
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
