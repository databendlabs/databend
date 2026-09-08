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

use std::alloc::Layout;
use std::collections::HashSet;
use std::hash::Hasher;
use std::marker::PhantomData;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::*;
use databend_common_expression::with_number_mapped_type;
use databend_common_hashtable::HashSet as TypedHashSet;
use databend_common_hashtable::HashtableKeyable;
use databend_common_hashtable::StackHashSet;
use databend_common_io::prelude::*;
use siphasher::sip128::Hasher128;
use siphasher::sip128::SipHasher24;

use super::AggregateRegistration;
use super::adaptors::*;

struct UniqBuilder;

impl UniqBuilder {
    fn register(registry: &mut AggregateRegistry) {
        Self::route().register(registry);
    }
}

inventory::submit! {
    AggregateRegistration {
        register: UniqBuilder::register,
    }
}

impl UniqBuilder {
    fn uniq_arguments() -> ArgumentsPattern {
        ArgumentsPattern::variadic(vec![], ArgumentPattern::any(), 1, Some(32))
    }

    const UNIQ_METADATA: AggregateMetadata = AggregateMetadata {
        null_argument_result: NullArgumentResult::UInt64Zero,
        is_decomposable: true,
        sort_policy: SortPolicy::Unsupported,
        documentation: AggregateDocumentation {
            category: "Aggregate",
            description: "counts distinct non-null input rows",
            definition: "uniq(expr[, ...])",
            example: "select uniq(number) from numbers(10)",
        },
    };
}

impl UniqBuilder {
    fn route() -> NameRoute {
        let arguments = Self::uniq_arguments();
        let metadata = Self::UNIQ_METADATA;
        NameRoute::new(&["uniq"], arguments, metadata, NullInput::Filter)
            .with_validator(Self::validate_request)
            .then(MergeRoute::new(false, UniqBuilder::create))
            .then(MergeRoute::new(true, UniqBuilder::create))
            .then(PlainRoute::new(UniqBuilder::create))
            .then(IfRoute::direct(UniqBuilder::create))
            .then(StateRoute::direct(UniqBuilder::create))
    }

    fn validate_request(request: &RawAggregateCall<'_>) -> Result<()> {
        if request.params.is_empty() {
            Ok(())
        } else {
            Err(ErrorCode::BadArguments(format!(
                "{} expects no parameters",
                request.name
            )))
        }
    }

    fn create(build: DirectBuildContext<'_, impl Combinator>) -> Result<AggregateCallRef> {
        if build.args_type().len() == 1 {
            let data_type = build.args_type()[0].remove_nullable();
            return with_number_mapped_type!(|NUM| match data_type {
                DataType::Number(NumberDataType::NUM) =>
                    Self::create_set::<TypedUniqSet<NumberType<NUM>>>(build),
                DataType::Date => Self::create_set::<TypedUniqSet<DateType>>(build),
                DataType::Timestamp => Self::create_set::<TypedUniqSet<TimestampType>>(build),
                DataType::String => Self::create_set::<StringUniqSet>(build),
                _ => Self::create_set::<RowUniqSet>(build),
            });
        }
        Self::create_set::<RowUniqSet>(build)
    }
}

impl UniqBuilder {
    fn create_set<S: UniqSet>(
        build: DirectBuildContext<'_, impl Combinator>,
    ) -> Result<AggregateCallRef> {
        let state =
            AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<S>())], vec![
                S::serde_item(),
            ])
            .with_manual_drop(true);
        build.create(
            UInt64Type::data_type(),
            state,
            MultiArgSkipNullEval::new(UniqEval::<S>(PhantomData)),
        )
    }
}

// Uniq owns a set, not a Distinct adaptor plus a replayed Count state. Its
// result is the set cardinality; merges only union sets, never add counts.
trait UniqSet: Send + Sync + 'static {
    fn new() -> Self;
    fn len(&self) -> usize;
    fn serde_item() -> StateSerdeItem;
    fn add(&mut self, columns: ProjectedBlock<'_>, row: usize) -> Result<()>;
    fn add_batch(&mut self, columns: ProjectedBlock<'_>, validity: Option<&Bitmap>) -> Result<()> {
        for row in 0..columns.num_rows() {
            if validity.is_none_or(|v| v.get(row).unwrap()) {
                self.add(columns, row)?;
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self);
    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()>;
    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<()>;
}

struct TypedUniqSet<T: ValueType>
where T::Scalar: HashtableKeyable
{
    keys: TypedHashSet<T::Scalar>,
}

impl<T> UniqSet for TypedUniqSet<T>
where
    T: ArgType,
    T::Scalar: Copy + HashtableKeyable + Send + Sync,
{
    fn new() -> Self {
        Self {
            keys: TypedHashSet::with_capacity(4),
        }
    }
    fn len(&self) -> usize {
        self.keys.len()
    }
    fn serde_item() -> StateSerdeItem {
        StateSerdeItem::DataType(ArrayType::<T>::data_type())
    }
    fn add(&mut self, columns: ProjectedBlock<'_>, row: usize) -> Result<()> {
        let column = columns[0].downcast::<T>().unwrap();
        let _ = self
            .keys
            .set_insert(T::to_owned_scalar(column.index(row).unwrap()));
        Ok(())
    }
    fn add_batch(&mut self, columns: ProjectedBlock<'_>, validity: Option<&Bitmap>) -> Result<()> {
        let column = columns[0].downcast::<T>().unwrap();
        for (row, value) in column.iter().enumerate() {
            if validity.is_none_or(|v| v.get(row).unwrap()) {
                let _ = self.keys.set_insert(T::to_owned_scalar(value));
            }
        }
        Ok(())
    }
    fn merge(&mut self, rhs: &Self) {
        self.keys.set_merge(&rhs.keys);
    }
    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()> {
        let mut builder = ArrayType::<T>::downcast_builder(builder);
        for key in self.keys.iter() {
            builder.put_item(T::to_scalar_ref(key.key()));
        }
        builder.commit_row();
        Ok(())
    }
    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<()> {
        let ScalarRef::Array(values) = value else {
            unreachable!()
        };
        let values = T::try_downcast_column(&values).unwrap();
        for value in T::iter_column(&values) {
            let _ = self.keys.set_insert(T::to_owned_scalar(value));
        }
        Ok(())
    }
}

// V1 stored SipHash-128 fingerprints, not recoverable strings. Keep the same
// hash and wire encoding so new inputs can be deduplicated against old states.
struct StringUniqSet {
    keys: StackHashSet<u128>,
}
impl UniqSet for StringUniqSet {
    fn new() -> Self {
        Self {
            keys: StackHashSet::new(),
        }
    }
    fn len(&self) -> usize {
        self.keys.len()
    }
    fn serde_item() -> StateSerdeItem {
        StateSerdeItem::Binary(None)
    }
    fn add(&mut self, columns: ProjectedBlock<'_>, row: usize) -> Result<()> {
        let column = columns[0].downcast::<StringType>().unwrap();
        let mut hasher = SipHasher24::new();
        hasher.write(column.index(row).unwrap().as_bytes());
        let _ = self.keys.set_insert(hasher.finish128().into());
        Ok(())
    }
    fn add_batch(&mut self, columns: ProjectedBlock<'_>, validity: Option<&Bitmap>) -> Result<()> {
        let column = columns[0].downcast::<StringType>().unwrap();
        for (row, value) in column.iter().enumerate() {
            if validity.is_none_or(|v| v.get(row).unwrap()) {
                let mut hasher = SipHasher24::new();
                hasher.write(value.as_bytes());
                let _ = self.keys.set_insert(hasher.finish128().into());
            }
        }
        Ok(())
    }
    fn merge(&mut self, rhs: &Self) {
        self.keys.set_merge(&rhs.keys);
    }
    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()> {
        let builder = builder.as_binary_mut().unwrap();
        builder.data.write_uvarint(self.keys.len() as u64)?;
        for key in self.keys.iter() {
            key.key().serialize(&mut builder.data)?;
        }
        builder.commit_row();
        Ok(())
    }
    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<()> {
        let ScalarRef::Binary(mut bytes) = value else {
            unreachable!()
        };
        let count = bytes.read_uvarint()?;
        for _ in 0..count {
            let _ = self.keys.set_insert(u128::deserialize_reader(&mut bytes)?);
        }
        Ok(())
    }
}

// Generic and multi-argument keys retain v1's Borsh Vec<Scalar> representation.
struct RowUniqSet {
    keys: HashSet<Vec<u8>>,
}
impl UniqSet for RowUniqSet {
    fn new() -> Self {
        Self {
            keys: HashSet::new(),
        }
    }
    fn len(&self) -> usize {
        self.keys.len()
    }
    fn serde_item() -> StateSerdeItem {
        StateSerdeItem::DataType(ArrayType::<BinaryType>::data_type())
    }
    fn add(&mut self, columns: ProjectedBlock<'_>, row: usize) -> Result<()> {
        let values = columns
            .iter()
            .map(|column| column.index(row).unwrap().to_owned())
            .collect::<Vec<Scalar>>();
        let mut bytes = Vec::new();
        values.serialize(&mut bytes)?;
        self.keys.insert(bytes);
        Ok(())
    }
    fn merge(&mut self, rhs: &Self) {
        self.keys.extend(rhs.keys.iter().cloned());
    }
    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()> {
        let mut builder = ArrayType::<BinaryType>::downcast_builder(builder);
        for key in &self.keys {
            builder.put_item(key);
        }
        builder.commit_row();
        Ok(())
    }
    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<()> {
        let ScalarRef::Array(values) = value else {
            unreachable!()
        };
        let values = BinaryType::try_downcast_column(&values).unwrap();
        self.keys
            .extend(BinaryType::iter_column(&values).map(<[u8]>::to_vec));
        Ok(())
    }
}

struct UniqEval<S>(PhantomData<fn() -> S>);
impl<S: UniqSet> AggregateEval for UniqEval<S> {
    fn init_state(&self, state: AggrState<'_>) {
        state.write(S::new);
    }
    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        input
            .state
            .get::<S>()
            .add_batch(input.columns, input.validity)
    }
    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            state.get::<S>().add(input.columns, row)?;
        }
        Ok(())
    }
    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        input.state.get::<S>().add(input.columns, input.row)
    }
    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        for state in input.states.iter() {
            state.get::<S>().serialize(&mut input.builders[0])?;
        }
        Ok(())
    }
    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            if input.filter.is_none_or(|v| v.get(row).unwrap()) {
                state
                    .get::<S>()
                    .merge_serialized(super::serialized_scalar_at(input.state, row, 0))?;
            }
        }
        Ok(())
    }
    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        input.state.get::<S>().merge(input.rhs.get::<S>());
        Ok(())
    }
    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        self.merge_result_read_only(input)
    }
    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        input.builder.push(ScalarRef::Number(NumberScalar::UInt64(
            input.state.get::<S>().len() as u64,
        )));
        Ok(())
    }
    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { std::ptr::drop_in_place(state.get::<S>()) };
    }
}
