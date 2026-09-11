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
use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use bumpalo::Bump;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::*;
use databend_common_expression::with_number_mapped_type;
use databend_common_hashtable::HashSet as TypedHashSet;
use databend_common_hashtable::HashtableKeyable;
use databend_common_hashtable::HashtableLike;
use databend_common_hashtable::ShortStringHashSet;
use databend_common_hashtable::StackHashSet;
use databend_common_io::prelude::*;
use siphasher::sip128::Hasher128;
use siphasher::sip128::SipHasher24;

use super::AggregateRegistration;
use super::adaptors::*;

pub(super) struct UniqBuilder;

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
        eager_aggregation: EagerAggregation::Unsupported,
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

    pub(super) fn create(
        build: DirectBuildContext<'_, impl Combinator>,
    ) -> Result<AggregateCallRef> {
        if build.args_type().len() > 1 {
            return super::multi_arg_uniq::create(build);
        }
        let data_type = build.args_type()[0].remove_nullable();
        with_number_mapped_type!(|NUM| match data_type {
            DataType::Number(NumberDataType::NUM) =>
                Self::create_set::<TypedUniqSet<NumberType<NUM>>>(build),
            DataType::Date => Self::create_set::<TypedUniqSet<DateType>>(build),
            DataType::Timestamp => Self::create_set::<TypedUniqSet<TimestampType>>(build),
            DataType::String => Self::create_set::<StringUniqSet>(build),
            _ => Self::create_set::<ScalarUniqSet>(build),
        })
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
pub(super) trait UniqSet: Send + Sync + 'static {
    fn new() -> Self;
    fn len(&self) -> usize;
    fn serde_item() -> StateSerdeItem;
    fn merge(&mut self, rhs: &Self) -> bool;
    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()>;
    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<bool>;

    type Type: AccessType;
    fn insert(&mut self, value: <Self::Type as AccessType>::ScalarRef<'_>) -> Result<bool>;

    fn add_batch(&mut self, entry: &BlockEntry, validity: Option<&Bitmap>) -> Result<bool> {
        if entry.len() == 0 || validity.is_some_and(|v| v.true_count() == 0) {
            return Ok(false);
        }
        let view = entry.downcast::<Self::Type>().unwrap();
        if matches!(entry, BlockEntry::Const(..)) {
            return self.insert(view.index(0).unwrap());
        }
        let mut changed = false;
        if let Some(validity) = validity {
            for (value, valid) in view.iter().zip(validity.iter()) {
                if valid {
                    changed |= self.insert(value)?;
                }
            }
        } else {
            for value in view.iter() {
                changed |= self.insert(value)?;
            }
        }
        Ok(changed)
    }
}

pub(super) struct TypedUniqSet<T: ValueType>
where T::Scalar: HashtableKeyable
{
    keys: TypedHashSet<T::Scalar>,
}

impl<T> UniqSet for TypedUniqSet<T>
where
    T: ArgType,
    T::Scalar: Copy + HashtableKeyable + Send + Sync,
{
    type Type = T;
    fn insert(&mut self, value: T::ScalarRef<'_>) -> Result<bool> {
        Ok(self.keys.set_insert(T::to_owned_scalar(value)).is_ok())
    }

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

    fn merge(&mut self, rhs: &Self) -> bool {
        let mut changed = false;
        for key in rhs.keys.iter() {
            changed |= self.keys.set_insert(*key.key()).is_ok();
        }
        changed
    }
    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()> {
        let mut builder = ArrayType::<T>::downcast_builder(builder);
        for key in self.keys.iter() {
            builder.put_item(T::to_scalar_ref(key.key()));
        }
        builder.commit_row();
        Ok(())
    }
    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<bool> {
        let mut changed = false;
        let ScalarRef::Array(values) = value else {
            unreachable!()
        };
        let values = T::try_downcast_column(&values).unwrap();
        for value in T::iter_column(&values) {
            changed |= self.keys.set_insert(T::to_owned_scalar(value)).is_ok();
        }
        Ok(changed)
    }
}

// V1 stored SipHash-128 fingerprints, not recoverable strings. Keep the same
// hash and wire encoding so new inputs can be deduplicated against old states.
struct StringUniqSet {
    keys: StackHashSet<u128>,
}
impl UniqSet for StringUniqSet {
    type Type = StringType;
    fn insert(&mut self, value: &str) -> Result<bool> {
        let mut hasher = SipHasher24::new();
        hasher.write(value.as_bytes());
        Ok(self.keys.set_insert(hasher.finish128().into()).is_ok())
    }

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

    fn merge(&mut self, rhs: &Self) -> bool {
        let mut changed = false;
        for key in rhs.keys.iter() {
            changed |= self.keys.set_insert(*key.key()).is_ok();
        }
        changed
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
    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<bool> {
        let mut changed = false;
        let ScalarRef::Binary(mut bytes) = value else {
            unreachable!()
        };
        let count = bytes.read_uvarint()?;
        for _ in 0..count {
            changed |= self
                .keys
                .set_insert(u128::deserialize_reader(&mut bytes)?)
                .is_ok();
        }
        Ok(changed)
    }
}

// Scalar fallback retains the v1 single-element row encoding on the wire.

pub(super) struct ScalarUniqSet {
    keys: HashSet<Vec<u8>>,
}
impl UniqSet for ScalarUniqSet {
    type Type = AnyType;
    fn insert(&mut self, value: ScalarRef<'_>) -> Result<bool> {
        Ok(self
            .keys
            .insert(borsh::to_vec(std::slice::from_ref(&value.to_owned()))?))
    }

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

    fn merge(&mut self, rhs: &Self) -> bool {
        let mut changed = false;
        for key in rhs.keys.iter() {
            changed |= self.keys.insert(key.clone());
        }
        changed
    }
    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()> {
        let mut builder = ArrayType::<BinaryType>::downcast_builder(builder);
        for key in &self.keys {
            builder.put_item(key);
        }
        builder.commit_row();
        Ok(())
    }
    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<bool> {
        let mut changed = false;
        let ScalarRef::Array(values) = value else {
            unreachable!()
        };
        let values = BinaryType::try_downcast_column(&values).unwrap();
        for value in BinaryType::iter_column(&values) {
            changed |= self.keys.insert(value.to_vec());
        }
        Ok(changed)
    }
}

// Only sets retaining their original values can replay into another aggregate.
pub(super) trait DistinctSet: UniqSet {
    fn build_column(&self, data_type: &DataType) -> Result<Column>;
}

impl<T> DistinctSet for TypedUniqSet<T>
where
    T: ArgType,
    T::Scalar: Copy + HashtableKeyable + Send + Sync,
{
    fn build_column(&self, _: &DataType) -> Result<Column> {
        let mut builder = T::create_builder(self.keys.len(), &[]);
        for key in self.keys.iter() {
            T::push_item(&mut builder, T::to_scalar_ref(key.key()));
        }
        Ok(T::upcast_column(T::build_column(builder)))
    }
}

impl DistinctSet for ScalarUniqSet {
    fn build_column(&self, data_type: &DataType) -> Result<Column> {
        let mut builder = ColumnBuilder::with_capacity(data_type, self.keys.len());
        for key in &self.keys {
            let row = Vec::<Scalar>::deserialize(&mut key.as_slice())?;
            debug_assert_eq!(row.len(), 1);
            builder.push(row[0].as_ref());
        }
        Ok(builder.build())
    }
}

pub(super) struct StringDistinctSet {
    keys: ShortStringHashSet<[u8]>,
}
impl UniqSet for StringDistinctSet {
    type Type = StringType;
    fn insert(&mut self, value: &str) -> Result<bool> {
        Ok(self.keys.set_insert(value.as_bytes()))
    }

    fn new() -> Self {
        #[allow(clippy::arc_with_non_send_sync)]
        let arena = Arc::new(Bump::new());
        Self {
            keys: ShortStringHashSet::with_capacity(4, arena),
        }
    }
    fn len(&self) -> usize {
        self.keys.len()
    }
    fn serde_item() -> StateSerdeItem {
        StateSerdeItem::DataType(ArrayType::<BinaryType>::data_type())
    }

    fn merge(&mut self, rhs: &Self) -> bool {
        let mut changed = false;
        for key in rhs.keys.iter() {
            changed |= self.keys.set_insert(key.key());
        }
        changed
    }
    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()> {
        let mut builder = ArrayType::<BinaryType>::downcast_builder(builder);
        for key in self.keys.iter() {
            builder.put_item(key.key());
        }
        builder.commit_row();
        Ok(())
    }
    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<bool> {
        let mut changed = false;
        let ScalarRef::Array(column) = value else {
            unreachable!()
        };
        let column = BinaryType::try_downcast_column(&column).unwrap();
        for key in column.iter() {
            changed |= self.keys.set_insert(key);
        }
        Ok(changed)
    }
}
impl DistinctSet for StringDistinctSet {
    fn build_column(&self, _: &DataType) -> Result<Column> {
        let mut builder = StringColumnBuilder::with_capacity(self.keys.len());
        for key in self.keys.iter() {
            builder.put_and_commit(std::str::from_utf8(key.key()).unwrap());
        }
        Ok(Column::String(builder.build()))
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
            .add_batch(&input.columns[0], input.validity)
            .map(|_| ())
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        let view = input.columns[0].downcast::<S::Type>().unwrap();
        for (value, state) in view.iter().zip(input.states.iter()) {
            state.get::<S>().insert(value)?;
        }
        Ok(())
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        let view = input.columns[0].downcast::<S::Type>().unwrap();
        input
            .state
            .get::<S>()
            .insert(view.index(input.row).unwrap())
            .map(|_| ())
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

#[cfg(test)]
mod tests {
    use databend_common_expression::BlockEntry;
    use databend_common_expression::FromData;

    use super::*;

    fn round_trip<S: DistinctSet>(column: Column, expected: usize) -> Result<()> {
        let data_type = column.data_type();
        let entries = [BlockEntry::from(column)];
        let mut set = S::new();
        assert!(set.add_batch(&entries[0], None)?);
        assert!(!set.add_batch(&entries[0], None)?);
        assert_eq!(set.len(), expected);
        let field = match S::serde_item() {
            StateSerdeItem::DataType(ty) => ty,
            _ => unreachable!(),
        };
        let mut builder = ColumnBuilder::with_capacity(&field, 1);
        set.serialize(&mut builder)?;
        let mut restored = S::new();
        let serialized = builder.build_scalar();
        assert!(restored.merge_serialized(serialized.as_ref())?);
        assert!(!restored.merge_serialized(serialized.as_ref())?);
        assert_eq!(restored.len(), expected);
        assert!(!restored.merge(&set));
        assert_eq!(restored.len(), expected);
        let rebuilt = restored.build_column(&data_type)?;
        let mut replayed = S::new();
        replayed.add_batch(&BlockEntry::from(rebuilt), None)?;
        assert_eq!(replayed.len(), expected);
        Ok(())
    }

    #[test]
    fn string_distinct_storage_boundaries() -> Result<()> {
        let values = [
            "",
            "abc",
            "123456789",
            "12345678901234567",
            "1234567890123456789012345",
            "abc\0",
        ];
        let column = StringType::from_data(values.to_vec());
        round_trip::<StringDistinctSet>(column.clone(), values.len())?;
        let mut set = StringDistinctSet::new();
        set.add_batch(&BlockEntry::from(column), None)?;
        let Column::String(rebuilt) = set.build_column(&DataType::String)? else {
            unreachable!()
        };
        let mut actual = rebuilt.iter().collect::<Vec<_>>();
        let mut expected = values.to_vec();
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn scalar_distinct_row_format_round_trip() -> Result<()> {
        round_trip::<ScalarUniqSet>(BooleanType::from_data(vec![true, false, true]), 2)?;
        let mut set = ScalarUniqSet::new();
        assert!(set.insert(ScalarRef::Boolean(true))?);
        assert!(
            set.keys
                .contains(&borsh::to_vec(&vec![Scalar::Boolean(true)])?)
        );
        Ok(())
    }

    #[test]
    fn typed_distinct_float_equality() -> Result<()> {
        round_trip::<TypedUniqSet<Float32Type>>(
            Float32Type::from_data(vec![
                F32::from(-0.0),
                F32::from(0.0),
                F32::from(f32::from_bits(0x7fc00001)),
                F32::from(f32::from_bits(0xffc00002)),
            ]),
            2,
        )?;
        round_trip::<TypedUniqSet<Float64Type>>(
            Float64Type::from_data(vec![
                F64::from(-0.0),
                F64::from(0.0),
                F64::from(f64::from_bits(0x7ff8000000000001)),
                F64::from(f64::from_bits(0xfff8000000000002)),
            ]),
            2,
        )
    }
}
