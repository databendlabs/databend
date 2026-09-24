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
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::AggrState;
use super::AggrStateLoc;
use super::AggrStateType;
use super::StateAddr;
use super::StateSerdeType;
use super::StatesLayout;
use crate::BlockEntry;
use crate::ColumnBuilder;
use crate::ProjectedBlock;
use crate::Scalar;
use crate::ScalarRef;
use crate::StateSerdeItem;
use crate::Symbol;
use crate::types::DataType;

pub type AggregateCallRef = Arc<dyn AggregateCall>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateSignature {
    pub name: String,
    pub params: Vec<Scalar>,
    pub args_type: Vec<DataType>,
    pub distinct: bool,
    pub order_by: Vec<AggregateBoundOrderByItem>,
    pub return_type: DataType,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AggregateBoundOrderByItem {
    pub index: Symbol,
    pub source: AggregateBoundOrderBySource,
    pub data_type: DataType,
    pub asc: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AggregateBoundOrderBySource {
    Argument { index: usize },
    Derived,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateRuntimeOrderByItem {
    pub input: AggregateRuntimeOrderByInput,
    pub data_type: DataType,
    pub asc: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateRuntimeOrderByInput {
    Argument { offset: usize },
    SortKey { offset: usize },
}

/// Maps the logical aggregate inputs (`arguments` followed by derived ORDER BY
/// keys) to the column order consumed by a concrete function instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunctionInputLayout {
    Identity,
    Projection(Vec<usize>),
}

impl FunctionInputLayout {
    pub fn new(input_len: usize, projection: Vec<usize>) -> Result<Self> {
        if projection.len() != input_len {
            return Err(ErrorCode::Internal(format!(
                "aggregate input projection has {} entries for {input_len} inputs",
                projection.len()
            )));
        }

        let mut seen = vec![false; input_len];
        for &index in &projection {
            if index >= input_len || std::mem::replace(&mut seen[index], true) {
                return Err(ErrorCode::Internal(format!(
                    "aggregate input projection is not a permutation: {projection:?}"
                )));
            }
        }
        if projection.iter().copied().eq(0..input_len) {
            Ok(Self::Identity)
        } else {
            Ok(Self::Projection(projection))
        }
    }

    pub fn project<'a, T: Clone>(&self, inputs: &'a [T]) -> Result<Cow<'a, [T]>> {
        let Self::Projection(projection) = self else {
            return Ok(Cow::Borrowed(inputs));
        };
        if inputs.len() != projection.len() {
            return Err(ErrorCode::Internal(format!(
                "aggregate input layout expects {} inputs, got {}",
                projection.len(),
                inputs.len()
            )));
        }
        Ok(Cow::Owned(
            projection
                .iter()
                .map(|&index| inputs[index].clone())
                .collect(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArgumentPattern {
    pub kind: ArgumentKind,
    pub nullability: ArgumentNullability,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgumentKind {
    Exact(DataType),
    AnyNumber,
    AnyDecimal,
    AnyNumeric,
    Any,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ArgumentNullability {
    #[default]
    Any,
    NonNullable,
    Nullable,
}

impl ArgumentPattern {
    pub fn exact(data_type: DataType) -> Self {
        Self {
            kind: ArgumentKind::Exact(data_type),
            nullability: ArgumentNullability::Any,
        }
    }

    pub fn any_number() -> Self {
        Self {
            kind: ArgumentKind::AnyNumber,
            nullability: ArgumentNullability::Any,
        }
    }

    pub fn any_decimal() -> Self {
        Self {
            kind: ArgumentKind::AnyDecimal,
            nullability: ArgumentNullability::Any,
        }
    }

    pub fn any_numeric() -> Self {
        Self {
            kind: ArgumentKind::AnyNumeric,
            nullability: ArgumentNullability::Any,
        }
    }

    pub fn any() -> Self {
        Self {
            kind: ArgumentKind::Any,
            nullability: ArgumentNullability::Any,
        }
    }

    pub fn non_nullable(mut self) -> Self {
        self.nullability = ArgumentNullability::NonNullable;
        self
    }

    pub fn nullable(mut self) -> Self {
        self.nullability = ArgumentNullability::Nullable;
        self
    }

    pub fn matches_type(&self, data_type: &DataType) -> bool {
        let data_type = match self.nullability {
            ArgumentNullability::Any if data_type.is_null() => return true,
            ArgumentNullability::Any => data_type.remove_nullable(),
            ArgumentNullability::NonNullable => {
                if data_type.is_nullable_or_null() {
                    return false;
                }
                data_type.clone()
            }
            ArgumentNullability::Nullable => {
                let DataType::Nullable(inner) = data_type else {
                    return false;
                };
                (**inner).clone()
            }
        };

        match &self.kind {
            ArgumentKind::Exact(expected) => expected == &data_type,
            ArgumentKind::AnyNumber => matches!(data_type, DataType::Number(_)),
            ArgumentKind::AnyDecimal => matches!(data_type, DataType::Decimal(_)),
            ArgumentKind::AnyNumeric => {
                matches!(data_type, DataType::Number(_) | DataType::Decimal(_))
            }
            ArgumentKind::Any => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgumentsPattern {
    Fixed(Vec<ArgumentPattern>),
    OneOf(Vec<ArgumentsPattern>),
    If(Box<ArgumentsPattern>),
    Variadic {
        prefix: Vec<ArgumentPattern>,
        repeated: ArgumentPattern,
        min_repeats: usize,
        max_repeats: Option<usize>,
    },
}

impl ArgumentsPattern {
    pub fn fixed(args: impl Into<Vec<ArgumentPattern>>) -> Self {
        Self::Fixed(args.into())
    }

    pub fn one_of(patterns: impl Into<Vec<ArgumentsPattern>>) -> Self {
        Self::OneOf(patterns.into())
    }

    pub fn if_condition(arguments: ArgumentsPattern) -> Self {
        Self::If(Box::new(arguments))
    }

    pub fn variadic(
        prefix: impl Into<Vec<ArgumentPattern>>,
        repeated: ArgumentPattern,
        min_repeats: usize,
        max_repeats: Option<usize>,
    ) -> Self {
        Self::Variadic {
            prefix: prefix.into(),
            repeated,
            min_repeats,
            max_repeats,
        }
    }

    pub fn matches_types(&self, args_type: &[DataType]) -> bool {
        match self {
            Self::Fixed(args) => {
                args.len() == args_type.len()
                    && args
                        .iter()
                        .zip(args_type.iter())
                        .all(|(pattern, data_type)| pattern.matches_type(data_type))
            }
            Self::OneOf(patterns) => patterns
                .iter()
                .any(|pattern| pattern.matches_types(args_type)),
            Self::If(arguments) => {
                let Some((condition, nested_args)) = args_type.split_last() else {
                    return false;
                };
                ArgumentPattern::exact(DataType::Boolean).matches_type(condition)
                    && arguments.matches_types(nested_args)
            }
            Self::Variadic {
                prefix,
                repeated,
                min_repeats,
                max_repeats,
            } => {
                let Some(repeats) = args_type.len().checked_sub(prefix.len()) else {
                    return false;
                };
                if repeats < *min_repeats || max_repeats.is_some_and(|max| repeats > max) {
                    return false;
                }
                prefix
                    .iter()
                    .zip(args_type.iter())
                    .all(|(pattern, data_type)| pattern.matches_type(data_type))
                    && args_type[prefix.len()..]
                        .iter()
                        .all(|data_type| repeated.matches_type(data_type))
            }
        }
    }

    /// Whether this pattern can accept an argument list of `arity` elements.
    ///
    /// This is a cheap necessary condition derived from the pattern shape alone,
    /// intended for pruning candidate argument lists before they are built.
    /// `matches_types` remains the authoritative check.
    pub fn accepts_arity(&self, arity: usize) -> bool {
        match self {
            Self::Fixed(args) => args.len() == arity,
            Self::OneOf(patterns) => patterns.iter().any(|pattern| pattern.accepts_arity(arity)),
            Self::If(arguments) => arity
                .checked_sub(1)
                .is_some_and(|nested_arity| arguments.accepts_arity(nested_arity)),
            Self::Variadic {
                prefix,
                min_repeats,
                max_repeats,
                ..
            } => {
                let Some(repeats) = arity.checked_sub(prefix.len()) else {
                    return false;
                };
                repeats >= *min_repeats && !max_repeats.is_some_and(|max| repeats > max)
            }
        }
    }
}

impl From<Vec<ArgumentPattern>> for ArgumentsPattern {
    fn from(args: Vec<ArgumentPattern>) -> Self {
        Self::fixed(args)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SortPolicy {
    #[default]
    Unsupported,
    Optional,
    Required,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum DistinctPolicy {
    #[default]
    Unsupported,
    /// `DISTINCT` does not change this aggregate's result. Consume the
    /// modifier and resolve the original function name.
    Idempotent,
    /// Resolve a semantic `DISTINCT` request through this explicitly named
    /// aggregate function.
    Redirect {
        target: String,
        aliases: Vec<(String, String)>,
    },
}

/// How the eager aggregation optimizer combines finalized results below a join.
/// This is not the ability to merge intermediate aggregate states.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EagerAggregation {
    #[default]
    Unsupported,
    /// Sum local sums, compensating for join multiplicity.
    Sum,
    /// Sum local counts, compensating for join multiplicity and returning zero
    /// for empty input.
    Count,
    /// Apply the same minimum/maximum aggregate to local extrema. Repeating a
    /// value does not affect the result, so no multiplicity compensation is needed.
    MinMax,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AggregateFeatures {
    pub eager_aggregation: EagerAggregation,
    pub supports_filter: bool,
    /// Whether this name supports a corresponding `<name>_state` call.
    pub supports_state: bool,
    pub sort_policy: SortPolicy,
    pub distinct_policy: DistinctPolicy,
    pub hide_doc: bool,
    pub category: &'static str,
    pub description: &'static str,
    pub definition: &'static str,
    pub example: &'static str,
}

/// Memory layout, serialized fields, and manual destruction requirements for
/// an evaluator's state. Describes the state contract without implementing its
/// operations; those belong to [`AggregateEval`].
#[derive(Debug, Clone, Default)]
pub struct AggregateStateDescription {
    fields: Vec<AggrStateType>,
    serde_items: Vec<StateSerdeItem>,
    need_manual_drop: bool,
    state_version: u64,
}

impl AggregateStateDescription {
    pub fn new(
        fields: impl Into<Vec<AggrStateType>>,
        serde_items: impl Into<Vec<StateSerdeItem>>,
    ) -> Self {
        Self {
            fields: fields.into(),
            serde_items: serde_items.into(),
            need_manual_drop: false,
            state_version: 0,
        }
    }

    pub fn with_state_version(mut self, state_version: u64) -> Self {
        self.state_version = state_version;
        self
    }

    pub fn with_manual_drop(mut self, need_manual_drop: bool) -> Self {
        self.need_manual_drop = need_manual_drop;
        self
    }

    pub fn with_null_flag(mut self) -> Self {
        self.fields.push(AggrStateType::Bool);
        self.serde_items
            .push(StateSerdeItem::DataType(DataType::Boolean));
        self
    }

    pub fn fields(&self) -> &[AggrStateType] {
        &self.fields
    }

    pub fn serde_items(&self) -> &[StateSerdeItem] {
        &self.serde_items
    }

    pub fn need_manual_drop(&self) -> bool {
        self.need_manual_drop
    }

    pub fn state_version(&self) -> u64 {
        self.state_version
    }

    pub fn data_type(&self) -> DataType {
        StateSerdeType::new(self.serde_items.clone()).data_type()
    }
}

pub(crate) fn state_at<T>(state: AggrState<'_>, index: usize) -> &mut T
where T: Send + 'static {
    state.addr.next(state.loc[index].offset()).get::<T>()
}

pub(crate) fn write_state_at<T>(state: AggrState<'_>, index: usize, value: T)
where T: Send + 'static {
    state
        .addr
        .next(state.loc[index].offset())
        .write_state(value)
}

pub struct AggregateStateSet<'a> {
    places: &'a [StateAddr],
    loc: &'a [AggrStateLoc],
}

#[inline(always)]
fn for_each_selected_item<T>(
    items: impl IntoIterator<Item = T>,
    selection: Option<&Bitmap>,
    mut f: impl FnMut(T),
) {
    match selection {
        Some(selection) => {
            for (item, selected) in items.into_iter().zip(selection.iter()) {
                if selected {
                    f(item);
                }
            }
        }
        None => {
            for item in items {
                f(item);
            }
        }
    }
}

#[inline(always)]
fn try_for_each_selected_item<T>(
    items: impl IntoIterator<Item = T>,
    selection: Option<&Bitmap>,
    mut f: impl FnMut(T) -> Result<()>,
) -> Result<()> {
    match selection {
        Some(selection) => {
            for (item, selected) in items.into_iter().zip(selection.iter()) {
                if selected {
                    f(item)?;
                }
            }
        }
        None => {
            for item in items {
                f(item)?;
            }
        }
    }
    Ok(())
}

impl<'a> AggregateStateSet<'a> {
    pub fn new(places: &'a [StateAddr], loc: &'a [AggrStateLoc]) -> Self {
        Self { places, loc }
    }

    pub fn len(&self) -> usize {
        self.places.len()
    }

    pub fn is_empty(&self) -> bool {
        self.places.is_empty()
    }

    pub fn get(&self, index: usize) -> AggrState<'_> {
        AggrState::new(self.places[index], self.loc)
    }

    fn typed_state_offset(&self) -> Result<usize> {
        if self.loc.len() != 1 {
            return Err(ErrorCode::Internal(
                "typed state iteration requires exactly one custom state field",
            ));
        }
        self.first_state_offset()
    }

    fn first_state_offset(&self) -> Result<usize> {
        match self.loc.first() {
            Some(AggrStateLoc::Custom(_, offset)) => Ok(*offset),
            _ => Err(ErrorCode::Internal(
                "aggregate state does not start with a custom state field",
            )),
        }
    }

    fn last_flag_offset(&self) -> Result<usize> {
        match self.loc.last() {
            Some(AggrStateLoc::Bool(_, offset)) => Ok(*offset),
            _ => Err(ErrorCode::Internal(
                "aggregate state does not end with a boolean flag",
            )),
        }
    }

    /// Visits typed aggregate states selected by `selection`.
    pub fn for_each_state<T>(
        &self,
        selection: Option<&Bitmap>,
        mut f: impl FnMut(&mut T),
    ) -> Result<()>
    where
        T: Send + 'static,
    {
        let offset = self.typed_state_offset()?;
        for_each_selected_item(self.places.iter(), selection, |place| {
            f(place.next(offset).get::<T>());
        });
        Ok(())
    }

    /// Visits typed aggregate states selected by `selection` with a fallible callback.
    ///
    /// The state offset is resolved once per batch. The row loop traverses raw
    /// places directly instead of constructing an `AggrState` with a slice fat
    /// pointer for every row.
    pub fn try_for_each_state<T>(
        &self,
        selection: Option<&Bitmap>,
        mut f: impl FnMut(&mut T) -> Result<()>,
    ) -> Result<()>
    where
        T: Send + 'static,
    {
        let offset = self.typed_state_offset()?;
        try_for_each_selected_item(self.places.iter(), selection, |place| {
            f(place.next(offset).get::<T>())
        })
    }

    /// Visits the leading custom state field selected by `selection`.
    pub fn for_each_first_state<T>(
        &self,
        selection: Option<&Bitmap>,
        mut f: impl FnMut(&mut T),
    ) -> Result<()>
    where
        T: Send + 'static,
    {
        let offset = self.first_state_offset()?;
        for_each_selected_item(self.places.iter(), selection, |place| {
            f(place.next(offset).get::<T>());
        });
        Ok(())
    }

    /// Visits the leading custom state field with a fallible callback.
    pub fn try_for_each_first_state<T>(
        &self,
        selection: Option<&Bitmap>,
        mut f: impl FnMut(&mut T) -> Result<()>,
    ) -> Result<()>
    where
        T: Send + 'static,
    {
        let offset = self.first_state_offset()?;
        try_for_each_selected_item(self.places.iter(), selection, |place| {
            f(place.next(offset).get::<T>())
        })
    }

    /// Visits the leading custom state field with its corresponding input values.
    pub fn for_each_first_state_value<T, V>(
        &self,
        values: impl IntoIterator<Item = V>,
        selection: Option<&Bitmap>,
        mut f: impl FnMut(&mut T, V),
    ) -> Result<()>
    where
        T: Send + 'static,
    {
        let offset = self.first_state_offset()?;
        for_each_selected_item(
            self.places.iter().zip(values),
            selection,
            |(place, value)| {
                f(place.next(offset).get::<T>(), value);
            },
        );
        Ok(())
    }

    /// Visits the leading custom state field with a fallible callback.
    pub fn try_for_each_first_state_value<T, V>(
        &self,
        values: impl IntoIterator<Item = V>,
        selection: Option<&Bitmap>,
        mut f: impl FnMut(&mut T, V) -> Result<()>,
    ) -> Result<()>
    where
        T: Send + 'static,
    {
        let offset = self.first_state_offset()?;
        try_for_each_selected_item(
            self.places.iter().zip(values),
            selection,
            |(place, value)| f(place.next(offset).get::<T>(), value),
        )
    }

    /// Marks the trailing boolean flag for states selected by `selection`.
    pub fn mark_last_flag(&self, selection: Option<&Bitmap>) -> Result<()> {
        let offset = self.last_flag_offset()?;
        try_for_each_selected_item(self.places.iter(), selection, |place| {
            *place.next(offset).get::<u8>() = 1;
            Ok(())
        })
    }

    /// Serializes the trailing boolean flag for every state.
    pub fn serialize_last_flag(&self, builder: &mut ColumnBuilder) -> Result<()> {
        let offset = self.last_flag_offset()?;
        for place in self.places {
            builder.push(ScalarRef::Boolean(*place.next(offset).get::<u8>() != 0));
        }
        Ok(())
    }

    /// Visits typed aggregate states together with their corresponding input values.
    pub fn for_each_state_value<T, V>(
        &self,
        values: impl IntoIterator<Item = V>,
        selection: Option<&Bitmap>,
        mut f: impl FnMut(&mut T, V),
    ) -> Result<()>
    where
        T: Send + 'static,
    {
        let offset = self.typed_state_offset()?;
        for_each_selected_item(
            self.places.iter().zip(values),
            selection,
            |(place, value)| {
                f(place.next(offset).get::<T>(), value);
            },
        );
        Ok(())
    }

    /// Visits typed aggregate states with a fallible callback.
    ///
    /// Keeping the state offset and raw place traversal inside this method lets the
    /// compiler keep the offset loop-invariant and avoids constructing an `AggrState`
    /// containing a slice fat pointer for every row.
    pub fn try_for_each_state_value<T, V>(
        &self,
        values: impl IntoIterator<Item = V>,
        selection: Option<&Bitmap>,
        mut f: impl FnMut(&mut T, V) -> Result<()>,
    ) -> Result<()>
    where
        T: Send + 'static,
    {
        let offset = self.typed_state_offset()?;
        try_for_each_selected_item(
            self.places.iter().zip(values),
            selection,
            |(place, value)| f(place.next(offset).get::<T>(), value),
        )
    }

    pub fn without_first_loc(&self) -> AggregateStateSet<'a> {
        AggregateStateSet::new(self.places, &self.loc[1..])
    }

    pub fn without_last_loc(&self) -> AggregateStateSet<'a> {
        AggregateStateSet::new(self.places, &self.loc[..self.loc.len() - 1])
    }

    pub fn with_places<'b>(&'b self, places: &'b [StateAddr]) -> AggregateStateSet<'b> {
        AggregateStateSet::new(places, self.loc)
    }

    pub fn iter(&self) -> impl Iterator<Item = AggrState<'_>> {
        self.places
            .iter()
            .map(|place| AggrState::new(*place, self.loc))
    }
}

pub struct AccumulateInput<'a> {
    pub state: AggrState<'a>,
    pub columns: ProjectedBlock<'a>,
    pub validity: Option<&'a Bitmap>,
}

pub struct AccumulateKeysInput<'a> {
    pub states: AggregateStateSet<'a>,
    pub columns: ProjectedBlock<'a>,
    pub validity: Option<&'a Bitmap>,
}

pub struct AccumulateRowInput<'a> {
    pub state: AggrState<'a>,
    pub columns: ProjectedBlock<'a>,
    pub row: usize,
}

pub struct AccumulateRowCountInput<'a> {
    pub state: AggrState<'a>,
    pub rows: usize,
}

pub struct AccumulateRowCountKeysInput<'a> {
    pub states: AggregateStateSet<'a>,
}

pub struct SerializeInput<'a> {
    pub states: AggregateStateSet<'a>,
    pub builders: &'a mut [ColumnBuilder],
}

pub struct MergeSerializedInput<'a> {
    pub states: AggregateStateSet<'a>,
    pub state: &'a BlockEntry,
    pub filter: Option<&'a Bitmap>,
}

impl MergeSerializedInput<'_> {
    pub fn for_each_state<T>(&self, f: impl FnMut(&mut T, usize)) -> Result<()>
    where T: Send + 'static {
        self.states
            .for_each_state_value(0..self.state.len(), self.filter, f)
    }

    pub fn try_for_each_state<T>(&self, f: impl FnMut(&mut T, usize) -> Result<()>) -> Result<()>
    where T: Send + 'static {
        self.states
            .try_for_each_state_value(0..self.state.len(), self.filter, f)
    }

    pub fn for_each_first_state<T>(&self, f: impl FnMut(&mut T, usize)) -> Result<()>
    where T: Send + 'static {
        self.states
            .for_each_first_state_value(0..self.state.len(), self.filter, f)
    }

    pub fn try_for_each_first_state<T>(
        &self,
        f: impl FnMut(&mut T, usize) -> Result<()>,
    ) -> Result<()>
    where
        T: Send + 'static,
    {
        self.states
            .try_for_each_first_state_value(0..self.state.len(), self.filter, f)
    }
}

pub struct MergeStatesInput<'a> {
    pub state: AggrState<'a>,
    pub rhs: AggrState<'a>,
}

pub struct MergeResultInput<'a> {
    pub state: AggrState<'a>,
    pub builder: &'a mut ColumnBuilder,
}

/// State operations implemented by a concrete aggregate evaluator.
///
/// Owns the behavior for initialization, accumulation, serialization, merging,
/// finalization, and destruction. Its state accesses must agree with the
/// accompanying [`AggregateStateDescription`]. Call metadata is exposed
/// separately through [`AggregateCall`].
pub trait AggregateEval: Send + Sync + 'static {
    fn init_state(&self, state: AggrState<'_>);

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()>;

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()>;

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()>;

    fn accumulate_row_count(&self, input: AccumulateRowCountInput<'_>) -> Result<()> {
        if input.rows == 0 {
            Ok(())
        } else {
            Err(ErrorCode::BadArguments(
                "aggregate does not support rows-only input",
            ))
        }
    }

    fn accumulate_row_count_keys(&self, input: AccumulateRowCountKeysInput<'_>) -> Result<()> {
        for state in input.states.iter() {
            self.accumulate_row_count(AccumulateRowCountInput { state, rows: 1 })?;
        }
        Ok(())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()>;

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()>;

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()>;

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()>;

    /// Default for implementations whose result path does not consume state.
    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        self.merge_result(input)
    }

    /// # Safety
    /// The caller must ensure the state belongs to this aggregate
    /// implementation.
    unsafe fn drop_state(&self, state: AggrState<'_>);
}

/// A completed aggregate call exposed to the execution engine.
///
/// Combines the evaluator's state operations with its signature, features,
/// physical input layout, and [`AggregateStateDescription`]. Construction and
/// composition are complete before a builder returns an [`AggregateCallRef`];
/// the execution engine uses this interface to run the resulting call.
pub trait AggregateCall: fmt::Display + Send + Sync + 'static {
    fn signature(&self) -> &AggregateSignature;

    fn features(&self) -> &AggregateFeatures;

    /// Physical input order expected by `accumulate*`.
    fn input_layout(&self) -> &FunctionInputLayout;

    fn state(&self) -> &AggregateStateDescription;

    fn init_state(&self, state: AggrState<'_>);

    fn accumulate(&self, state: AggrState<'_>, columns: ProjectedBlock<'_>) -> Result<()>;

    fn accumulate_keys(
        &self,
        states: AggregateStateSet<'_>,
        columns: ProjectedBlock<'_>,
    ) -> Result<()>;

    fn accumulate_row(
        &self,
        state: AggrState<'_>,
        columns: ProjectedBlock<'_>,
        row: usize,
    ) -> Result<()>;

    fn accumulate_row_count(&self, state: AggrState<'_>, rows: usize) -> Result<()>;

    fn serialize(
        &self,
        states: AggregateStateSet<'_>,
        builders: &mut [ColumnBuilder],
    ) -> Result<()>;

    fn merge_serialized(&self, states: AggregateStateSet<'_>, state: &BlockEntry) -> Result<()>;

    fn merge_states(&self, state: AggrState<'_>, rhs: AggrState<'_>) -> Result<()>;

    fn merge_result(&self, state: AggrState<'_>, builder: &mut ColumnBuilder) -> Result<()>;

    fn merge_result_read_only(
        &self,
        state: AggrState<'_>,
        builder: &mut ColumnBuilder,
    ) -> Result<()>;

    /// # Safety
    /// The caller must ensure the state belongs to this function.
    unsafe fn drop_state(&self, state: AggrState<'_>);
}

pub struct AggregateCallInstance<I> {
    signature: AggregateSignature,
    input_layout: FunctionInputLayout,
    features: AggregateFeatures,
    state: AggregateStateDescription,
    implementation: I,
}

impl<I> AggregateCallInstance<I>
where I: AggregateEval
{
    pub fn new(
        signature: AggregateSignature,
        input_layout: FunctionInputLayout,
        features: AggregateFeatures,
        state: AggregateStateDescription,
        implementation: I,
    ) -> Self {
        Self {
            signature,
            input_layout,
            features,
            state,
            implementation,
        }
    }
}

impl<I> AggregateCall for AggregateCallInstance<I>
where I: AggregateEval
{
    fn signature(&self) -> &AggregateSignature {
        &self.signature
    }

    fn input_layout(&self) -> &FunctionInputLayout {
        &self.input_layout
    }

    fn features(&self) -> &AggregateFeatures {
        &self.features
    }

    fn state(&self) -> &AggregateStateDescription {
        &self.state
    }

    fn init_state(&self, state: AggrState<'_>) {
        self.implementation.init_state(state)
    }

    fn accumulate(&self, state: AggrState<'_>, columns: ProjectedBlock<'_>) -> Result<()> {
        self.implementation.accumulate(AccumulateInput {
            state,
            columns,
            validity: None,
        })
    }

    fn accumulate_keys(
        &self,
        states: AggregateStateSet<'_>,
        columns: ProjectedBlock<'_>,
    ) -> Result<()> {
        self.implementation.accumulate_keys(AccumulateKeysInput {
            states,
            columns,
            validity: None,
        })
    }

    fn accumulate_row(
        &self,
        state: AggrState<'_>,
        columns: ProjectedBlock<'_>,
        row: usize,
    ) -> Result<()> {
        self.implementation.accumulate_row(AccumulateRowInput {
            state,
            columns,
            row,
        })
    }

    fn accumulate_row_count(&self, state: AggrState<'_>, rows: usize) -> Result<()> {
        self.implementation
            .accumulate_row_count(AccumulateRowCountInput { state, rows })
    }

    fn serialize(
        &self,
        states: AggregateStateSet<'_>,
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        self.implementation
            .serialize(SerializeInput { states, builders })
    }

    fn merge_serialized(&self, states: AggregateStateSet<'_>, state: &BlockEntry) -> Result<()> {
        self.implementation.merge_serialized(MergeSerializedInput {
            states,
            state,
            filter: None,
        })
    }

    fn merge_states(&self, state: AggrState<'_>, rhs: AggrState<'_>) -> Result<()> {
        self.implementation
            .merge_states(MergeStatesInput { state, rhs })
    }

    fn merge_result(&self, state: AggrState<'_>, builder: &mut ColumnBuilder) -> Result<()> {
        self.implementation
            .merge_result(MergeResultInput { state, builder })
    }

    fn merge_result_read_only(
        &self,
        state: AggrState<'_>,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        self.implementation
            .merge_result_read_only(MergeResultInput { state, builder })
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { self.implementation.drop_state(state) }
    }
}

impl<I> fmt::Display for AggregateCallInstance<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.signature.name)
    }
}

#[derive(Clone)]
pub struct RawAggregateCall<'a> {
    pub name: &'a str,
    pub params: &'a [Scalar],
    pub args_type: &'a [DataType],
    pub distinct: bool,
    pub order_by: &'a [AggregateBoundOrderByItem],
}

/// Layout settings selected by the function for one aggregate call. Execution
/// may use a different layout from the persisted input or output format; a
/// read's input version never implicitly determines the version written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregateStateSettings {
    /// Version of the selected output (or execution-only) layout, not inherited
    /// from the input state. An execution-only version is not a promise to
    /// persist states in that format.
    pub state_version: u64,
    /// Whether the selected output/internal layout retains the outer
    /// input-presence flag where the state has one.
    pub preserve_nullable_input_rows_flag: bool,
    /// Whether the serialized input has the outer input-presence flag. This may
    /// differ from the output setting when reading an older persisted layout.
    pub input_nullable_input_rows_flag: bool,
}

impl AggregateStateSettings {
    /// Retain the v0 nullable-input layout when writing or reading states
    /// whose format must remain compatible with existing persisted data.
    pub const fn v0_compatibility() -> Self {
        Self {
            preserve_nullable_input_rows_flag: true,
            state_version: 0,
            input_nullable_input_rows_flag: true,
        }
    }
}

pub trait AggregateCallBuilder: Send + Sync + 'static {
    fn arguments(&self) -> &ArgumentsPattern;

    fn features(&self) -> &AggregateFeatures;

    fn build(&self, request: RawAggregateCall<'_>) -> Result<AggregateCallRef>;

    fn build_with_state_settings(
        &self,
        request: RawAggregateCall<'_>,
        settings: AggregateStateSettings,
    ) -> Result<AggregateCallRef> {
        if settings != AggregateStateSettings::v0_compatibility() {
            return Err(ErrorCode::BadDataValueType(format!(
                "Aggregate {} does not support the requested state format",
                request.name
            )));
        }
        self.build(request)
    }
}

/// How a registered aggregate interacts with persisted aggregate states.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AggregateStateAccess {
    #[default]
    Execution,
    Read,
    Write,
    ReadWrite,
}

impl AggregateStateAccess {
    pub fn reads(self) -> bool {
        matches!(self, Self::Read | Self::ReadWrite)
    }

    pub fn writes(self) -> bool {
        matches!(self, Self::Write | Self::ReadWrite)
    }
}

/// The production policy for persisted aggregate states.
#[derive(Clone, Copy, Debug, Default)]
pub enum AggregateStateWritePolicy {
    #[default]
    Compatible,
    /// Select the function's newest supported format (for controlled migrations).
    Latest,
}

/// Function-specific selection of execution and persisted state layouts.
/// `input_version` comes from the input AggregateState type; None denotes a
/// legacy physical state without format metadata. `read` may convert that
/// format into an independent execution layout. `write` chooses a format from
/// the global policy; `rewrite` must choose its output from that policy, not
/// from the input version. The function defines the versions and conversions.
pub trait AggregateStateSettingsSelector: Send + Sync + 'static {
    fn execution(&self, request: &RawAggregateCall<'_>) -> Result<AggregateStateSettings>;

    fn read(
        &self,
        request: &RawAggregateCall<'_>,
        input_version: Option<u64>,
    ) -> Result<AggregateStateSettings>;

    fn write(
        &self,
        request: &RawAggregateCall<'_>,
        policy: AggregateStateWritePolicy,
    ) -> Result<AggregateStateSettings>;

    fn rewrite(
        &self,
        request: &RawAggregateCall<'_>,
        policy: AggregateStateWritePolicy,
        input_version: Option<u64>,
    ) -> Result<AggregateStateSettings>;
}

pub struct AggregateDescriptor {
    pub name: String,
    pub aliases: Vec<String>,
    arguments: ArgumentsPattern,
    features: AggregateFeatures,
    builder: Arc<dyn AggregateCallBuilder>,
    state_settings: Option<Arc<dyn AggregateStateSettingsSelector>>,
    state_access: AggregateStateAccess,
}

impl AggregateDescriptor {
    pub fn from_builder(name: impl Into<String>, builder: Arc<dyn AggregateCallBuilder>) -> Self {
        let arguments = builder.arguments().clone();
        let features = builder.features().clone();
        Self {
            name: name.into(),
            aliases: Vec::new(),
            arguments,
            features,
            builder,
            state_settings: None,
            state_access: AggregateStateAccess::Execution,
        }
    }

    /// Select the execution layout or a versioned format for this descriptor.
    pub fn with_state_settings(mut self, select: Arc<dyn AggregateStateSettingsSelector>) -> Self {
        self.state_settings = Some(select);
        self
    }

    pub fn with_state_access(mut self, access: AggregateStateAccess) -> Self {
        self.state_access = access;
        self
    }

    /// Select internal layout and decode format independently of the write policy.
    /// A read-only route may use a newer internal layout without persisting it.
    fn select_state_settings(
        &self,
        request: &RawAggregateCall<'_>,
        policy: AggregateStateWritePolicy,
    ) -> Result<AggregateStateSettings> {
        let input = if self.state_access.reads() {
            request.args_type.first().map(DataType::remove_nullable)
        } else {
            None
        };
        let input = input.as_ref().and_then(|ty| match ty {
            DataType::AggregateState(state) => Some(state.as_ref()),
            _ => None,
        });
        let select = self.state_settings.as_ref().ok_or_else(|| {
            ErrorCode::BadDataValueType(format!(
                "No aggregate state settings defined for {}",
                self.name
            ))
        })?;
        let input_version = input.map(|state| state.state_version);
        match self.state_access {
            AggregateStateAccess::Execution => select.execution(request),
            AggregateStateAccess::Read => select.read(request, input_version),
            AggregateStateAccess::Write => select.write(request, policy),
            AggregateStateAccess::ReadWrite => select.rewrite(request, policy, input_version),
        }
    }

    pub fn with_aliases(mut self, aliases: impl Into<Vec<String>>) -> Self {
        self.aliases = aliases.into();
        self
    }

    pub fn with_metadata(
        mut self,
        arguments: ArgumentsPattern,
        features: AggregateFeatures,
    ) -> Self {
        self.arguments = arguments;
        self.features = features;
        self
    }

    pub fn arguments(&self) -> &ArgumentsPattern {
        &self.arguments
    }

    pub fn features(&self) -> &AggregateFeatures {
        &self.features
    }
}

#[derive(Default)]
pub struct AggregateRegistry {
    functions: HashMap<String, AggregateDescriptor>,
    aliases: HashMap<String, String>,
}

impl AggregateRegistry {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn register(&mut self, descriptor: AggregateDescriptor) {
        let name = descriptor.name.to_ascii_lowercase();
        match self.functions.entry(name) {
            Entry::Vacant(entry) => {
                for alias in &descriptor.aliases {
                    self.aliases
                        .insert(alias.to_ascii_lowercase(), entry.key().clone());
                }
                entry.insert(descriptor);
            }
            Entry::Occupied(entry) => {
                panic!("duplicate aggregate function registration: {}", entry.key())
            }
        }
    }

    pub fn registered_names(&self) -> Vec<String> {
        BTreeSet::from_iter(self.functions.keys().chain(self.aliases.keys()))
            .into_iter()
            .cloned()
            .collect()
    }

    pub fn aliases(&self) -> Vec<(&str, &str)> {
        let mut aliases = self
            .aliases
            .iter()
            .map(|(alias, target)| (alias.as_str(), target.as_str()))
            .collect::<Vec<_>>();
        aliases.sort_by_key(|(alias, _)| *alias);
        aliases
    }

    pub fn descriptor(&self, name: &str) -> Option<&AggregateDescriptor> {
        let name = self.canonical_name(name);
        self.functions.get(name.as_str())
    }

    pub fn contains(&self, name: &str) -> bool {
        self.descriptor(name).is_some()
    }

    pub fn resolve(&self, request: RawAggregateCall<'_>) -> Result<AggregateCallRef> {
        let settings =
            self.select_state_settings(&request, AggregateStateWritePolicy::default())?;
        self.resolve_with_state_settings(request, settings)
    }

    fn select_state_settings(
        &self,
        request: &RawAggregateCall<'_>,
        policy: AggregateStateWritePolicy,
    ) -> Result<AggregateStateSettings> {
        let descriptor = self.descriptor(request.name).ok_or_else(|| {
            ErrorCode::UnknownAggregateFunction(format!(
                "Unsupported AggregateFunction: {}",
                request.name
            ))
        })?;
        descriptor.select_state_settings(request, policy)
    }

    /// Override the format policy for tests or a controlled migration.
    /// The aggregate defines which layout each policy selects.
    pub fn resolve_with_state_policy(
        &self,
        request: RawAggregateCall<'_>,
        policy: AggregateStateWritePolicy,
    ) -> Result<AggregateCallRef> {
        let settings = self.select_state_settings(&request, policy)?;
        self.resolve_with_state_settings(request, settings)
    }

    /// Build using settings already selected by the registry.
    pub fn resolve_with_state_settings(
        &self,
        request: RawAggregateCall<'_>,
        settings: AggregateStateSettings,
    ) -> Result<AggregateCallRef> {
        let requested_name = request.name.to_ascii_lowercase();
        let name = self.canonical_name(&requested_name);
        let descriptor = self.functions.get(&name).ok_or_else(|| {
            ErrorCode::UnknownAggregateFunction(format!(
                "Unsupported AggregateFunction: {requested_name}"
            ))
        })?;

        if request.distinct {
            if descriptor.features().distinct_policy == DistinctPolicy::Idempotent {
                return self.resolve_with_state_settings(
                    RawAggregateCall {
                        name: requested_name.as_str(),
                        params: request.params,
                        args_type: request.args_type,
                        distinct: false,
                        order_by: request.order_by,
                    },
                    settings,
                );
            }

            // The target owns the DISTINCT signature. Resolve the redirect
            // before checking the source signature because they may accept
            // different argument counts (for example, count and
            // count_distinct).
            if let Some(target) = descriptor
                .features()
                .distinct_policy
                .target_for(&requested_name)
            {
                let redirected = RawAggregateCall {
                    name: target,
                    params: request.params,
                    args_type: request.args_type,
                    distinct: false,
                    order_by: request.order_by,
                };
                return self.resolve_with_state_settings(redirected, settings);
            }
            return Err(ErrorCode::UnknownAggregateFunction(format!(
                "Unsupported AggregateFunction signature: {requested_name}({:?})",
                request.args_type
            )));
        }

        if !descriptor
            .arguments()
            .accepts_arity(request.args_type.len())
        {
            return Err(ErrorCode::NumberArgumentsNotMatch(format!(
                "Aggregate function {requested_name} does not accept {} arguments",
                request.args_type.len()
            )));
        }

        if !descriptor.arguments().matches_types(request.args_type)
            || !descriptor.features().sort_policy.accepts(request.order_by)
        {
            return Err(ErrorCode::UnknownAggregateFunction(format!(
                "Unsupported AggregateFunction signature: {requested_name}({:?})",
                request.args_type
            )));
        }

        let request = RawAggregateCall {
            name: requested_name.as_str(),
            params: request.params,
            args_type: request.args_type,
            distinct: false,
            order_by: request.order_by,
        };
        descriptor
            .builder
            .build_with_state_settings(request, settings)
    }

    fn canonical_name(&self, name: &str) -> String {
        let name = name.to_lowercase();
        self.aliases.get(&name).cloned().unwrap_or(name)
    }
}

impl SortPolicy {
    fn accepts(&self, order_by: &[AggregateBoundOrderByItem]) -> bool {
        match self {
            Self::Unsupported => order_by.is_empty(),
            Self::Optional => true,
            Self::Required => !order_by.is_empty(),
        }
    }
}

impl DistinctPolicy {
    pub fn redirect(target: impl Into<String>) -> Self {
        Self::Redirect {
            target: target.into(),
            aliases: Vec::new(),
        }
    }

    pub fn redirect_with_aliases(
        target: impl Into<String>,
        aliases: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        Self::Redirect {
            target: target.into(),
            aliases: aliases.into_iter().collect(),
        }
    }

    pub fn target_for(&self, requested_name: &str) -> Option<&str> {
        match self {
            Self::Unsupported | Self::Idempotent => None,
            Self::Redirect { target, aliases } => aliases
                .iter()
                .find_map(|(alias, target)| {
                    alias
                        .eq_ignore_ascii_case(requested_name)
                        .then_some(target.as_str())
                })
                .or(Some(target.as_str())),
        }
    }
}

pub fn get_states_layout(functions: &[AggregateCallRef]) -> Result<StatesLayout> {
    let mut states = Vec::new();
    let mut offsets = Vec::with_capacity(functions.len() + 1);
    let mut serialize_type = Vec::with_capacity(functions.len());
    offsets.push(0);

    for function in functions {
        states.extend_from_slice(function.state().fields());
        offsets.push(states.len());
        serialize_type.push(StateSerdeType::new(function.state().serde_items().to_vec()));
    }

    let (layout, locs) = sort_states(states);
    let states_loc = offsets
        .windows(2)
        .map(|window| locs[window[0]..window[1]].to_vec().into_boxed_slice())
        .collect();

    Ok(StatesLayout {
        layout,
        states_loc,
        serialize_type,
    })
}

pub struct AggregateStateOwner {
    addr: StateAddr,
    layout: StatesLayout,
    functions: Vec<AggregateCallRef>,
    _arena: Bump,
}

impl AggregateStateOwner {
    pub fn new(functions: Vec<AggregateCallRef>) -> Result<Self> {
        let layout = get_states_layout(&functions)?;
        let _arena = Bump::new();
        let addr = _arena.alloc_layout(layout.layout).into();

        let owner = Self {
            addr,
            layout,
            functions,
            _arena,
        };

        for (index, function) in owner.functions.iter().enumerate() {
            function.init_state(owner.state(index));
        }

        Ok(owner)
    }

    pub fn state(&self, index: usize) -> AggrState<'_> {
        AggrState::new(self.addr, &self.layout.states_loc[index])
    }

    pub fn state_set(&self, index: usize) -> AggregateStateSet<'_> {
        AggregateStateSet::new(
            std::slice::from_ref(&self.addr),
            &self.layout.states_loc[index],
        )
    }
}

impl Drop for AggregateStateOwner {
    fn drop(&mut self) {
        for (index, function) in self.functions.iter().enumerate() {
            if function.state().need_manual_drop() {
                unsafe {
                    function.drop_state(AggrState::new(self.addr, &self.layout.states_loc[index]));
                }
            }
        }
    }
}

fn sort_states(states: Vec<AggrStateType>) -> (Layout, Vec<AggrStateLoc>) {
    if states.is_empty() {
        return (Layout::from_size_align(0, 1).unwrap(), Vec::new());
    }

    let mut states = states
        .iter()
        .enumerate()
        .map(|(idx, state)| {
            let layout = match state {
                AggrStateType::Bool => (1, 1),
                AggrStateType::Custom(layout) => (layout.align(), layout.pad_to_align().size()),
            };
            (idx, state, layout)
        })
        .collect::<Vec<_>>();

    states.sort_by_key(|(_, _, (align, _))| std::cmp::Reverse(*align));

    let mut locs = vec![AggrStateLoc::Bool(0, 0); states.len()];
    let mut acc = 0;
    let mut max_align = 0;
    for (idx, state, (align, size)) in states {
        max_align = max_align.max(align);
        let offset = acc;
        acc += size;
        locs[idx] = match state {
            AggrStateType::Bool => AggrStateLoc::Bool(idx, offset),
            AggrStateType::Custom(_) => AggrStateLoc::Custom(idx, offset),
        };
    }

    let layout = Layout::from_size_align(acc, max_align).unwrap();

    (layout, locs)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;

    use super::*;

    prop_compose! {
        fn arb_state_type()(size in 1..100_usize, align in 0..5_u8) -> AggrStateType {
            let layout = Layout::from_size_align(size, 1 << align).unwrap();
            AggrStateType::Custom(layout)
        }
    }

    #[test]
    fn test_sort_states_empty_and_boolean_fields() {
        let (layout, locs) = sort_states(vec![]);
        assert_eq!(layout.size(), 0);
        assert_eq!(layout.align(), 1);
        assert!(locs.is_empty());

        let (layout, locs) = sort_states(vec![
            AggrStateType::Bool,
            AggrStateType::Custom(Layout::new::<u64>()),
            AggrStateType::Bool,
        ]);
        assert_eq!(layout.align(), 8);
        assert_eq!(layout.size(), 10);
        assert_eq!(locs[0].offset(), 8);
        assert_eq!(locs[1].offset(), 0);
        assert_eq!(locs[2].offset(), 9);
        assert_eq!(
            locs.iter().map(AggrStateLoc::index).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    }

    #[test]
    fn test_state_set_visits_typed_values_with_selection() -> Result<()> {
        let (layout, locs) = sort_states(vec![
            AggrStateType::Custom(Layout::new::<u64>()),
            AggrStateType::Custom(Layout::new::<u64>()),
        ]);
        let arena = Bump::new();
        let first: StateAddr = arena.alloc_layout(layout).into();
        let second: StateAddr = arena.alloc_layout(layout).into();
        let state_loc = std::slice::from_ref(&locs[1]);
        let offset = state_loc[0].offset();
        let first_offset = locs[0].offset();
        first.next(first_offset).write_state(0_u64);
        second.next(first_offset).write_state(0_u64);
        first.next(offset).write_state(0_u64);
        second.next(offset).write_state(0_u64);

        let places = [first, second, first];
        let selection = Bitmap::from([true, false, true]);
        AggregateStateSet::new(&places, state_loc).try_for_each_state_value::<u64, _>(
            [1_u64, 2, 3],
            Some(&selection),
            |state, value| {
                *state += value;
                Ok(())
            },
        )?;
        assert_eq!(*first.next(offset).get::<u64>(), 4);
        assert_eq!(*second.next(offset).get::<u64>(), 0);

        AggregateStateSet::new(&[first, second], state_loc).for_each_state_value::<u64, _>(
            [5_u64, 6],
            None,
            |state, value| *state += value,
        )?;
        assert_eq!(*first.next(offset).get::<u64>(), 9);
        assert_eq!(*second.next(offset).get::<u64>(), 6);

        let selection = Bitmap::from([false, true]);
        AggregateStateSet::new(&[first, second], state_loc)
            .for_each_state::<u64>(Some(&selection), |state| *state += 10)?;
        assert_eq!(*first.next(offset).get::<u64>(), 9);
        assert_eq!(*second.next(offset).get::<u64>(), 16);

        AggregateStateSet::new(&[first], &locs).for_each_first_state_value::<u64, _>(
            [7_u64],
            None,
            |state, value| *state += value,
        )?;
        AggregateStateSet::new(&[first], &locs)
            .for_each_first_state::<u64>(None, |state| *state += 1)?;
        assert_eq!(*first.next(first_offset).get::<u64>(), 8);
        assert_eq!(*first.next(offset).get::<u64>(), 9);

        let (flag_layout, flag_locs) = sort_states(vec![
            AggrStateType::Custom(Layout::new::<u64>()),
            AggrStateType::Bool,
        ]);
        let flag_state: StateAddr = arena.alloc_layout(flag_layout).into();
        let flag_offset = flag_locs[1].offset();
        flag_state.next(flag_offset).write_state(0_u8);
        let flag_places = [flag_state];
        let flag_states = AggregateStateSet::new(&flag_places, &flag_locs);
        flag_states.mark_last_flag(None)?;
        assert_eq!(*flag_state.next(flag_offset).get::<u8>(), 1);
        let mut flag_builder = ColumnBuilder::with_capacity(&DataType::Boolean, 1);
        flag_states.serialize_last_flag(&mut flag_builder)?;
        assert_eq!(
            flag_builder.build().index(0),
            Some(ScalarRef::Boolean(true))
        );

        assert!(
            AggregateStateSet::new(&[first], &locs)
                .try_for_each_state_value::<u64, _>([1_u64], None, |_, _| Ok(()))
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn test_sort_states() {
        let mut runner = TestRunner::default();
        let input_s = prop::collection::vec(arb_state_type(), 1..20);

        for _ in 0..100 {
            let input = input_s.new_tree(&mut runner).unwrap().current();
            run_sort_states(input);
        }
    }

    fn check_offset(layout: &Layout, offset: usize) -> bool {
        let align = layout.align();
        offset & (align - 1) == 0
    }

    fn run_sort_states(input: Vec<AggrStateType>) {
        let (layout, locs) = sort_states(input.clone());

        let is_aligned = input
            .iter()
            .zip(locs.iter())
            .all(|(state, loc)| match state {
                AggrStateType::Custom(layout) => check_offset(layout, loc.offset()),
                _ => unreachable!(),
            });

        assert!(is_aligned, "states are not aligned, input: {input:?}");

        let size = layout.size();
        let mut memory = vec![false; size];
        for (state, loc) in input.iter().zip(locs.iter()) {
            match state {
                AggrStateType::Custom(layout) => {
                    let start = loc.offset();
                    let end = start + layout.size();
                    for memory in &mut memory[start..end] {
                        assert!(!*memory, "layout is overlap, input: {input:?}");
                        *memory = true;
                    }
                }
                _ => unreachable!(),
            }
        }
    }
}
