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

//! Production decimal result sizing and admission decisions, isolated from
//! execution and storage. Verus checks these same implementations against
//! mathematical range contracts; correctness of the coefficient arithmetic
//! kernels is outside this proof.

#[cfg(not(verus_keep_ghost))]
use databend_common_expression::types::decimal::DecimalSize;
use verus_builtin_macros::proof;
use verus_builtin_macros::verus;
use verus_builtin_macros::verus_spec;
use verus_builtin_macros::verus_verify;
#[cfg(verus_keep_ghost)]
use vstd::arithmetic::div_mod::lemma_fundamental_div_mod;
#[cfg(verus_keep_ghost)]
use vstd::arithmetic::div_mod::lemma_mod_bound;
#[cfg(verus_keep_ghost)]
use vstd::arithmetic::power::lemma_pow_adds;
#[cfg(verus_keep_ghost)]
use vstd::arithmetic::power::lemma_pow_increases;
#[cfg(verus_keep_ghost)]
use vstd::arithmetic::power::lemma_pow_positive;
#[cfg(verus_keep_ghost)]
use vstd::arithmetic::power::pow;
#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg(verus_keep_ghost)]
use super::DecimalSize;

// The decisions below spell storage capacities as 18/38/76.
#[cfg(not(verus_keep_ghost))]
const _: () = {
    use databend_common_expression::types::decimal::Decimal;
    use databend_common_expression::types::decimal::i256;
    assert!(i64::MAX_PRECISION == 18 && i128::MAX_PRECISION == 38 && i256::MAX_PRECISION == 76);
};

// Shared binding metadata.

/// Sizes of one bound decimal binary operation, derived once by the factory.
#[verus_verify]
#[derive(Clone, Copy, Debug)]
pub(super) struct BinaryBinding {
    /// Argument sizes as declared by the signature.
    pub(super) original: [DecimalSize; 2],
    /// Operand sizes the computation is bound to.
    pub(super) left: DecimalSize,
    pub(super) right: DecimalSize,
    pub(super) result: DecimalSize,
}

#[verus_verify]
impl BinaryBinding {
    /// Both converted operands share one precision, hence one computation
    /// storage type.
    #[verus_spec(out => ensures out == self.left)]
    pub(super) fn compute(&self) -> DecimalSize {
        self.left
    }
}

#[verus_spec(out =>
    ensures out == storage(precision as nat),
)]
pub(super) fn storage_precision(precision: u8) -> u8 {
    if precision <= 18 {
        18
    } else if precision <= 38 {
        38
    } else {
        76
    }
}

verus! {

pub(super) open spec fn max_nat(a: nat, b: nat) -> nat {
    if a >= b {
        a
    } else {
        b
    }
}

pub(super) open spec fn max_scale(a: DecimalSize, b: DecimalSize) -> nat {
    max_nat(a.scale as nat, b.scale as nat)
}

pub(super) open spec fn storage(p: nat) -> nat {
    if p <= 18 {
        18
    } else if p <= 38 {
        38
    } else {
        76
    }
}

// Every size is legal, and both converted operands share one computation
// precision that holds each argument and the result.
pub(super) open spec fn valid_binding(d: BinaryBinding) -> bool {
    let [a, b] = d.original;
    legal(a) && legal(b) && legal(d.result) && legal(d.left) && legal(d.right) && d.left.precision
        == d.right.precision && max_nat(
        max_nat(a.precision as nat, b.precision as nat),
        d.result.precision as nat,
    ) <= d.left.precision
}

pub(super) open spec fn coefficients_fit(a: int, b: int, d: BinaryBinding) -> bool {
    coefficient(a, d.original[0].precision as nat) && coefficient(b, d.original[1].precision as nat)
}

pub(super) open spec fn legal(s: DecimalSize) -> bool {
    1 <= s.precision <= 76 && s.scale <= s.precision
}

// Per-operation binding metadata: ensured by result sizing, required by the
// plan selectors.
pub(super) open spec fn add_subtract_binding(d: BinaryBinding) -> bool {
    valid_binding(d) && d.result.scale == max_scale(d.original[0], d.original[1]) && d.result.scale
        == max_scale(d.left, d.right)
}

pub(super) open spec fn multiply_binding(d: BinaryBinding) -> bool {
    valid_binding(d) && max_scale(d.original[0], d.original[1]) <= d.result.scale
        <= d.original[0].scale + d.original[1].scale
}

pub(super) open spec fn divide_binding(d: BinaryBinding) -> bool {
    valid_binding(d) && d.original[0].scale <= d.result.scale
}

pub(super) open spec fn modulo_binding(d: BinaryBinding) -> bool {
    valid_binding(d) && d.result.scale == max_scale(d.left, d.right)
}

} // verus!

// Result sizing. Precision and scale rules follow Snowflake:
// https://docs.snowflake.com/sql-reference/operators-arithmetic

// A legal decimal signature always has bound input/result sizes. Sizing rule
// violations are internal errors, never an overload miss.
#[verus_spec(out =>
    requires 1 <= precision <= 76, scale <= precision,
    ensures out.precision == precision, out.scale == scale,
)]
fn size(precision: u8, scale: u8) -> DecimalSize {
    match DecimalSize::new(precision, scale) {
        Ok(size) => size,
        Err(_) => unreachable!("internal decimal binding produced Decimal({precision},{scale})"),
    }
}

// If the args both are Decimal128, clamp the precision to 38.
#[verus_spec(out =>
    ensures
        a.precision <= 38 && b.precision <= 38
            ==> out == (if precision <= 38 { precision } else { 38 }),
        !(a.precision <= 38 && b.precision <= 38)
            ==> out == (if precision <= 76 { precision } else { 76 }),
)]
fn clamp_precision(precision: u8, a: DecimalSize, b: DecimalSize) -> u8 {
    if a.precision() <= 38 && b.precision() <= 38 {
        precision.min(38)
    } else {
        precision.min(76)
    }
}

#[verus_spec(out =>
    requires legal(original[0]), legal(original[1]),
    ensures out.original == *original, add_subtract_binding(out),
)]
pub(super) fn add_subtract_size(original: &[DecimalSize; 2]) -> BinaryBinding {
    let [a, b] = *original;
    let scale = a.scale().max(b.scale());
    // for addition/subtraction, we add 1 to the width to ensure we don't overflow
    let required = a.leading_digits().max(b.leading_digits()) + scale + 1;
    let precision = clamp_precision(required, a, b);
    let result = size(precision, scale);
    let (left, right) = if required <= precision {
        (result, result)
    } else {
        (size(precision, a.scale()), size(precision, b.scale()))
    };
    BinaryBinding {
        original: *original,
        left,
        right,
        result,
    }
}

#[verus_spec(out =>
    requires legal(original[0]), legal(original[1]),
    ensures out.original == *original, multiply_binding(out),
)]
pub(super) fn multiply_size(original: &[DecimalSize; 2]) -> BinaryBinding {
    let [a, b] = *original;
    let scale = (a.scale() + b.scale()).min(a.scale().max(b.scale()).max(12));
    let leading = a.leading_digits() + b.leading_digits();
    let precision = clamp_precision(leading + scale, a, b);
    BinaryBinding {
        original: *original,
        left: size(precision, a.scale()),
        right: size(precision, b.scale()),
        result: size(precision, scale),
    }
}

#[verus_spec(out =>
    requires legal(original[0]), legal(original[1]),
    ensures out.original == *original, divide_binding(out),
)]
pub(super) fn divide_size(original: &[DecimalSize; 2]) -> BinaryBinding {
    let [a, b] = *original;
    let scale = a.scale().max((a.scale() + 6).min(12)); // scale must be >= a.scale()
    let leading = a.leading_digits() + b.scale(); // leading must be >= a.leading_digits()
    let precision = clamp_precision(leading + scale, a, b); // so precision must be >= a.precision()
    let compute = precision.max(a.precision()).max(b.precision());
    BinaryBinding {
        original: *original,
        left: size(compute, a.scale()),
        right: size(compute, b.scale()),
        result: size(precision, scale),
    }
}

// Align inside the arithmetic operation: aligned operands can exceed even
// Decimal256 while the remainder fits. Preserve each operand's own scale
// and use the larger input precision independently of the result precision.
#[verus_spec(out =>
    requires legal(original[0]), legal(original[1]),
    ensures out.original == *original, modulo_binding(out),
)]
pub(super) fn modulo_size(original: &[DecimalSize; 2]) -> BinaryBinding {
    let [a, b] = *original;
    let scale = a.scale().max(b.scale());
    let precision = a.precision().max(b.precision());
    let result = size(a.leading_digits().min(b.leading_digits()) + scale, scale);
    let aligned = a.leading_digits().max(b.leading_digits()) + scale;
    let (left, right) = if aligned <= 76 {
        let operands = size(aligned, scale);
        (operands, operands)
    } else {
        (size(precision, a.scale()), size(precision, b.scale()))
    };
    BinaryBinding {
        original: *original,
        left,
        right,
        result,
    }
}

// Alignment and add/subtract decisions.
/// Which operand is scaled up to the result scale, and by how many digits.
/// The result scale is the larger operand scale, so the other operand never
/// moves.
#[verus_verify]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Alignment {
    Left(u32),
    Right(u32),
}

#[verus_verify]
impl Alignment {
    #[verus_spec(
        requires binding.result.scale == max_scale(binding.left, binding.right),
    )]
    pub(super) fn new(binding: &BinaryBinding) -> Option<Self> {
        let scale = binding.result.scale();
        match (scale - binding.left.scale(), scale - binding.right.scale()) {
            (0, 0) => None,
            (shift, 0) => Some(Self::Left(shift as u32)),
            (0, shift) => Some(Self::Right(shift as u32)),
            _ => unreachable!("internal alignment scales both operands of {binding:?}"),
        }
    }
}

#[verus_verify]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AddSubtractPlan {
    /// The unclamped result precision has room for the carry.
    Native,
    /// Same-scale operands whose result precision was clamped.
    Checked,
    Aligned(Alignment),
}

#[verus_verify]
impl AddSubtractPlan {
    #[verus_spec(out =>
        requires add_subtract_binding(*binding),
        ensures add_safe(out, *binding),
    )]
    pub(super) fn new(binding: &BinaryBinding) -> Self {
        let [a, b] = binding.original;
        let result = binding.result;
        if a.leading_digits().max(b.leading_digits()) + result.scale() < result.precision() {
            proof! { prove_add_safe(*binding); }
            return Self::Native;
        }
        match Alignment::new(binding) {
            None => Self::Checked,
            Some(alignment) => Self::Aligned(alignment),
        }
    }
}

verus! {

pub(super) open spec fn add_limits(a: int, b: int, d: BinaryBinding) -> bool {
    let x = a * ten((d.result.scale - d.original[0].scale) as nat);
    let y = b * ten((d.result.scale - d.original[1].scale) as nat);
    let p = d.result.precision as nat;
    let c = storage(p);
    fits_machine(x, c) && fits_machine(y, c) && coefficient(x + y, p) && coefficient(x - y, p)
        && fits_machine(x + y, c) && fits_machine(x - y, c)
}

pub(super) open spec fn add_safe(plan: AddSubtractPlan, d: BinaryBinding) -> bool {
    matches!(plan, AddSubtractPlan::Native) ==> forall|a: int, b: int| #[trigger]
        coefficients_fit(a, b, d) ==> add_limits(a, b, d)
}

pub(super) proof fn prove_add_safe(d: BinaryBinding)
    requires
        add_subtract_binding(d),
        max_nat(
            (d.original[0].precision - d.original[0].scale) as nat,
            (d.original[1].precision - d.original[1].scale) as nat,
        ) + d.result.scale < d.result.precision,
    ensures
        add_safe(AddSubtractPlan::Native, d),
{
    assert forall|a: int, b: int| #[trigger] coefficients_fit(a, b, d) implies add_limits(
        a,
        b,
        d,
    ) by {
        add_subtract_sound(
            a,
            b,
            d.original[0].precision as nat,
            d.original[0].scale as nat,
            d.original[1].precision as nat,
            d.original[1].scale as nat,
            d.result.scale as nat,
            d.result.precision as nat,
            storage(d.result.precision as nat),
        );
    }
}

// Native means one full carry digit remains after aligning both operands.
// Proves both operations, both scaled operands, decimal precision and storage.
pub(super) proof fn add_subtract_sound(
    a: int,
    b: int,
    pa: nat,
    sa: nat,
    pb: nat,
    sb: nat,
    s: nat,
    p: nat,
    c: nat,
)
    requires
        1 <= pa <= 76,
        sa <= pa,
        1 <= pb <= 76,
        sb <= pb,
        s == max_nat(sa, sb),
        p <= c,
        capacity(c),
        coefficient(a, pa),
        coefficient(b, pb),
        max_nat((pa - sa) as nat, (pb - sb) as nat) + s < p,
    ensures
        fits_machine(a * ten((s - sa) as nat), c),
        fits_machine(b * ten((s - sb) as nat), c),
        coefficient(a * ten((s - sa) as nat) + b * ten((s - sb) as nat), p),
        coefficient(a * ten((s - sa) as nat) - b * ten((s - sb) as nat), p),
        fits_machine(a * ten((s - sa) as nat) + b * ten((s - sb) as nat), c),
        fits_machine(a * ten((s - sa) as nat) - b * ten((s - sb) as nat), c),
{
    let k = max_nat((pa - sa) as nat, (pb - sb) as nat) + s;
    scale_coefficient(a, pa, (s - sa) as nat);
    scale_coefficient(b, pb, (s - sb) as nat);
    lemma_pow_increases(10, (pa + s - sa) as nat, k);
    lemma_pow_increases(10, (pb + s - sb) as nat, k);
    lemma_pow_increases(10, k + 1, p);
    lemma_pow_adds(10, k, 1);
    reveal(pow);
    assert(ten(1) == 10) by (compute);
    magnitude_sum(a * ten((s - sa) as nat), b * ten((s - sb) as nat));
    powers(p, c);
}

} // verus!

// Multiplication decisions and runtime product admission.
#[verus_verify]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MultiplyPlan {
    Integral,
    Rounded(u32),
    CheckedIntegral,
    CheckedRounded(u32),
}

#[verus_verify]
impl MultiplyPlan {
    #[verus_spec(out =>
        requires multiply_binding(*binding),
        ensures multiply_safe(out, *binding),
    )]
    pub(super) fn new(binding: &BinaryBinding) -> Self {
        let [a, b] = binding.original;
        let result = binding.result;
        let shift = a.scale() + b.scale() - result.scale();
        let product = a.precision() + b.precision();
        let direct = product <= storage_precision(result.precision())
            && product - shift <= result.precision();
        proof! {
            if direct { prove_multiply_safe(*binding); }
        }
        match (shift as u32, direct) {
            (0, true) => Self::Integral,
            (0, false) => Self::CheckedIntegral,
            (shift, true) => Self::Rounded(shift),
            (shift, false) => Self::CheckedRounded(shift),
        }
    }
}

// Runtime admission is also a decision, not the multiplication implementation.
#[verus_spec(out =>
    requires 0 <= budget,
    ensures out <==> mag(a as int * b as int) <= budget,
)]
#[inline]
pub(super) fn can_mul128_with_budget(a: i128, b: i128, budget: i128) -> bool {
    if a == 0 || b == 0 {
        proof! { magnitude_product(a as int, b as int); }
        return true;
    }
    if a == i128::MIN || b == i128::MIN {
        proof! {
            magnitude_product(a as int, b as int);
            assert(mag(a as int * b as int) > budget) by (nonlinear_arith)
                requires
                    mag(a as int * b as int) == mag(a as int) * mag(b as int),
                    mag(a as int) >= 1, mag(b as int) >= 1,
                    mag(a as int) > budget || mag(b as int) > budget,
            ;
        }
        return false;
    }
    let magnitude_a = if a < 0 { -a } else { a };
    let magnitude_b = if b < 0 { -b } else { b };
    proof! {
        magnitude_product(a as int, b as int);
        product_budget(magnitude_a as int, magnitude_b as int, budget as int);
    }
    magnitude_a <= budget / magnitude_b
}

verus! {

pub(super) open spec fn multiply_limits(a: int, b: int, d: BinaryBinding) -> bool {
    let shift = (d.original[0].scale + d.original[1].scale - d.result.scale) as nat;
    let half = ten(shift) / 2;
    let p = d.result.precision as nat;
    let c = storage(p);
    fits_machine(a * b, c) && fits_machine(a * b + half, c) && fits_machine(a * b - half, c)
        && coefficient((mag(a * b) + half) / ten(shift), p)
}

pub(super) open spec fn multiply_safe(plan: MultiplyPlan, d: BinaryBinding) -> bool {
    let shift = d.original[0].scale + d.original[1].scale - d.result.scale;
    (match plan {
        MultiplyPlan::Integral | MultiplyPlan::CheckedIntegral => shift == 0,
        MultiplyPlan::Rounded(s) | MultiplyPlan::CheckedRounded(s) => s == shift && s > 0,
    }) && ((matches!(plan, MultiplyPlan::Integral) || matches!(plan, MultiplyPlan::Rounded(_)))
        ==> forall|a: int, b: int| #[trigger]
        coefficients_fit(a, b, d) ==> multiply_limits(a, b, d))
}

pub(super) proof fn prove_multiply_safe(d: BinaryBinding)
    requires
        multiply_binding(d),
        d.original[0].precision + d.original[1].precision <= storage(d.result.precision as nat),
        d.original[0].precision + d.original[1].precision - (d.original[0].scale
            + d.original[1].scale - d.result.scale) <= d.result.precision,
    ensures
        forall|a: int, b: int| #[trigger] coefficients_fit(a, b, d) ==> multiply_limits(a, b, d),
{
    assert forall|a: int, b: int| #[trigger] coefficients_fit(a, b, d) implies multiply_limits(
        a,
        b,
        d,
    ) by {
        multiply_sound(
            a,
            b,
            d.original[0].precision as nat,
            d.original[1].precision as nat,
            (d.original[0].scale + d.original[1].scale - d.result.scale) as nat,
            d.result.precision as nat,
            storage(d.result.precision as nat),
        );
    }
}

// The caller supplies a result scale at least max(sa, sb), hence
// shift <= min(sa, sb) <= min(pa, pb). That constraint is important: it pays
// for the rounding half inside the gap between (10^pa-1)(10^pb-1) and 10^(pa+pb).
pub(super) proof fn multiply_sound(a: int, b: int, pa: nat, pb: nat, shift: nat, p: nat, c: nat)
    requires
        1 <= pa <= 76,
        1 <= pb <= 76,
        shift <= pa,
        shift <= pb,
        p <= c,
        capacity(c),
        coefficient(a, pa),
        coefficient(b, pb),
        pa + pb <= c,
        pa + pb - shift <= p,
    ensures
        fits_machine(a * b, c),
        fits_machine(a * b + ten(shift) / 2, c),
        fits_machine(a * b - ten(shift) / 2, c),
        coefficient((mag(a * b) + ten(shift) / 2) / ten(shift), p),
{
    let aa = mag(a);
    let bb = mag(b);
    let x = ten(pa);
    let y = ten(pb);
    let d = ten(shift);
    lemma_pow_positive(10, pa);
    lemma_pow_positive(10, pb);
    lemma_pow_positive(10, shift);
    lemma_pow_adds(10, pa, pb);
    lemma_pow_increases(10, shift, pa);
    lemma_pow_increases(10, shift, pb);
    lemma_fundamental_div_mod(d, 2);
    lemma_mod_bound(d, 2);
    magnitude_product(a, b);
    assert(aa * bb + d / 2 < x * y) by (nonlinear_arith)
        requires
            0 <= aa < x,
            0 <= bb < y,
            1 <= d <= x,
            d <= y,
            0 <= d / 2 <= d,
            x >= 1,
            y >= 1,
    ;
    magnitude_sum(a * b, d / 2);
    lemma_pow_adds(10, (pa + pb - shift) as nat, shift);
    lemma_pow_increases(10, (pa + pb - shift) as nat, p);
    lemma_pow_positive(10, (pa + pb - shift) as nat);
    quotient_bound(aa * bb + d / 2, d, ten((pa + pb - shift) as nat));
    powers((pa + pb) as nat, c);
    powers(p, c);
}

pub(super) proof fn product_budget(a: int, b: int, budget: int)
    requires
        a > 0,
        b > 0,
        budget >= 0,
    ensures
        (a <= budget / b) <==> a * b <= budget,
{
    lemma_fundamental_div_mod(budget, b);
    lemma_mod_bound(budget, b);
    assert(budget == (budget / b) * b + budget % b) by (nonlinear_arith)
        requires
            budget == b * (budget / b) + budget % b,
    ;
    assert((a <= budget / b) <==> a * b <= budget) by (nonlinear_arith)
        requires
            a > 0,
            b > 0,
            budget >= 0,
            budget == (budget / b) * b + budget % b,
            0 <= budget % b < b,
    ;
}

} // verus!

// Division decisions.
#[verus_verify]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DividePlan {
    Native64 { shift: u32 },
    Native128 { shift: u32 },
    Checked { shift: u32 },
}

#[verus_verify]
impl DividePlan {
    #[verus_spec(out =>
        requires divide_binding(*binding),
        ensures divide_safe(out, *binding),
    )]
    pub(super) fn new(binding: &BinaryBinding) -> Self {
        let [a, b] = binding.original;
        let result = binding.result;
        let shift = b.scale() + result.scale() - a.scale();
        // An unclamped quotient precision is exactly a.precision + shift, so
        // only a clamped (38 or 76 digit) result is ever protected.
        let direct = a.precision() + shift <= result.precision();
        let shift = shift as u32;
        if direct && result.precision() <= 18 {
            proof! { prove_divide64_safe(*binding); }
            Self::Native64 { shift }
        } else if direct && binding.compute().precision() <= 38 {
            proof! { prove_divide128_safe(*binding); }
            Self::Native128 { shift }
        } else {
            Self::Checked { shift }
        }
    }
}

verus! {

pub(super) open spec fn divide_limits(a: int, b: int, d: BinaryBinding, c: nat) -> bool {
    let n = a * ten((d.original[1].scale + d.result.scale - d.original[0].scale) as nat);
    fits_machine(n, c) && -machine_max(c) <= n + mag(b) / 2 <= machine_max(c) && -machine_max(c)
        <= n - mag(b) / 2 <= machine_max(c) && coefficient(
        (mag(n) + mag(b) / 2) / mag(b),
        d.result.precision as nat,
    )
}

pub(super) open spec fn divide_safe(plan: DividePlan, d: BinaryBinding) -> bool {
    let shift = d.original[1].scale + d.result.scale - d.original[0].scale;
    match plan {
        DividePlan::Native64 { shift: s } => s == shift && forall|a: int, b: int|
            coefficient(a, d.original[0].precision as nat) && fits_machine(b, 18) && b != 0
                ==> #[trigger] divide_limits(a, b, d, 18),
        DividePlan::Native128 { shift: s } => s == shift && forall|a: int, b: int| #[trigger]
            coefficients_fit(a, b, d) && b != 0 ==> divide_limits(a, b, d, 38),
        DividePlan::Checked { shift: s } => s == shift,
    }
}

pub(super) proof fn prove_divide64_safe(d: BinaryBinding)
    requires
        divide_binding(d),
        d.original[0].precision + d.original[1].scale + d.result.scale - d.original[0].scale
            <= d.result.precision <= 18,
    ensures
        forall|a: int, b: int|
            coefficient(a, d.original[0].precision as nat) && fits_machine(b, 18) && b != 0
                ==> #[trigger] divide_limits(a, b, d, 18),
{
    assert forall|a: int, b: int|
        coefficient(a, d.original[0].precision as nat) && fits_machine(b, 18) && b
            != 0 implies #[trigger] divide_limits(a, b, d, 18) by {
        divide64_sound(
            a,
            b,
            d.original[0].precision as nat,
            (d.original[1].scale + d.result.scale - d.original[0].scale) as nat,
            d.result.precision as nat,
        );
    }
}

pub(super) proof fn prove_divide128_safe(d: BinaryBinding)
    requires
        divide_binding(d),
        d.left.precision <= 38,
        d.original[0].precision + d.original[1].scale + d.result.scale - d.original[0].scale
            <= d.result.precision,
    ensures
        forall|a: int, b: int| #[trigger]
            coefficients_fit(a, b, d) && b != 0 ==> divide_limits(a, b, d, 38),
{
    assert forall|a: int, b: int| #[trigger]
        coefficients_fit(a, b, d) && b != 0 implies divide_limits(a, b, d, 38) by {
        divide128_sound(
            a,
            b,
            d.original[0].precision as nat,
            d.original[1].precision as nat,
            (d.original[1].scale + d.result.scale - d.original[0].scale) as nat,
            d.result.precision as nat,
        );
    }
}

// Native64 has a saturating divisor view, so even i64::MIN is admissible.
// An adjusted numerator stays strictly above MIN: neither wrapping nor MIN/-1
// is needed for visible rows. Wide-divisor saturation correctness is separate.
pub(super) proof fn divide64_sound(a: int, b: int, pa: nat, shift: nat, p: nat)
    requires
        1 <= pa <= 18,
        p <= 18,
        coefficient(a, pa),
        fits_machine(b, 18),
        b != 0,
        pa + shift <= p,
    ensures
        fits_machine(a * ten(shift), 18),
        -machine_max(18) <= a * ten(shift) + mag(b) / 2 <= machine_max(18),
        -machine_max(18) <= a * ten(shift) - mag(b) / 2 <= machine_max(18),
        coefficient((mag(a * ten(shift)) + mag(b) / 2) / mag(b), p),
{
    scale_coefficient(a, pa, shift);
    powers((pa + shift) as nat, 18);
    powers(p, 18);
    lemma_pow_increases(10, (pa + shift) as nat, p);
    lemma_fundamental_div_mod(mag(b), 2);
    lemma_mod_bound(mag(b), 2);
    rounded_quotient_bound(mag(a * ten(shift)), mag(b));
}

// Native128: both operands are read in <=128-bit computation storage, even if
// the result itself is narrower. Include the divisor's half in the width proof.
pub(super) proof fn divide128_sound(a: int, b: int, pa: nat, pb: nat, shift: nat, p: nat)
    requires
        1 <= pa <= 38,
        1 <= pb <= 38,
        p <= 38,
        coefficient(a, pa),
        coefficient(b, pb),
        b != 0,
        pa + shift <= p,
    ensures
        fits_machine(a * ten(shift), 38),
        -machine_max(38) <= a * ten(shift) + mag(b) / 2 <= machine_max(38),
        -machine_max(38) <= a * ten(shift) - mag(b) / 2 <= machine_max(38),
        coefficient((mag(a * ten(shift)) + mag(b) / 2) / mag(b), p),
{
    scale_coefficient(a, pa, shift);
    powers((pa + shift) as nat, 38);
    powers(p, 38);
    powers(pb, 38);
    lemma_pow_increases(10, (pa + shift) as nat, p);
    lemma_fundamental_div_mod(mag(b), 2);
    lemma_mod_bound(mag(b), 2);
    magnitude_sum(a * ten(shift), mag(b) / 2);
    rounded_quotient_bound(mag(a * ten(shift)), mag(b));
}

// Rounded division cannot increase an integer numerator's magnitude for |d|>=1.
pub(super) proof fn rounded_quotient_bound(n: int, d: int)
    requires
        0 <= n,
        1 <= d,
    ensures
        0 <= (n + d / 2) / d <= n,
{
    lemma_fundamental_div_mod(d, 2);
    lemma_mod_bound(d, 2);
    assert(n + d / 2 < (n + 1) * d) by (nonlinear_arith)
        requires
            n >= 0,
            d >= 1,
            0 <= d / 2 < d,
    ;
    quotient_bound(n + d / 2, d, n + 1);
}

} // verus!

// Mathematical range facts shared by the plans.
verus! {

pub(super) open spec fn ten(p: nat) -> int {
    pow(10, p)
}

pub(super) open spec fn mag(x: int) -> int {
    if x < 0 {
        -x
    } else {
        x
    }
}

pub(super) open spec fn coefficient(x: int, p: nat) -> bool {
    mag(x) < ten(p)
}

pub(super) open spec fn capacity(c: nat) -> bool {
    c == 18 || c == 38 || c == 76
}

pub(super) open spec fn machine_max(c: nat) -> int {
    if c == 18 {
        9_223_372_036_854_775_807
    } else if c == 38 {
        170_141_183_460_469_231_731_687_303_715_884_105_727
    } else {
        57_896_044_618_658_097_711_785_492_504_343_953_926_634_992_332_820_282_019_728_792_003_956_564_819_967int
    }
}

pub(super) open spec fn storage_power(c: nat) -> int {
    if c == 18 {
        1_000_000_000_000_000_000
    } else if c == 38 {
        100_000_000_000_000_000_000_000_000_000_000_000_000
    } else {
        10_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000int
    }
}

pub(super) open spec fn fits_machine(x: int, c: nat) -> bool {
    -machine_max(c) - 1 <= x <= machine_max(c)
}

pub(super) proof fn powers(p: nat, c: nat)
    requires
        p <= c,
        capacity(c),
    ensures
        1 <= ten(p) <= ten(c),
        ten(c) == storage_power(c),
        ten(c) <= machine_max(c),
{
    reveal(pow);
    lemma_pow_positive(10, p);
    lemma_pow_increases(10, p, c);
    if c == 18 {
        assert(ten(18) == 1_000_000_000_000_000_000) by (compute);
    } else if c == 38 {
        assert(ten(38) == 100_000_000_000_000_000_000_000_000_000_000_000_000) by (compute);
    } else {
        assert(ten(76)
            == 10_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000_000int)
            by (compute);
    }
}

pub(super) proof fn magnitude_product(a: int, b: int)
    ensures
        mag(a * b) == mag(a) * mag(b),
{
    assert(mag(a * b) == mag(a) * mag(b)) by (nonlinear_arith);
}

pub(super) proof fn magnitude_sum(a: int, b: int)
    ensures
        mag(a + b) <= mag(a) + mag(b),
        mag(a - b) <= mag(a) + mag(b),
{
}

// Decimal alignment: proof of the input-conversion bound used by Native add/sub.
pub(super) proof fn scale_coefficient(a: int, p: nat, shift: nat)
    requires
        coefficient(a, p),
    ensures
        coefficient(a * ten(shift), p + shift),
{
    lemma_pow_positive(10, p);
    lemma_pow_positive(10, shift);
    lemma_pow_adds(10, p, shift);
    magnitude_product(a, ten(shift));
    assert(mag(a) * ten(shift) < ten(p) * ten(shift)) by (nonlinear_arith)
        requires
            0 <= mag(a) < ten(p),
            ten(shift) > 0,
    ;
}

// Positive divisor arithmetic. This uses mathematical floor only on magnitudes;
// signed Rust / truncates toward zero, so its result magnitude is n / d.
pub(super) proof fn quotient_bound(n: int, d: int, limit: int)
    requires
        0 <= n,
        0 < d,
        0 < limit,
        n < limit * d,
    ensures
        0 <= n / d < limit,
{
    lemma_fundamental_div_mod(n, d);
    lemma_mod_bound(n, d);
    assert(n == (n / d) * d + n % d) by (nonlinear_arith)
        requires
            n == d * (n / d) + n % d,
    ;
    assert(0 <= n / d < limit) by (nonlinear_arith)
        requires
            n == (n / d) * d + n % d,
            0 <= n % d < d,
            n >= 0,
            d > 0,
            limit > 0,
            n < limit * d,
    ;
}

} // verus!
