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

//! Differential test of scan pruning: a scan may skip blocks, or skip evaluating its filter on
//! them, from what their statistics tell about the filter. Whatever it skips, it must select
//! exactly the rows where the filter is true.
//!
//! For each generated filter `F`, the rows of `SELECT .. WHERE F`, in several scan
//! configurations, are compared with the rows where `F` evaluates to true when no filter is
//! pushed to the scan: `SELECT count_if(F), sum(if(F, id, null))`. The ids are unique, so the
//! sum tells which rows are selected. Filters that fail to evaluate are skipped.
//!
//! Each block of the tables is one insert, so that the statistics of the blocks have the
//! shapes that pruning decides on: one value, a narrow range, nulls, only nulls, NaN and
//! infinities, long strings with truncated statistics, the extremes of the types, columns
//! added after the block was written, empty arrays and maps.
//!
//! The filters are generated from a fixed seed. Those of a kind that is known to prune wrongly
//! are kept apart in ignored tests, see [`Known`]: fixing a kind is checked by running its
//! test, which is then enabled.
//!
//! The test checks a sample of the filters. To check more of them, or others:
//!
//! ```text
//! PRUNING_DIFFERENTIAL_ATOMS=5000 PRUNING_DIFFERENTIAL_SEED=7 \
//!     cargo test -p databend-query --test it pruning_differential
//! ```

use std::collections::BTreeMap;

use databend_common_base::runtime::CatchUnwindFuture;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_query::test_kits::*;
use futures::StreamExt;
use futures::TryStreamExt;

/// The kinds of filters that prune wrongly today: #21216, and #21166 for
/// [`Known::EmptyCollection`].
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Known {
    /// `CAST(column AS type) = constant` is folded as `column = constant'`, with the constant
    /// converted to the type of the column, also when the conversion changes it.
    CastComparison,
    /// The domains of `-x`, `x + y`, `x - y` and `x * y` leave out the NaN that NaN and
    /// infinite floats produce.
    FloatArithmetic,
    /// The statistics of a block where every array or map is empty read as elements that are
    /// all null.
    EmptyCollection,
    /// Comparisons of variants: `IS DISTINCT FROM` assumes that the comparison of two values
    /// is not null, and a variant compares with a number by a `TRY_CAST`.
    VariantComparison,
}

/// A filter, and the kind it is of if that kind prunes wrongly.
#[derive(Clone)]
struct Case {
    filter: String,
    known: Option<Known>,
}

/// SplitMix64: the filters are the same on every run.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }

    fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            items.swap(i, self.below(i + 1));
        }
    }
}

/// What an expression evaluates to, as far as comparing it with a constant goes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Int,
    F32,
    F64,
    Dec,
    Str,
    Bool,
    Date,
    Ts,
    Variant,
    Other,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConstKind {
    Int,
    /// A decimal literal with a fraction.
    Frac,
    Float,
    Str,
    Bool,
    Date,
    Ts,
    Null,
}

type Const = (&'static str, ConstKind);

/// What a form over a column evaluates to.
#[derive(Clone, Copy)]
enum Res {
    /// The kind of the column.
    Same,
    To(Kind),
    /// A cast of the column: its equality with a constant is a [`Known::CastComparison`].
    Cast(Kind),
    /// The kind of the column, by arithmetic whose domain is wrong for floats.
    Arith,
}

type Form = (&'static str, Res);

const OPS: &[&str] = &["=", "<>", "<", "<=", ">", ">="];

/// Whether `expr = constant` compares without converting the constant to another value: the
/// other comparisons are those of [`Known::CastComparison`].
fn exact_comparison(kind: Kind, constant: ConstKind) -> bool {
    use ConstKind as C;
    matches!(
        (kind, constant),
        (_, C::Null)
            | (Kind::Int | Kind::F32 | Kind::Dec | Kind::Other, C::Int)
            | (Kind::F64, C::Int | C::Float)
            | (Kind::Str, C::Str)
            | (Kind::Bool, C::Bool)
            | (Kind::Date, C::Date)
            | (Kind::Ts, C::Ts | C::Date)
    )
}

const NUM_CONSTS: &[Const] = &[
    ("-3", ConstKind::Int),
    ("0", ConstKind::Int),
    ("1", ConstKind::Int),
    ("2", ConstKind::Int),
    ("3", ConstKind::Int),
    ("100", ConstKind::Int),
    ("127", ConstKind::Int),
    ("128", ConstKind::Int),
    ("255", ConstKind::Int),
    ("256", ConstKind::Int),
    ("-128", ConstKind::Int),
    ("-129", ConstKind::Int),
    ("2147483647", ConstKind::Int),
    ("2147483648", ConstKind::Int),
    ("9223372036854775807", ConstKind::Int),
    ("18446744073709551615", ConstKind::Int),
    ("2::tinyint", ConstKind::Int),
    ("2::bigint", ConstKind::Int),
    ("2.0", ConstKind::Frac),
    ("2.4", ConstKind::Frac),
    ("2.5", ConstKind::Frac),
    ("-0.0", ConstKind::Frac),
    ("2.40::decimal(10,2)", ConstKind::Frac),
    ("2::decimal(38,10)", ConstKind::Frac),
    ("1.5e0", ConstKind::Float),
    ("1e10", ConstKind::Float),
    ("2::double", ConstKind::Float),
    ("2::float", ConstKind::Float),
    ("'nan'::double", ConstKind::Float),
    ("'inf'::double", ConstKind::Float),
    ("'-inf'::double", ConstKind::Float),
    ("'2'", ConstKind::Str),
    ("'02'", ConstKind::Str),
    ("' 2'", ConstKind::Str),
    ("'2.0'", ConstKind::Str),
    ("'+2'", ConstKind::Str),
    ("true", ConstKind::Bool),
    ("null", ConstKind::Null),
];

const NUM_FORMS: &[Form] = &[
    ("{c}", Res::Same),
    ("{c}::string", Res::Cast(Kind::Str)),
    ("{c}::bigint", Res::Cast(Kind::Int)),
    ("{c}::int", Res::Cast(Kind::Int)),
    ("{c}::double", Res::Cast(Kind::F64)),
    ("{c}::float", Res::Cast(Kind::F32)),
    ("{c}::decimal(10,2)", Res::Cast(Kind::Dec)),
    ("{c}::decimal(20,1)", Res::Cast(Kind::Dec)),
    ("try_cast({c} as tinyint)", Res::To(Kind::Int)),
    ("try_cast({c} as tinyint unsigned)", Res::To(Kind::Int)),
    ("{c}::boolean", Res::Cast(Kind::Bool)),
    ("{c} + 1", Res::Same),
    ("{c} - 1", Res::Same),
    ("{c} * 2", Res::Same),
    ("{c} / 2", Res::To(Kind::F64)),
    ("{c} % 3", Res::Same),
    ("{c} div 2", Res::To(Kind::Other)),
    ("-{c}", Res::Arith),
    ("{c} + {c}", Res::Arith),
    ("{c} - {c}", Res::Arith),
    ("{c} * {c}", Res::Arith),
    ("abs({c})", Res::Same),
    ("floor({c})", Res::To(Kind::Other)),
    ("ceil({c})", Res::To(Kind::Other)),
    ("round({c})", Res::To(Kind::Other)),
    ("sign({c})", Res::To(Kind::Other)),
    ("to_string({c})", Res::Cast(Kind::Str)),
    ("{c}::string || 'x'", Res::To(Kind::Str)),
    ("{c}::string::int", Res::To(Kind::Int)),
    ("length({c}::string)", Res::To(Kind::Int)),
    ("coalesce({c}, 2)", Res::Same),
    ("if({c} > 1, {c}, 0)", Res::Same),
    ("{c}::variant", Res::To(Kind::Variant)),
];

const STR_CONSTS: &[Const] = &[
    ("''", ConstKind::Str),
    ("'a'", ConstKind::Str),
    ("'b'", ConstKind::Str),
    ("'2'", ConstKind::Str),
    ("'02'", ConstKind::Str),
    ("' 2'", ConstKind::Str),
    ("'10'", ConstKind::Str),
    ("'z'", ConstKind::Str),
    ("'A'", ConstKind::Str),
    ("'abc'", ConstKind::Str),
    // Around the lengths the string statistics are truncated to.
    ("'aaaaaaaaaaaaaaa'", ConstKind::Str),
    ("'aaaaaaaaaaaaaaaa'", ConstKind::Str),
    ("'aaaaaaaaaaaaaaaaa'", ConstKind::Str),
    ("'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'", ConstKind::Str),
    ("'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'", ConstKind::Str),
    (
        "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab'",
        ConstKind::Str,
    ),
    ("'数据'", ConstKind::Str),
    ("'数'", ConstKind::Str),
    ("'true'", ConstKind::Str),
    ("'2020-01-03'", ConstKind::Str),
    ("0", ConstKind::Int),
    ("1", ConstKind::Int),
    ("2", ConstKind::Int),
    ("40", ConstKind::Int),
    ("41", ConstKind::Int),
    ("null", ConstKind::Null),
];

const STR_FORMS: &[Form] = &[
    ("{c}", Res::Same),
    ("length({c})", Res::To(Kind::Int)),
    ("octet_length({c})", Res::To(Kind::Int)),
    ("char_length({c})", Res::To(Kind::Int)),
    ("bit_length({c})", Res::To(Kind::Int)),
    ("length({c}) + 1", Res::To(Kind::Int)),
    ("ascii({c})", Res::To(Kind::Int)),
    ("position('a' in {c})", Res::To(Kind::Int)),
    ("upper({c})", Res::Same),
    ("lower({c})", Res::Same),
    ("substr({c}, 1, 2)", Res::Same),
    ("substr({c}, 2)", Res::Same),
    ("concat({c}, 'x')", Res::Same),
    ("concat('x', {c})", Res::Same),
    ("trim({c})", Res::Same),
    ("left({c}, 1)", Res::Same),
    ("right({c}, 1)", Res::Same),
    ("reverse({c})", Res::Same),
    ("repeat({c}, 2)", Res::Same),
    ("{c} || {c}", Res::Same),
    ("md5({c})", Res::Same),
    ("coalesce({c}, 'a')", Res::Same),
    ("if({c} = '', 'a', {c})", Res::Same),
    ("try_cast({c} as int)", Res::To(Kind::Int)),
    ("try_cast({c} as double)", Res::To(Kind::F64)),
    ("try_cast({c} as date)", Res::To(Kind::Date)),
    ("{c}::variant", Res::To(Kind::Variant)),
    ("{c} like 'a%'", Res::To(Kind::Bool)),
    ("{c} like '%a'", Res::To(Kind::Bool)),
    ("{c} like '2'", Res::To(Kind::Bool)),
    ("{c} like ''", Res::To(Kind::Bool)),
    ("{c} not like 'a%'", Res::To(Kind::Bool)),
    ("regexp_like({c}, '^a')", Res::To(Kind::Bool)),
];

const TIME_CONSTS: &[Const] = &[
    ("'2020-01-01'", ConstKind::Str),
    ("'2020-01-03'", ConstKind::Str),
    ("'2020-01-03 12:30:00'", ConstKind::Str),
    ("'2020-01-03'::date", ConstKind::Date),
    ("'2019-12-29'::date", ConstKind::Date),
    ("'2020-05-10'::date", ConstKind::Date),
    ("'2020-01-03'::timestamp", ConstKind::Ts),
    ("'2020-01-03 12:00:00'::timestamp", ConstKind::Ts),
    ("'2020-01-03 12:30:00'::timestamp", ConstKind::Ts),
    ("'2020-01-03 00:00:00.000001'::timestamp", ConstKind::Ts),
    ("2020", ConstKind::Int),
    ("202001", ConstKind::Int),
    ("20200103", ConstKind::Int),
    ("3", ConstKind::Int),
    ("null", ConstKind::Null),
];

const TIME_FORMS: &[Form] = &[
    ("{c}", Res::Same),
    ("{c}::timestamp", Res::Cast(Kind::Ts)),
    ("{c}::date", Res::Cast(Kind::Date)),
    ("{c}::string", Res::Cast(Kind::Str)),
    ("to_year({c})", Res::To(Kind::Int)),
    ("to_yyyymm({c})", Res::To(Kind::Int)),
    ("to_yyyymmdd({c})", Res::To(Kind::Int)),
    ("to_month({c})", Res::To(Kind::Int)),
    ("to_day_of_month({c})", Res::To(Kind::Int)),
    ("to_day_of_week({c})", Res::To(Kind::Int)),
    ("{c} + interval 1 day", Res::To(Kind::Other)),
    ("{c} - interval 1 day", Res::To(Kind::Other)),
    ("{c} + interval 12 hour", Res::To(Kind::Other)),
    ("date_add(day, 1, {c})", Res::To(Kind::Other)),
    ("to_start_of_month({c})", Res::To(Kind::Date)),
    ("to_start_of_day({c})", Res::To(Kind::Ts)),
    ("date_trunc(month, {c})", Res::To(Kind::Other)),
    ("{c}::variant", Res::To(Kind::Variant)),
];

/// The atoms that compare the forms of a column with constants.
fn compare_atoms(
    rng: &mut Rng,
    out: &mut Vec<Case>,
    column: &str,
    column_kind: Kind,
    forms: &[Form],
    constants: &[Const],
) {
    for (form, res) in forms {
        let expr = form.replace("{c}", column);
        let (kind, cast, arith) = match res {
            Res::Same => (column_kind, false, false),
            Res::To(kind) => (*kind, false, false),
            Res::Cast(kind) => (*kind, true, false),
            Res::Arith => (column_kind, false, true),
        };
        let known = if kind == Kind::Variant {
            Some(Known::VariantComparison)
        } else if arith && matches!(column_kind, Kind::F32 | Kind::F64) {
            Some(Known::FloatArithmetic)
        } else {
            None
        };
        // Equality of a cast, or with a constant that is converted, is known to prune wrongly.
        let equality = |constants: &[&Const]| {
            let exact = !cast && constants.iter().all(|c| exact_comparison(kind, c.1));
            known.or((!exact).then_some(Known::CastComparison))
        };
        let mut push = |filter: String, known: Option<Known>| out.push(Case { filter, known });
        if kind == Kind::Bool {
            push(expr.clone(), known);
        }
        for _ in 0..5 {
            let (op, c) = (*rng.pick(OPS), rng.pick(constants));
            let known = if op == "=" { equality(&[c]) } else { known };
            push(format!("{expr} {op} {}", c.0), known);
        }
        let (a, b) = (rng.pick(constants), rng.pick(constants));
        push(format!("{expr} = {}", a.0), equality(&[a]));
        push(format!("{expr} in ({}, {})", a.0, b.0), equality(&[a, b]));
        push(
            format!("{expr} not in ({}, {})", a.0, b.0),
            equality(&[a, b]),
        );
        push(format!("{expr} between {} and {}", a.0, b.0), known);
        push(format!("{expr} is null"), known);
        push(format!("{expr} is not null"), known);
        push(format!("{expr} is distinct from {}", a.0), equality(&[a]));
        push(
            format!("{expr} is not distinct from {}", a.0),
            equality(&[a]),
        );
    }
}

const SCALAR_COLUMNS: &[(&str, Kind)] = &[
    ("i8", Kind::Int),
    ("i32", Kind::Int),
    ("i64", Kind::Int),
    ("u8", Kind::Int),
    ("u64", Kind::Int),
    ("f32", Kind::F32),
    ("f64", Kind::F64),
    ("d1", Kind::Dec),
    ("d2", Kind::Dec),
    ("s", Kind::Str),
    ("dt", Kind::Date),
    ("ts", Kind::Ts),
];

/// The atoms over the table `t`.
fn scalar_atoms(rng: &mut Rng) -> Vec<Case> {
    let mut out = vec![];
    for prefix in ["", "n_"] {
        for (name, kind) in SCALAR_COLUMNS {
            let column = format!("{prefix}{name}");
            let (forms, constants) = match kind {
                Kind::Str => (STR_FORMS, STR_CONSTS),
                Kind::Date | Kind::Ts => (TIME_FORMS, TIME_CONSTS),
                _ => (NUM_FORMS, NUM_CONSTS),
            };
            compare_atoms(rng, &mut out, &column, *kind, forms, constants);
        }
        for form in [
            "{c}",
            "not {c}",
            "{c} = true",
            "{c} is null",
            "{c}::int = 1",
        ] {
            out.push(Case {
                filter: form.replace("{c}", &format!("{prefix}b")),
                known: None,
            });
        }
    }
    // Columns that the earlier blocks do not have.
    compare_atoms(rng, &mut out, "late", Kind::Int, NUM_FORMS, NUM_CONSTS);
    compare_atoms(rng, &mut out, "late_n", Kind::Str, STR_FORMS, STR_CONSTS);
    // Columns compared with each other.
    let pairs = [
        ("i8", "i32"),
        ("i32", "f64"),
        ("u8", "i8"),
        ("u64", "i64"),
        ("d1", "i32"),
        ("d1", "d2"),
        ("f32", "f64"),
        ("s", "n_s"),
        ("dt", "ts"),
        ("i32", "n_i32"),
        ("late", "i32"),
    ];
    for (a, b) in pairs {
        for op in OPS {
            out.push(Case {
                filter: format!("{a} {op} {b}"),
                known: None,
            });
        }
    }
    // Floats whose sums, differences and products are NaN: `fa`, `fb` and `fc` hold
    // infinities of both signs and zeros.
    for expr in ["fa + fb", "fa - fa", "fa * fc", "-fa", "fb - fa"] {
        for constant in ["0", "'nan'::double", "'inf'::double"] {
            for op in OPS {
                out.push(Case {
                    filter: format!("{expr} {op} {constant}"),
                    known: Some(Known::FloatArithmetic),
                });
            }
        }
    }
    out
}

/// The atoms over the table `n` of nested columns. In `earr` and `emp`, a block has only
/// empty arrays and maps.
fn nested_atoms() -> Vec<Case> {
    let empty = Some(Known::EmptyCollection);
    let ints = [
        ("tp.1", None),
        ("n_tp.1", None),
        ("arr[1]", None),
        ("arr[2]", None),
        ("n_arr[1]", None),
        ("n_arr[2]", None),
        ("length(arr)", None),
        ("length(n_arr)", None),
        ("array_sum(arr)", None),
        ("array_max(arr)", None),
        ("mp['k']", None),
        ("mp['zz']", None),
        ("map_size(mp)", None),
        ("try_cast(v['a'] as int)", None),
        ("id + _row_id", None),
        ("length(earr)", empty),
        ("earr[1]", empty),
        ("map_size(emp)", empty),
        ("emp['k']", empty),
    ];
    let strings = [
        ("tp.2", None),
        ("n_tp.2", None),
        ("ev[1]", None),
        ("ev[2]", None),
        ("v['b']::string", None),
    ];
    let predicates = [
        ("contains(arr, 2)", None),
        ("arr = [2, 2]", None),
        ("tp = (2, 'a')", None),
        ("n_tp is null", None),
        ("n_arr is null", None),
        ("arr is not null", None),
        ("mp is not null", None),
        ("ev[2] = ''", None),
        ("octet_length(ev[2]) = 0", None),
        ("is_null(v['a'])", None),
        ("bm", None),
        ("earr is not null", empty),
        ("earr is null", empty),
        ("earr = []", empty),
        ("emp is not null", empty),
        ("length(earr) is null", empty),
    ];
    let mut out = vec![];
    let mut compare = |exprs: &[(&str, Option<Known>)], constants: &[&str]| {
        for (expr, known) in exprs {
            for constant in constants {
                for op in OPS {
                    out.push(Case {
                        filter: format!("{expr} {op} {constant}"),
                        known: *known,
                    });
                }
            }
            out.push(Case {
                filter: format!("{expr} is null"),
                known: *known,
            });
        }
    };
    compare(&ints, &["0", "1", "2", "7"]);
    compare(&strings, &["''", "'a'", "'02'", "'zz'"]);
    for (expr, known) in predicates {
        out.push(Case {
            filter: expr.to_string(),
            known,
        });
    }
    out
}

/// The smallest filters of each known kind that prune wrongly: the samples of the atoms always
/// have them, so that the test of a kind fails until the kind is fixed.
fn pinned_atoms() -> Vec<Case> {
    let cast = [
        "i32::string = '02'",
        "i32 = 2.4",
        "f32 = 2.4",
        "dt = '2020-01-03 12:00:00'::timestamp",
    ];
    let float = [
        "-f64 = 2",
        "-fa = 'inf'::double",
        "fa + fb = 'nan'::double",
        "fa - fa = 'nan'::double",
        "fa * fc = 'nan'::double",
    ];
    let variant = ["i64::variant is distinct from 128"];
    let of = |filters: &[&str], known: Known| {
        filters
            .iter()
            .map(|filter| Case {
                filter: filter.to_string(),
                known: Some(known),
            })
            .collect::<Vec<_>>()
    };
    [
        of(&cast, Known::CastComparison),
        of(&float, Known::FloatArithmetic),
        of(&variant, Known::VariantComparison),
    ]
    .concat()
}

/// `pinned` and at most `limit` of the `atoms` of each kind, their negations, and combinations
/// of them.
fn combine(
    rng: &mut Rng,
    pinned: Vec<Case>,
    atoms: Vec<Case>,
    limit: usize,
) -> BTreeMap<Option<Known>, Vec<Case>> {
    let mut by_kind: BTreeMap<Option<Known>, Vec<Case>> = BTreeMap::new();
    for atom in atoms {
        by_kind.entry(atom.known).or_default().push(atom);
    }
    for atoms in by_kind.values_mut() {
        rng.shuffle(atoms);
        atoms.truncate(limit);
    }
    for atom in pinned {
        by_kind.entry(atom.known).or_default().insert(0, atom);
    }
    for (known, atoms) in by_kind.iter_mut() {
        let mut cases = Vec::with_capacity(atoms.len() * 3);
        for atom in atoms.iter() {
            let (a, b) = (&atom.filter, &rng.pick(atoms).filter);
            cases.push(a.clone());
            cases.push(format!("not ({a})"));
            cases.push(match rng.below(8) {
                0 => format!("({a}) and ({b})"),
                1 => format!("({a}) or ({b})"),
                2 => format!("not (({a}) and ({b}))"),
                3 => format!("not (({a}) or ({b}))"),
                4 => format!("coalesce({a}, true)"),
                5 => format!("coalesce({a}, false)"),
                6 => format!("if({a}, false, true)"),
                _ => format!("({a}) = ({b})"),
            });
        }
        *atoms = cases
            .into_iter()
            .map(|filter| Case {
                filter,
                known: *known,
            })
            .collect();
    }
    by_kind
}

/// How the scan that prunes is run: `(name, settings, output)`.
type Scan = (&'static str, &'static str, &'static str);

/// Every filter is checked by a scan that outputs the rows,
const ROWS_SCAN: Scan = ("rows", "", "count(*), sum(id)");

/// and by one of these in turn: without output columns (such a scan may not read the blocks
/// at all), in batches smaller than a block, and with the condition cache, twice, so that the
/// second scan reads what the first recorded.
const OTHER_SCANS: &[&[Scan]] = &[
    &[("count", "", "count(*)")],
    &[(
        "batches",
        "settings (max_block_size = 7) ",
        "count(*), sum(id)",
    )],
    &[
        (
            "condition cache",
            "settings (enable_condition_cache = 1) ",
            "count(*), sum(id)",
        ),
        (
            "condition cache again",
            "settings (enable_condition_cache = 1) ",
            "count(*), sum(id)",
        ),
    ],
];

enum Outcome {
    /// The filter does not evaluate on every row, e.g. a cast overflows.
    Skipped,
    Same,
    Differs(String),
}

struct Tables {
    fixture: TestFixture,
    db: String,
}

impl Tables {
    /// The first row of the result of `sql`, as text. A panic while planning is an error, as
    /// one while executing is.
    async fn first_row(&self, sql: &str) -> Result<Vec<String>> {
        let run = async {
            let blocks: Vec<DataBlock> =
                self.fixture.execute_query(sql).await?.try_collect().await?;
            let block = blocks
                .iter()
                .find(|block| block.num_rows() > 0)
                .ok_or_else(|| ErrorCode::Internal(format!("no row: {sql}")))?;
            Ok((0..block.num_columns())
                .map(|i| format!("{:?}", block.get_by_offset(i).index(0).unwrap()))
                .collect())
        };
        match CatchUnwindFuture::create(run).await {
            Ok(row) => row,
            Err(panic) => Err(ErrorCode::UnwindError(panic.message())),
        }
    }

    /// Whether the scans of `table` with `filter` select the rows where it is true: the scan
    /// of the rows and the `turn`-th of the other scans.
    async fn check(&self, table: &str, filter: &str, turn: usize) -> Outcome {
        let db = &self.db;
        let every_row =
            format!("select count_if({filter}), sum(if({filter}, id, null)) from {db}.{table}");
        let expected = match self.first_row(&every_row).await {
            Ok(expected) => expected,
            // The filters are meant not to panic: a query that panics may not return.
            Err(err) if err.code() == ErrorCode::UNWIND_ERROR => {
                return Outcome::Differs(format!("{filter}\n    panics: {}", err.message()));
            }
            Err(_) => return Outcome::Skipped,
        };
        let mut differences = vec![];
        let others = OTHER_SCANS[turn % OTHER_SCANS.len()];
        for (name, settings, output) in [ROWS_SCAN].iter().chain(others) {
            let sql = format!("{settings}select {output} from {db}.{table} where {filter}");
            match self.first_row(&sql).await {
                Ok(row) if row[..] == expected[..row.len()] => {}
                Ok(row) => differences.push(format!("{name}: {row:?}")),
                Err(err) => differences.push(format!("{name}: {}", err.message())),
            }
        }
        if differences.is_empty() {
            return Outcome::Same;
        }
        Outcome::Differs(format!(
            "{filter}\n    every row evaluated: {expected:?}\n    {}",
            differences.join("\n    ")
        ))
    }

    /// What is wrong with the scans of `table` with the filters of `cases`, if anything: the
    /// filters whose scans do not select the rows where they are true.
    async fn failures(&self, table: &str, cases: &[Case]) -> Vec<String> {
        let outcomes: Vec<Outcome> = futures::stream::iter(cases.iter().enumerate())
            .map(|(turn, case)| self.check(table, &case.filter, turn))
            .buffered(8)
            .collect()
            .await;
        let checked = outcomes
            .iter()
            .filter(|outcome| !matches!(outcome, Outcome::Skipped))
            .count();
        let differ: Vec<&str> = outcomes
            .iter()
            .filter_map(|outcome| match outcome {
                Outcome::Differs(difference) => Some(difference.as_str()),
                _ => None,
            })
            .collect();
        let mut failures = vec![];
        // Most filters must evaluate, or the test checks little.
        if checked * 3 < cases.len() {
            failures.push(format!(
                "only {checked} of {} filters on {table} evaluate",
                cases.len()
            ));
        }
        if !differ.is_empty() {
            failures.push(format!(
                "{} of {checked} filters on {table} select other rows than those where they \
                 are true, or panic:\n{}",
                differ.len(),
                differ[..differ.len().min(40)].join("\n")
            ));
        }
        failures
    }
}

const T_COLUMNS: &[(&str, &str)] = &[
    ("i8", "tinyint"),
    ("i32", "int"),
    ("i64", "bigint"),
    ("u8", "tinyint unsigned"),
    ("u64", "bigint unsigned"),
    ("f32", "float"),
    ("f64", "double"),
    ("d1", "decimal(10,2)"),
    ("d2", "decimal(38,10)"),
    ("b", "boolean"),
    ("dt", "date"),
    ("ts", "timestamp"),
    ("s", "string"),
];

/// The value of `column` in a row whose columns all derive from `seed`.
fn literal(column: &str, seed: i64) -> String {
    match column {
        "i8" => seed.clamp(-128, 127).to_string(),
        "i32" | "i64" => seed.to_string(),
        "u8" => seed.abs().min(255).to_string(),
        "u64" => seed.abs().to_string(),
        "f32" | "f64" if seed % 3 == 0 => format!("{seed}.5"),
        "f32" | "f64" => format!("{seed}.0"),
        "d1" if seed % 2 == 0 => format!("{seed}.00"),
        "d1" => format!("{seed}.40"),
        "d2" if seed % 2 == 0 => format!("{seed}.0000000000"),
        "d2" => format!("{seed}.4000000000"),
        "b" => (seed % 2 == 0).to_string(),
        "dt" => format!("to_date('2020-01-01') + {seed}"),
        "ts" if seed % 2 == 0 => {
            format!("to_timestamp('2020-01-01 00:00:00') + interval {seed} day")
        }
        "ts" => format!("to_timestamp('2020-01-01 12:30:00') + interval {seed} day"),
        "s" => format!("'{seed}'"),
        _ => unreachable!("{column}"),
    }
}

/// The rows of a block of `t`, without their ids: row `r` has the values of `seeds[r]`, but
/// in the columns of `overrides`, which cycle through the given values, and nulls in the
/// nullable columns every `null_every` rows.
fn block_of(seeds: &[i64], null_every: usize, overrides: &[(&str, &[&str])]) -> Vec<String> {
    let value = |column: &str, r: usize| {
        let values = overrides.iter().find(|(name, _)| *name == column);
        match values {
            Some((_, values)) => values[r % values.len()].to_string(),
            None if column.starts_with('f') && column.len() == 2 => "1".to_string(),
            None => literal(column, seeds[r]),
        }
    };
    (0..seeds.len())
        .map(|r| {
            let mut values = vec![];
            values.extend(T_COLUMNS.iter().map(|(column, _)| value(column, r)));
            values.extend(T_COLUMNS.iter().map(|(column, _)| {
                match null_every > 0 && r % null_every == 0 {
                    true => "null".to_string(),
                    false => value(column, r),
                }
            }));
            values.extend(["fa", "fb", "fc"].iter().map(|column| value(column, r)));
            values.join(", ")
        })
        .collect()
}

/// The blocks of `t`.
fn blocks_of_t() -> Vec<Vec<String>> {
    let mut blocks = vec![];
    // One value per block.
    for seed in [0, 2, -3, 100, 127, 1, 3] {
        blocks.push(block_of(&[seed; 40], 0, &[]));
        blocks.push(block_of(&[seed; 40], 4, &[]));
    }
    // Nullable columns that are all null.
    blocks.push(block_of(&[2; 40], 1, &[]));
    // Narrow ranges.
    for (low, high) in [(0, 5), (2, 3), (-5, 5), (90, 130), (1, 2), (2, 40)] {
        let seeds: Vec<i64> = (low..=high).chain(low..=high).collect();
        blocks.push(block_of(&seeds, 0, &[]));
        blocks.push(block_of(&seeds, 5, &[]));
    }
    let mut overridden = |columns: &[&str], values: &[&str]| {
        let overrides: Vec<(&str, &[&str])> = columns.iter().map(|c| (*c, values)).collect();
        blocks.push(block_of(&[2; 30], 0, &overrides));
        blocks.push(block_of(&[2; 30], 3, &overrides));
    };
    // NaN, infinities and zeros of both signs, alone and with other values.
    let floats: &[&[&str]] = &[
        &["'nan'::double"],
        &["'inf'::double"],
        &["'-inf'::double"],
        &["-0.0", "0.0"],
        &["'nan'::double", "1.0"],
        &["'inf'::double", "'-inf'::double", "0.0"],
        &["'nan'::double", "'inf'::double"],
        &["2.4"],
        &["1e30", "-1e30"],
    ];
    for values in floats {
        overridden(&["f32", "f64"], values);
    }
    // Strings: empty, like numbers, longer than the prefix their statistics keep, with a
    // common prefix, and not ASCII.
    let a40 = "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'";
    let a40b = "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab'";
    let a15z = "'aaaaaaaaaaaaaaazzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'";
    let strings: &[&[&str]] = &[
        &["''"],
        &["'2'"],
        &["'02'"],
        &["' 2'"],
        &["'2.0'"],
        &["'a'"],
        &["''", "'a'"],
        &[a40],
        &[a40, a40b],
        &[a40b, a15z],
        &["'数据'"],
        &["'数据'", "'数据库'"],
        &["'A'", "'a'"],
        &["'abc'", "'abx'"],
        &["repeat('é', 20)", "repeat('é', 21)"],
        &["'2'", "'10'"],
        &["'+2'"],
        &["'2020-01-03'"],
        &["'z'", "'zz'", "'zzz'"],
    ];
    for values in strings {
        overridden(&["s"], values);
    }
    // The extremes of the types. Those of the 64-bit integers are only beyond the 32-bit ones:
    // arithmetic on greater values overflows, which panics in a debug build.
    let extremes: &[(&str, &[&str])] = &[
        ("i8", &["-128", "127"]),
        ("i32", &["-2147483648", "2147483647"]),
        ("i64", &["-3000000000", "3000000000"]),
        ("u8", &["0", "255"]),
        ("u64", &["0", "3000000000"]),
        ("d1", &["-99999999.99", "99999999.99"]),
    ];
    blocks.push(block_of(&[2; 20], 0, extremes));
    let maxima: Vec<(&str, &[&str])> = extremes.iter().map(|(c, v)| (*c, &v[1..])).collect();
    blocks.push(block_of(&[2; 20], 0, &maxima));
    // Floats whose sums, differences and products are NaN.
    let nan_makers: &[(&str, &[&str])] = &[
        ("fa", &["'inf'::double", "'-inf'::double", "0", "1", "-1"]),
        ("fb", &["'-inf'::double", "'inf'::double", "0", "1", "-1"]),
        ("fc", &["'inf'::double"]),
    ];
    blocks.push(block_of(&[2; 20], 0, nan_makers));
    blocks
}

/// The blocks of `n`: `(tuple, arr, mp, variant, earr, emp)`, where `earr` and `emp` cycle
/// through the given values.
type NestedBlock = (
    (i64, &'static str),
    &'static str,
    &'static str,
    &'static str,
    &'static [&'static str],
    &'static [&'static str],
);

const NESTED_BLOCKS: &[NestedBlock] = &[
    (
        (2, "a"),
        "[2, 2]",
        "{'k': 2}",
        r#"{"a": 2, "b": "x"}"#,
        &["[1]"],
        &["{'k': 1}"],
    ),
    // Only empty arrays and maps in `earr` and `emp`.
    ((2, ""), "[2]", "{'j': 2}", r#"{"a": 2.4}"#, &["[]"], &[
        "{}",
    ]),
    (
        (-3, "02"),
        "[1, 2, 3]",
        "{'k': 1, 'j': 2}",
        r#"{"a": "02"}"#,
        &["[]", "[3]"],
        &["{}", "{'k': 3}"],
    ),
    ((7, "zz"), "[7]", "{'k': 7}", "[1, 2]", &["[7, 8]"], &[
        "{'k': 7}",
    ]),
    ((0, "a"), "[0, 0, 0]", "{'k': 0}", "null", &["[0]"], &[
        "{'z': 0}",
    ]),
];

/// The rows of a block of `n`, without their ids.
fn nested_block_of(block: &NestedBlock, nulls: bool) -> Vec<String> {
    let ((n, s), arr, mp, variant, earr, emp) = block;
    (0..30)
        .map(|r| {
            let n_tp = match (nulls, r % 3) {
                (true, 0) => "null".to_string(),
                (true, 1) => format!("({n}, null)"),
                _ => format!("({n}, '{s}')"),
            };
            let n_arr = match (nulls, r % 4) {
                (true, 0) => "null",
                (true, 1) => "[null, 2]",
                _ => arr,
            };
            let bm = match (nulls && r % 5 == 0, n % 2 == 0) {
                (true, _) => "null",
                (false, even) => ["false", "true"][even as usize],
            };
            format!(
                "({n}, '{s}'), {n_tp}, {arr}, {n_arr}, {mp}, parse_json('{variant}'), {bm}, \
                 ['{s}', ''], {}, {}",
                earr[r % earr.len()],
                emp[r % emp.len()]
            )
        })
        .collect()
}

impl Tables {
    async fn create() -> Result<Tables> {
        let fixture = TestFixture::setup().await?;
        fixture.create_default_database().await?;
        // Each insert stays a block of its own.
        fixture
            .default_session()
            .get_settings()
            .set_setting("enable_compact_after_write".to_string(), "0".to_string())?;
        let db = fixture.default_db_name();

        let columns = |nullable: bool| {
            T_COLUMNS
                .iter()
                .map(|(name, data_type)| match nullable {
                    true => format!("n_{name} {data_type} null"),
                    false => format!("{name} {data_type} not null"),
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        fixture
            .execute_command(&format!(
                "create table {db}.t(id int not null, {}, {}, fa double not null, \
                 fb double not null, fc double not null) row_per_block = 100",
                columns(false),
                columns(true)
            ))
            .await?;
        let blocks = blocks_of_t();
        let mut id = 0;
        for (i, rows) in blocks.iter().enumerate() {
            // The blocks before these columns read them as their defaults.
            if i == blocks.len() / 2 {
                for column in ["late int not null default 7", "late_n string null"] {
                    let sql = format!("alter table {db}.t add column {column}");
                    fixture.execute_command(&sql).await?;
                }
            }
            let late = match (i >= blocks.len() / 2, i % 3) {
                (false, _) => String::new(),
                (true, 0) => format!(", {}, null", i % 5 + 5),
                (true, _) => format!(", {}, '{}'", i % 5 + 5, i % 4),
            };
            let rows: Vec<String> = rows
                .iter()
                .map(|row| {
                    id += 1;
                    // Ids far apart: their sum tells which rows a filter selects.
                    format!("({}, {row}{late})", id * 7919 % 1000003)
                })
                .collect();
            let sql = format!("insert into {db}.t values {}", rows.join(", "));
            fixture.execute_command(&sql).await?;
        }

        fixture
            .execute_command(&format!(
                "create table {db}.n(id int not null, tp tuple(int, string), \
                 n_tp tuple(int null, string null) null, arr array(int), \
                 n_arr array(int null) null, mp map(string, int), v variant, bm boolean null, \
                 ev array(string), earr array(int), emp map(string, int)) row_per_block = 100"
            ))
            .await?;
        for nulls in [false, true] {
            for block in NESTED_BLOCKS {
                let rows: Vec<String> = nested_block_of(block, nulls)
                    .iter()
                    .map(|row| {
                        id += 1;
                        format!("({}, {row})", id * 7919 % 1000003)
                    })
                    .collect();
                let sql = format!("insert into {db}.n values {}", rows.join(", "));
                fixture.execute_command(&sql).await?;
            }
        }
        Ok(Tables { fixture, db })
    }
}

/// The filters over `t` and over `n`, by kind.
type Cases = BTreeMap<Option<Known>, Vec<Case>>;

/// `PRUNING_DIFFERENTIAL_ATOMS` atoms of each kind are drawn with the seed
/// `PRUNING_DIFFERENTIAL_SEED`: by default few enough for the test to take about a minute.
/// Each atom makes three filters.
fn cases() -> (Cases, Cases) {
    let env = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    };
    let atoms = env("PRUNING_DIFFERENTIAL_ATOMS", 80) as usize;
    let mut rng = Rng(env("PRUNING_DIFFERENTIAL_SEED", 0x5eed));
    let scalar = scalar_atoms(&mut rng);
    let scalar = combine(&mut rng, pinned_atoms(), scalar, atoms);
    let nested = combine(&mut rng, vec![], nested_atoms(), atoms / 2);
    (scalar, nested)
}

async fn check_kind(known: Option<Known>) -> Result<()> {
    let tables = Tables::create().await?;
    let (scalar, nested) = cases();
    let mut failures = vec![];
    for (table, cases) in [("t", scalar), ("n", nested)] {
        if let Some(cases) = cases.get(&known) {
            failures.extend(tables.failures(table, cases).await);
        }
    }
    // The test fails once the fixture is gone: unwinding through it can hang the test.
    drop(tables);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pruned_scans_select_the_rows_where_the_filter_is_true() -> Result<()> {
    check_kind(None).await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "prunes wrongly: #21216, see `Known::CastComparison`"]
async fn test_pruned_scans_of_cast_comparisons() -> Result<()> {
    check_kind(Some(Known::CastComparison)).await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "prunes wrongly: #21216, see `Known::FloatArithmetic`"]
async fn test_pruned_scans_of_float_arithmetic() -> Result<()> {
    check_kind(Some(Known::FloatArithmetic)).await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "prunes wrongly: #21166, see `Known::EmptyCollection`"]
async fn test_pruned_scans_of_empty_collections() -> Result<()> {
    check_kind(Some(Known::EmptyCollection)).await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "prunes wrongly: #21216, see `Known::VariantComparison`"]
async fn test_pruned_scans_of_variant_comparisons() -> Result<()> {
    check_kind(Some(Known::VariantComparison)).await
}
