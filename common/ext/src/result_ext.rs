// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

pub trait ResultExt<T, E> {
    fn zip<LT>(self, r: Result<LT, E>) -> Result<(LT, T), E>;
    fn zip_to_right<RT>(self, r: Result<RT, E>) -> Result<(T, RT), E>;

    fn and_match<NT, NE, F: FnOnce(Result<T, E>) -> Result<NT, NE>>(self, f: F) -> Result<NT, NE>;
}

pub trait ResultTupleExt<LT, RT, E> {
    fn left_map<U, F: FnOnce(LT) -> U>(self, op: F) -> Result<(U, RT), E>;
    fn right_map<U, F: FnOnce(RT) -> U>(self, op: F) -> Result<(LT, U), E>;

    fn left_and_then<U, F: FnOnce(LT) -> Result<U, E>>(self, op: F) -> Result<(U, RT), E>;
    fn right_and_then<U, F: FnOnce(RT) -> Result<U, E>>(self, op: F) -> Result<(LT, U), E>;

    fn and_then_tuple<U, F: FnOnce(LT, RT) -> Result<U, E>>(self, op: F) -> Result<U, E>;
}

impl<T, E> ResultExt<T, E> for Result<T, E> {
    fn zip<LT>(self, r: Result<LT, E>) -> Result<(LT, T), E> {
        self.and_then(|item| r.map(|l_item| (l_item, item)))
    }

    fn zip_to_right<RT>(self, r: Result<RT, E>) -> Result<(T, RT), E> {
        self.and_then(|item| r.map(|r_item| (item, r_item)))
    }

    fn and_match<NT, NE, F: FnOnce(Result<T, E>) -> Result<NT, NE>>(self, f: F) -> Result<NT, NE> {
        self.map(Some).transpose().map(f).unwrap()
    }
}

impl<LT, RT, E> ResultTupleExt<LT, RT, E> for Result<(LT, RT), E> {
    fn left_map<U, F: FnOnce(LT) -> U>(self, op: F) -> Result<(U, RT), E> {
        self.map(|(left, right)| (op(left), right))
    }

    fn right_map<U, F: FnOnce(RT) -> U>(self, op: F) -> Result<(LT, U), E> {
        self.map(|(left, right)| (left, op(right)))
    }

    fn left_and_then<U, F: FnOnce(LT) -> Result<U, E>>(self, op: F) -> Result<(U, RT), E> {
        self.and_then(|(left, right)| op(left).map(|left| (left, right)))
    }

    fn right_and_then<U, F: FnOnce(RT) -> Result<U, E>>(self, op: F) -> Result<(LT, U), E> {
        self.and_then(|(left, right)| op(right).map(|right| (left, right)))
    }

    fn and_then_tuple<U, F: FnOnce(LT, RT) -> Result<U, E>>(self, op: F) -> Result<U, E> {
        self.and_then(|(left, right)| op(left, right))
    }
}
