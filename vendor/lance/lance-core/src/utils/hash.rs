// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::hash::Hasher;

// A wrapper for &[u8] to allow &[u8] as hash keys,
// the equality for this `U8SliceKey` means that the &[u8] contents are equal.
#[derive(Eq)]
pub struct U8SliceKey<'a>(pub &'a [u8]);
impl PartialEq for U8SliceKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl std::hash::Hash for U8SliceKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}
