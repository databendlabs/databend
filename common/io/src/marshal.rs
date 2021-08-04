// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

pub trait Marshal {
    fn marshal(&self, scratch: &mut [u8]);
}

impl Marshal for u8 {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self;
    }
}

impl Marshal for u16 {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
        scratch[1] = (self >> 8) as u8;
    }
}

impl Marshal for u32 {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
        scratch[1] = (self >> 8) as u8;
        scratch[2] = (self >> 16) as u8;
        scratch[3] = (self >> 24) as u8;
    }
}

impl Marshal for u64 {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
        scratch[1] = (self >> 8) as u8;
        scratch[2] = (self >> 16) as u8;
        scratch[3] = (self >> 24) as u8;

        scratch[4] = (self >> 32) as u8;
        scratch[5] = (self >> 40) as u8;
        scratch[6] = (self >> 48) as u8;
        scratch[7] = (self >> 56) as u8;
    }
}

impl Marshal for i8 {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
    }
}

impl Marshal for i16 {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
        scratch[1] = (self >> 8) as u8;
    }
}

impl Marshal for i32 {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
        scratch[1] = (self >> 8) as u8;
        scratch[2] = (self >> 16) as u8;
        scratch[3] = (self >> 24) as u8;
    }
}

impl Marshal for i64 {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
        scratch[1] = (self >> 8) as u8;
        scratch[2] = (self >> 16) as u8;
        scratch[3] = (self >> 24) as u8;

        scratch[4] = (self >> 32) as u8;
        scratch[5] = (self >> 40) as u8;
        scratch[6] = (self >> 48) as u8;
        scratch[7] = (self >> 56) as u8;
    }
}

impl Marshal for f32 {
    fn marshal(&self, scratch: &mut [u8]) {
        let bits = self.to_bits();
        scratch[0] = bits as u8;
        scratch[1] = (bits >> 8) as u8;
        scratch[2] = (bits >> 16) as u8;
        scratch[3] = (bits >> 24) as u8;
    }
}

impl Marshal for f64 {
    fn marshal(&self, scratch: &mut [u8]) {
        let bits = self.to_bits();
        scratch[0] = bits as u8;
        scratch[1] = (bits >> 8) as u8;
        scratch[2] = (bits >> 16) as u8;
        scratch[3] = (bits >> 24) as u8;
        scratch[4] = (bits >> 32) as u8;
        scratch[5] = (bits >> 40) as u8;
        scratch[6] = (bits >> 48) as u8;
        scratch[7] = (bits >> 56) as u8;
    }
}

impl Marshal for bool {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
    }
}
