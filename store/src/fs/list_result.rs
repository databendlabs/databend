// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
use std::fmt::Display;
use std::fmt::Formatter;

#[derive(Debug)]
pub struct ListResult {
    pub dirs: Vec<String>,
    pub files: Vec<String>,
}

impl PartialEq for ListResult {
    fn eq(&self, other: &Self) -> bool {
        if self.dirs.len() != other.dirs.len() {
            return false;
        }
        if self.files.len() != other.files.len() {
            return false;
        }

        for i in 0..self.dirs.len() {
            let a = &self.dirs[i];
            let b = &other.dirs[i];
            if a != b {
                return false;
            }
        }
        for i in 0..self.files.len() {
            let a = &self.files[i];
            let b = &other.files[i];
            if a != b {
                return false;
            }
        }
        true
    }
}

impl Eq for ListResult {}

impl Display for ListResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for d in self.dirs.iter() {
            write!(f, "{:}/, ", d)?;
        }
        for x in self.files.iter() {
            write!(f, "{:}, ", x)?;
        }
        write!(f, "]")
    }
}
