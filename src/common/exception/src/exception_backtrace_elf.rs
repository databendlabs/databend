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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::ffi::CStr;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Write;
use std::num::NonZeroU64;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::path::PathBuf;
use std::ptr;
use std::sync::Arc;

use gimli::constants;
use gimli::write::RangeListOffsets;
use gimli::Abbreviations;
use gimli::Attribute;
use gimli::AttributeValue;
use gimli::DebugAbbrev;
use gimli::DebugAbbrevOffset;
use gimli::DebugAddr;
use gimli::DebugAddrBase;
use gimli::DebugAranges;
use gimli::DebugInfo;
use gimli::DebugInfoOffset;
use gimli::DebugLine;
use gimli::DebugLineOffset;
use gimli::DebugLocListsBase;
use gimli::DebugRanges;
use gimli::DebugRngLists;
use gimli::DebugRngListsBase;
use gimli::DebugStrOffsetsBase;
use gimli::DebuggingInformationEntry;
use gimli::DwAt;
use gimli::EndianSlice;
use gimli::EntriesTree;
use gimli::EntriesTreeNode;
use gimli::Error;
use gimli::FileEntry;
use gimli::NativeEndian;
use gimli::RangeLists;
use gimli::RangeListsOffset;
use gimli::RawRngListEntry;
use gimli::Reader;
use gimli::UnitHeader;
use gimli::UnitOffset;
use gimli::UnitType;
use libc::size_t;
use object::CompressionFormat;
use object::Object;
use object::ObjectSection;
use object::ObjectSymbol;
use object::ObjectSymbolTable;
use once_cell::sync::OnceCell;
use tantivy::HasLen;

use crate::exception_backtrace::ResolvedStackFrame;
use crate::exception_backtrace::StackFrame;

pub struct Location {
    pub file: String,
    pub line: Option<u32>,
    pub column: Option<u32>,
}

impl Location {
    pub fn unknown() -> Location {
        Location {
            file: String::from("<unknown>"),
            line: None,
            column: None,
        }
    }
}

#[derive(Copy, Clone)]
pub enum HighPc {
    Addr(u64),
    Offset(u64),
}
