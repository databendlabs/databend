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

use crate::elf::library_loader::LibraryLoader;
use crate::elf::library_manager::Library;
use crate::elf::library_manager::LibraryManager;
use crate::elf::library_symbol::Symbol;
use crate::exception_backtrace::ResolvedStackFrame;
use crate::exception_backtrace::StackFrame;

#[cfg(target_pointer_width = "32")]
type ElfFile = object::read::elf::ElfFile32<'static, object::NativeEndian, &'static [u8]>;

#[cfg(target_pointer_width = "64")]
type ElfFile = object::read::elf::ElfFile64<'static, object::NativeEndian, &'static [u8]>;

static INSTANCE: OnceCell<Arc<LibraryManager>> = OnceCell::new();

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

struct Unit<'a> {
    debug_line: &'a [u8],
    debug_addr: DebugAddr<EndianSlice<'a, NativeEndian>>,
    range_list: RangeLists<EndianSlice<'a, NativeEndian>>,
    addr_base: DebugAddrBase,

    abbreviations: Abbreviations,
    loclists_base: DebugLocListsBase,
    rnglists_base: DebugRngListsBase,
    str_offsets_base: DebugStrOffsetsBase,

    pub head: UnitHeader<EndianSlice<'a, NativeEndian>>,
}

pub enum HighPc {
    Addr(u64),
    Offset(u64),
}

struct LocationDieAttrs<'a> {
    high_pc: Option<AttributeValue<EndianSlice<'a, NativeEndian>>>,
    low_pc: Option<u64>,
    base_addr: Option<u64>,
    name: Option<EndianSlice<'a, NativeEndian>>,
    comp_dir: Option<EndianSlice<'a, NativeEndian>>,
    ranges_offset: Option<RangeListsOffset>,
    debug_line_offset: Option<DebugLineOffset>,
}

impl<'a> LocationDieAttrs<'a> {
    pub fn create() -> LocationDieAttrs<'a> {
        LocationDieAttrs {
            high_pc: None,
            low_pc: None,
            base_addr: None,
            name: None,
            comp_dir: None,
            ranges_offset: None,
            debug_line_offset: None,
        }
    }

    pub fn set_attr<R: Reader>(&mut self, attr: Attribute<R>) {
        match (attr.name(), attr.value()) {
            (gimli::DW_AT_high_pc, v) => {
                self.high_pc = Some(v);
            }
            (gimli::DW_AT_low_pc, AttributeValue::Addr(v)) => {
                self.low_pc = Some(v);
                self.base_addr = Some(v);
            }
            (gimli::DW_AT_name, AttributeValue::String(v)) => {
                self.name = Some(v);
            }
            (gimli::DW_AT_entry_pc, AttributeValue::Addr(v)) => {
                self.base_addr = Some(v);
            }
            (gimli::DW_AT_comp_dir, AttributeValue::String(v)) => {
                self.comp_dir = Some(v);
            }
            (gimli::DW_AT_ranges, AttributeValue::RangeListsRef(v)) => {
                self.ranges_offset = Some(RangeListsOffset(v.0));
            }
            (gimli::DW_AT_stmt_list, AttributeValue::DebugLineRef(v)) => {
                self.debug_line_offset = Some(v);
            }
            _ => {}
        }
    }
}
