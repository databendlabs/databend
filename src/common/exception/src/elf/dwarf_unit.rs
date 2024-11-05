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

use std::num::NonZeroU64;
use std::path::Path;
use std::path::PathBuf;

use gimli::Abbreviations;
use gimli::Attribute;
use gimli::AttributeValue;
use gimli::DebugAddr;
use gimli::DebugAddrBase;
use gimli::DebugLine;
use gimli::DebugLineOffset;
use gimli::DebugLocListsBase;
use gimli::DebugRngListsBase;
use gimli::DebugStrOffsetsBase;
use gimli::EndianSlice;
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

use crate::elf::dwarf::CallLocation;
use crate::elf::dwarf_subprogram::SubprogramAttrs;
use crate::exception_backtrace_elf::HighPc;

// Unit first die attrs
pub struct UnitAttrs<R: Reader> {
    pub high_pc: Option<HighPc>,
    pub low_pc: Option<u64>,
    pub base_addr: Option<u64>,
    pub name: Option<AttributeValue<R>>,
    pub comp_dir: Option<AttributeValue<R>>,

    pub ranges_offset: Option<RangeListsOffset<R::Offset>>,

    addr_base: DebugAddrBase<R::Offset>,
    loclists_base: DebugLocListsBase<R::Offset>,
    rnglists_base: DebugRngListsBase<R::Offset>,
    str_offsets_base: DebugStrOffsetsBase<R::Offset>,
    debug_line_offset: Option<DebugLineOffset<R::Offset>>,
}

impl<R: Reader> UnitAttrs<R> {
    pub fn create() -> UnitAttrs<R> {
        UnitAttrs {
            high_pc: None,
            low_pc: None,
            base_addr: None,
            name: None,
            comp_dir: None,
            ranges_offset: None,

            debug_line_offset: None,
            addr_base: DebugAddrBase(0),
            loclists_base: DebugLocListsBase(0),
            rnglists_base: DebugRngListsBase(0),
            str_offsets_base: DebugStrOffsetsBase(0),
        }
    }

    pub fn file_name(&self, file: Option<AttributeValue<R>>) -> String {
        let mut path = PathBuf::new();

        if let Some(AttributeValue::String(ref comp_dir)) = self.comp_dir {
            path.push(comp_dir.to_string_lossy().unwrap().into_owned());
        }

        if let Some(ref file) = file {
            path.push(file.to_string_lossy().unwrap().into_owned());
            // if file.directory_index() != 0 {
            //     if let Some(AttributeValue::String(v)) = file.directory(rows.header()) {
            //         path.push(v.to_string_lossy().into_owned());
            //     }
            // }
        }

        if let AttributeValue::String(v) = file.path_name() {
            path.push(v.to_string_lossy().into_owned());
        }

        if let Some(file_name) = self.name {
            let file_path = Path::new(file_name.to_string().unwrap());

            return match self.comp_dir {
                None => format!("{}", file_path.display()),
                Some(ref comp_dir) => match comp_dir.to_string() {
                    Err(_) => format!("{}", file_path.display()),
                    Ok(dir) => format!("{}", Path::new(dir).join(file_path).display()),
                },
            };
        }

        String::from("<unknown>")
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
            (gimli::DW_AT_name, v) => {
                self.name = Some(v);
            }
            (gimli::DW_AT_entry_pc, AttributeValue::Addr(v)) => {
                self.base_addr = Some(v);
            }
            (gimli::DW_AT_comp_dir, v) => {
                self.comp_dir = Some(v);
            }
            (gimli::DW_AT_ranges, AttributeValue::RangeListsRef(v)) => {
                self.ranges_offset = Some(RangeListsOffset(v.0));
            }
            (gimli::DW_AT_stmt_list, AttributeValue::DebugLineRef(v)) => {
                self.debug_line_offset = Some(v);
            }
            (gimli::DW_AT_addr_base, AttributeValue::DebugAddrBase(base)) => {
                self.addr_base = base;
            }
            (gimli::DW_AT_GNU_addr_base, AttributeValue::DebugAddrBase(base)) => {
                self.addr_base = base;
            }
            (gimli::DW_AT_loclists_base, AttributeValue::DebugLocListsBase(base)) => {
                self.loclists_base = base;
            }
            (gimli::DW_AT_rnglists_base, AttributeValue::DebugRngListsBase(base)) => {
                self.rnglists_base = base;
            }
            (gimli::DW_AT_GNU_ranges_base, AttributeValue::DebugRngListsBase(base)) => {
                self.rnglists_base = base;
            }
            (gimli::DW_AT_str_offsets_base, AttributeValue::DebugStrOffsetsBase(base)) => {
                self.str_offsets_base = base;
            }
            _ => {}
        }
    }
}

pub struct Unit<R: Reader> {
    pub(crate) head: UnitHeader<R>,

    pub(crate) debug_line: DebugLine<R>,
    pub(crate) debug_addr: DebugAddr<R>,
    pub(crate) range_list: RangeLists<R>,

    // addr_base: DebugAddrBase,
    pub(crate) abbreviations: Abbreviations,
    // loclists_base: DebugLocListsBase,
    // rnglists_base: DebugRngListsBase,
    // str_offsets_base: DebugStrOffsetsBase,
    pub(crate) attrs: UnitAttrs<R>,
}

impl<R: Reader> Unit<R> {
    pub fn match_range(&self, probe: u64, offset: RangeListsOffset) -> bool {
        let mut base_addr = self.attrs.base_addr;
        if let Ok(mut ranges) = self.range_list.raw_ranges(offset, self.head.encoding()) {
            while let Ok(Some(range)) = ranges.next() {
                match range {
                    RawRngListEntry::BaseAddress { addr } => {
                        base_addr = Some(addr);
                    }
                    RawRngListEntry::AddressOrOffsetPair { begin, end } => {
                        if matches!(base_addr, Some(base_addr) if probe >= begin + base_addr && probe < end + base_addr)
                        {
                            return true;
                        }
                    }
                    RawRngListEntry::BaseAddressx { addr } => {
                        let Ok(addr) = self.debug_addr.get_address(
                            self.head.encoding().address_size,
                            self.attrs.addr_base,
                            addr,
                        ) else {
                            return false;
                        };

                        base_addr = Some(addr);
                    }
                    RawRngListEntry::StartxEndx { begin, end } => {
                        let Ok(begin) = self.debug_addr.get_address(
                            self.head.encoding().address_size,
                            self.attrs.addr_base,
                            begin,
                        ) else {
                            return false;
                        };

                        let Ok(end) = self.debug_addr.get_address(
                            self.head.encoding().address_size,
                            self.attrs.addr_base,
                            end,
                        ) else {
                            return false;
                        };

                        if probe >= begin && probe < end {
                            return true;
                        }
                    }
                    RawRngListEntry::StartxLength { begin, length } => {
                        let Ok(begin) = self.debug_addr.get_address(
                            self.head.encoding().address_size,
                            self.attrs.addr_base,
                            begin,
                        ) else {
                            return false;
                        };

                        let end = begin + length;
                        if begin != end && probe >= begin && probe < end {
                            return true;
                        }
                    }
                    RawRngListEntry::OffsetPair { begin, end } => {
                        if matches!(base_addr, Some(base_addr) if probe >= (base_addr + begin) && probe < (base_addr + end))
                        {
                            return true;
                        }
                    }
                    RawRngListEntry::StartEnd { begin, end } => {
                        if probe >= begin && probe < end {
                            return true;
                        }
                    }
                    RawRngListEntry::StartLength { begin, length } => {
                        let end = begin + length;
                        if probe >= begin && probe < end {
                            return true;
                        }
                    }
                }
            }
        }

        false
    }

    fn find_line(&self, probe: u64) -> gimli::Result<Option<CallLocation>> {
        // let mut location = Location::unknown();
        // location.file = self.attrs.file_name(None);

        if let Some(offset) = &self.attrs.debug_line_offset {
            let program = self.debug_line.program(
                *offset,
                self.head.address_size(),
                self.attrs.comp_dir,
                self.attrs.name,
            )?;

            let mut is_candidate: bool = false;
            let mut file_idx = 0;
            let mut line = 0;
            let mut column = 0;

            let mut rows = program.rows();
            while let Some((_, row)) = rows.next_row()? {
                if !is_candidate && !row.end_sequence() && row.address() <= probe {
                    is_candidate = true;
                }

                if is_candidate {
                    if row.address() > probe {
                        let mut path_buf = PathBuf::new();

                        if let Some(AttributeValue::String(ref dir)) = self.attrs.comp_dir {
                            path_buf.push(dir.to_string_lossy().unwrap().into_owned());
                        }

                        let header = rows.header();
                        if let Some(file) = header.file(file_idx) {
                            if file.directory_index() != 0 {
                                if let Some(AttributeValue::String(ref v)) = file.directory(header) {
                                    path_buf.push(v.to_string_lossy().unwrap().into_owned());
                                }
                            }

                            if let AttributeValue::String(ref v) = file.path_name() {
                                path_buf.push(v.to_string_lossy().unwrap().into_owned());
                            }
                        }

                        return Ok(Some(CallLocation {
                            symbol: None,
                            file: Some(format!("{}", path_buf.display())),
                            line: Some(line),
                            column: Some(column),
                        }));
                    }

                    file_idx = row.file_index();
                    line = row.line().map(NonZeroU64::get).unwrap_or(0) as u32;
                    column = match row.column() {
                        gimli::ColumnType::LeftEdge => 0,
                        gimli::ColumnType::Column(x) => x.get() as u32,
                    };
                }

                if row.end_sequence() {
                    is_candidate = false;
                }
            }
        }

        Ok(None)
    }

    pub fn match_pc(&self, probe: u64) -> bool {
        let pc_matched = match (self.attrs.low_pc, self.attrs.high_pc) {
            (Some(low), Some(high)) => {
                probe >= low
                    && match high {
                    HighPc::Addr(high) => probe < high,
                    HighPc::Offset(size) => probe < low + size,
                }
            }
            _ => false,
        };

        let range_match = match self.attrs.ranges_offset {
            None => false,
            Some(range_offset) => self.match_range(probe, range_offset),
        };

        pc_matched || range_match
    }

    pub fn find_location(&self, probe: u64) -> gimli::Result<Vec<CallLocation>> {
        let Some(location) = self.find_line(probe)? else {
            return Ok(vec![]);
        };

        // probe inlined functions
        let mut inlined_functions = vec![];
        if let Some(offset) = self.find_subprogram(probe) {
            // self.find_inlined_functions(probe, offset, &mut inlined_functions)?;
        }

        inlined_functions.push(location);
        Ok(inlined_functions)
    }
}
