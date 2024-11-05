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
use gimli::DebugAbbrev;
use gimli::DebugAddr;
use gimli::DebugAddrBase;
use gimli::DebugAddrIndex;
use gimli::DebugInfo;
use gimli::DebugLine;
use gimli::DebugLineOffset;
use gimli::DebugLineStr;
use gimli::DebugLineStrOffset;
use gimli::DebugLocListsBase;
use gimli::DebugRngListsBase;
use gimli::DebugStr;
use gimli::DebugStrOffsets;
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
use gimli::ReaderOffset;
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
    pub name: Option<R>,
    pub comp_dir: Option<R>,

    pub ranges_offset: Option<RangeListsOffset<R::Offset>>,

    pub addr_base: DebugAddrBase<R::Offset>,
    loclists_base: DebugLocListsBase<R::Offset>,
    rnglists_base: DebugRngListsBase<R::Offset>,
    pub str_offsets_base: DebugStrOffsetsBase<R::Offset>,
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
            addr_base: DebugAddrBase(R::Offset::from_u8(0)),
            loclists_base: DebugLocListsBase(R::Offset::from_u8(0)),
            rnglists_base: DebugRngListsBase(R::Offset::from_u8(0)),
            str_offsets_base: DebugStrOffsetsBase(R::Offset::from_u8(0)),
        }
    }

    pub fn set_attr(&mut self, debug_str: &DebugStr<R>, attr: Attribute<R>) {
        match (attr.name(), attr.value()) {
            (gimli::DW_AT_high_pc, v) => {
                self.high_pc = match v {
                    AttributeValue::Addr(v) => Some(HighPc::Addr(v)),
                    AttributeValue::Udata(v) => Some(HighPc::Offset(v)),
                    _ => unreachable!(),
                };
            }
            (gimli::DW_AT_low_pc, AttributeValue::Addr(v)) => {
                self.low_pc = Some(v);
                self.base_addr = Some(v);
            }
            (gimli::DW_AT_name, v) => {
                self.name = v.string_value(debug_str);
            }
            // (gimli::DW_AT_entry_pc, AttributeValue::Addr(v)) => {
            //     self.base_addr = Some(v);
            // }
            (gimli::DW_AT_comp_dir, v) => {
                self.comp_dir = v.string_value(debug_str);
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

    pub(crate) debug_str: DebugStr<R>,
    pub(crate) debug_info: DebugInfo<R>,
    pub(crate) debug_abbrev: DebugAbbrev<R>,
    pub(crate) debug_line: DebugLine<R>,
    pub(crate) debug_line_str: DebugLineStr<R>,
    pub(crate) debug_addr: DebugAddr<R>,
    pub(crate) range_list: RangeLists<R>,
    pub(crate) debug_str_offsets: DebugStrOffsets<R>,

    // addr_base: DebugAddrBase,
    pub(crate) abbreviations: Abbreviations,
    // loclists_base: DebugLocListsBase,
    // rnglists_base: DebugRngListsBase,
    // str_offsets_base: DebugStrOffsetsBase,
    pub(crate) attrs: UnitAttrs<R>,
}

impl<R: Reader> Unit<R> {
    pub fn match_range(&self, probe: u64, offset: RangeListsOffset<R::Offset>) -> bool {
        if let Ok(mut ranges) = self.range_list.ranges(
            offset,
            self.head.encoding(),
            self.attrs.low_pc.unwrap_or(0),
            &self.debug_addr,
            self.attrs.addr_base,
        ) {
            while let Ok(Some(range)) = ranges.next() {
                if probe >= range.begin && probe < range.end {
                    return true;
                }
            }
        }

        false
    }

    fn find_line(&self, probe: u64) -> gimli::Result<Option<CallLocation>> {
        if let Some(offset) = &self.attrs.debug_line_offset {
            let program = self.debug_line.program(
                *offset,
                self.head.address_size(),
                self.attrs.comp_dir.clone(),
                self.attrs.name.clone(),
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

                        if let Some(dir) = &self.attrs.comp_dir {
                            path_buf.push(dir.to_string_lossy().unwrap().into_owned());
                        }

                        let header = rows.header();
                        if let Some(file) = header.file(file_idx) {
                            if file.directory_index() != 0 {
                                if let Some(v) = file.directory(header) {
                                    if let Some(v) = v.string_value(&self.debug_str) {
                                        path_buf.push(v.to_string_lossy().unwrap().into_owned());
                                    }
                                }
                            }

                            if let Some(v) = file.path_name().string_value(&self.debug_str) {
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

        let mut inlined_functions = vec![];
        if let Some(offset) = self.find_subprogram(probe)? {
            eprintln!("begin find inlined functions");
            let _ = self.find_function(offset, probe, &mut inlined_functions);
        }

        inlined_functions.push(location);
        Ok(inlined_functions)
    }

    pub fn get_address(&self, idx: DebugAddrIndex<R::Offset>) -> u64 {
        self.debug_addr
            .get_address(self.head.encoding().address_size, self.attrs.addr_base, idx)
            .unwrap()
    }
}
