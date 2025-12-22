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

use std::sync::Arc;

use gimli::DebugAbbrev;
use gimli::DebugAddr;
use gimli::DebugAranges;
use gimli::DebugInfo;
use gimli::DebugInfoOffset;
use gimli::DebugLine;
use gimli::DebugLineStr;
use gimli::DebugRanges;
use gimli::DebugRngLists;
use gimli::DebugStr;
use gimli::DebugStrOffsets;
use gimli::EndianSlice;
use gimli::NativeEndian;
use gimli::RangeLists;
use gimli::Reader;
use gimli::UnitHeader;
use gimli::UnitType;
use object::CompressionFormat;
use object::Object;
use object::ObjectSection;

use crate::elf::ElfFile;
use crate::elf::dwarf_unit::Unit;
use crate::elf::dwarf_unit::UnitAttrs;

#[derive(Debug)]
pub struct CallLocation {
    pub symbol: Option<String>,
    pub file: Option<String>,
    pub line: Option<u32>,
    pub column: Option<u32>,
    pub is_inlined: bool,
}

pub struct Dwarf {
    #[allow(unused)]
    elf: Arc<ElfFile>,
    debug_str: DebugStr<EndianSlice<'static, NativeEndian>>,
    debug_info: DebugInfo<EndianSlice<'static, NativeEndian>>,
    debug_line: DebugLine<EndianSlice<'static, NativeEndian>>,
    debug_line_str: DebugLineStr<EndianSlice<'static, NativeEndian>>,
    debug_str_offsets: DebugStrOffsets<EndianSlice<'static, NativeEndian>>,
    debug_aranges: DebugAranges<EndianSlice<'static, NativeEndian>>,
    debug_abbrev: DebugAbbrev<EndianSlice<'static, NativeEndian>>,
    debug_addr: DebugAddr<EndianSlice<'static, NativeEndian>>,
    debug_range_list: RangeLists<EndianSlice<'static, NativeEndian>>,
}

static EMPTY_BYTES: &[u8] = &[];

impl Dwarf {
    pub fn create(elf: Arc<ElfFile>) -> Option<Dwarf> {
        fn get_debug_section(elf: &ElfFile, name: &str) -> EndianSlice<'static, NativeEndian> {
            let Some(section) = elf.section_by_name(name) else {
                return EndianSlice::new(EMPTY_BYTES, NativeEndian);
            };

            // Unsupported compress debug info
            let Ok(compressed) = section.compressed_file_range() else {
                return EndianSlice::new(EMPTY_BYTES, NativeEndian);
            };

            #[allow(clippy::missing_transmute_annotations)]
            unsafe {
                match compressed.format != CompressionFormat::None {
                    true => EndianSlice::new(EMPTY_BYTES, NativeEndian),
                    false => match section.data() {
                        Err(_) => EndianSlice::new(EMPTY_BYTES, NativeEndian),
                        Ok(data) => EndianSlice::new(std::mem::transmute(data), NativeEndian),
                    },
                }
            }
        }

        for name in [".debug_info", ".debug_abbrev", ".debug_line"] {
            if get_debug_section(&elf, name).is_empty() {
                return None;
            }
        }

        Some(Dwarf {
            debug_str: DebugStr::from(get_debug_section(&elf, ".debug_str")),
            debug_info: DebugInfo::from(get_debug_section(&elf, ".debug_info")),
            debug_line: DebugLine::from(get_debug_section(&elf, ".debug_line")),
            debug_line_str: DebugLineStr::from(get_debug_section(&elf, ".debug_line_str")),
            debug_str_offsets: DebugStrOffsets::from(get_debug_section(&elf, ".debug_str_offsets")),
            debug_aranges: DebugAranges::from(get_debug_section(&elf, ".debug_aranges")),
            debug_abbrev: DebugAbbrev::from(get_debug_section(&elf, ".debug_abbrev")),
            debug_range_list: RangeLists::new(
                DebugRanges::from(get_debug_section(&elf, ".debug_ranges")),
                DebugRngLists::from(get_debug_section(&elf, ".debug_rnglists")),
            ),
            debug_addr: DebugAddr::from(get_debug_section(&elf, ".debug_addr")),
            elf,
        })
    }

    fn find_debug_info_offset(&self, probe: u64) -> Option<DebugInfoOffset<usize>> {
        let mut heads = self.debug_aranges.headers();
        while let Some(head) = heads.next().ok()? {
            let mut entries = head.entries();
            while let Some(entry) = entries.next().ok()? {
                if probe >= entry.address() && probe <= entry.address() + entry.length() {
                    return Some(head.debug_info_offset());
                }
            }
        }

        None
    }

    fn get_unit(
        &self,
        head: UnitHeader<EndianSlice<'static, NativeEndian>>,
    ) -> gimli::Result<Option<Unit<EndianSlice<'static, NativeEndian>>>> {
        let abbrev_offset = head.debug_abbrev_offset();
        let Ok(abbreviations) = self.debug_abbrev.abbreviations(abbrev_offset) else {
            return Ok(None);
        };

        let mut cursor = head.entries(&abbreviations);
        let (_idx, root) = cursor.next_dfs()?.unwrap();

        let mut attrs = root.attrs();
        let mut unit_attrs = UnitAttrs::create();

        while let Some(attr) = attrs.next()? {
            unit_attrs.set_attr(&self.debug_str, attr);
        }

        Ok(Some(Unit {
            head,
            abbreviations,
            attrs: unit_attrs,
            debug_str: self.debug_str,
            debug_info: self.debug_info,
            debug_abbrev: self.debug_abbrev,
            debug_line: self.debug_line,
            debug_line_str: self.debug_line_str,
            debug_str_offsets: self.debug_str_offsets,
            debug_addr: self.debug_addr,
            range_list: self.debug_range_list,
        }))
    }

    fn fast_find_frames(&self, probe: u64) -> gimli::Result<Option<Vec<CallLocation>>> {
        if let Some(debug_info_offset) = self.find_debug_info_offset(probe) {
            let head = self.debug_info.header_from_offset(debug_info_offset)?;

            let type_ = head.type_();
            if matches!(type_, UnitType::Compilation | UnitType::Skeleton(_))
                && let Some(unit) = self.get_unit(head)?
            {
                return Ok(Some(unit.find_frames(probe)?));
            }
        }

        Ok(None)
    }

    fn slow_find_frames(&self, probe: u64) -> gimli::Result<Vec<CallLocation>> {
        let mut units = self.debug_info.units();
        while let Some(head) = units.next()? {
            if matches!(head.type_(), UnitType::Compilation | UnitType::Skeleton(_))
                && let Some(unit) = self.get_unit(head)?
                && unit.match_pc(probe)
            {
                return unit.find_frames(probe);
            }
        }

        Ok(vec![])
    }

    pub fn find_frames(&self, probe: u64) -> gimli::Result<Vec<CallLocation>> {
        match self.fast_find_frames(probe)? {
            Some(location) => Ok(location),
            None => self.slow_find_frames(probe),
        }
    }
}

// #[cfg(target_os = "linux")]
#[derive(Copy, Clone, Debug)]
pub enum HighPc {
    Addr(u64),
    Offset(u64),
}
