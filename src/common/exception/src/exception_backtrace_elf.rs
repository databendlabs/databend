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
use std::path::PathBuf;
use std::ptr;
use std::sync::Arc;

use gimli::constants;
use gimli::Abbreviations;
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
use gimli::EndianSlice;
use gimli::Error;
use gimli::NativeEndian;
use gimli::RangeLists;
use gimli::RangeListsOffset;
use gimli::RawRngListEntry;
use gimli::UnitHeader;
use gimli::UnitType;
use libc::size_t;
use object::CompressionFormat;
use object::Object;
use object::ObjectSection;
use object::ObjectSymbol;
use object::ObjectSymbolTable;
use once_cell::sync::OnceCell;

use crate::exception_backtrace::ResolvedStackFrame;
use crate::exception_backtrace::StackFrame;

#[cfg(target_pointer_width = "32")]
type ElfFile = object::read::elf::ElfFile32<'static, object::NativeEndian, &'static [u8]>;

#[cfg(target_pointer_width = "64")]
type ElfFile = object::read::elf::ElfFile64<'static, object::NativeEndian, &'static [u8]>;

struct Symbol {
    name: &'static [u8],
    address_begin: u64,
    address_end: u64,
}

impl Debug for Symbol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Symbol")
            .field(
                "name",
                &rustc_demangle::demangle(std::str::from_utf8(self.name).unwrap()),
            )
            .field("address_begin", &self.address_begin)
            .field("address_end", &self.address_end)
            .finish()
    }
}

struct Library {
    name: String,
    address_begin: usize,
    address_end: usize,
    #[allow(unused)]
    library_data: (*const u8, usize),
    elf: Option<ElfFile>,
}

unsafe impl Send for Library {}

unsafe impl Sync for Library {}

impl Debug for Library {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Library")
            .field("name", &self.name)
            .field("address_begin", &self.address_begin)
            .field("address_end", &self.address_end)
            .finish()
    }
}

static INSTANCE: OnceCell<Arc<LibraryManager>> = OnceCell::new();

// #[derive(Debug)]
pub struct LibraryManager {
    symbols: Vec<Symbol>,
    libraries: Vec<Library>,
}

impl Debug for LibraryManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LibraryManager")
            .field("libraries", &self.libraries)
            .field("symbols", &self.symbols.len())
            .finish()
    }
}

pub struct Location {
    pub file: String,
    pub line: Option<u32>,
    pub column: Option<u32>,
}

struct Dwarf<'a> {
    #[allow(unused)]
    elf: &'a ElfFile,
    debug_info: &'a [u8],
    debug_aranges: &'a [u8],
    debug_ranges: &'a [u8],
    debug_rnglists: &'a [u8],
    debug_abbrev: &'a [u8],
    debug_addr: &'a [u8],
    debug_line: &'a [u8],
}

struct Unit<'a> {
    head: UnitHeader<EndianSlice<'a, NativeEndian>>,

    addr_base: DebugAddrBase,
    abbreviations: Abbreviations,
    loclists_base: DebugLocListsBase,
    rnglists_base: DebugRngListsBase,
    str_offsets_base: DebugStrOffsetsBase,
}

impl<'a> Dwarf<'a> {
    pub fn create(elf: &'a ElfFile) -> Option<Dwarf> {
        fn get_debug_section<'a>(elf: &'a ElfFile, name: &str) -> &'a [u8] {
            let Some(section) = elf.section_by_name(name) else {
                return &[];
            };

            // Unsupported compress debug info
            section
                .compressed_file_range()
                .ok()
                .filter(|x| x.format == CompressionFormat::None)
                .and_then(|_| section.data().ok())
                .unwrap_or(&[])
        }

        let dwarf = Dwarf {
            elf,
            debug_info: get_debug_section(elf, ".debug_info"),
            debug_aranges: get_debug_section(elf, ".debug_aranges"),
            debug_ranges: get_debug_section(elf, ".debug_ranges"),
            debug_rnglists: get_debug_section(elf, ".debug_rnglists"),
            debug_addr: get_debug_section(elf, ".debug_addr"),
            debug_abbrev: get_debug_section(elf, ".debug_abbrev"),
            debug_line: get_debug_section(elf, ".debug_line"),
        };

        match dwarf.debug_info.is_empty()
            || dwarf.debug_abbrev.is_empty()
            || dwarf.debug_line.is_empty()
        {
            true => None,
            false => Some(dwarf),
        }
    }

    fn find_debug_info_offset(&self, probe: u64) -> Option<DebugInfoOffset<usize>> {
        if self.debug_aranges.is_empty() {
            return None;
        }

        let section = DebugAranges::new(self.debug_aranges, NativeEndian);
        let mut headers = section.headers();
        while let Some(header) = headers.next().ok()? {
            let mut entries = header.entries();
            while let Some(entry) = entries.next().ok()? {
                if probe >= entry.address() && probe <= entry.address() + entry.length() {
                    return Some(header.debug_info_offset());
                }
            }
        }

        None
    }

    fn get_abbreviations(&self, offset: DebugAbbrevOffset) -> Option<Abbreviations> {
        DebugAbbrev::new(self.debug_abbrev, NativeEndian)
            .abbreviations(offset)
            .ok()
    }

    fn in_range_list(
        &self,
        unit: &Unit,
        address: u64,
        mut base_addr: Option<u64>,
        offset: RangeListsOffset,
    ) -> bool {
        let range_list = RangeLists::new(
            DebugRanges::new(self.debug_ranges, NativeEndian),
            DebugRngLists::new(self.debug_rnglists, NativeEndian),
        );

        if let Ok(mut ranges) = range_list.raw_ranges(offset, unit.head.encoding()) {
            let debug_addr = DebugAddr::from(EndianSlice::new(self.debug_addr, NativeEndian));

            while let Ok(Some(range)) = ranges.next() {
                match range {
                    RawRngListEntry::BaseAddress { addr } => {
                        base_addr = Some(addr);
                    }
                    RawRngListEntry::AddressOrOffsetPair { begin, end } => {
                        if matches!(base_addr, Some(base_addr) if address >= begin + base_addr && address < end + base_addr)
                        {
                            return true;
                        }
                    }
                    RawRngListEntry::BaseAddressx { addr } => {
                        let Ok(addr) = debug_addr.get_address(
                            unit.head.encoding().address_size,
                            unit.addr_base,
                            addr,
                        ) else {
                            return false;
                        };

                        base_addr = Some(addr);
                    }
                    RawRngListEntry::StartxEndx { begin, end } => {
                        let Ok(begin) = debug_addr.get_address(
                            unit.head.encoding().address_size,
                            unit.addr_base,
                            begin,
                        ) else {
                            return false;
                        };

                        let Ok(end) = debug_addr.get_address(
                            unit.head.encoding().address_size,
                            unit.addr_base,
                            end,
                        ) else {
                            return false;
                        };

                        if address >= begin && address < end {
                            return true;
                        }
                    }
                    RawRngListEntry::StartxLength { begin, length } => {
                        let Ok(begin) = debug_addr.get_address(
                            unit.head.encoding().address_size,
                            unit.addr_base,
                            begin,
                        ) else {
                            return false;
                        };

                        let end = begin + length;
                        if begin != end && address >= begin && address < end {
                            return true;
                        }
                    }
                    RawRngListEntry::OffsetPair { begin, end } => {
                        if matches!(base_addr, Some(base_addr) if address >= (base_addr + begin) && address < (base_addr + end))
                        {
                            return true;
                        }
                    }
                    RawRngListEntry::StartEnd { begin, end } => {
                        if address >= begin && address < end {
                            return true;
                        }
                    }
                    RawRngListEntry::StartLength { begin, length } => {
                        let end = begin + length;
                        if address >= begin && address < end {
                            return true;
                        }
                    }
                }
            }
        }

        false
    }

    fn find_line(
        &self,
        address: u64,
        offset: DebugLineOffset,
        unit: &Unit,
        comp_dir: Option<EndianSlice<NativeEndian>>,
        name: Option<EndianSlice<NativeEndian>>,
    ) -> Option<Location> {
        if self.debug_line.is_empty() {
            return None;
        }

        let debug_line = DebugLine::new(self.debug_line, NativeEndian);
        let program = debug_line
            .program(offset, unit.head.address_size(), comp_dir, name)
            .ok()?;

        let mut is_candidate: bool = false;
        let mut file_index = 0;
        let mut line = 0;
        let mut column = 0;

        let mut rows = program.rows();
        while let Some((_, row)) = rows.next_row().ok()? {
            if !is_candidate && !row.end_sequence() && row.address() <= address {
                is_candidate = true;
            }

            if is_candidate {
                if row.address() > address {
                    let file = rows.header().file(file_index)?;

                    let mut path = PathBuf::new();

                    if let Some(ref comp_dir) = comp_dir {
                        path.push(comp_dir.to_string_lossy().into_owned());
                    }

                    if file.directory_index() != 0 {
                        if let Some(AttributeValue::String(v)) = file.directory(rows.header()) {
                            path.push(v.to_string_lossy().into_owned());
                        }
                    }

                    if let AttributeValue::String(v) = file.path_name() {
                        path.push(v.to_string_lossy().into_owned());
                    }

                    return Some(Location {
                        file: format!("{}", path.display()),
                        line: Some(line),
                        column: Some(column),
                    });
                }

                file_index = row.file_index();
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

        None
    }

    fn get_location(
        &self,
        address: u64,
        unit: &Unit<'a>,
        from_debug_aranges: bool,
    ) -> Option<Location> {
        let mut cursor = unit.head.entries(&unit.abbreviations);
        cursor.next_dfs().ok()?;
        let root = cursor.current().ok_or(Error::MissingUnitDie).ok()?;
        let mut attrs = root.attrs();

        let mut name = None;
        let mut comp_dir = None;
        let mut line_program_offset = None;
        let mut low_pc_attr = None;
        let mut high_pc_attr = None;
        let mut base_addr_cu = None;
        let mut range_offset = None;

        while let Some(attr) = attrs.next().ok()? {
            match attr.name() {
                gimli::DW_AT_stmt_list => {
                    if let AttributeValue::DebugLineRef(offset) = attr.value() {
                        line_program_offset = Some(offset);
                    }
                }
                gimli::DW_AT_comp_dir => {
                    // TODO(winter): unsupported lookup by string table
                    if let AttributeValue::String(value) = attr.value() {
                        comp_dir = Some(value);
                    }
                }
                gimli::DW_AT_name => {
                    // TODO(winter): unsupported lookup by string table
                    if let AttributeValue::String(value) = attr.value() {
                        name = Some(value);
                    }
                }
                gimli::DW_AT_entry_pc => {
                    if let AttributeValue::Addr(addr) = attr.value() {
                        base_addr_cu = Some(addr);
                    }
                }
                gimli::DW_AT_ranges => {
                    if let AttributeValue::RangeListsRef(v) = attr.value() {
                        range_offset = Some(RangeListsOffset(v.0));
                    }
                }
                gimli::DW_AT_low_pc => {
                    if let AttributeValue::Addr(addr) = attr.value() {
                        low_pc_attr = Some(addr);
                        base_addr_cu = Some(addr);
                    }
                }
                gimli::DW_AT_high_pc => {
                    high_pc_attr = Some(attr.value());
                }
                _ => {}
            }
        }

        if !from_debug_aranges && range_offset.is_some() {
            let pc_matched = match (low_pc_attr, high_pc_attr) {
                (Some(low), Some(high)) => {
                    address >= low
                        && match high {
                            AttributeValue::Addr(high) => address < high,
                            AttributeValue::Udata(size) => address < low + size,
                            _ => false,
                        }
                }
                _ => false,
            };

            let range_match = match range_offset {
                None => false,
                Some(range_offset) => self.in_range_list(unit, address, base_addr_cu, range_offset),
            };

            if !range_match && !pc_matched {
                return None;
            }
        }

        if let Some(offset) = line_program_offset {
            if let Some(location) = self.find_line(address, offset, unit, comp_dir, name) {
                return Some(location);
            }
        }

        if let Some(name) = name {
            let mut path = PathBuf::new();
            if let Some(ref comp_dir) = comp_dir {
                path.push(comp_dir.to_string_lossy().into_owned());
            };

            path.push(name.to_string_lossy().into_owned());

            return Some(Location {
                file: format!("{}", path.display()),
                line: None,
                column: None,
            });
        }

        None
    }

    pub fn find_location(&self, probe: u64) -> Option<Location> {
        if let Some(debug_info_offset) = self.find_debug_info_offset(probe) {
            let unit = self.get_compilation_unit(debug_info_offset)?;

            if !matches!(
                unit.head.type_(),
                UnitType::Compilation | UnitType::Skeleton(_)
            ) {
                return None;
            }

            if let Some(location) = self.get_location(probe, &unit, true) {
                return Some(location);
            }

            return None;
        }

        let mut offset = DebugInfoOffset(0);

        while offset.0 < self.debug_info.len() {
            let Some(unit) = self.get_compilation_unit(offset) else {
                break;
            };

            if !matches!(
                unit.head.type_(),
                UnitType::Compilation | UnitType::Skeleton(_)
            ) {
                continue;
            }

            offset.0 += unit.head.length_including_self();
            if let Some(location) = self.get_location(probe, &unit, false) {
                return Some(location);
            }
        }

        None
    }

    fn get_compilation_unit(&self, offset: DebugInfoOffset) -> Option<Unit> {
        let info = DebugInfo::new(self.debug_info, NativeEndian);

        let Ok(header) = info.header_from_offset(offset) else {
            return None;
        };

        let mut unit = Unit {
            head: header,
            addr_base: DebugAddrBase(0),
            abbreviations: Abbreviations::default(),
            loclists_base: DebugLocListsBase(0),
            rnglists_base: DebugRngListsBase(0),
            str_offsets_base: DebugStrOffsetsBase(0),
        };

        if let Some(abbreviations) = self.get_abbreviations(header.debug_abbrev_offset()) {
            unit.abbreviations = abbreviations;

            let mut cursor = header.entries(&unit.abbreviations);
            cursor.next_dfs().ok()?;
            let root = cursor.current().ok_or(Error::MissingUnitDie).ok()?;
            let mut attrs = root.attrs();

            while let Some(attr) = attrs.next().ok()? {
                match attr.name() {
                    gimli::DW_AT_addr_base | gimli::DW_AT_GNU_addr_base => {
                        if let AttributeValue::DebugAddrBase(base) = attr.value() {
                            unit.addr_base = base;
                        }
                    }
                    gimli::DW_AT_loclists_base => {
                        if let AttributeValue::DebugLocListsBase(base) = attr.value() {
                            unit.loclists_base = base;
                        }
                    }
                    gimli::DW_AT_rnglists_base | constants::DW_AT_GNU_ranges_base => {
                        if let AttributeValue::DebugRngListsBase(base) = attr.value() {
                            unit.rnglists_base = base;
                        }
                    }
                    gimli::DW_AT_str_offsets_base => {
                        if let AttributeValue::DebugStrOffsetsBase(base) = attr.value() {
                            unit.str_offsets_base = base;
                        }
                    }
                    _ => {}
                };
            }
        };

        Some(unit)
    }
}

impl LibraryManager {
    fn find_library(&self, addr: usize) -> Option<&Library> {
        self.libraries
            .iter()
            .find(|library| library.address_begin <= addr && addr <= library.address_end)
    }

    fn find_symbol(&self, addr: usize) -> Option<&Symbol> {
        self.symbols.iter().find(|symbol| {
            symbol.address_begin as usize <= addr && addr <= symbol.address_end as usize
        })
    }

    pub fn resolve_frames<E, F: FnMut(ResolvedStackFrame) -> std::result::Result<(), E>>(
        &self,
        frames: &[StackFrame],
        only_address: bool,
        mut f: F,
    ) -> std::result::Result<(), E> {
        let mut dwarf_cache = HashMap::with_capacity(self.libraries.len());

        for frame in frames {
            let StackFrame::Ip(addr) = frame;
            let Some(library) = self.find_library(*addr) else {
                f(ResolvedStackFrame {
                    virtual_address: *addr,
                    physical_address: *addr,
                    symbol: String::from("<unknown>"),
                    inlined: false,
                    location: None,
                })?;

                continue;
            };

            let physical_address = *addr - library.address_begin;

            let mut location = None;

            if !only_address {
                let dwarf = match library.elf.as_ref() {
                    None => &None,
                    Some(elf) => match dwarf_cache.get(&library.name) {
                        Some(v) => v,
                        None => {
                            dwarf_cache.insert(library.name.clone(), Dwarf::create(elf));
                            dwarf_cache.get(&library.name).unwrap()
                        }
                    },
                };

                if let Some(dwarf) = dwarf {
                    let adjusted_addr = (physical_address - 1) as u64;

                    location = dwarf.find_location(adjusted_addr);
                }

                if let Some(symbol) = self.find_symbol(*addr) {
                    f(ResolvedStackFrame {
                        location,
                        physical_address,
                        inlined: false,
                        virtual_address: *addr,
                        symbol: format!(
                            "{}",
                            rustc_demangle::demangle(std::str::from_utf8(symbol.name).unwrap())
                        ),
                    })?;

                    continue;
                }
            }

            f(ResolvedStackFrame {
                location,
                physical_address,
                virtual_address: *addr,
                inlined: false,
                symbol: String::from("<unknown>"),
            })?;
        }

        Ok(())
    }

    pub fn create() -> LibraryManager {
        unsafe {
            let mut data = Data {
                symbols: vec![],
                libraries: vec![],
            };
            libc::dl_iterate_phdr(Some(callback), core::ptr::addr_of_mut!(data).cast());

            data.symbols
                .sort_by(|a, b| a.address_begin.cmp(&b.address_begin));
            data.libraries
                .sort_by(|a, b| a.address_begin.cmp(&b.address_begin));
            data.symbols.dedup_by(|a, b| {
                a.address_begin == b.address_begin && a.address_end == b.address_end
            });

            LibraryManager {
                symbols: data.symbols,
                libraries: data.libraries,
            }
        }
    }

    pub fn instance() -> Arc<LibraryManager> {
        INSTANCE
            .get_or_init(|| Arc::new(LibraryManager::create()))
            .clone()
    }
}

struct Data {
    symbols: Vec<Symbol>,
    libraries: Vec<Library>,
}

pub fn library_debug_path(library_name: OsString, build_id: &[u8]) -> std::io::Result<PathBuf> {
    let binary_path = std::fs::canonicalize(library_name)?.to_path_buf();

    let mut binary_debug_path = binary_path.clone();
    binary_debug_path.set_extension("debug");

    if std::fs::exists(&binary_debug_path)? {
        return Ok(binary_debug_path);
    }

    let relative_path = binary_path.strip_prefix("/").unwrap();
    let mut system_named_debug_path = std::path::Path::new("/uar/lib/debug").join(relative_path);
    system_named_debug_path.set_extension("debug");

    if std::fs::exists(&system_named_debug_path)? {
        return Ok(system_named_debug_path);
    }

    if build_id.len() >= 2 {
        let system_dir = std::path::Path::new("/uar/lib/debug").to_path_buf();

        fn encode_hex(bytes: &[u8]) -> String {
            let mut encoded_hex = String::with_capacity(bytes.len() * 2);
            for &b in bytes {
                write!(&mut encoded_hex, "{:02x}", b).unwrap();
            }
            encoded_hex
        }

        let mut system_id_debug_path = system_dir
            .join(encode_hex(&build_id[..1]))
            .join(encode_hex(&build_id[1..]));

        system_id_debug_path.set_extension("debug");

        if std::fs::exists(&system_id_debug_path)? {
            return Ok(system_id_debug_path);
        }
    }

    Ok(binary_path)
}

unsafe extern "C" fn callback(
    info: *mut libc::dl_phdr_info,
    _size: libc::size_t,
    vec: *mut libc::c_void,
) -> libc::c_int {
    let info = &*info;
    let data = &mut *vec.cast::<Data>();

    let library_name = match info.dlpi_name.is_null() || *info.dlpi_name == 0 {
        true => match data.libraries.is_empty() {
            true => OsString::from("/proc/self/exe"),
            false => OsString::new(),
        },
        false => {
            let bytes = CStr::from_ptr(info.dlpi_name).to_bytes();
            OsStr::from_bytes(bytes).to_owned()
        }
    };

    let Ok(library_path) = library_debug_path(library_name, &[]) else {
        return 0;
    };

    if let Some((ptr, len)) = mmap_library(library_path.clone()) {
        let mut library = Library {
            name: format!("{}", library_path.display()),
            address_begin: info.dlpi_addr as usize,
            address_end: info.dlpi_addr as usize + len,
            library_data: (ptr, len),
            elf: None,
        };

        let library_data = std::slice::from_raw_parts(ptr, len);
        symbols_from_elf(&mut library, library_data, &mut data.symbols);
        data.libraries.push(library);
    }

    0
}

unsafe fn mmap_library(library_path: PathBuf) -> Option<(*const u8, size_t)> {
    let file = std::fs::File::open(library_path).ok()?;
    let len = file.metadata().ok()?.len().try_into().ok()?;
    let ptr = libc::mmap(
        ptr::null_mut(),
        len,
        libc::PROT_READ,
        libc::MAP_PRIVATE,
        file.as_raw_fd(),
        0,
    );

    match ptr == libc::MAP_FAILED {
        true => None,
        false => Some((ptr as *const u8, len)),
    }
}

fn symbols_from_elf(library: &mut Library, data: &'static [u8], symbols: &mut Vec<Symbol>) -> bool {
    let Ok(elf) = ElfFile::parse(data) else {
        return false;
    };

    let symbol_table = match elf.symbol_table() {
        Some(symbol_table) => symbol_table,
        None => match elf.dynamic_symbol_table() {
            Some(dynamic_symbol_table) => dynamic_symbol_table,
            None => {
                return false;
            }
        },
    };

    for symbol in symbol_table.symbols() {
        let Ok(sym_name) = symbol.name() else {
            continue;
        };

        if sym_name.is_empty() || symbol.size() == 0 {
            continue;
        }

        symbols.push(Symbol {
            #[allow(clippy::missing_transmute_annotations)]
            name: unsafe { std::mem::transmute(sym_name) },
            address_begin: library.address_begin as u64 + symbol.address(),
            address_end: library.address_begin as u64 + symbol.address() + symbol.size(),
        });
    }

    library.elf = Some(elf);
    true
}
