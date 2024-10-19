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

use core::slice;
use std::borrow::Cow;
use std::collections::HashMap;
use std::ffi::CStr;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Write;
use std::fs::File;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::ptr;
use std::sync::Arc;

use addr2line::fallible_iterator::FallibleIterator;
use addr2line::Frame;
use addr2line::FrameIter;
use addr2line::Location;
use addr2line::LookupContinuation;
use addr2line::LookupResult;
use databend_common_arrow::arrow::array::ViewType;
use gimli::EndianSlice;
use gimli::NativeEndian;
use libc::iovec;
use libc::size_t;
use object::read::elf::FileHeader;
use object::read::elf::SectionHeader;
use object::read::elf::SectionTable;
use object::read::elf::Sym;
use object::CompressedFileRange;
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
        // object::files
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

// #[derive(Debug)]
struct Library {
    name: String,
    address_begin: usize,
    address_end: usize,
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

struct Dwarf<'a> {
    dwarf: addr2line::Context<EndianSlice<'a, NativeEndian>>,
}

impl<'a> Dwarf<'a> {
    pub fn create(elf: &'a ElfFile) -> Option<Dwarf> {
        let dwarf = gimli::read::Dwarf::load(|index| {
            let section_data = match elf.section_by_name(index.name()) {
                // Unsupported --compress-debug-sections
                Some(section) => {
                    let compressed_file_range = section.compressed_file_range()?;

                    match compressed_file_range.format {
                        CompressionFormat::None => section.data()?,
                        _ => &[],
                    }
                }
                None => &[],
            };

            Ok::<_, object::read::Error>(EndianSlice::new(section_data, NativeEndian {}))
        });

        match dwarf {
            Err(_) => None,
            Ok(dwarf) => match addr2line::Context::from_dwarf(dwarf) {
                Err(_) => None,
                Ok(ctx) => Some(Dwarf { dwarf: ctx }),
            },
        }
    }

    pub fn find_location(&self, probe: u64) -> Option<Location<'_>> {
        self.dwarf.find_location(probe).ok().flatten()
    }

    pub fn find_frames(
        &self,
        probe: u64,
    ) -> Vec<Frame<'static, EndianSlice<'static, NativeEndian>>> {
        let mut frames = match self.dwarf.find_frames(probe) {
            // TODO(winter):Unsupported split DWARF
            LookupResult::Load { .. } => None,
            LookupResult::Output(res) => res.ok(),
        };

        let mut res_frames = Vec::with_capacity(8);
        if let Some(mut frames) = frames {
            while let Ok(Some(frame)) = frames.next() {
                res_frames.push(unsafe { std::mem::transmute(frame) });
            }
        }

        res_frames
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
        mut f: F,
    ) -> std::result::Result<(), E> {
        let mut dwarf_cache = HashMap::with_capacity(self.libraries.len());

        for frame in frames {
            if let StackFrame::Unresolved(addr) = frame {
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

                let mut location = None;
                let physical_address = *addr - library.address_begin;

                if let Some(dwarf) = dwarf {
                    let adjusted_addr = (physical_address - 1) as u64;

                    let mut frames = dwarf.find_frames(adjusted_addr);

                    if !frames.is_empty() {
                        let last = frames.pop();

                        for frame in frames.into_iter() {
                            let mut symbol = String::from("<unknown>");

                            if let Some(function) = frame.function {
                                if let Ok(name) = function.demangle() {
                                    symbol = name.to_string();
                                }
                            }

                            f(ResolvedStackFrame {
                                symbol,
                                physical_address,
                                inlined: true,
                                virtual_address: *addr,
                                location: unsafe { std::mem::transmute(frame.location) },
                            })?;
                        }

                        if let Some(last) = last {
                            let mut symbol = String::from("<unknown>");

                            if let Some(function) = last.function {
                                if let Ok(name) = function.demangle() {
                                    symbol = name.to_string();
                                }
                            }

                            f(ResolvedStackFrame {
                                symbol,
                                physical_address,
                                inlined: false,
                                virtual_address: *addr,
                                location: unsafe { std::mem::transmute(last.location) },
                            })?;
                        }

                        continue;
                    }

                    location = dwarf.find_location(adjusted_addr);
                }

                if let Some(symbol) = self.find_symbol(*addr) {
                    f(ResolvedStackFrame {
                        physical_address,
                        inlined: false,
                        virtual_address: *addr,
                        location: unsafe { std::mem::transmute(location) },
                        symbol: format!(
                            "{}",
                            rustc_demangle::demangle(std::str::from_utf8(symbol.name).unwrap())
                        ),
                    })?;

                    continue;
                }

                f(ResolvedStackFrame {
                    physical_address,
                    virtual_address: *addr,
                    location: None,
                    inlined: false,
                    symbol: String::from("<unknown>"),
                })?;
            }
        }

        Ok(())
    }

    // #[cfg(target_os = "linux")]
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
                a.address_begin == b.address_begin && b.address_end == b.address_end
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
    let mut binary_path = std::fs::canonicalize(library_name)?.to_path_buf();

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

// #[cfg(target_os = "linux")]
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

    let Ok(library_path) = library_debug_path(library_name, &vec![]) else {
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

// #[cfg(target_os = "linux")]
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

pub fn symbols_from_elf(
    library: &mut Library,
    data: &'static [u8],
    symbols: &mut Vec<Symbol>,
) -> bool {
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

        if sym_name.is_empty() {
            continue;
        }

        symbols.push(Symbol {
            name: unsafe { std::mem::transmute(sym_name) },
            address_begin: library.address_begin as u64 + symbol.address(),
            address_end: library.address_begin as u64 + symbol.address() + symbol.size(),
        })
    }

    library.elf = Some(elf);
    true
}
