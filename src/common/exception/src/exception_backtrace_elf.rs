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
use std::cell::OnceCell;
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

use libc::iovec;
use libc::size_t;
use object::read::elf::FileHeader;
use object::read::elf::SectionHeader;
use object::read::elf::Sym;

use crate::exception_backtrace::ResolvedStackFrame;
use crate::exception_backtrace::StackFrame;

#[cfg(target_pointer_width = "32")]
type Elf = object::elf::FileHeader32<object::NativeEndian>;
#[cfg(target_pointer_width = "64")]
type Elf = object::elf::FileHeader64<object::NativeEndian>;

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

#[derive(Debug)]
struct Library {
    name: PathBuf,
    address_begin: usize,
    address_end: usize,
    // library_data: Vec<u8>,
    library_data: (*const u8, usize),
    // std::shared_ptr<Elf> elf;
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

impl LibraryManager {
    fn find_library(&self, addr: usize) -> Option<&Library> {
        match self
            .libraries
            .iter()
            .find(|library| library.address_begin <= addr)
        {
            None => None,
            Some(v) => match v.address_end >= addr {
                true => Some(v),
                false => None,
            },
        }
    }

    fn find_symbol(&self, addr: usize) -> Option<&Symbol> {
        match self
            .symbols
            .iter()
            .find(|symbol| symbol.address_begin as usize <= addr)
        {
            None => None,
            Some(v) => match v.address_end as usize >= addr {
                true => Some(v),
                false => None,
            },
        }
    }

    pub fn resolve_frames(&self, frames: &[StackFrame]) -> Vec<ResolvedStackFrame> {
        let mut res = Vec::with_capacity(frames.len());
        for frame in frames {
            if let StackFrame::Unresolved(addr) = frame {
                let Some(library) = self.find_library(*addr) else {
                    res.push(ResolvedStackFrame {
                        virtual_address: *addr,
                        physical_address: *addr,
                        library: String::new(),
                        symbol: String::from("<unknown>"),
                    });

                    continue;
                };

                if let Some(symbol) = self.find_symbol(*addr) {
                    res.push(ResolvedStackFrame {
                        virtual_address: *addr,
                        physical_address: *addr - library.address_begin,
                        library: String::new(),
                        symbol: format!(
                            "{}",
                            rustc_demangle::demangle(std::str::from_utf8(symbol.name).unwrap())
                        ),
                    });
                }
            }
        }

        res
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
        let mut system_dir = std::path::Path::new("/uar/lib/debug").to_path_buf();

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
        let library = Library {
            name: library_path,
            address_begin: info.dlpi_addr as usize,
            address_end: info.dlpi_addr as usize + len,
            library_data: (ptr, len),
        };

        let library_data =
            std::slice::from_raw_parts(library.library_data.0, library.library_data.1);
        symbols_from_elf(
            library.address_begin as u64,
            library_data,
            &mut data.symbols,
        );
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

pub fn symbols_from_elf(base: u64, data: &[u8], symbols: &mut Vec<Symbol>) -> bool {
    let Ok(elf) = Elf::parse(data) else {
        return false;
    };

    let Ok(endian) = elf.endian() else {
        return false;
    };

    let Ok(sections) = elf.sections(endian, data) else {
        return false;
    };

    let symbol_table = match sections.symbols(endian, data, object::elf::SHT_SYMTAB) {
        Ok(st) => st,
        Err(_) => match sections.symbols(endian, data, object::elf::SHT_DYNSYM) {
            Ok(st) => st,
            Err(_) => {
                return false;
            }
        },
    };

    let string_table = symbol_table.strings();
    for (_idx, symbol) in symbol_table.enumerate() {
        let Ok(sym_name) = symbol.name(endian, string_table) else {
            continue;
        };

        if sym_name.is_empty() {
            continue;
        }

        symbols.push(Symbol {
            name: unsafe { std::mem::transmute(sym_name) },
            address_begin: base + symbol.st_value(endian),
            address_end: base + symbol.st_value(endian) + symbol.st_size(endian),
        })
    }

    true
}
