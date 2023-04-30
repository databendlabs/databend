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

use std::error::Error;
use std::ffi::CString;
use std::fs::File;
use std::io::Read;

use libc::c_char;

const PROF_DUMP: &[u8] = b"prof.dump\0";
const OPT_PROF: &[u8] = b"opt.prof\0";

// caller site is supposed to clean up the tmp_file
// e.g. use `tempfile` to generate a temporary file
pub fn dump_profile(tmp_file_path: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    // precheck
    if !is_prof_enabled()? {
        return Err("opt.prof is not ON,
                   please start the application with proper MALLOC env.
                   e.g.  MALLOC_CONF=prof:true "
            .into());
    }

    let mut bytes = CString::new(tmp_file_path)?.into_bytes_with_nul();
    let ptr = bytes.as_mut_ptr() as *mut c_char;
    unsafe {
        tikv_jemalloc_ctl::raw::write(PROF_DUMP, ptr).map_err(|e| {
            format!(
                "dump Jemalloc prof to path {}: failure: {}",
                tmp_file_path, e
            )
        })?
    }
    let mut f = File::open(tmp_file_path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    Ok(buf)
}

fn is_prof_enabled() -> Result<bool, Box<dyn Error>> {
    Ok(unsafe {
        tikv_jemalloc_ctl::raw::read::<bool>(OPT_PROF)
            .map_err(|e| format!("read opt.prof failure: {}", e))?
    })
}
