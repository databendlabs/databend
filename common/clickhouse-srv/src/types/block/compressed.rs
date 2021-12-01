// Copyright 2021 Datafuse Labs.
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

use std::io;
use std::io::Read;
use std::mem;
use std::os::raw::c_char;
use std::os::raw::c_int;

use byteorder::LittleEndian;
use byteorder::WriteBytesExt;
use lz4::liblz4::LZ4_decompress_safe;
use naive_cityhash::cityhash128;
use naive_cityhash::U128;

use crate::binary::ReadEx;
use crate::errors::Error;
use crate::errors::Result;

const DBMS_MAX_COMPRESSED_SIZE: u32 = 0x4000_0000; // 1GB

pub(crate) struct CompressedReader<'a, R> {
    reader: &'a mut R,
    cursor: io::Cursor<Vec<u8>>,
}

pub(crate) fn make<R>(reader: &mut R) -> CompressedReader<R> {
    CompressedReader {
        reader,
        cursor: io::Cursor::new(Vec::new()),
    }
}

impl<'a, R> CompressedReader<'a, R>
where R: Read + ReadEx
{
    fn is_empty(&self) -> bool {
        let len = self.cursor.get_ref().len();
        let pos = self.cursor.position() as usize;
        len == pos
    }

    fn fill(&mut self) -> Result<()> {
        let cursor = mem::replace(&mut self.cursor, io::Cursor::new(Vec::new()));
        let buffer = cursor.into_inner();

        let tmp = decompress_buffer(&mut self.reader, buffer)?;
        self.cursor = io::Cursor::new(tmp);
        Ok(())
    }
}

impl<'a, R> Read for CompressedReader<'a, R>
where R: Read + ReadEx
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.is_empty() {
            self.fill()?;
        }

        self.cursor.read(buf)
    }
}

pub fn decompress_buffer<R>(reader: &mut R, mut buffer: Vec<u8>) -> Result<Vec<u8>>
where R: ReadEx {
    let h = U128 {
        lo: reader.read_scalar()?,
        hi: reader.read_scalar()?,
    };

    let method: u8 = reader.read_scalar()?;
    if method != 0x82 {
        let message: String = format!("unsupported compression method {}", method);
        return Err(raise_error(message));
    }

    let compressed: u32 = reader.read_scalar()?;
    let original: u32 = reader.read_scalar()?;

    if compressed > DBMS_MAX_COMPRESSED_SIZE {
        return Err(raise_error("compressed data too big".to_string()));
    }

    buffer.resize(compressed as usize, 0_u8);
    {
        let mut cursor = io::Cursor::new(&mut buffer);
        cursor.write_u8(0x82)?;
        cursor.write_u32::<LittleEndian>(compressed)?;
        cursor.write_u32::<LittleEndian>(original)?;
    }
    reader.read_bytes(&mut buffer[9..])?;

    if h != cityhash128(&buffer) {
        return Err(raise_error("data was corrupted".to_string()));
    }

    let data = vec![0_u8; original as usize];
    let status = unsafe {
        LZ4_decompress_safe(
            (buffer.as_mut_ptr() as *const c_char).add(9),
            data.as_ptr() as *mut c_char,
            (compressed - 9) as c_int,
            original as c_int,
        )
    };

    if status < 0 {
        return Err(raise_error("can't decompress data".to_string()));
    }

    Ok(data)
}

fn raise_error(message: String) -> Error {
    message.into()
}
