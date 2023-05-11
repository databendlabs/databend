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

static ASCII_TABLE: [&str; 128] = [
    "<NUL>",
    "<SOH>",
    "<STX>",
    "<ETX>",
    "<EOT>",
    "<ENQ>",
    "<ACK>",
    "<BEL>",
    "<BACKSPACE>",
    "<TAB>",
    "<LINE FEED>",
    "<VERTICAL TAB>",
    "<FROM FEED>",
    "<CARRIAGE RETURN>",
    "<SO>",
    "<SI>",
    "<DLE>",
    "<DC1/XON>",
    "<DC2>",
    "<DC3/XOFF>",
    "<DC4>",
    "<NAK>",
    "<SYN>",
    "<ETB>",
    "<CAN>",
    "<EM>",
    "<SUB>",
    "<ESC>",
    "<FS>",
    "<GS>",
    "<RS>",
    "<US>",
    " ",
    "!",
    "<DOUBLE QUOTE>",
    "#",
    "$",
    "%",
    "&",
    "<SINGLE QUOTE>",
    "(",
    ")",
    "*",
    "+",
    ",",
    "-",
    ".",
    "/",
    "0",
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    ":",
    ";",
    "<",
    "=",
    ">",
    "?",
    "@",
    "A",
    "B",
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "I",
    "J",
    "K",
    "L",
    "M",
    "N",
    "O",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "U",
    "V",
    "W",
    "X",
    "Y",
    "Z",
    "[",
    "<BACKSLASH>",
    "]",
    "^",
    "_",
    "`",
    "a",
    "b",
    "c",
    "d",
    "e",
    "f",
    "g",
    "h",
    "i",
    "j",
    "k",
    "l",
    "m",
    "n",
    "o",
    "p",
    "q",
    "r",
    "s",
    "t",
    "u",
    "v",
    "w",
    "x",
    "y",
    "z",
    "{",
    "|",
    "}",
    "~",
    "<DEL>",
];

pub fn verbose_char(c: u8) -> String {
    if c < 128 {
        ASCII_TABLE[c as usize].to_string()
    } else {
        (c as char).to_string()
    }
}

pub fn verbose_string(buf: &[u8], out: &mut String) {
    out.push('"');

    if buf.is_empty() {
        out.push_str("<EMPTY>");
    }

    for &c in buf.iter() {
        if c < 128 {
            out.push_str(ASCII_TABLE[c as usize]);
        } else {
            out.push(c as char);
        }
    }

    out.push('"');
}
