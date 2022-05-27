use std::char;
use std::iter::Peekable;

pub fn unescape(s: &str) -> Option<String> {
    let mut chars = s.chars().peekable();
    let mut s = String::new();

    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('b') => s.push('\u{0008}'),
                Some('f') => s.push('\u{000C}'),
                Some('n') => s.push('\n'),
                Some('r') => s.push('\r'),
                Some('t') => s.push('\t'),
                Some('\'') => s.push('\''),
                Some('\"') => s.push('\"'),
                Some('\\') => s.push('\\'),
                Some('u') => s.push(unescape_unicode(&mut chars)?),
                Some('x') => s.push(unescape_byte(&mut chars)?),
                Some(c) if c.is_digit(8) => s.push(unescape_octal(c, &mut chars)),
                Some(c) => {
                    s.push('\\');
                    s.push(c);
                }
                None => {
                    s.push('\\');
                }
            };
        } else if c == '\'' {
            s.push('\'');
            match chars.next() {
                Some('\'') | None => (),
                Some(c) => {
                    s.push(c);
                }
            }
        } else {
            s.push(c);
            continue;
        }
    }

    Some(s)
}

fn unescape_unicode(chars: &mut Peekable<impl Iterator<Item = char>>) -> Option<char> {
    let mut s = String::new();

    for _ in 0..4 {
        s.push(chars.next()?);
    }

    let u = u32::from_str_radix(&s, 16).ok()?;
    char::from_u32(u)
}

fn unescape_byte(chars: &mut Peekable<impl Iterator<Item = char>>) -> Option<char> {
    let mut s = String::new();

    for _ in 0..2 {
        s.push(chars.next()?);
    }

    let u = u32::from_str_radix(&s, 16).ok()?;
    char::from_u32(u)
}

fn unescape_octal(c1: char, chars: &mut Peekable<impl Iterator<Item = char>>) -> char {
    let mut oct = c1.to_digit(8).unwrap();

    while let Some(c) = chars.peek() {
        if let Some(digit) = c.to_digit(8) {
            oct = oct * 8 + digit;
            chars.next();
        } else {
            break;
        }
    }

    char::from_u32(oct).unwrap()
}
