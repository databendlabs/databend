/// Mask a string by "******". If the lenght of the string is equal or greater than 9, keep the 3-byte suffix.
#[inline]
pub fn mask_string(s: &str) -> String {
    if s.is_empty() {
        return "".to_string();
    }
    let mut ret = "******".to_string();
    if s.len() >= 9 {
        ret.push_str(&s[s.len() - 3..]);
    }
    ret
}
