use std::fmt;

// TODO Using strings as a keys
#[derive(Clone, Copy)]
pub struct Enum8(pub(crate) i8);

#[derive(Clone, Copy)]
pub struct Enum16(pub(crate) i16);

impl Default for Enum8 {
    fn default() -> Self {
        Self(0)
    }
}

impl Default for Enum16 {
    fn default() -> Self {
        Self(0)
    }
}

impl PartialEq for Enum16 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl fmt::Display for Enum16 {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Enum({})", self.0)
    }
}

impl fmt::Debug for Enum16 {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Enum({})", self.0)
    }
}

impl Enum16 {
    pub fn of(source: i16) -> Self {
        Self(source)
    }
    #[inline(always)]
    pub fn internal(self) -> i16 {
        self.0
    }
}
impl PartialEq for Enum8 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl fmt::Display for Enum8 {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Enum8({})", self.0)
    }
}

impl fmt::Debug for Enum8 {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Enum8({})", self.0)
    }
}

impl Enum8 {
    pub fn of(source: i8) -> Self {
        Self(source)
    }
    #[inline(always)]
    pub fn internal(self) -> i8 {
        self.0
    }
}
