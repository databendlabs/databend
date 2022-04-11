use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;

pub struct FormatVec(pub Vec<String>);

impl Display for FormatVec {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        let mut comma_separated = String::new();

        for s in &self.0[0..self.0.len() - 1] {
            comma_separated.push_str(s);
            comma_separated.push_str(", ");
        }

        comma_separated.push_str(&self.0[self.0.len() - 1]);
        write!(f, "{}", comma_separated)
    }
}
