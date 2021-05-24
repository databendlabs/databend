use crate::FactoryFuncRef;
use common_exception::Result;
use crate::hashes::siphash::SipHashFunction;

#[derive(Clone)]
pub struct HashesFunction;

impl HashesFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("siphash", SipHashFunction::try_create);
        Ok(())
    }
}