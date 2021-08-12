pub trait IHasher<Key>
{
    fn hash(key: &Key) -> u64;
}

pub struct DefaultHasher<T> {
    /// Generics hold
    type_hold: PhantomData<T>,
}


macro_rules! primitive_hasher_impl {
    ($primitive_type:ty) => {
        impl IHasher<$primitive_type> for DefaultHasher<$primitive_type> {
            #[inline(always)]
            fn hash(key: &$primitive_type) -> u64 {
                let mut hash_value = *key as u64;
                hash_value ^= hash_value >> 33;
                hash_value = hash_value.wrapping_mul(0xff51afd7ed558ccd_u64);
                hash_value ^= hash_value >> 33;
                hash_value = hash_value.wrapping_mul(0xc4ceb9fe1a85ec53_u64);
                hash_value ^= hash_value >> 33;
                hash_value
            }
        }
    };
}

primitive_hasher_impl!(i8);
primitive_hasher_impl!(i16);
primitive_hasher_impl!(i32);
primitive_hasher_impl!(i64);
primitive_hasher_impl!(u8);
primitive_hasher_impl!(u16);
primitive_hasher_impl!(u32);
primitive_hasher_impl!(u64);

#[test]
fn test_primitive_hasher() {
    println!("{:?}", DefaultHasher::<i8>::hash(&1_i8));
    println!("{:?}", DefaultHasher::<i8>::hash(&2_i8));
}

