use std::mem;

pub fn bytearray_to_i32(b: &[u8]) -> i32 {
    let mut a = [0; 4];
    a.copy_from_slice(b);

    unsafe {
        mem::transmute::<[u8;4], i32>(a)
    }
}

pub fn i32_to_bytearray(n: i32) -> [u8; 4] {
    unsafe {
        mem::transmute::<i32, [u8;4]>(n)
    }
}


pub fn bytearray_to_usize(b: &[u8]) -> usize {
    let mut a = [0; 8];
    a.copy_from_slice(b);

    unsafe {
        mem::transmute::<[u8;8], usize>(a)
    }
}
