use std::mem;

pub fn bytearray_to_i32(b: Vec<u8>) -> i32 {
    assert_eq!(b.len(), 4);
    let mut a = [0; 4];

    for i in 0..b.len() {
        a[i] = b[i];
    }

    unsafe {
        mem::transmute::<[u8;4], i32>(a)
    }
}

pub fn i32_to_bytearray(n: i32) -> [u8; 4] {
    unsafe {
        mem::transmute::<i32, [u8;4]>(n)
    }
}


pub fn bytearray_to_usize(b: Vec<u8>) -> usize {
    assert_eq!(b.len(), 8);
    let mut a = [0; 8];

    for i in 0..b.len() {
        a[i] = b[i];
    }

    unsafe {
        mem::transmute::<[u8;8], usize>(a)
    }
}
