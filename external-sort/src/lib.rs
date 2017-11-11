use std::fs::File;
use std::fs::OpenOptions;
use std::io::SeekFrom;
use std::io::prelude::*;
use std::mem;

const PAGE_SIZE : usize = 4096;
const HEADER_SIZE : usize = 8;  // page header

fn bytearray_to_i32(b: Vec<u8>) -> i32 {
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


fn bytearray_to_usize(b: Vec<u8>) -> usize {
    assert_eq!(b.len(), 8);
    let mut a = [0; 8];

    for i in 0..b.len() {
        a[i] = b[i];
    }

    unsafe {
        mem::transmute::<[u8;8], usize>(a)
    }
}

struct RowOffsets {
    key_offset: usize,
    val_offset: usize,
    row_end: usize,
}

// Representation of a file page resident in buffer pool
struct Page {
    pub id: usize,
    // pub storage: [u8; PAGE_SIZE],
    pub records: Vec<(i32,Vec<u8>)>,
    pub num_records: usize,
}

impl Page {
    pub fn new() -> Page {
        Page {
            id: 0,
            records: vec![],
            num_records: 0,
        }
    }
    
    pub fn sort(&mut self) {
        self.records.sort_by(|&(ref ak, ref av), &(ref bk, ref bv)|
                             ak.cmp(&bk));
    }

}

fn compute_offsets(row_num: usize, valsize: usize) -> RowOffsets {
    let keysize = 4;        // i32
    // let valsize = mem::size_of::<V>();
    let total_size = keysize + valsize;

    let row_offset = HEADER_SIZE + (row_num * total_size);
    let key_offset = row_offset;
    let val_offset = key_offset + keysize;
    let row_end = val_offset + valsize;

    RowOffsets {
        key_offset: key_offset,
        val_offset: val_offset,
        row_end: row_end,
    }
}


fn read_record(offsets: RowOffsets, data: Vec<u8>, row_num: usize)
               -> (i32,Vec<u8>) {
    let key = bytearray_to_i32(
        data[offsets.key_offset..offsets.val_offset].to_vec());
    let val = data[offsets.val_offset..offsets.row_end].to_vec();
    (key, val)
}

pub fn mem_move(dest: &mut [u8], src: &[u8]) {
    for (d, s) in dest.iter_mut().zip(src) {
        *d = *s
    }
}

pub struct BufferPoolManager {
    input_file: File,
    // runs of fileA go into fileB as they are sorted; in the next
    // pass, runs from fileB get moved to fileA, and so on.
    fileA: File,
    fileB: File,
    output_buffer: Page,
    input_buffers: Vec<Page>,
}

impl BufferPoolManager {
    pub fn new(input_filename: &str) -> BufferPoolManager {
        let input_file = OpenOptions::new()
            .read(true)
            .open(input_filename);
        let fileA = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("/tmp/fileA");
        let fileB = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("/tmp/fileB");
        BufferPoolManager {
            input_file: input_file.unwrap(),
            fileA: fileA.unwrap(),
            fileB: fileB.unwrap(),
            output_buffer: Page::new(),
            input_buffers: vec![Page::new(), Page::new()]
        }
    }

    pub fn read_records(&self, data: &[u8]) -> Vec<(i32,Vec<u8>)> {
        let num_records_bytes = data[0..8].to_vec();
        let num_records = bytearray_to_usize(num_records_bytes);
        println!("num_records: {}", num_records);
        let mut records = Vec::with_capacity(num_records);
        let valsize = 4;        // NOTE: fixed size(hacky)
        for i in 0..num_records {
            let offsets = compute_offsets(i, valsize);
            records.push(read_record(offsets, data.to_vec(), i));
        }

        records
    }

    pub fn fetch_page(&mut self, page_id: usize, bufpool_id: usize) {
        let offset = (page_id * PAGE_SIZE) as u64;
        // TODO: file itself should change. ie. later passes must read
        // from fileA, fileB
        self.input_file.seek(SeekFrom::Start(offset))
            .expect("Could not seek to offset");
        let mut storage = [0; PAGE_SIZE];
        self.input_file.read(&mut storage)
            .expect("Could not read file");
        self.input_buffers[bufpool_id].records =
            self.read_records(&storage);
    }
    
    fn merge(&mut self, a: usize, b: usize) {
        // fetch pages a and b into input buffers
        self.fetch_page(a, 0);
        self.fetch_page(b, 1);
        
        // merge a and b into output_buffer
        let mut a_iter = self.input_buffers[0].records
            .iter().peekable();
        let mut b_iter = self.input_buffers[1].records
            .iter().peekable();
        let mut first_a = a_iter.next();
        let mut first_b = b_iter.next();
        loop {
            match (first_a, first_b) {
                (None, None) => break,
                (Some(fa), Some(fb)) => {
                    if fa.0 < fb.0 {
                        self.output_buffer.records.push(fa.clone());
                        first_a = a_iter.next();
                    } else {
                        self.output_buffer.records.push(fb.clone());
                        first_b = b_iter.next();
                        
                    }
                },
                (Some(fa), None) => {
                    self.output_buffer.records.push(fa.clone());
                    first_a = a_iter.next();
                },
                (None, Some(fb)) => {
                    self.output_buffer.records.push(fb.clone());
                    first_b = b_iter.next();
                },
            }

            // TODO: check if output buffer is full(# records >
            // max). If it is full, write to file. And continue with
            // empty buffer.
        }

        println!("{:?}", self.output_buffer.records);
    }
}

#[cfg(test)]
mod tests {
    use BufferPoolManager;
    
    #[test]
    fn it_works() {
        let mut bp = BufferPoolManager::new("test2");
        bp.merge(0, 1);
        
        assert_eq!(2 + 2, 4);
    }
}
