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


fn read_record(offsets: RowOffsets, data: Vec<u8>)
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

fn write_record(offsets: RowOffsets, data: &mut [u8],
                key: i32, val: &[u8]) {
    mem_move(&mut data[offsets.key_offset..offsets.val_offset], &i32_to_bytearray(key));
    mem_move(&mut data[offsets.val_offset..offsets.row_end], val);
}

fn serialize_records(records: Vec<(i32, Vec<u8>)>) -> [u8; PAGE_SIZE] {
    let mut storage = [0; PAGE_SIZE];
    let valsize = 4;
    for (i, (k,v)) in records.into_iter().enumerate() {
        let offsets = compute_offsets(i, valsize);
        write_record(offsets, &mut storage, k, &v);
    }
    storage
}

pub struct BufferPoolManager {
    // runs of file_a go into file_b as they are sorted; in the next
    // pass, runs from file_b get moved to file_a, and so on.
    files: [File; 3],
    output_buffer: Page,
    input_buffers: Vec<Page>,
    // how many records can a page hold
    records_per_page: usize,
}

impl BufferPoolManager {
    pub fn new(input_filename: &str) -> BufferPoolManager {
        let input_file = OpenOptions::new()
            .read(true)
            .open(input_filename);
        let file_a = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("/tmp/file_a");
        let file_b = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("/tmp/file_b");
        BufferPoolManager {
            files: [input_file.unwrap(), file_a.unwrap(),file_b.unwrap()],
            output_buffer: Page::new(),
            input_buffers: vec![Page::new(), Page::new()],
            records_per_page: (PAGE_SIZE - 1) / (4 + 4),
        }
    }

    pub fn write_page(mut file: &File, page_id: usize, data: &[u8]) {
        let offset = (page_id * PAGE_SIZE) as u64;
        file.seek(SeekFrom::Start(offset))
            .expect("Could not seek to offset");
        file.write(data).expect("write failed");
        file.flush().expect("flush failed");
    }

    pub fn read_records(&self, data: &[u8]) -> Vec<(i32,Vec<u8>)> {
        let num_records_bytes = data[0..8].to_vec();
        let num_records = bytearray_to_usize(num_records_bytes);
        println!("num_records: {}", num_records);
        let mut records = Vec::with_capacity(num_records);
        let valsize = 4;        // NOTE: fixed size(hacky)
        for i in 0..num_records {
            let offsets = compute_offsets(i, valsize);
            records.push(read_record(offsets, data.to_vec()));
        }

        records
    }

    pub fn fetch_page(&mut self, input_file_index: usize,
                      page_id: usize, bufpool_id: usize) {
        let offset = (page_id * PAGE_SIZE) as u64;

        let mut input_file = &self.files[input_file_index];
        input_file.seek(SeekFrom::Start(offset))
            .expect("Could not seek to offset");
        let mut storage = [0; PAGE_SIZE];
        input_file.read(&mut storage)
            .expect("Could not read file");
        self.input_buffers[bufpool_id].records =
            self.read_records(&storage);
    }

    
    fn merge(&mut self, a: usize, b: usize, 
             input_file_index: usize, output_file_index: usize) {
        // fetch pages a and b into input buffers
        self.fetch_page(input_file_index, a, 0);
        self.fetch_page(input_file_index, b, 1);
        
        // merge a and b into output_buffer
        let mut a_iter = self.input_buffers[0].records
            .iter().peekable();
        let mut b_iter = self.input_buffers[1].records
            .iter().peekable();
        let mut first_a = a_iter.next();
        let mut first_b = b_iter.next();
        
        let output_buffer = [0; PAGE_SIZE];
        let mut output_page_id = 0;
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
            };

            // check if output buffer is full(# records > max). If it
            // is full, write to file. And continue with empty buffer.
            if self.output_buffer.records.len() >= self.records_per_page {
                let records = mem::replace(&mut self.output_buffer.records,
                                           vec![]);
                let new_page_data =
                    serialize_records(records);
                Self::write_page(&self.files[output_file_index],
                                 output_page_id,
                                 &new_page_data);
                output_page_id += 1;
            }
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
        bp.merge(0, 1, 0, 1);
        
        assert_eq!(2 + 2, 4);
    }
}
