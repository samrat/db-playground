use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::SeekFrom;
use std::io::prelude::*;
use std::mem;

mod util;
use util::*;

const PAGE_SIZE : usize = 4096;
const HEADER_SIZE : usize = 8;  // page header

// these are currently fixed at compile-time. Make this more generic
// by bringing in a serialization/deserialization library.
const KEY_SIZE : usize = 4;     // i32
const VAL_SIZE : usize = 4;


struct RowOffsets {
    key_offset: usize,
    val_offset: usize,
    row_end: usize,
}

// Representation of a file page resident in buffer pool
struct Page {
    pub id: usize,
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

fn compute_offsets(row_num: usize, keysize: usize, valsize: usize) -> RowOffsets {
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

fn mem_move(dest: &mut [u8], src: &[u8]) {
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
    let keysize = KEY_SIZE;
    let valsize = VAL_SIZE;
    mem_move(&mut storage[0..8], &i32_to_bytearray(records.len() as i32));
    for (i, (k,v)) in records.into_iter().enumerate() {
        let offsets = compute_offsets(i, keysize, valsize);
        write_record(offsets, &mut storage, k, &v);
    }
    storage
}

pub struct ExternalMergeSort {
    // runs of file_a go into file_b as they are sorted; in the next
    // pass, runs from file_b get moved to file_a, and so on.
    files: [File; 3],
    output_buffer: Page,
    input_buffers: Vec<Page>,
    // how many records can a page hold
    records_per_page: usize,
}

impl ExternalMergeSort {
    fn new(input_filename: &str) -> ExternalMergeSort {
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
        ExternalMergeSort {
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

    pub fn read_records(data: &[u8]) -> Vec<(i32,Vec<u8>)> {
        let num_records_bytes = data[0..8].to_vec();
        let num_records = bytearray_to_usize(num_records_bytes);
        println!("num_records: {}", num_records);
        let mut records = Vec::with_capacity(num_records);
        let keysize = KEY_SIZE;
        let valsize = VAL_SIZE;
        for i in 0..num_records {
            let offsets = compute_offsets(i, keysize, valsize);
            records.push(read_record(offsets, data.to_vec()));
        }

        records
    }

    fn fetch_page(&mut self, input_file_index: usize,
                      page_id: usize, bufpool_id: usize) {
        let offset = (page_id * PAGE_SIZE) as u64;
        println!("fetch_page: page_id={}", page_id);
        let mut input_file = &self.files[input_file_index];
        input_file.seek(SeekFrom::Start(offset))
            .expect("Could not seek to offset");
        let mut storage = [0; PAGE_SIZE];
        input_file.read(&mut storage)
            .expect("Could not read file");
        self.input_buffers[bufpool_id].records =
            Self::read_records(&storage);
    }

    /// Sort records within pages(this is "pass 0" in the algorithm)
    pub fn sort_pages(&mut self, num_pages: usize) {
        for i in 0..num_pages {
            // get from input file
            self.fetch_page(0, i, 0);
            self.input_buffers[0].sort();
            let sorted_records = &self.input_buffers[0].records;
            let new_page_data =
                serialize_records(sorted_records.to_vec());
            // write to file_a
            Self::write_page(&self.files[1],
                             i,
                             &new_page_data);
        }
    }

    fn flush_output_buffer(&mut self, output_file_index: usize, output_page_id: usize) {
        // println!("page={}, writing {:?}", output_page_id, self.output_buffer.records);
        let records = mem::replace(&mut self.output_buffer.records,
                                   vec![]);
        let new_page_data =
            serialize_records(records);
        Self::write_page(&self.files[output_file_index],
                         output_page_id,
                         &new_page_data);
    }

    // merge two `run`s
    fn merge(&mut self, mut a: usize, mut b: usize, run_size: usize,
             input_file_index: usize, output_file_index: usize) {
        // fetch pages a and b into input buffers
        self.fetch_page(input_file_index, a, 0);
        self.fetch_page(input_file_index, b, 1);

        // merge a and b into output_buffer
        let mut a_iter = self.input_buffers[0].records.clone()
            .into_iter();
        let mut b_iter = self.input_buffers[1].records.clone()
            .into_iter();
        let mut first_a = a_iter.next();
        let mut first_b = b_iter.next();
        let a_end = a+run_size-1;
        let b_end = b+run_size-1;

        let output_buffer = [0; PAGE_SIZE];
        // start filling at page a in output file
        let mut output_page_id = a;
        loop {
            match (first_a.clone(), first_b.clone()) {
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
                    println!("b={} b_end={}", b, b_end);
                    if b < b_end {
                        b += 1;
                        self.fetch_page(input_file_index, b, 1);
                        b_iter = self.input_buffers[1].records.clone()
                            .into_iter();
                        first_b = b_iter.next();
                        continue;
                    } else {
                        self.output_buffer.records.push(fa.clone());
                        first_a = a_iter.next();
                    }
                },
                (None, Some(fb)) => {
                    println!("a={} a_end={}", a, a_end);
                    if a < a_end {
                        a += 1;
                        self.fetch_page(input_file_index, a, 0);
                        a_iter = self.input_buffers[0].records.clone()
                            .into_iter();
                        first_a = a_iter.next();
                        continue;
                    } else {
                        self.output_buffer.records.push(fb.clone());
                        first_b = b_iter.next();
                    }
                },
            };

            // check if output buffer is full(# records > max). If it
            // is full, write to file. And continue with empty buffer.
            if self.output_buffer.records.len() >= self.records_per_page {
                self.flush_output_buffer(output_file_index, output_page_id);
                output_page_id += 1;
            }
        }

        self.flush_output_buffer(output_file_index, output_page_id);
    }

    pub fn sort_all(&mut self, num_pages: usize) -> usize {
        self.sort_pages(num_pages);
        let mut run_size = 1;

        let mut src_file = 1;
        let mut dest_file = 2;
        while run_size <= (num_pages / 2) {
            let group_size = run_size * 2;
            let num_groups = num_pages / group_size;
            println!("run_size = {}, group_size={} num_groups={}",
                     run_size, group_size, num_groups);
            for i in 0..num_groups {
                let group_start = i*group_size;
                let first_run = group_start;
                let second_run = group_start + run_size;
                self.merge(first_run, second_run, run_size, src_file, dest_file);
                println!("\tfirst_run: {} second_run: {}", first_run, second_run);
            }

            run_size *= 2;

            // delete the file whose pages we just merged
            let src_file_name = match src_file {
                1 => "/tmp/file_a",
                2 => "/tmp/file_b",
                _ => panic!("impossible"),
            };
            fs::remove_file(src_file_name);
            // create a new file to use as the destination in next
            // iteration
            let new_src_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(src_file_name)
                .ok()
                .unwrap();
            self.files[src_file] = new_src_file;

            // swap
            let temp = src_file;
            src_file = dest_file;
            dest_file = temp;
        }
        // last file written to
        src_file
    }

    pub fn sort_file(input_filename: &str, output_filename: &str) {
        let mut ems = ExternalMergeSort::new(input_filename);
        let num_bytes = std::fs::metadata(input_filename).ok().unwrap().len();
        let num_pages = (num_bytes as f32 / PAGE_SIZE as f32).ceil() as usize;
        let file_index = ems.sort_all(num_pages);
        let filename = match file_index {
            1 => "/tmp/file_a",
            2 => "/tmp/file_b",
            _ => panic!("impossible"),
        };
        fs::rename(filename, output_filename);
    }
}


#[cfg(test)]
mod tests {
    use ExternalMergeSort;
    use serialize_records;
    use util::*;
    use PAGE_SIZE;

    use std::fs::OpenOptions;
    use std::io::prelude::*;
    use std::io::SeekFrom;

    extern crate rand;
    use tests::rand::Rng;

    fn create_rand_file() -> Vec<(i32, Vec<u8>)> {
        let records_per_page = 511;
        let mut rng = rand::thread_rng();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("/tmp/gen_rand")
            .ok()
            .unwrap();
        let mut all_records = Vec::with_capacity(8*records_per_page);
        for p in 0..8 {
            let mut records = Vec::with_capacity(records_per_page);
            for i in 0..records_per_page {
                records.push((rng.gen::<i32>(), vec![111,0,0,0]));
            }
            let storage = serialize_records(records.clone());
            all_records.append(&mut records);
            ExternalMergeSort::write_page(&file, p, &storage)
        }

        all_records.sort_by(|a, b| a.0.cmp(&b.0));
        all_records
    }

    #[test]
    fn test_sort() {
        let expected = create_rand_file();
        ExternalMergeSort::sort_file("/tmp/gen_rand", "/tmp/gen_rand-sorted");

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("/tmp/gen_rand-sorted")
            .ok()
            .unwrap();
        let mut actual = vec![];
        for i in 0..8 {
            let mut storage = [0; PAGE_SIZE];
            file.seek(SeekFrom::Start(i*PAGE_SIZE as u64))
                .expect("Could not seek to offset");
            file.read(&mut storage)
                .expect("Could not read file");

            actual.append(&mut ExternalMergeSort::read_records(&storage));
        }
        println!("[test] expected.len = {} actual.len = {}", expected.len(), actual.len());
        assert_eq!(expected, actual);
    }

    fn it_works() {
        let mut bp = ExternalMergeSort::sort_file("/tmp/randfile", "/tmp/randfile-sorted");
        // bp.sort_all(4);
        println!("{:?}", create_rand_file());
        assert_eq!(2 + 2, 4);
    }
}
