use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::hash::BuildHasherDefault;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::rc::Rc;

const DEFAULT_PAGE_SIZE: usize = 64 * 1024; // 64KiB
const MIN_PAGE_SIZE: usize = 512;
const MAX_PAGE_SIZE: usize = 1024 * 1024; // 1MiB
const DEFAULT_CAPACITY: usize = 16 * 1024 * 1024; // 16MiB
const MIN_CAPACITY: usize = MIN_PAGE_SIZE;
const MAX_CAPACITY: usize = 1024 * 1024 * 1024; // 1GiB

type LinkedListNode = Rc<RefCell<LinkedListNodeInner>>;
pub type AHashMap<K, V> = HashMap<K, V, BuildHasherDefault<ahash::AHasher>>;

struct LinkedListNodeInner {
    data: Vec<u8>,
}

pub struct WriteThroughCache {
    page_size: usize,
    capacity: usize,
    cache: AHashMap<u64, LinkedListNode>,
    usage_order: VecDeque<u64>,
    file: File,
    file_size: u64,
}

impl WriteThroughCache {
    pub fn new(
        file_path: &PathBuf,
        page_size: Option<usize>,
        capacity: Option<usize>,
    ) -> std::io::Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)?;

        let file_size = file.metadata()?.len();
        let page_size = page_size.unwrap_or(DEFAULT_PAGE_SIZE);
        let capacity = capacity.unwrap_or(DEFAULT_CAPACITY);

        if page_size < MIN_PAGE_SIZE || capacity < MIN_CAPACITY {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Page size must be at least {} bytes and capacity must be at least {} bytes",
                    MIN_PAGE_SIZE, MIN_CAPACITY
                ),
            ));
        }

        if page_size > MAX_PAGE_SIZE || capacity > MAX_CAPACITY {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Page size must be at most {} bytes and capacity must be at most {} bytes",
                    MAX_PAGE_SIZE, MAX_CAPACITY
                ),
            ));
        }

        Ok(Self {
            page_size,
            capacity,
            cache: AHashMap::default(),
            usage_order: VecDeque::new(),
            file,
            file_size,
        })
    }

    pub fn read(&mut self, address: u64, size: usize) -> std::io::Result<Vec<u8>> {
        let mut buffer = vec![0; size];
        let mut remaining_size = size;
        let mut current_address = address;

        while remaining_size > 0 {
            let page_id = current_address / self.page_size as u64;
            let offset = (current_address % self.page_size as u64) as usize;
            let read_size = std::cmp::min(remaining_size, self.page_size - offset);

            let data = self.read_page(page_id)?;
            let buf_start = size - remaining_size;
            buffer[buf_start..buf_start + read_size]
                .copy_from_slice(&data[offset..offset + read_size]);

            remaining_size -= read_size;
            current_address += read_size as u64;
        }

        Ok(buffer)
    }

    pub fn write(&mut self, address: u64, data: &[u8]) -> std::io::Result<()> {
        let mut remaining_size = data.len();
        let mut current_address = address;

        while remaining_size > 0 {
            let page_id = current_address / self.page_size as u64;
            let offset = (current_address % self.page_size as u64) as usize;
            let write_size = std::cmp::min(remaining_size, self.page_size - offset);

            let mut page_data = match self.read_page(page_id) {
                Ok(data) => data,
                Err(_) => vec![0; self.page_size],
            };
            page_data[offset..offset + write_size].copy_from_slice(
                &data[data.len() - remaining_size..data.len() - remaining_size + write_size],
            );

            self.write_page(page_id, &page_data)?;

            remaining_size -= write_size;
            current_address += write_size as u64;
        }

        Ok(())
    }

    fn read_page(&mut self, page_id: u64) -> std::io::Result<Vec<u8>> {
        // First check cache for the page
        if let Some(node) = self.cache.get(&page_id) {
            let data = node.borrow().data.clone();
            self.promote(page_id);
            return Ok(data);
        }

        if (page_id + 1) * self.page_size as u64 > self.file_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Page out of bounds",
            ));
        }

        // Read the entire page from disk
        self.file
            .seek(SeekFrom::Start(page_id * self.page_size as u64))?;

        let file_size = self.file_size;
        let read_size = if (page_id + 1) * self.page_size as u64 > file_size {
            file_size - page_id * self.page_size as u64
        } else {
            self.page_size as u64
        } as usize;

        let mut buffer = vec![0; self.page_size];
        self.file.read_exact(&mut buffer[..read_size])?;

        self.add_to_cache(page_id, buffer.clone());

        Ok(buffer)
    }

    fn write_page(&mut self, page_id: u64, data: &[u8]) -> std::io::Result<()> {
        if data.len() != self.page_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Data size must match page size",
            ));
        }

        self.file
            .seek(SeekFrom::Start(page_id * self.page_size as u64))?;
        self.file.write_all(data)?;
        self.file.sync_all()?;

        if let Some(node) = self.cache.get_mut(&page_id) {
            let mut node_data = node.borrow_mut();
            node_data.data.copy_from_slice(data);
        } else {
            self.add_to_cache(page_id, data.to_vec());
        }

        self.file_size = std::cmp::max(
            self.file_size,
            page_id * self.page_size as u64 + data.len() as u64,
        );

        self.promote(page_id);

        Ok(())
    }

    fn add_to_cache(&mut self, page_id: u64, data: Vec<u8>) {
        if self.cache.len() * self.page_size >= self.capacity {
            if let Some(oldest_page) = self.usage_order.pop_front() {
                self.cache.remove(&oldest_page);
            }
        }

        let node = Rc::new(RefCell::new(LinkedListNodeInner { data }));
        self.cache.insert(page_id, node);
        self.usage_order.push_back(page_id);
    }

    fn promote(&mut self, page_id: u64) {
        self.usage_order.retain(|&x| x != page_id);
        self.usage_order.push_back(page_id);
    }
}
