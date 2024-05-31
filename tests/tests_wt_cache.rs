use std::{io::ErrorKind, path::PathBuf};
use tempfile::NamedTempFile;
use wt_cache::WriteThroughCache;

fn tmp_file() -> PathBuf {
    NamedTempFile::new().unwrap().path().to_path_buf()
}

#[test]
fn test_read_write_basic() {
    let mut cache =
        WriteThroughCache::new(&tmp_file(), Some(64 * 1024), Some(16 * 1024 * 1024)).unwrap();

    let address = 0;
    let data = vec![1; 1024]; // Write 1024 bytes

    cache.write(address, &data).unwrap();
    let read_data = cache.read(address, 1024).unwrap();

    assert_eq!(data, read_data);
}

#[test]
fn test_read_write_straddle_pages() {
    let mut cache =
        WriteThroughCache::new(&tmp_file(), Some(64 * 1024), Some(16 * 1024 * 1024)).unwrap();

    let address = 64 * 1024 - 512; // Start 512 bytes before the end of the first page
    let data = vec![1; 1024]; // Write 1024 bytes, straddling the page boundary

    cache.write(address, &data).unwrap();
    let read_data = cache.read(address, 1024).unwrap();

    assert_eq!(data, read_data);
}

#[test]
fn test_read_write_multiple_pages() {
    let mut cache =
        WriteThroughCache::new(&tmp_file(), Some(64 * 1024), Some(16 * 1024 * 1024)).unwrap();

    let address = 64 * 1024; // Start at the beginning of the second page
    let data = vec![2; 128 * 1024]; // Write 128 KiB, covering two pages

    cache.write(address, &data).unwrap();
    let read_data = cache.read(address, 68 * 1024).unwrap();

    if data[..68 * 1024] != read_data[..68 * 1024] {
        panic!("Read data does not match written data");
    }
}

#[test]
fn test_cache_eviction() {
    let page_size = 64 * 1024;
    let capacity = 2 * page_size; // Only enough capacity for two pages
    let mut cache = WriteThroughCache::new(&tmp_file(), Some(page_size), Some(capacity)).unwrap();

    let data1 = vec![1; page_size];
    let data2 = vec![2; page_size];
    let data3 = vec![3; page_size];

    cache.write(0, &data1).unwrap(); // Write to first page
    cache.write(page_size as u64, &data2).unwrap(); // Write to second page
    cache.write(2 * page_size as u64, &data3).unwrap(); // Write to third page, causing eviction

    let read_data1 = cache.read(0u64, page_size).unwrap();
    let read_data2 = cache.read(page_size as u64, page_size).unwrap();
    let read_data3 = cache.read(2 * page_size as u64, page_size).unwrap();

    assert_eq!(data1, read_data1);
    assert_eq!(data2, read_data2);
    assert_eq!(data3, read_data3);
}

#[test]
fn test_read_beyond_file() {
    let mut cache =
        WriteThroughCache::new(&tmp_file(), Some(64 * 1024), Some(16 * 1024 * 1024)).unwrap();

    let address = 0;
    let size = 128 * 1024; // Attempt to read beyond the end of an empty file

    let result = cache.read(address, size);

    assert!(result.is_err());
    if let Err(e) = result {
        assert_eq!(e.kind(), ErrorKind::InvalidInput);
    }
}

#[test]
fn test_partial_page_write() {
    let mut cache =
        WriteThroughCache::new(&tmp_file(), Some(64 * 1024), Some(16 * 1024 * 1024)).unwrap();

    let address = 0;
    let data = vec![1; 32 * 1024]; // Write 32 KiB, half a page

    cache.write(address, &data).unwrap();
    let read_data = cache.read(address, 32 * 1024).unwrap();

    assert_eq!(data, read_data);

    let full_page_data = cache.read(address, 64 * 1024).unwrap();
    assert_eq!(&full_page_data[..32 * 1024], &data[..]);
    assert_eq!(&full_page_data[32 * 1024..], &[0; 32 * 1024]);
}

#[test]
fn test_multiple_partial_page_writes() {
    let mut cache =
        WriteThroughCache::new(&tmp_file(), Some(64 * 1024), Some(16 * 1024 * 1024)).unwrap();

    let address1 = 0;
    let data1 = vec![1; 32 * 1024]; // Write 32 KiB, half a page

    let address2 = 32 * 1024;
    let data2 = vec![2; 32 * 1024]; // Write another 32 KiB, the other half of the page

    cache.write(address1, &data1).unwrap();
    cache.write(address2, &data2).unwrap();

    let full_page_data = cache.read(0, 64 * 1024).unwrap();
    assert_eq!(&full_page_data[..32 * 1024], &data1[..]);
    assert_eq!(&full_page_data[32 * 1024..], &data2[..]);
}

#[test]
fn test_write_partial_read_straddle() {
    let mut cache =
        WriteThroughCache::new(&tmp_file(), Some(64 * 1024), Some(16 * 1024 * 1024)).unwrap();

    let address1 = 64 * 1024 - 32 * 1024;
    let data1 = vec![1; 32 * 1024]; // Write 32 KiB, spanning half a page and straddling

    cache.write(address1, &data1).unwrap();
    let read_data = cache.read(address1, 32 * 1024).unwrap();

    assert_eq!(data1, read_data);
}

#[test]
fn test_partial_page_read() {
    let mut cache =
        WriteThroughCache::new(&tmp_file(), Some(64 * 1024), Some(16 * 1024 * 1024)).unwrap();

    let address = 0;
    let data = vec![1; 64 * 1024]; // Write 64 KiB, a full page

    cache.write(address, &data).unwrap();
    let read_data = cache.read(address, 32 * 1024).unwrap(); // Read 32 KiB, half a page

    assert_eq!(&data[..32 * 1024], &read_data[..]);
}

#[test]
fn test_non_aligned_read_write() {
    let mut cache =
        WriteThroughCache::new(&tmp_file(), Some(64 * 1024), Some(16 * 1024 * 1024)).unwrap();

    let address = 1234;
    let data = vec![42; 2048]; // Write 2048 bytes at a non-aligned address

    cache.write(address, &data).unwrap();
    let read_data = cache.read(address, 2048).unwrap();

    assert_eq!(data, read_data);
}

#[test]
fn test_large_data() {
    let mut cache =
        WriteThroughCache::new(&tmp_file(), Some(64 * 1024), Some(16 * 1024 * 1024)).unwrap();

    let address = 0;
    let data = vec![1; 16 * 1024 * 1024]; // Write 16 MiB, the full cache capacity

    cache.write(address, &data).unwrap();
    let read_data = cache.read(address, 16 * 1024 * 1024).unwrap();

    assert_eq!(data, read_data);
}

#[test]
fn test_empty_read() {
    let mut cache =
        WriteThroughCache::new(&tmp_file(), Some(64 * 1024), Some(16 * 1024 * 1024)).unwrap();

    let address = 0;
    let size = 64 * 1024; // Read a full page size from an empty file

    let result = cache.read(address, size);

    assert!(result.is_err());
    if let Err(e) = result {
        assert_eq!(e.kind(), ErrorKind::InvalidInput);
    }
}

#[test]
fn test_partial_file_read() {
    let mut cache =
        WriteThroughCache::new(&tmp_file(), Some(64 * 1024), Some(16 * 1024 * 1024)).unwrap();

    let address = 0;
    let data = vec![1; 32 * 1024]; // Write 32 KiB

    cache.write(address, &data).unwrap();
    let read_data = cache.read(address, 64 * 1024).unwrap(); // Read 64 KiB, beyond the written data

    assert_eq!(&read_data[..32 * 1024], &data[..]);
    assert_eq!(&read_data[32 * 1024..], &[0; 32 * 1024]);
}

#[test]
fn test_file_eviction() {
    let page_size = 64 * 1024;
    let capacity = 2 * page_size; // Only enough capacity for two pages
    let mut cache = WriteThroughCache::new(&tmp_file(), Some(page_size), Some(capacity)).unwrap();

    let data1 = vec![1; page_size];
    let data2 = vec![2; page_size];
    let data3 = vec![3; page_size];

    cache.write(0, &data1).unwrap(); // Write to first page
    cache.write(page_size as u64, &data2).unwrap(); // Write to second page
    cache.write(2 * page_size as u64, &data3).unwrap(); // Write to third page, causing eviction

    let read_data1 = cache.read(0u64, page_size).unwrap();
    let read_data2 = cache.read(page_size as u64, page_size).unwrap();
    let read_data3 = cache.read(2 * page_size as u64, page_size).unwrap();

    assert_eq!(data1, read_data1);
    assert_eq!(data2, read_data2);
    assert_eq!(data3, read_data3);
}

#[test]
fn test_cache_with_zero_size() {
    let result = WriteThroughCache::new(&tmp_file(), Some(0), Some(0));
    assert!(result.is_err());
}

#[test]
fn test_cache_with_large_size() {
    let result = WriteThroughCache::new(&tmp_file(), Some(usize::MAX), Some(usize::MAX));
    assert!(result.is_err());
}

#[test]
fn test_eviction_policy() {
    let page_size = 64 * 1024;
    let capacity = 2 * page_size; // Only enough capacity for two pages
    let mut cache = WriteThroughCache::new(&tmp_file(), Some(page_size), Some(capacity)).unwrap();

    let data1 = vec![1; page_size];
    let data2 = vec![2; page_size];
    let data3 = vec![3; page_size];

    cache.write(0, &data1).unwrap(); // Write to first page
    cache.write(page_size as u64, &data2).unwrap(); // Write to second page
    cache.read(0, page_size).unwrap(); // Access first page to promote it
    cache.write(2 * page_size as u64, &data3).unwrap(); // Write to third page, causing eviction

    // Now the second page should be evicted, since the first page was promoted
    let read_data1 = cache.read(0, page_size).unwrap();
    let read_data2 = cache.read(page_size as u64, page_size).unwrap();
    let read_data3 = cache.read(2 * page_size as u64, page_size).unwrap();

    assert_eq!(data1, read_data1);
    assert_eq!(data2, read_data2);
    assert_eq!(data3, read_data3);
}

#[test]
fn test_partial_page_eviction() {
    let page_size = 64 * 1024;
    let capacity = 2 * page_size; // Only enough capacity for two pages
    let mut cache = WriteThroughCache::new(&tmp_file(), Some(page_size), Some(capacity)).unwrap();

    let mut data1 = vec![1; 32 * 1024];
    let data2 = vec![2; page_size];
    let data3 = vec![3; page_size];

    cache.write(0, &data1).unwrap(); // Write to first half-page
    data1.resize(page_size, 0); // The second half-page will be 0 by default, but we didn't write it
    cache.write(page_size as u64, &data2).unwrap(); // Write to second page
    cache.write(2 * page_size as u64, &data3).unwrap(); // Write to third page, causing eviction

    let read_data1: Vec<u8> = cache.read(0u64, page_size).unwrap();
    let read_data2: Vec<u8> = cache.read(page_size as u64, page_size).unwrap();
    let read_data3 = cache.read(2 * page_size as u64, page_size).unwrap();

    assert_eq!(data1, read_data1);
    assert_eq!(data2, read_data2);
    assert_eq!(data3, read_data3);
}

#[test]
fn test_write_then_read_partial_page() {
    let mut cache =
        WriteThroughCache::new(&tmp_file(), Some(64 * 1024), Some(16 * 1024 * 1024)).unwrap();

    let address = 0;
    let data = vec![1; 32 * 1024]; // Write 32 KiB, half a page

    cache.write(address, &data).unwrap();
    let read_data = cache.read(address, 32 * 1024).unwrap();

    assert_eq!(data, read_data);
}

#[test]
fn test_write_multiple_pages_then_read() {
    let mut cache =
        WriteThroughCache::new(&tmp_file(), Some(64 * 1024), Some(16 * 1024 * 1024)).unwrap();

    let address = 0;
    let data = vec![1; 128 * 1024]; // Write 128 KiB, covering two pages

    cache.write(address, &data).unwrap();
    let read_data = cache.read(address, 128 * 1024).unwrap();

    assert_eq!(data, read_data);
}

#[test]
fn test_write_read_non_aligned() {
    let mut cache =
        WriteThroughCache::new(&tmp_file(), Some(64 * 1024), Some(16 * 1024 * 1024)).unwrap();

    let address = 1234;
    let data = vec![42; 2048]; // Write 2048 bytes at a non-aligned address

    cache.write(address, &data).unwrap();
    let read_data = cache.read(address, 2048).unwrap();

    assert_eq!(data, read_data);
}

#[test]
fn test_write_beyond_capacity() {
    let page_size = 64 * 1024;
    let capacity = 2 * page_size; // Only enough capacity for two pages
    let mut cache = WriteThroughCache::new(&tmp_file(), Some(page_size), Some(capacity)).unwrap();

    let data1 = vec![1; page_size];
    let data2 = vec![2; page_size];
    let data3 = vec![3; page_size];

    cache.write(0, &data1).unwrap(); // Write to first page
    cache.write(page_size as u64, &data2).unwrap(); // Write to second page
    cache.write(2 * page_size as u64, &data3).unwrap(); // Write to third page, causing eviction

    let read_data1 = cache.read(0u64, page_size).unwrap();
    let read_data2 = cache.read(page_size as u64, page_size).unwrap();
    let read_data3 = cache.read(2 * page_size as u64, page_size).unwrap();

    assert_eq!(data1, read_data1);
    assert_eq!(data2, read_data2);
    assert_eq!(data3, read_data3);
}

#[test]
fn test_write_zero_length() {
    let mut cache =
        WriteThroughCache::new(&tmp_file(), Some(64 * 1024), Some(16 * 1024 * 1024)).unwrap();

    let address = 0;
    let data = vec![]; // Write zero bytes

    cache.write(address, &data).unwrap();
    let read_data = cache.read(address, 0).unwrap();

    assert_eq!(data, read_data);
}

#[test]
fn test_read_zero_length() {
    let mut cache =
        WriteThroughCache::new(&tmp_file(), Some(64 * 1024), Some(16 * 1024 * 1024)).unwrap();

    let address = 0;
    let data = cache.read(address, 0).unwrap();

    assert_eq!(data.len(), 0);
}

#[test]
fn test_write_then_read_beyond_capacity() {
    let page_size = 64 * 1024;
    let capacity = 2 * page_size; // Only enough capacity for two pages
    let mut cache = WriteThroughCache::new(&tmp_file(), Some(page_size), Some(capacity)).unwrap();

    let data1 = vec![1; page_size];
    let data2 = vec![2; page_size];
    let data3 = vec![3; page_size];

    cache.write(0, &data1).unwrap(); // Write to first page
    cache.write(page_size as u64, &data2).unwrap(); // Write to second page
    cache.write(2 * page_size as u64, &data3).unwrap(); // Write to third page, causing eviction

    let read_data1 = cache.read(0u64, page_size).unwrap();
    let read_data2 = cache.read(page_size as u64, page_size).unwrap();
    let read_data3 = cache.read(2 * page_size as u64, page_size).unwrap();

    assert_eq!(data1, read_data1);
    assert_eq!(data2, read_data2);
    assert_eq!(data3, read_data3);
}

#[test]
fn test_write_partial_then_read_partial() {
    let mut cache =
        WriteThroughCache::new(&tmp_file(), Some(64 * 1024), Some(16 * 1024 * 1024)).unwrap();

    let address = 0;
    let data = vec![1; 32 * 1024]; // Write 32 KiB, half a page

    cache.write(address, &data).unwrap();
    let read_data = cache.read(address, 32 * 1024).unwrap();

    assert_eq!(data, read_data);
}

#[test]
fn test_write_then_read_multiple_pages() {
    let mut cache =
        WriteThroughCache::new(&tmp_file(), Some(64 * 1024), Some(16 * 1024 * 1024)).unwrap();

    let address = 0;
    let data = vec![1; 128 * 1024]; // Write 128 KiB, covering two pages

    cache.write(address, &data).unwrap();
    let read_data = cache.read(address, 128 * 1024).unwrap();

    assert_eq!(data, read_data);
}

#[test]
fn test_write_read_non_aligned_address() {
    let mut cache =
        WriteThroughCache::new(&tmp_file(), Some(64 * 1024), Some(16 * 1024 * 1024)).unwrap();

    let address = 1234;
    let data = vec![42; 2048]; // Write 2048 bytes at a non-aligned address

    cache.write(address, &data).unwrap();
    let read_data = cache.read(address, 2048).unwrap();

    assert_eq!(data, read_data);
}
