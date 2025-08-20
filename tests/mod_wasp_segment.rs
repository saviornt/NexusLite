use nexus_lite::wasp::{Page, SegmentFile, SegmentFooter};
use tempfile::tempdir;

#[test]
fn test_segment_flush_and_read() {
    let dir = tempdir().unwrap();
    let seg_path = dir.path().join("test_segment.bin");
    let mut seg = SegmentFile::open(seg_path).unwrap();
    let page1 = Page::new(1, 1, 2, b"foo".to_vec());
    let page2 = Page::new(2, 1, 2, b"bar".to_vec());
    let footer = SegmentFooter {
        key_range: (b"foo".to_vec(), b"bar".to_vec()),
        fence_keys: vec![b"foo".to_vec(), b"bar".to_vec()],
        bloom_filter: vec![0, 1, 2],
    };
    seg.flush_segment(&[page1.clone(), page2.clone()], &footer).unwrap();
    let (pages, read_footer) = seg.read_segment().unwrap();
    assert_eq!(pages.len(), 2);
    assert_eq!(pages[0].data, b"foo");
    assert_eq!(pages[1].data, b"bar");
    assert_eq!(read_footer.key_range.0, b"foo");
    assert_eq!(read_footer.key_range.1, b"bar");
}
