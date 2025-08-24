use std::io::{self, BufWriter, Write};

pub trait DocSink {
    fn write_doc(&mut self, doc: &bson::Document) -> io::Result<()>;
    fn finish(self: Box<Self>) -> io::Result<()>;
}

pub struct NdjsonSink<W: Write> {
    w: BufWriter<W>,
}
impl<W: Write> NdjsonSink<W> {
    pub fn new(inner: W) -> Self { Self { w: BufWriter::new(inner) } }
}
impl<W: Write> DocSink for NdjsonSink<W> {
    fn write_doc(&mut self, doc: &bson::Document) -> io::Result<()> {
        let v = bson::to_bson(doc).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let s = serde_json::to_string(&v)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        writeln!(self.w, "{s}")
    }
    fn finish(mut self: Box<Self>) -> io::Result<()> { self.w.flush() }
}

pub struct CsvSink<W: Write> {
    w: csv::Writer<BufWriter<W>>,
    headers: Option<Vec<String>>,
    write_headers: bool,
}
impl<W: Write> CsvSink<W> {
    pub fn new(inner: W, delimiter: u8, write_headers: bool) -> Self {
        let w = csv::WriterBuilder::new().delimiter(delimiter).from_writer(BufWriter::new(inner));
        Self { w, headers: None, write_headers }
    }
}
impl<W: Write> DocSink for CsvSink<W> {
    fn write_doc(&mut self, doc: &bson::Document) -> io::Result<()> {
        if self.headers.is_none() {
            let hdrs: Vec<String> = doc.keys().cloned().collect();
            if self.write_headers {
                self.w.write_record(&hdrs).map_err(|e| io::Error::other(e.to_string()))?;
            }
            self.headers = Some(hdrs);
        }
        let mut row: Vec<String> = Vec::new();
        if let Some(hdrs) = &self.headers {
            for k in hdrs {
                row.push(doc.get(k).map(bson_to_string).unwrap_or_default());
            }
        }
        self.w.write_record(&row).map_err(|e| io::Error::other(e.to_string()))
    }
    fn finish(mut self: Box<Self>) -> io::Result<()> {
        self.w.flush().map_err(|e| io::Error::other(e.to_string()))
    }
}

pub struct BsonSink<W: Write> {
    w: BufWriter<W>,
}
impl<W: Write> BsonSink<W> {
    pub fn new(inner: W) -> Self { Self { w: BufWriter::new(inner) } }
}
impl<W: Write> DocSink for BsonSink<W> {
    fn write_doc(&mut self, doc: &bson::Document) -> io::Result<()> {
        let mut buf = Vec::new();
        doc.to_writer(&mut buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.w.write_all(&buf)
    }
    fn finish(mut self: Box<Self>) -> io::Result<()> { self.w.flush() }
}

fn bson_to_string(v: &bson::Bson) -> String {
    match v {
        bson::Bson::String(s) => s.clone(),
        bson::Bson::Int32(i) => i.to_string(),
        bson::Bson::Int64(i) => i.to_string(),
        bson::Bson::Double(f) => f.to_string(),
        bson::Bson::Boolean(b) => b.to_string(),
        other => other.to_string(),
    }
}
