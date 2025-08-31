use crate::{
    compression::Decompressor,
    spec::{ArchiveEntryHeader, ArchiveHeader, Deserialize},
};
use std::io::Read;

fn u24_bytes_to_u32(bytes: [u8; 3]) -> u32 {
    ((bytes[0] as u32) << 16) | ((bytes[1] as u32) << 8) | (bytes[2] as u32)
}

pub struct Archive<R: Read> {
    reader: R,
    header: Option<ArchiveHeader>,
}

impl<R: Read> Archive<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            header: None,
        }
    }

    pub fn header(&mut self) -> std::io::Result<&ArchiveHeader> {
        if let Some(ref data) = self.header {
            return Ok(data);
        }

        self.header = Some(ArchiveHeader::deserialize(&mut self.reader)?);
        self.header.as_ref().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to read start data")
        })
    }

    pub fn entries(
        &mut self,
        decompressor: Box<dyn Decompressor>,
    ) -> std::io::Result<ArchiveEntriesReader<'_, R>> {
        self.header()?;

        Ok(ArchiveEntriesReader {
            archive: self,
            decompressor,
        })
    }
}

pub struct ArchiveEntriesReader<'a, R: Read> {
    archive: &'a mut Archive<R>,
    decompressor: Box<dyn Decompressor>,
}

impl<'a, R: Read> ArchiveEntriesReader<'a, R> {
    pub fn next_entry<'b>(&'b mut self) -> Option<std::io::Result<ArchiveEntry<'b, R>>> {
        let header = match ArchiveEntryHeader::deserialize(&mut self.archive.reader) {
            Ok(header) => header,
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return None,
            Err(err) => return Some(Err(err)),
        };

        let compression_chunk_size = self
            .archive
            .header
            .as_ref()
            .map_or(0, |h| h.compression_chunk_size);

        Some(Ok(ArchiveEntry {
            reader: &mut self.archive.reader,
            decompressor: &mut self.decompressor,
            compression_chunk_size,
            compression_chunk_buffer: Vec::new(),
            read_bytes: 0,
            chunks: *header.size / compression_chunk_size as u64
                + if *header.size % compression_chunk_size as u64 > 0 {
                    1
                } else {
                    0
                },
            read_chunks: 0,
            header,
        }))
    }
}

pub struct ArchiveEntry<'a, R: Read> {
    reader: &'a mut R,
    decompressor: &'a mut Box<dyn Decompressor>,

    compression_chunk_size: u32,
    compression_chunk_buffer: Vec<u8>,

    header: ArchiveEntryHeader,
    read_bytes: u64,

    chunks: u64,
    read_chunks: u64,
}

impl<'a, R: Read> ArchiveEntry<'a, R> {
    #[inline]
    pub fn header(&self) -> &ArchiveEntryHeader {
        &self.header
    }
}

impl<'a, R: Read> Read for ArchiveEntry<'a, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if *self.header.size == 0 || self.read_bytes >= *self.header.size {
            return Ok(0);
        }

        if !self.compression_chunk_buffer.is_empty() {
            let to_read = std::cmp::min(buf.len(), self.compression_chunk_buffer.len());
            let data = self.compression_chunk_buffer.drain(0..to_read);
            for (i, byte) in data.enumerate() {
                buf[i] = byte;
            }

            self.read_bytes += to_read as u64;

            Ok(to_read)
        } else {
            let decompress_inputs = self.decompressor.decompress_inputs();

            if self.compression_chunk_buffer.capacity()
                < self.compression_chunk_size as usize * decompress_inputs
            {
                self.compression_chunk_buffer
                    .reserve_exact(self.compression_chunk_size as usize * decompress_inputs);
            }

            let mut chunk_buffers = Vec::new();
            chunk_buffers.reserve_exact(decompress_inputs);

            for _ in 0..decompress_inputs {
                if self.read_chunks >= self.chunks {
                    break;
                }

                let mut raw_chunk_size_bytes = [0; 3];
                self.reader.read_exact(&mut raw_chunk_size_bytes)?;
                let raw_chunk_size = u24_bytes_to_u32(raw_chunk_size_bytes);

                let mut chunk_buffer = vec![0; raw_chunk_size as usize];
                self.reader.read_exact(&mut chunk_buffer)?;

                self.read_chunks += 1;

                chunk_buffers.push(chunk_buffer);
            }

            self.decompressor.decompress(
                chunk_buffers,
                &mut self.compression_chunk_buffer,
                self.compression_chunk_size,
            )?;

            self.read(buf)
        }
    }
}

impl<'a, R: Read> Drop for ArchiveEntry<'a, R> {
    fn drop(&mut self) {
        if self.read_bytes < *self.header.size {
            std::io::copy(self, &mut std::io::sink()).unwrap();
        }
    }
}
