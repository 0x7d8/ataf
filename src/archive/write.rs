use crate::{
    compression::Compressor,
    spec::{ArchiveEntryHeader, ArchiveHeader, Serialize},
};
use std::{
    io::{Read, Write},
    marker::PhantomData,
};

pub struct ChunkWriter<W: Write + Send> {
    writer: W,
    chunk_count: u64,
}

impl<W: Write + Send> ChunkWriter<W> {
    pub fn write_chunk(&mut self, chunk: &[u8]) -> std::io::Result<()> {
        self.writer
            .write_all(&u32_to_u24_bytes(chunk.len() as u32))?;
        self.writer.write_all(chunk)?;
        self.chunk_count -= 1;

        Ok(())
    }
}

#[inline]
fn u32_to_u24_bytes(value: u32) -> [u8; 3] {
    [(value >> 16) as u8, (value >> 8) as u8, value as u8]
}

pub struct ArchiveWriter<W: Write + Send, R: Read> {
    writer: W,
    _reader: PhantomData<R>,
    compressor: Box<dyn Compressor<W, R>>,
    header: ArchiveHeader,
}

impl<W: Write + Send, R: Read> ArchiveWriter<W, R> {
    pub fn new(
        mut writer: W,
        compressor: Box<dyn Compressor<W, R>>,
        compression_chunk_size: u32,
    ) -> std::io::Result<Self> {
        let header = ArchiveHeader {
            version: 1,
            compression: String::from(compressor.name()),
            compression_chunk_size,
        };

        header.serialize(&mut writer)?;

        Ok(Self {
            writer,
            _reader: PhantomData,
            compressor,
            header,
        })
    }

    pub fn write_entry(&mut self, entry: ArchiveEntryHeader, mut input: R) -> std::io::Result<()> {
        entry.serialize(&mut self.writer)?;

        let chunk_count = *entry.size / self.header.compression_chunk_size as u64
            + if *entry.size % self.header.compression_chunk_size as u64 > 0 {
                1
            } else {
                0
            };

        let mut chunk_writer = ChunkWriter {
            writer: &mut self.writer,
            chunk_count,
        };

        while chunk_writer.chunk_count > 0 {
            self.compressor.compress(
                &mut input,
                chunk_count as usize,
                self.header.compression_chunk_size,
                &mut chunk_writer,
            )?;
        }

        Ok(())
    }
}
