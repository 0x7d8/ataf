use crate::{
    compression::Compressor,
    spec::{ArchiveEntryHeader, ArchiveHeader, Serialize, VariableSizedU32},
};
use std::{
    io::{Read, Write},
    marker::PhantomData,
};

pub struct ArchiveWriter<W: Write, R: Read> {
    writer: W,
    _reader: PhantomData<R>,
    compressor: Box<dyn Compressor<R>>,
    header: ArchiveHeader,
}

impl<W: Write, R: Read> ArchiveWriter<W, R> {
    pub fn new(
        mut writer: W,
        compressor: Box<dyn Compressor<R>>,
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

        let mut chunk_count = *entry.size / self.header.compression_chunk_size as u64
            + if *entry.size % self.header.compression_chunk_size as u64 > 0 {
                1
            } else {
                0
            };

        while chunk_count > 0 {
            let chunks = self.compressor.compress(
                &mut input,
                chunk_count as usize,
                self.header.compression_chunk_size,
            )?;
            for chunk in chunks {
                VariableSizedU32::new(chunk.len() as u32).serialize(&mut self.writer)?;
                self.writer.write_all(&chunk)?;
                chunk_count -= 1;
            }
        }

        Ok(())
    }
}
