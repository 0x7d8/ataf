use clap::ValueEnum;
use std::io::{Read, Write};

#[cfg(feature = "flate2")]
pub use flate2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionFormat {
    None,
    #[cfg(feature = "flate2")]
    Flate2,
}

impl ValueEnum for CompressionFormat {
    fn value_variants<'a>() -> &'a [Self] {
        &[
            Self::None,
            #[cfg(feature = "flate2")]
            Self::Flate2,
        ]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        match self {
            Self::None => Some(clap::builder::PossibleValue::new("none")),
            #[cfg(feature = "flate2")]
            Self::Flate2 => Some(clap::builder::PossibleValue::new("flate2")),
        }
    }
}

pub trait Compressor<R: Read> {
    fn name(&self) -> &'static str;

    fn compress(
        &mut self,
        input: &mut R,
        remaining_chunks: usize,
        chunk_size: u32,
    ) -> std::io::Result<Vec<Vec<u8>>>;
}

pub trait Decompressor {
    fn decompress_inputs(&mut self) -> usize;

    fn decompress(&mut self, inputs: Vec<Vec<u8>>, output: &mut Vec<u8>) -> std::io::Result<()>;
}

pub struct NoCompressor {
    chunk_buffer: Vec<u8>,
}

impl Default for NoCompressor {
    fn default() -> Self {
        Self::new()
    }
}

impl NoCompressor {
    pub fn new() -> Self {
        Self {
            chunk_buffer: Vec::new(),
        }
    }
}

impl<R: Read> Compressor<R> for NoCompressor {
    fn name(&self) -> &'static str {
        "none"
    }

    fn compress(
        &mut self,
        input: &mut R,
        _remaining_chunks: usize,
        chunk_size: u32,
    ) -> std::io::Result<Vec<Vec<u8>>> {
        if self.chunk_buffer.capacity() < chunk_size as usize {
            self.chunk_buffer = vec![0; chunk_size as usize];
        }

        let bytes_copied = input.take(chunk_size as u64).read(&mut self.chunk_buffer)?;
        Ok(vec![self.chunk_buffer[..bytes_copied].to_vec()])
    }
}

#[cfg(feature = "flate2")]
pub struct Flate2Compressor {
    threads: usize,
    compression: flate2::Compression,
    input_buffers: Vec<Vec<u8>>,
}

#[cfg(feature = "flate2")]
impl Flate2Compressor {
    pub fn new(threads: usize, compression: flate2::Compression) -> Self {
        Self {
            threads,
            compression,
            input_buffers: Vec::new(),
        }
    }
}

#[cfg(feature = "flate2")]
impl<R: Read> Compressor<R> for Flate2Compressor {
    fn name(&self) -> &'static str {
        "flate2"
    }

    fn compress(
        &mut self,
        input: &mut R,
        remaining_chunks: usize,
        chunk_size: u32,
    ) -> std::io::Result<Vec<Vec<u8>>> {
        let threads = std::cmp::min(self.threads, remaining_chunks);

        if self.input_buffers.len() < threads {
            self.input_buffers.resize_with(threads, Vec::new);
        }
        self.input_buffers.truncate(threads);

        for i in 0..threads {
            let buffer = &mut self.input_buffers[i];
            if buffer.capacity() < chunk_size as usize {
                buffer.reserve(chunk_size as usize - buffer.capacity());
            }
            buffer.clear();
            buffer.resize(chunk_size as usize, 0);
        }

        let mut io_slices = Vec::with_capacity(threads);
        for buffer in &mut self.input_buffers {
            io_slices.push(std::io::IoSliceMut::new(buffer));
        }

        let mut slices_to_read = &mut io_slices[..];
        let mut chunks_with_data = threads;

        while !slices_to_read.is_empty() {
            match input.read_vectored(slices_to_read)? {
                0 => {
                    chunks_with_data = threads - slices_to_read.len();
                    break;
                }
                n => {
                    let mut bytes_read = n;
                    let mut slices_read = 0;

                    for slice in slices_to_read.iter() {
                        if bytes_read >= slice.len() {
                            bytes_read -= slice.len();
                            slices_read += 1;
                        } else {
                            break;
                        }
                    }

                    if slices_read > 0 {
                        slices_to_read = &mut slices_to_read[slices_read..];
                    }

                    if bytes_read > 0 && !slices_to_read.is_empty() {
                        let current_slice_index = threads - slices_to_read.len();
                        self.input_buffers[current_slice_index].truncate(bytes_read);
                        chunks_with_data = current_slice_index + 1;
                        break;
                    }
                }
            }
        }

        let mut thread_handles = Vec::new();
        thread_handles.reserve_exact(chunks_with_data);

        for i in 0..chunks_with_data {
            let input_data = self.input_buffers[i].clone();
            let compression = self.compression;

            thread_handles.push(std::thread::spawn(move || {
                let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), compression);
                encoder.write_all(&input_data)?;
                encoder.finish()
            }));
        }

        let mut results = Vec::new();
        results.reserve_exact(chunks_with_data);

        for handle in thread_handles {
            let output = handle
                .join()
                .map_err(|_| std::io::Error::other("Thread panicked"))??;
            results.push(output);
        }

        Ok(results)
    }
}

pub struct NoDecompressor;

impl Decompressor for NoDecompressor {
    fn decompress_inputs(&mut self) -> usize {
        1
    }

    fn decompress(&mut self, inputs: Vec<Vec<u8>>, output: &mut Vec<u8>) -> std::io::Result<()> {
        for input in inputs {
            std::io::copy(&mut input.as_slice(), output)?;
        }

        Ok(())
    }
}

#[cfg(feature = "flate2")]
pub struct Flate2Decompressor {
    threads: usize,
}

#[cfg(feature = "flate2")]
impl Flate2Decompressor {
    pub fn new(threads: usize) -> Self {
        Self { threads }
    }
}

#[cfg(feature = "flate2")]
impl Decompressor for Flate2Decompressor {
    fn decompress_inputs(&mut self) -> usize {
        self.threads
    }

    fn decompress(
        &mut self,
        inputs: Vec<Vec<u8>>,
        archive_output: &mut Vec<u8>,
    ) -> std::io::Result<()> {
        let mut outputs = Vec::new();
        outputs.reserve_exact(inputs.len());

        for input in inputs {
            outputs.push(std::thread::spawn(move || {
                let mut decoder = flate2::read::ZlibDecoder::new(&input[..]);
                let mut buffer = Vec::new();
                decoder.read_to_end(&mut buffer).map(|_| buffer)
            }));
        }

        for output in outputs {
            let output = output
                .join()
                .map_err(|_| std::io::Error::other("Thread panicked"))??;
            archive_output.extend(output);
        }

        Ok(())
    }
}
