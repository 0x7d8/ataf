use crate::archive::write::ChunkWriter;
use clap::ValueEnum;
use std::{
    io::{Read, Write},
    sync::{Arc, Mutex},
};

#[cfg(feature = "brotli")]
pub use brotli;
#[cfg(feature = "flate2")]
pub use flate2;
#[cfg(feature = "lz4")]
pub use lz4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionFormat {
    None,
    #[cfg(feature = "flate2")]
    Flate2,
    #[cfg(feature = "brotli")]
    Brotli,
    #[cfg(feature = "lz4")]
    Lz4,
}

impl ValueEnum for CompressionFormat {
    fn value_variants<'a>() -> &'a [Self] {
        &[
            Self::None,
            #[cfg(feature = "flate2")]
            Self::Flate2,
            #[cfg(feature = "brotli")]
            Self::Brotli,
            #[cfg(feature = "lz4")]
            Self::Lz4,
        ]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        match self {
            Self::None => Some(clap::builder::PossibleValue::new("none")),
            #[cfg(feature = "flate2")]
            Self::Flate2 => Some(clap::builder::PossibleValue::new("flate2")),
            #[cfg(feature = "brotli")]
            Self::Brotli => Some(clap::builder::PossibleValue::new("brotli")),
            #[cfg(feature = "lz4")]
            Self::Lz4 => Some(clap::builder::PossibleValue::new("lz4")),
        }
    }
}

pub struct WriteCounter<W: Write> {
    writer: W,
    bytes_written: usize,
}

impl<W: Write> WriteCounter<W> {
    #[inline]
    fn new(writer: W) -> Self {
        Self {
            writer,
            bytes_written: 0,
        }
    }

    #[inline]
    fn into_written(self) -> usize {
        self.bytes_written
    }
}

impl<W: Write> Write for WriteCounter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let bytes_written = self.writer.write(buf)?;
        self.bytes_written += bytes_written;

        Ok(bytes_written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

pub trait Compressor<W: Write + Send, R: Read> {
    fn name(&self) -> &'static str;

    fn compress(
        &mut self,
        input: &mut R,
        remaining_chunks: usize,
        chunk_size: u32,
        chunk_writer: &mut ChunkWriter<&mut W>,
    ) -> std::io::Result<()>;
}

pub trait Decompressor {
    fn decompress_inputs(&mut self) -> usize;

    fn decompress(
        &mut self,
        inputs: Vec<Vec<u8>>,
        output: &mut Vec<u8>,
        chunk_size: u32,
    ) -> std::io::Result<()>;
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

impl<W: Write + Send, R: Read> Compressor<W, R> for NoCompressor {
    fn name(&self) -> &'static str {
        "none"
    }

    fn compress(
        &mut self,
        input: &mut R,
        _remaining_chunks: usize,
        chunk_size: u32,
        chunk_writer: &mut ChunkWriter<&mut W>,
    ) -> std::io::Result<()> {
        if self.chunk_buffer.capacity() < chunk_size as usize {
            self.chunk_buffer = vec![0; chunk_size as usize];
        }

        let bytes_copied = input.take(chunk_size as u64).read(&mut self.chunk_buffer)?;
        chunk_writer.write_chunk(&self.chunk_buffer[..bytes_copied])?;

        Ok(())
    }
}

#[cfg(feature = "flate2")]
pub struct Flate2Compressor {
    threads: usize,
    compression: flate2::Compression,
    input_buffers: Vec<Vec<u8>>,
    thread_pool: rayon::ThreadPool,
}

#[cfg(feature = "flate2")]
impl Flate2Compressor {
    pub fn new(threads: usize, compression: flate2::Compression) -> Self {
        Self {
            threads,
            compression,
            input_buffers: Vec::new(),
            thread_pool: rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap(),
        }
    }
}

#[cfg(feature = "flate2")]
impl<W: Write + Send, R: Read> Compressor<W, R> for Flate2Compressor {
    fn name(&self) -> &'static str {
        "flate2"
    }

    fn compress(
        &mut self,
        input: &mut R,
        remaining_chunks: usize,
        chunk_size: u32,
        chunk_writer: &mut ChunkWriter<&mut W>,
    ) -> std::io::Result<()> {
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

        let mut io_slices = Vec::new();
        io_slices.reserve_exact(threads);
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

        let chunk_writer = Arc::new(Mutex::new(chunk_writer));

        self.thread_pool.in_place_scope(|scope| {
            let error = Arc::new(Mutex::new(None));

            for i in 0..chunks_with_data {
                let input_data = &self.input_buffers[i];
                let compression = self.compression;
                let chunk_writer = Arc::clone(&chunk_writer);
                let error = Arc::clone(&error);

                scope.spawn(move |_| {
                    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), compression);
                    if let Err(err) = encoder.write_all(input_data) {
                        *error.lock().unwrap() = Some(err);
                        return;
                    }

                    match encoder.finish() {
                        Ok(result) => {
                            if let Err(err) = chunk_writer.lock().unwrap().write_chunk(&result) {
                                *error.lock().unwrap() = Some(err);
                            }
                        }
                        Err(err) => {
                            *error.lock().unwrap() = Some(err);
                        }
                    }
                });
            }

            if let Some(err) = error.lock().unwrap().take() {
                return Err(err);
            }

            Ok(())
        })?;

        Ok(())
    }
}

#[cfg(feature = "brotli")]
pub struct BrotliCompressor {
    threads: usize,
    params: Arc<brotli::enc::BrotliEncoderParams>,
    input_buffers: Vec<Vec<u8>>,
    thread_pool: rayon::ThreadPool,
}

#[cfg(feature = "brotli")]
impl BrotliCompressor {
    pub fn new(threads: usize, params: brotli::enc::BrotliEncoderParams) -> Self {
        Self {
            threads,
            params: Arc::new(params),
            input_buffers: Vec::new(),
            thread_pool: rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap(),
        }
    }
}

#[cfg(feature = "brotli")]
impl<W: Write + Send, R: Read> Compressor<W, R> for BrotliCompressor {
    fn name(&self) -> &'static str {
        "brotli"
    }

    fn compress(
        &mut self,
        input: &mut R,
        remaining_chunks: usize,
        chunk_size: u32,
        chunk_writer: &mut ChunkWriter<&mut W>,
    ) -> std::io::Result<()> {
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

        let mut io_slices = Vec::new();
        io_slices.reserve_exact(threads);
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

        let chunk_writer = Arc::new(Mutex::new(chunk_writer));

        self.thread_pool.in_place_scope(|scope| {
            let error = Arc::new(Mutex::new(None));

            for i in 0..chunks_with_data {
                let input_data = &self.input_buffers[i];
                let params = Arc::clone(&self.params);
                let chunk_writer = Arc::clone(&chunk_writer);
                let error = Arc::clone(&error);

                scope.spawn(move |_| {
                    let mut result = Vec::new();
                    if let Err(err) = brotli::enc::BrotliCompress(
                        &mut std::io::Cursor::new(input_data),
                        &mut result,
                        &params,
                    ) {
                        *error.lock().unwrap() = Some(err);
                        return;
                    };

                    if let Err(err) = chunk_writer.lock().unwrap().write_chunk(&result) {
                        *error.lock().unwrap() = Some(err);
                    }
                });
            }

            if let Some(err) = error.lock().unwrap().take() {
                return Err(err);
            }

            Ok(())
        })?;

        Ok(())
    }
}

#[cfg(feature = "lz4")]
pub struct Lz4Compressor {
    threads: usize,
    level: u32,
    input_buffers: Vec<Vec<u8>>,
    thread_pool: rayon::ThreadPool,
}

#[cfg(feature = "lz4")]
impl Lz4Compressor {
    pub fn new(threads: usize, level: u32) -> Self {
        Self {
            threads,
            level,
            input_buffers: Vec::new(),
            thread_pool: rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap(),
        }
    }
}

#[cfg(feature = "lz4")]
impl<W: Write + Send, R: Read> Compressor<W, R> for Lz4Compressor {
    fn name(&self) -> &'static str {
        "lz4"
    }

    fn compress(
        &mut self,
        input: &mut R,
        remaining_chunks: usize,
        chunk_size: u32,
        chunk_writer: &mut ChunkWriter<&mut W>,
    ) -> std::io::Result<()> {
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

        let mut io_slices = Vec::new();
        io_slices.reserve_exact(threads);
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

        let chunk_writer = Arc::new(Mutex::new(chunk_writer));

        self.thread_pool.in_place_scope(|scope| {
            let error = Arc::new(Mutex::new(None));

            for i in 0..chunks_with_data {
                let input_data = &self.input_buffers[i];
                let level = self.level;
                let chunk_writer = Arc::clone(&chunk_writer);
                let error = Arc::clone(&error);

                scope.spawn(move |_| {
                    let mut encoder = lz4::EncoderBuilder::new()
                        .level(level)
                        .build(Vec::new())
                        .unwrap();
                    if let Err(err) = encoder.write_all(input_data) {
                        *error.lock().unwrap() = Some(err);
                        return;
                    }

                    match encoder.finish() {
                        (result, Ok(())) => {
                            if let Err(err) = chunk_writer.lock().unwrap().write_chunk(&result) {
                                *error.lock().unwrap() = Some(err);
                            }
                        }
                        (_, Err(err)) => {
                            *error.lock().unwrap() = Some(err);
                        }
                    }
                });
            }

            if let Some(err) = error.lock().unwrap().take() {
                return Err(err);
            }

            Ok(())
        })?;

        Ok(())
    }
}

pub struct NoDecompressor;

impl Decompressor for NoDecompressor {
    fn decompress_inputs(&mut self) -> usize {
        1
    }

    fn decompress(
        &mut self,
        inputs: Vec<Vec<u8>>,
        output: &mut Vec<u8>,
        _chunk_size: u32,
    ) -> std::io::Result<()> {
        for input in inputs {
            std::io::copy(&mut input.as_slice(), output)?;
        }

        Ok(())
    }
}

#[cfg(feature = "flate2")]
pub struct Flate2Decompressor {
    threads: usize,
    thread_pool: rayon::ThreadPool,
    chunk_buffers: Vec<Arc<Mutex<Vec<u8>>>>,
}

#[cfg(feature = "flate2")]
impl Flate2Decompressor {
    pub fn new(threads: usize) -> Self {
        Self {
            threads,
            thread_pool: rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap(),
            chunk_buffers: Vec::new(),
        }
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
        chunk_size: u32,
    ) -> std::io::Result<()> {
        if self.chunk_buffers.len() < inputs.len() {
            self.chunk_buffers.resize_with(inputs.len(), || {
                Arc::new(Mutex::new(vec![0; chunk_size as usize]))
            });
        }

        let inputs_len = inputs.len();

        self.thread_pool.in_place_scope(|scope| {
            let error = Arc::new(Mutex::new(None));

            for (input, chunk_buffer) in inputs.into_iter().zip(self.chunk_buffers.iter().cloned())
            {
                let error = Arc::clone(&error);

                scope.spawn(move |_| {
                    let mut decoder = flate2::read::ZlibDecoder::new(&input[..]);
                    let mut chunk_buffer = chunk_buffer.lock().unwrap();

                    match decoder.read_to_end(&mut chunk_buffer) {
                        Ok(n) => chunk_buffer.truncate(n),
                        Err(err) => {
                            *error.lock().unwrap() = Some(err);
                        }
                    }
                });
            }

            if let Some(err) = error.lock().unwrap().take() {
                return Err(err);
            }

            Ok(())
        })?;

        for chunk_buffer in self.chunk_buffers.iter().take(inputs_len) {
            archive_output.write_all(&chunk_buffer.lock().unwrap())?;
        }

        Ok(())
    }
}

#[cfg(feature = "brotli")]
pub struct BrotliDecompressor {
    threads: usize,
    thread_pool: rayon::ThreadPool,
    chunk_buffers: Vec<Arc<Mutex<Vec<u8>>>>,
}

#[cfg(feature = "brotli")]
impl BrotliDecompressor {
    pub fn new(threads: usize) -> Self {
        Self {
            threads,
            thread_pool: rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap(),
            chunk_buffers: Vec::new(),
        }
    }
}

#[cfg(feature = "brotli")]
impl Decompressor for BrotliDecompressor {
    fn decompress_inputs(&mut self) -> usize {
        self.threads
    }

    fn decompress(
        &mut self,
        inputs: Vec<Vec<u8>>,
        archive_output: &mut Vec<u8>,
        chunk_size: u32,
    ) -> std::io::Result<()> {
        if self.chunk_buffers.len() < inputs.len() {
            self.chunk_buffers.resize_with(inputs.len(), || {
                Arc::new(Mutex::new(vec![0; chunk_size as usize]))
            });
        }

        let inputs_len = inputs.len();

        self.thread_pool.in_place_scope(|scope| {
            let error = Arc::new(Mutex::new(None));

            for (input, chunk_buffer) in inputs.into_iter().zip(self.chunk_buffers.iter().cloned())
            {
                let error = Arc::clone(&error);

                scope.spawn(move |_| {
                    let mut chunk_buffer = chunk_buffer.lock().unwrap();
                    let mut write_counter = WriteCounter::new(&mut *chunk_buffer);

                    if let Err(err) = brotli::BrotliDecompress(
                        &mut std::io::Cursor::new(input),
                        &mut write_counter,
                    ) {
                        *error.lock().unwrap() = Some(err);
                    };

                    let n = write_counter.into_written();
                    chunk_buffer.truncate(n);
                });
            }

            if let Some(err) = error.lock().unwrap().take() {
                return Err(err);
            }

            Ok(())
        })?;

        for chunk_buffer in self.chunk_buffers.iter().take(inputs_len) {
            archive_output.write_all(&chunk_buffer.lock().unwrap())?;
        }

        Ok(())
    }
}

#[cfg(feature = "lz4")]
pub struct Lz4Decompressor {
    threads: usize,
    thread_pool: rayon::ThreadPool,
    chunk_buffers: Vec<Arc<Mutex<Vec<u8>>>>,
}

#[cfg(feature = "lz4")]
impl Lz4Decompressor {
    pub fn new(threads: usize) -> Self {
        Self {
            threads,
            thread_pool: rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap(),
            chunk_buffers: Vec::new(),
        }
    }
}

#[cfg(feature = "flate2")]
impl Decompressor for Lz4Decompressor {
    fn decompress_inputs(&mut self) -> usize {
        self.threads
    }

    fn decompress(
        &mut self,
        inputs: Vec<Vec<u8>>,
        archive_output: &mut Vec<u8>,
        chunk_size: u32,
    ) -> std::io::Result<()> {
        if self.chunk_buffers.len() < inputs.len() {
            self.chunk_buffers.resize_with(inputs.len(), || {
                Arc::new(Mutex::new(vec![0; chunk_size as usize]))
            });
        }

        let inputs_len = inputs.len();

        self.thread_pool.in_place_scope(|scope| {
            let error = Arc::new(Mutex::new(None));

            for (input, chunk_buffer) in inputs.into_iter().zip(self.chunk_buffers.iter().cloned())
            {
                let error = Arc::clone(&error);

                scope.spawn(move |_| {
                    let mut decoder = lz4::Decoder::new(&input[..]).unwrap();
                    let mut chunk_buffer = chunk_buffer.lock().unwrap();

                    match decoder.read_to_end(&mut chunk_buffer) {
                        Ok(n) => chunk_buffer.truncate(n),
                        Err(err) => {
                            *error.lock().unwrap() = Some(err);
                        }
                    }
                });
            }

            if let Some(err) = error.lock().unwrap().take() {
                return Err(err);
            }

            Ok(())
        })?;

        for chunk_buffer in self.chunk_buffers.iter().take(inputs_len) {
            archive_output.write_all(&chunk_buffer.lock().unwrap())?;
        }

        Ok(())
    }
}
