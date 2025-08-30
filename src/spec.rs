use std::{
    fmt::Debug,
    io::{Read, Write},
    ops::Deref,
};

pub trait Serialize {
    fn serialize(&self, output: impl Write) -> std::io::Result<()>;
}

pub trait Deserialize: Sized {
    fn deserialize(input: impl Read) -> std::io::Result<Self>;
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct VariableSizedU32(u32);

impl Debug for VariableSizedU32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl VariableSizedU32 {
    pub fn new(value: u32) -> Self {
        VariableSizedU32(value)
    }
}

impl Deref for VariableSizedU32 {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for VariableSizedU32 {
    fn serialize(&self, mut output: impl Write) -> std::io::Result<()> {
        let mut value = self.0;
        let mut bytes = Vec::new();

        loop {
            let byte = (value & 0x7F) as u8;
            value >>= 7;
            if value == 0 {
                bytes.push(byte);
                break;
            } else {
                bytes.push(byte | 0x80);
            }
        }

        output.write_all(&bytes)?;
        Ok(())
    }
}

impl Deserialize for VariableSizedU32 {
    fn deserialize(mut input: impl Read) -> std::io::Result<Self> {
        let mut value = 0u32;
        let mut shift = 0;

        loop {
            let mut byte = [0; 1];
            input.read_exact(&mut byte)?;
            let byte = byte[0];

            value |= ((byte & 0x7F) as u32) << shift;
            if (byte & 0x80) == 0 {
                break;
            }
            shift += 7;

            if shift >= 32 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "VariableSizedU32 is too large",
                ));
            }
        }

        Ok(VariableSizedU32(value))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct VariableSizedU64(u64);

impl Debug for VariableSizedU64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl VariableSizedU64 {
    pub fn new(value: u64) -> Self {
        VariableSizedU64(value)
    }
}

impl Deref for VariableSizedU64 {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for VariableSizedU64 {
    fn serialize(&self, mut output: impl Write) -> std::io::Result<()> {
        let mut value = self.0;
        let mut bytes = Vec::new();

        loop {
            let byte = (value & 0x7F) as u8;
            value >>= 7;
            if value == 0 {
                bytes.push(byte);
                break;
            } else {
                bytes.push(byte | 0x80);
            }
        }

        output.write_all(&bytes)?;
        Ok(())
    }
}

impl Deserialize for VariableSizedU64 {
    fn deserialize(mut input: impl Read) -> std::io::Result<Self> {
        let mut value = 0u64;
        let mut shift = 0;

        loop {
            let mut byte = [0; 1];
            input.read_exact(&mut byte)?;
            let byte = byte[0];

            value |= ((byte & 0x7F) as u64) << shift;
            if (byte & 0x80) == 0 {
                break;
            }
            shift += 7;

            if shift >= 64 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "VariableSizedU64 is too large",
                ));
            }
        }

        Ok(VariableSizedU64(value))
    }
}

#[derive(Debug, Clone)]
pub struct ArchiveHeader {
    pub version: u32,

    pub compression: String,
    pub compression_chunk_size: u32,
}

impl Serialize for ArchiveHeader {
    fn serialize(&self, mut output: impl Write) -> std::io::Result<()> {
        output.write_all(&self.version.to_le_bytes())?;
        output.write_all(&(self.compression.len() as u16).to_le_bytes())?;
        output.write_all(self.compression.as_bytes())?;
        output.write_all(&self.compression_chunk_size.to_le_bytes())?;

        Ok(())
    }
}

impl Deserialize for ArchiveHeader {
    fn deserialize(mut input: impl Read) -> std::io::Result<Self> {
        let mut version_bytes = [0; 4];
        input.read_exact(&mut version_bytes)?;
        let version = u32::from_le_bytes(version_bytes);

        let mut length_bytes = [0; 2];
        input.read_exact(&mut length_bytes)?;
        let length = u16::from_le_bytes(length_bytes) as usize;

        let mut compression = vec![0; length];
        input.read_exact(&mut compression)?;
        let compression = String::from_utf8(compression).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid UTF-8 in compression string",
            )
        })?;

        let mut chunk_size_bytes = [0; 4];
        input.read_exact(&mut chunk_size_bytes)?;
        let compression_chunk_size = u32::from_le_bytes(chunk_size_bytes);

        Ok(ArchiveHeader {
            version,
            compression,
            compression_chunk_size,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ArchiveEntryHeaderType {
    File,
    Directory,
    SymlinkFile,
    SymlinkDirectory,
}

impl Serialize for ArchiveEntryHeaderType {
    fn serialize(&self, mut output: impl Write) -> std::io::Result<()> {
        output.write_all(&[match self {
            Self::File => 0,
            Self::Directory => 1,
            Self::SymlinkFile => 2,
            Self::SymlinkDirectory => 3,
        }])
    }
}

impl Deserialize for ArchiveEntryHeaderType {
    fn deserialize(mut input: impl Read) -> std::io::Result<Self> {
        let mut byte = [0; 1];
        input.read_exact(&mut byte)?;

        match byte[0] {
            0 => Ok(Self::File),
            1 => Ok(Self::Directory),
            2 => Ok(Self::SymlinkFile),
            3 => Ok(Self::SymlinkDirectory),
            byte => Err(std::io::Error::other(format!(
                "invalid archive header type: {byte}"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArchiveEntryHeader {
    pub r#type: ArchiveEntryHeaderType,

    pub path: String,
    pub mode: u32,

    pub uid: VariableSizedU32,
    pub gid: VariableSizedU32,

    pub mtime: VariableSizedU64,

    pub size: VariableSizedU64,
}

impl Serialize for ArchiveEntryHeader {
    fn serialize(&self, mut output: impl Write) -> std::io::Result<()> {
        self.r#type.serialize(&mut output)?;
        VariableSizedU64(self.path.len() as u64).serialize(&mut output)?;
        output.write_all(self.path.as_bytes())?;
        output.write_all(&self.mode.to_le_bytes())?;
        self.uid.serialize(&mut output)?;
        self.gid.serialize(&mut output)?;
        self.mtime.serialize(&mut output)?;
        self.size.serialize(&mut output)?;

        Ok(())
    }
}

impl Deserialize for ArchiveEntryHeader {
    fn deserialize(mut input: impl Read) -> std::io::Result<Self> {
        let r#type = ArchiveEntryHeaderType::deserialize(&mut input)?;
        let path_length = VariableSizedU64::deserialize(&mut input)?.0;

        let mut path_bytes = vec![0u8; path_length as usize];
        input.read_exact(&mut path_bytes)?;
        let path = String::from_utf8(path_bytes).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid UTF-8 in path string",
            )
        })?;

        let mut mode_bytes = [0u8; 4];
        input.read_exact(&mut mode_bytes)?;
        let mode = u32::from_le_bytes(mode_bytes);

        let uid = VariableSizedU32::deserialize(&mut input)?;
        let gid = VariableSizedU32::deserialize(&mut input)?;

        let mtime = VariableSizedU64::deserialize(&mut input)?;
        let size = VariableSizedU64::deserialize(&mut input)?;

        Ok(ArchiveEntryHeader {
            r#type,
            path,
            mode,
            uid,
            gid,
            mtime,
            size,
        })
    }
}
