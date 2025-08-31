use ataf::{
    compression::{CompressionFormat, Compressor},
    spec::{VariableSizedU32, VariableSizedU64},
};
use clap::ArgMatches;
use std::{
    io::{BufWriter, IsTerminal},
    path::{Path, PathBuf},
    time::SystemTime,
};

macro_rules! println_if_terminal {
    ($fmt:expr $(, $args:expr)* $(,)?) => {
        if std::io::stdout().is_terminal() {
            println!($fmt $(, $args)*);
        }
    };
}

pub fn run(matches: &ArgMatches) -> i32 {
    let compression_format = matches
        .get_one::<CompressionFormat>("compression_format")
        .unwrap();
    let threads = matches.get_one::<usize>("threads").unwrap();
    let chunk_size = matches.get_one::<u32>("chunk_size").unwrap();
    let output = matches.get_one::<PathBuf>("output");
    let inputs = matches.get_many::<PathBuf>("input").unwrap();

    println_if_terminal!("creating archive with the following options:");
    println_if_terminal!("compression format: {:?}", compression_format);
    println_if_terminal!("number of threads: {}", threads);
    println_if_terminal!("chunk size: {}", chunk_size);

    type DynCompressor =
        dyn Compressor<BufWriter<Box<dyn std::io::Write + Send>>, Box<dyn std::io::Read>>;

    let compressor: Box<DynCompressor> = match compression_format {
        CompressionFormat::None => Box::new(ataf::compression::NoCompressor::new()),
        #[cfg(feature = "flate2")]
        CompressionFormat::Flate2 => Box::new(ataf::compression::Flate2Compressor::new(
            *threads,
            flate2::Compression::best(),
        )),
        #[cfg(feature = "brotli")]
        CompressionFormat::Brotli => Box::new(ataf::compression::BrotliCompressor::new(
            *threads,
            brotli::enc::BrotliEncoderParams::default(),
        )),
        #[cfg(feature = "lz4")]
        CompressionFormat::Lz4 => Box::new(ataf::compression::Lz4Compressor::new(*threads, 17)),
    };

    let writer: Box<dyn std::io::Write + Send> = match output {
        Some(path) => Box::new(std::fs::File::create(path).unwrap()),
        None => Box::new(std::io::stdout()),
    };
    let mut archive = ataf::archive::write::ArchiveWriter::new(
        BufWriter::with_capacity(1024 * 1024, writer),
        compressor,
        *chunk_size,
    )
    .unwrap();

    fn add_to_archive(
        archive: &mut ataf::archive::write::ArchiveWriter<
            BufWriter<Box<dyn std::io::Write + Send>>,
            Box<dyn std::io::Read>,
        >,
        input: &PathBuf,
        root: &Path,
    ) {
        println_if_terminal!("adding {} to archive...", input.display());

        let metadata = match std::fs::symlink_metadata(input) {
            Ok(metadata) => metadata,
            Err(err) => {
                eprintln!(
                    "ERROR failed to read metadata for {}: {}",
                    input.display(),
                    err
                );
                return;
            }
        };

        #[cfg(target_family = "unix")]
        let mode = {
            use std::os::unix::fs::PermissionsExt;

            metadata.permissions().mode()
        };
        #[cfg(target_family = "windows")]
        let mode = if metadata.permissions().readonly() {
            0o444
        } else {
            0o666
        };

        #[cfg(target_family = "unix")]
        let uid = {
            use std::os::unix::fs::MetadataExt;

            metadata.uid()
        };
        #[cfg(target_family = "windows")]
        let uid = 0;

        #[cfg(target_family = "unix")]
        let gid = {
            use std::os::unix::fs::MetadataExt;

            metadata.gid()
        };
        #[cfg(target_family = "windows")]
        let gid = 0;

        let path = input
            .strip_prefix(root)
            .unwrap_or(input)
            .to_string_lossy()
            .to_string();

        if metadata.is_file() {
            let file = match std::fs::File::open(input) {
                Ok(file) => file,
                Err(err) => {
                    eprintln!("ERROR failed to open {}: {}", input.display(), err);
                    return;
                }
            };

            let entry = ataf::spec::ArchiveEntryHeader {
                r#type: ataf::spec::ArchiveEntryHeaderType::File,
                path,
                mode,
                uid: VariableSizedU32::new(uid),
                gid: VariableSizedU32::new(gid),
                mtime: VariableSizedU64::new(
                    metadata
                        .modified()
                        .unwrap_or_else(|_| SystemTime::now())
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                ),
                size: VariableSizedU64::new(metadata.len()),
            };
            archive.write_entry(entry, Box::new(file)).unwrap();
        } else if metadata.is_dir() {
            let entry = ataf::spec::ArchiveEntryHeader {
                r#type: ataf::spec::ArchiveEntryHeaderType::Directory,
                path,
                mode,
                uid: VariableSizedU32::new(uid),
                gid: VariableSizedU32::new(gid),
                mtime: VariableSizedU64::new(
                    metadata
                        .modified()
                        .unwrap_or_else(|_| SystemTime::now())
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                ),
                size: VariableSizedU64::new(0),
            };
            archive
                .write_entry(entry, Box::new(std::io::empty()))
                .unwrap();

            let entries = match std::fs::read_dir(input) {
                Ok(entries) => entries,
                Err(err) => {
                    eprintln!(
                        "ERROR failed to read directory {}: {}",
                        input.display(),
                        err
                    );
                    return;
                }
            };

            for entry in entries {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(err) => {
                        eprintln!(
                            "ERROR failed to read directory entry {}: {}",
                            input.display(),
                            err
                        );
                        continue;
                    }
                };

                add_to_archive(archive, &entry.path(), root);
            }
        } else if metadata.is_symlink() {
            let symlink_target = match std::fs::read_link(input) {
                Ok(target) => target,
                Err(err) => {
                    eprintln!("ERROR failed to read symlink {}: {}", input.display(), err);
                    return;
                }
            };

            let entry = ataf::spec::ArchiveEntryHeader {
                r#type: if symlink_target.symlink_metadata().is_ok_and(|m| m.is_dir()) {
                    ataf::spec::ArchiveEntryHeaderType::SymlinkDirectory
                } else {
                    ataf::spec::ArchiveEntryHeaderType::SymlinkFile
                },
                path,
                mode,
                uid: VariableSizedU32::new(uid),
                gid: VariableSizedU32::new(gid),
                mtime: VariableSizedU64::new(
                    metadata
                        .modified()
                        .unwrap_or_else(|_| SystemTime::now())
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                ),
                size: VariableSizedU64::new(symlink_target.to_string_lossy().len() as u64),
            };
            archive
                .write_entry(
                    entry,
                    Box::new(std::io::Cursor::new(
                        symlink_target.to_string_lossy().as_bytes().to_vec(),
                    )),
                )
                .unwrap();
        }
    }

    for input in inputs {
        add_to_archive(
            &mut archive,
            input,
            if std::fs::metadata(input).is_ok_and(|m| m.is_dir()) {
                input
            } else {
                Path::new("")
            },
        );
    }

    0
}
