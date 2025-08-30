use ataf::compression::Decompressor;
use clap::ArgMatches;
use std::{
    io::{BufReader, IsTerminal, Read},
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

macro_rules! println_if_terminal {
    ($fmt:expr $(, $args:expr)* $(,)?) => {
        if std::io::stdout().is_terminal() {
            println!($fmt $(, $args)*);
        }
    };
}

pub fn run(matches: &ArgMatches) -> i32 {
    let threads = matches.get_one::<usize>("threads").unwrap();
    let input = matches.get_one::<PathBuf>("input");
    let output = matches.get_one::<PathBuf>("output").unwrap();

    println_if_terminal!("extracting archive with the following options:");
    println_if_terminal!("number of threads: {}", threads);

    let reader: Box<dyn std::io::Read> = match input {
        Some(path) => Box::new(std::fs::File::open(path).unwrap()),
        None => Box::new(std::io::stdin()),
    };
    let mut archive =
        ataf::archive::read::Archive::new(BufReader::with_capacity(1024 * 1024, reader));

    let decompressor: Box<dyn Decompressor> = match archive.header().unwrap().compression.as_str() {
        "none" => Box::new(ataf::compression::NoDecompressor),
        #[cfg(feature = "flate2")]
        "flate2" => Box::new(ataf::compression::Flate2Decompressor::new(*threads)),
        _ => {
            eprintln!(
                "ERROR unsupported compression format: {}",
                archive.header().unwrap().compression
            );
            return 1;
        }
    };

    let mut entries = archive.entries(decompressor).unwrap();

    while let Some(entry) = entries.next_entry() {
        match entry {
            Ok(mut entry) => {
                println!(
                    "processing: {}, size: {}",
                    entry.header().path,
                    *entry.header().size
                );

                let mut path = Path::new(&entry.header().path);
                if path.is_absolute() {
                    let mut components = path.components();
                    components.next();

                    path = components.as_path();
                }
                let destination = output.join(path);

                if let Some(parent) = destination.parent()
                    && !parent.exists()
                    && let Err(err) = std::fs::create_dir_all(parent)
                {
                    eprintln!("ERROR error creating parent directory: {}", err);
                }

                match entry.header().r#type {
                    ataf::spec::ArchiveEntryHeaderType::File => {
                        let mut writer = match std::fs::File::create(&destination) {
                            Ok(file) => file,
                            Err(err) => {
                                eprintln!(
                                    "ERROR error creating file {}: {}",
                                    destination.display(),
                                    err
                                );
                                continue;
                            }
                        };

                        if let Err(err) = std::io::copy(&mut entry, &mut writer) {
                            eprintln!(
                                "ERROR error writing to file {}: {}",
                                destination.display(),
                                err
                            );
                            continue;
                        }

                        writer
                            .set_modified(
                                SystemTime::UNIX_EPOCH + Duration::from_secs(*entry.header().mtime),
                            )
                            .unwrap();
                        #[cfg(target_family = "unix")]
                        {
                            use std::os::unix::fs::PermissionsExt;

                            writer
                                .set_permissions(std::fs::Permissions::from_mode(
                                    entry.header().mode,
                                ))
                                .unwrap();
                        }
                    }
                    ataf::spec::ArchiveEntryHeaderType::Directory => {
                        if let Err(err) = std::fs::create_dir(&destination) {
                            eprintln!(
                                "ERROR error creating directory {}: {}",
                                destination.display(),
                                err
                            );
                            continue;
                        }
                    }
                    ataf::spec::ArchiveEntryHeaderType::SymlinkFile => {
                        let mut symlink_target = String::new();
                        symlink_target.reserve_exact(*entry.header().size as usize);

                        if let Err(err) = entry.read_to_string(&mut symlink_target) {
                            eprintln!(
                                "ERROR error reading symlink target {}: {}",
                                entry.header().path,
                                err
                            );
                            continue;
                        }

                        #[cfg(target_family = "unix")]
                        {
                            if let Err(err) =
                                std::os::unix::fs::symlink(symlink_target, &destination)
                            {
                                eprintln!(
                                    "ERROR error creating symlink {}: {}",
                                    destination.display(),
                                    err
                                );
                                continue;
                            }
                        }
                        #[cfg(target_family = "windows")]
                        {
                            if let Err(err) =
                                std::os::windows::fs::symlink_file(symlink_target, &destination)
                            {
                                eprintln!(
                                    "ERROR error creating symlink {}: {}",
                                    destination.display(),
                                    err
                                );
                                continue;
                            }
                        }
                    }
                    ataf::spec::ArchiveEntryHeaderType::SymlinkDirectory => {
                        let mut symlink_target = String::new();
                        symlink_target.reserve_exact(*entry.header().size as usize);

                        if let Err(err) = entry.read_to_string(&mut symlink_target) {
                            eprintln!(
                                "ERROR error reading symlink target {}: {}",
                                entry.header().path,
                                err
                            );
                            continue;
                        }

                        #[cfg(target_family = "unix")]
                        {
                            if let Err(err) =
                                std::os::unix::fs::symlink(symlink_target, &destination)
                            {
                                eprintln!(
                                    "ERROR error creating symlink {}: {}",
                                    destination.display(),
                                    err
                                );
                                continue;
                            }
                        }
                        #[cfg(target_family = "windows")]
                        {
                            if let Err(err) =
                                std::os::windows::fs::symlink_dir(symlink_target, &destination)
                            {
                                eprintln!(
                                    "ERROR error creating symlink {}: {}",
                                    destination.display(),
                                    err
                                );
                                continue;
                            }
                        }
                    }
                }
            }
            Err(err) => {
                eprintln!("ERROR error reading entry: {}", err);
                return 1;
            }
        }
    }

    0
}
