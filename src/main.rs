use ataf::compression::CompressionFormat;
use clap::{Arg, Command};
use std::{io::IsTerminal, path::PathBuf};

mod commands;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn cli() -> Command {
    Command::new("ataf")
        .about("An archive format that supports native multithreading for compression and decompression.")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .version(VERSION)
        .subcommand(
            Command::new("create")
                .about("Creates an ataf archive")
                .arg(
                    Arg::new("compression_format")
                        .help("The compression format to use")
                        .short('c')
                        .long("compression-format")
                        .num_args(1)
                        .default_value("none")
                        .value_parser(clap::value_parser!(CompressionFormat))
                        .required(false),
                )
                .arg(
                    Arg::new("threads")
                        .help("The number of threads to use for compression")
                        .short('t')
                        .long("threads")
                        .num_args(1)
                        .default_value("1")
                        .value_parser(clap::value_parser!(usize))
                        .required(false),
                )
                .arg(
                    Arg::new("chunk_size")
                        .help("The chunk size to use for each compression block")
                        .short('s')
                        .long("chunk-size")
                        .num_args(1)
                        .default_value("65535")
                        .value_parser(clap::value_parser!(u32).range(1024..))
                        .required(false),
                )
                .arg(
                    Arg::new("output")
                        .help("The output file to write the archive to")
                        .short('o')
                        .long("output")
                        .num_args(1)
                        .value_parser(clap::value_parser!(PathBuf))
                        .required(std::io::stdout().is_terminal()),
                )
                .arg(
                    Arg::new("input")
                        .help("The input files or directories to archive")
                        .num_args(1..)
                        .required(true)
                        .value_parser(clap::value_parser!(PathBuf)),
                )
                .arg_required_else_help(false),
        )
        .subcommand(
            Command::new("extract")
                .about("Extracts an ataf archive")
                .arg(
                    Arg::new("threads")
                        .help("The number of threads to use for decompression")
                        .short('t')
                        .long("threads")
                        .num_args(1)
                        .default_value("1")
                        .value_parser(clap::value_parser!(usize))
                        .required(false),
                )
                .arg(
                    Arg::new("input")
                        .help("The input archive to extract")
                        .short('i')
                        .long("input")
                        .num_args(1)
                        .value_parser(clap::value_parser!(PathBuf))
                        .required(std::io::stdout().is_terminal()),
                )
                .arg(
                    Arg::new("output")
                        .help("The output directory to extract the archive to")
                        .short('o')
                        .long("output")
                        .num_args(1)
                        .value_parser(clap::value_parser!(PathBuf))
                        .required(true),
                )
                .arg_required_else_help(false),
        )
}

fn main() {
    let matches = cli().get_matches();

    match matches.subcommand() {
        Some(("create", sub_matches)) => std::process::exit(commands::create::run(sub_matches)),
        Some(("extract", sub_matches)) => std::process::exit(commands::extract::run(sub_matches)),
        _ => cli().print_help().unwrap(),
    }
}
