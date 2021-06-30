#![feature(allocator_api, slice_ptr_get)]

mod chain;
mod counter;
mod unigram;
mod clone_in;

use chain::Chain;
use clap::Clap;
use deunicode::deunicode;
use hashbrown::HashSet;
use regex::Regex;

use std::fs::File;
use std::io::{prelude::*, BufReader, BufWriter};
use std::time::Instant;

#[derive(Clap)]
struct Opts {
    input: String,

    #[clap(short, long)]
    output: Option<String>,

    #[clap(short, long)]
    stop_words: String,

    #[clap(long, default_value = "10")]
    print_period: usize,

    #[clap(long, default_value = "64")]
    half_para_len: usize,

    #[clap(long, default_value = "2.0")]
    prune_size_gib: f64,

    #[clap(long, default_value = "16")]
    prune_threshold: usize,
}

fn print_opts(opts: &Opts) {
    println!(
        "input: {}, output: {}, stop words: {}",
        opts.input,
        opts.output.clone().unwrap_or_else(|| "none".to_string()),
        opts.stop_words
    );

    println!(
        "half paragraph length: {}, prune threshold: {}, prune size: {} GiB",
        opts.half_para_len, opts.prune_threshold, opts.prune_size_gib
    );
}

struct LineProcessor<'a> {
    special_chars_re: Regex,
    stop_words: HashSet<&'a str>,
}

impl<'a> LineProcessor<'a> {
    fn new(stop_words: &'a str) -> Self {
        LineProcessor {
            special_chars_re: Regex::new(r"[^\w\s]").unwrap(),
            stop_words: stop_words.split_ascii_whitespace().collect(),
        }
    }

    fn sanitize(&self, line: &str) -> String {
        let mut line = deunicode(line);

        line = self.special_chars_re.replace_all(&line, "").to_string();
        line.make_ascii_lowercase();

        line.replace(" th ", " nth ")
    }

    fn split<'b>(&self, line: &'b str) -> Vec<&'b str> {
        line.split_ascii_whitespace()
            .filter(|s| !self.stop_words.contains(s))
            .collect()
    }
}

fn print_chain_info(chain: &Chain, newline: bool) {
    print!(
        "{:>7} entries, ~{:.3} GiB allocated\r",
        chain.num_entries(),
        chain.allocated_bytes() as f64 / bytesize::GIB as f64
    );

    if newline {
        println!();
    }
}

fn main() -> std::io::Result<()> {
    let opts = Opts::parse();

    print_opts(&opts);
    println!();

    let stop_words = std::fs::read_to_string(opts.stop_words)?;
    let stop_words = Regex::new(r"[^\w\s]")
        .unwrap()
        .replace_all(&stop_words, "")
        .to_string();

    let line_processor = LineProcessor::new(&stop_words);

    let input = File::open(opts.input)?;
    let reader = BufReader::new(input);

    let mut chain = Chain::new(
        opts.half_para_len,
        (opts.prune_size_gib * (bytesize::GIB as f64)) as usize,
        opts.prune_threshold,
    );

    let start = Instant::now();
    let mut section_times = (0f64, 0f64);

    for (i, line) in reader.lines().enumerate() {
        let mut section_start = Instant::now();

        let line = match line {
            Ok(line) => line_processor.sanitize(&line),
            Err(_) => break,
        };

        let words = line_processor.split(&line);

        section_times.0 += section_start.elapsed().as_secs_f64();
        section_start = Instant::now();

        chain.update(&words).unwrap();

        section_times.1 += section_start.elapsed().as_secs_f64();

        if (i + 1) % opts.print_period == 0 {
            print!("{:>7}: {} ... ", i + 1, &line[0..72],);
            print_chain_info(&chain, false);
            print!("\r");
        }
    }

    let duration = start.elapsed();

    print!(
        "\n\nfinished in {:.3}s ({:.3}s, {:.3}s), cleaning up... ",
        duration.as_secs_f64(),
        section_times.0,
        section_times.1,
    );

    chain.prune().unwrap();
    print_chain_info(&chain, true);

    if let Some(output) = opts.output {
        print!("writing to {}... ", output);

        let output = File::create(output)?;
        let mut writer = BufWriter::new(output);

        // let chain_map = chain.extract_map();
        // serde_pickle::to_writer(&mut writer, &chain_map, true).unwrap();

        println!(
            "{:.3}GiB written",
            writer.get_ref().metadata()?.len() as f64 / bytesize::GIB as f64
        );
    }

    Ok(())
}
