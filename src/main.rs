#![feature(allocator_api, slice_ptr_get)]

mod chain;
mod counter;

use chain::{Bigram, Chain, Unigram};

use clap::Clap;
use crossbeam::channel::{bounded, Receiver, Sender};
use deunicode::deunicode;
use hashbrown::HashSet;

use mongodb::{
    options::{Acknowledgment, InsertManyOptions, WriteConcern},
    sync::Client,
};

use regex::Regex;
use serde::Serialize;

use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::thread;
use std::time::Instant;

#[derive(Serialize)]
struct Doc {
    bigram: Bigram,
    topic: Bigram,

    next_unigrams: Vec<(i32, Option<Unigram>)>,
}

#[derive(Clap, Clone)]
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

    #[clap(long, default_value = "256")]
    chain_batch_period: usize,

    #[clap(long, default_value = "mongodb://localhost:27017")]
    mongo_uri: String,

    #[clap(long, default_value = "local")]
    mongo_db: String,

    #[clap(long, default_value = "nessie")]
    mongo_collection: String,
}

fn print_opts(opts: &Opts) {
    println!(
        "input: {}, output: {}, stop words: {}",
        opts.input,
        opts.output.clone().unwrap_or_else(|| "none".to_string()),
        opts.stop_words
    );

    println!("half paragraph length: {}", opts.half_para_len);
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

    fn split(&self, line: String) -> Vec<String> {
        line.split_ascii_whitespace()
            .filter(|s| !self.stop_words.contains(s))
            .map(|s| s.into())
            .collect::<Vec<String>>()
    }
}

fn worker(opts: Opts, rx: Receiver<Vec<String>>, tx: Sender<Chain>) {
    let mut iteration = 0;
    let mut chain = Chain::new(opts.half_para_len);

    loop {
        let words = match rx.recv() {
            Ok(words) => words,
            Err(_) => break,
        };

        chain.update(words);

        if (iteration + 1) % opts.print_period == 0 {
            println!("{:>7}: {} entries", iteration + 1, chain.num_entries());
        }

        if (iteration + 1) % opts.chain_batch_period == 0 {
            tx.send(chain).expect("could not forward chain");
            chain = Chain::new(opts.half_para_len);
        }

        iteration += 1;
    }

    tx.send(chain).expect("could not forward chain");
}

fn chain_converter(rx: Receiver<Chain>, tx: Sender<Vec<Doc>>) {
    loop {
        let chain = match rx.recv() {
            Ok(chain) => chain,
            Err(_) => break,
        };

        let mut docs = Vec::new();
        for (bigram, mut topic_map) in chain.extract_chain_map().drain() {
            for (topic, next_unigrams) in topic_map.drain() {
                docs.push(Doc {
                    bigram: bigram.clone(),
                    topic,

                    next_unigrams,
                });
            }
        }

        tx.send(docs).expect("could not forward docs");
    }
}

fn inserter(opts: Opts, rx: Receiver<Vec<Doc>>) {
    let client = Client::with_uri_str(opts.mongo_uri).expect("could not connect to db");

    let database = client.database(&opts.mongo_db);
    let collection = database.collection::<Doc>(&opts.mongo_collection);

    loop {
        let docs = match rx.recv() {
            Ok(docs) => docs,
            Err(_) => break,
        };

        let num_docs = docs.len();
        let start = Instant::now();

        collection
            .insert_many(docs, None)
            .expect("failed to insert into db");

        let duration = start.elapsed();
        println!(
            "inserted {} docs in {:.3}s",
            num_docs,
            duration.as_secs_f64()
        );
    }
}

fn main() -> std::io::Result<()> {
    let opts = Opts::parse();

    let worker_opts = opts.clone();
    let inserter_opts = opts.clone();

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

    let (tx_line, rx_line) = bounded::<Vec<String>>(8);
    let (tx_chain, rx_chain) = bounded::<Chain>(2);
    let (tx_doc, rx_doc) = bounded::<Vec<Doc>>(2);

    let worker = thread::spawn(move || worker(worker_opts, rx_line, tx_chain));
    let chain_converter = thread::spawn(move || chain_converter(rx_chain, tx_doc));
    let inserter = thread::spawn(move || inserter(inserter_opts, rx_doc));

    let start = Instant::now();
    let mut section_times = (0f64, 0f64);

    for line in reader.lines() {
        let mut section_start = Instant::now();

        let line = match line {
            Ok(line) => line_processor.sanitize(&line),
            Err(_) => break,
        };

        let words = line_processor.split(line);

        section_times.0 += section_start.elapsed().as_secs_f64();
        section_start = Instant::now();

        tx_line.send(words).expect("could not sending line");

        section_times.1 += section_start.elapsed().as_secs_f64();
    }

    drop(tx_line);

    worker.join().expect("could not join worker");
    chain_converter
        .join()
        .expect("could not join chain converter");
    inserter.join().expect("could not join inserter");

    let duration = start.elapsed();

    println!(
        "\nfinished in {:.3}s ({:.3}s, {:.3}s), cleaning up... ",
        duration.as_secs_f64(),
        section_times.0,
        section_times.1,
    );

    Ok(())
}
