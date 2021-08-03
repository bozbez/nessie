use nessie::chain::Chain;
use nessie::line_processor::LineProcessor;
use nessie::types::SqlTyped;
use nessie::types::*;

use clap::Clap;
use crossbeam::channel::{bounded, Receiver, Sender};
use log::{error, info, warn};

use postgres::{binary_copy::BinaryCopyInWriter, Client, NoTls};

use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::thread;
use std::time::Instant;

struct Doc {
    bigram: Bigram,
    topic: Bigram,

    next_unigrams: Vec<SeqUnigram>,
}

#[derive(Clap, Clone)]
struct Opts {
    input: String,

    #[clap(short, long)]
    stop_words: String,

    #[clap(short, long)]
    log_file: Option<String>,

    #[clap(long, default_value = "64")]
    half_para_len: usize,

    #[clap(long, default_value = "256")]
    chain_batch_period: usize,

    #[clap(long, default_value = "10000")]
    progress_log_period: usize,

    #[clap(long, default_value = "host=/var/run/postgresql user=nessie")]
    postgres_conn: String,

    #[clap(long, default_value = "public")]
    postgres_schema: String,

    #[clap(long, default_value = "chain")]
    postgres_table: String,
}

impl Opts {
    fn log_summary(&self) {
        info!(
            "input: \"{}\"; stop words: \"{}\"",
            self.input, self.stop_words
        );

        info!(
            "postgres string: \"{}\"; table: \"{}\"",
            self.postgres_conn, self.postgres_table
        );

        info!(
            "chain batch period: {}; half paragraph: {} words",
            self.chain_batch_period, self.half_para_len
        );

        if let Some(log_file) = &self.log_file {
            info!("log file: \"{}\"", log_file)
        }
    }
}

struct Timer {
    name: String,

    start: Instant,
    wait_start: Instant,
    wait: f64,
}

impl Timer {
    fn start(name: &str) -> Self {
        Timer {
            name: name.to_owned(),

            start: Instant::now(),
            wait_start: Instant::now(),
            wait: 0f64,
        }
    }

    fn finish(&self) {
        let time_self = self.start.elapsed().as_secs_f64() - self.wait;
        info!(
            "{}: {:.3}s self, {:.3}s waiting",
            self.name,
            time_self,
            self.wait,
        );
    }

    fn wait_start(&mut self) {
        self.wait_start = Instant::now();
    }

    fn wait_finish(&mut self) {
        self.wait += self.wait_start.elapsed().as_secs_f64();
    }
}

fn line_processor(opts: Opts, tx: Sender<Vec<String>>) {
    let input = match File::open(&opts.input) {
        Ok(file) => file,
        Err(err) => {
            error!("failed to open file: \"{}\" ({})", opts.input, err);
            return;
        }
    };

    let reader = BufReader::new(input);

    let stop_words = match std::fs::read_to_string(&opts.stop_words) {
        Ok(stop_words) => stop_words,
        Err(err) => {
            error!("failed to read stop words ({})", err);
            return;
        }
    };

    let line_processor = LineProcessor::new(stop_words);

    let mut start_iter = Instant::now();
    let mut timer = Timer::start("line processor");

    for (count, line) in reader.lines().enumerate() {
        let words = match line {
            Ok(line) => line_processor.process(&line),
            Err(_) => break,
        };

        timer.wait_start();

        if tx.send(words).is_err() {
            warn!("could not send words");
            return;
        }

        timer.wait_finish();

        if (count + 1) % opts.progress_log_period == 0 {
            info!(
                "{}: processed in {:.3}s",
                count + 1,
                start_iter.elapsed().as_secs_f64()
            );

            start_iter = Instant::now();
        }
    }

    timer.finish();
}

fn worker(opts: Opts, rx: Receiver<Vec<String>>, tx: Sender<Chain>) {
    let mut iteration = 0;
    let mut chain = Chain::new(opts.half_para_len);

    let mut timer = Timer::start("chain worker");

    while let Ok(words) = rx.recv() {
        timer.wait_finish();
        chain.update(words);

        if (iteration + 1) % opts.chain_batch_period == 0 {
            timer.wait_start();
            if tx.send(chain).is_err() {
                warn!("could not forward chain");
                return;
            }

            timer.wait_finish();
            chain = Chain::new(opts.half_para_len);
        }

        iteration += 1;
        timer.wait_start();
    }

    if tx.send(chain).is_err() {
        warn!("could not forward chain");
    }

    timer.wait_finish();
    timer.finish();
}

fn chain_converter(rx: Receiver<Chain>, tx: Sender<Vec<Doc>>) {
    let mut timer = Timer::start("chain converter");

    while let Ok(chain) = rx.recv() {
        timer.wait_finish();

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

        timer.wait_start();

        if tx.send(docs).is_err() {
            warn!("could not forward docs");
            return;
        }
    }

    timer.wait_finish();
    timer.finish();
}

fn inserter(opts: Opts, rx: Receiver<Vec<Doc>>) {
    let mut client = match Client::connect(&opts.postgres_conn, NoTls) {
        Ok(client) => client,
        Err(err) => {
            error!(
                "error connecting to database: \"{}\" ({})",
                opts.postgres_conn, err
            );
            return;
        }
    };

    let bigram_ty = match Bigram::sql_type(&mut client, &opts.postgres_schema) {
        Ok(ty) => ty,
        Err(err) => {
            error!("error fetching bigram type ({})", err);
            return;
        }
    };

    let seq_unigram_array_ty = match SeqUnigram::sql_array_type(&mut client, &opts.postgres_schema)
    {
        Ok(ty) => ty,
        Err(err) => {
            error!("error fetching _seq_unigram type ({})", err);
            return;
        }
    };

    let mut timer = Timer::start("inserter");

    while let Ok(docs) = rx.recv() {
        timer.wait_finish();

        let writer = client
            .copy_in("COPY chain FROM stdin (FORMAT BINARY)")
            .expect("could not create binary row writer");

        let mut bin_writer = BinaryCopyInWriter::new(
            writer,
            &[
                bigram_ty.clone(),
                bigram_ty.clone(),
                seq_unigram_array_ty.clone(),
            ],
        );

        for doc in docs {
            bin_writer
                .write(&[&doc.bigram, &doc.topic, &doc.next_unigrams.as_slice()])
                .expect("could not write binary row");
        }

        bin_writer
            .finish()
            .expect("could not finish binary row writier");

        timer.wait_start();
    }

    timer.wait_finish();
    timer.finish();
}

fn setup_logger(log_file: &Option<String>) {
    let mut logger = fern::Dispatch::new()
        .format(move |out, message, record| {
            out.finish(format_args!(
                "[{} - {}] {}",
                chrono::Local::now().format("%x %H:%M:%S"),
                record.level(),
                message,
            ))
        })
        .level(log::LevelFilter::Info)
        .level_for("postgres", log::LevelFilter::Warn)
        .chain(std::io::stdout());

    if let Some(lf) = log_file {
        logger = logger.chain(fern::log_file(lf).expect("could not open log file"));
    }

    logger.apply().expect("could not apply logger settings");
}

fn main() {
    let opts = Opts::parse();
    setup_logger(&opts.log_file);

    opts.log_summary();

    let line_processor_opts = opts.clone();
    let worker_opts = opts.clone();
    let inserter_opts = opts;

    let (tx_line, rx_line) = bounded(8);
    let (tx_chain, rx_chain) = bounded(2);
    let (tx_doc, rx_doc) = bounded(2);

    let start = Instant::now();

    let line_processor = thread::spawn(move || line_processor(line_processor_opts, tx_line));
    let worker = thread::spawn(move || worker(worker_opts, rx_line, tx_chain));
    let chain_converter = thread::spawn(move || chain_converter(rx_chain, tx_doc));
    let inserter = thread::spawn(move || inserter(inserter_opts, rx_doc));

    line_processor.join().unwrap();
    worker.join().unwrap();
    chain_converter.join().unwrap();
    inserter.join().unwrap();

    info!("finished in {:.3}s", start.elapsed().as_secs_f64());
}
