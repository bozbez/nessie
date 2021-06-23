mod counter;

use clap::Clap;
use counter::Counter;
use deunicode::deunicode;
use parity_util_mem::malloc_size;
use regex::Regex;
use rustc_hash::{FxHashMap, FxHashSet};

use std::cmp;
use std::fs::File;
use std::io::{prelude::*, BufReader, BufWriter};
use std::iter::FromIterator;

#[derive(Clap)]
struct Opts {
    input: String,

    #[clap(short, long)]
    output: Option<String>,

    #[clap(short, long)]
    stop_words: String,

    #[clap(long, default_value = "64")]
    half_para_len: usize,

    #[clap(long, default_value = "1024")]
    prune_period: usize,

    #[clap(long, default_value = "16")]
    prune_threshold: usize,
}

struct Chain {
    half_para_len: usize,
    chain: FxHashMap<String, FxHashMap<String, Vec<(i32, String)>>>,
}

impl Chain {
    fn new(half_para_len: usize) -> Self {
        Chain {
            half_para_len,
            chain: FxHashMap::default(),
        }
    }

    fn update(&mut self, words: &[&str]) {
        if words.len() < self.half_para_len {
            return;
        }

        let mut seq_num = 0;
        let mut previous_topic_bigram = String::new();

        let mut counter = Counter::<&str>::new();

        for i in 0..(words.len() - 1) {
            let start = i.saturating_sub(self.half_para_len);
            let end = cmp::min(i.saturating_add(self.half_para_len), words.len());

            let para = &words[start..end];

            if i == 0 {
                for word in para.iter().filter(|w| w.len() > 2) {
                    counter.add(word);
                }
            } else {
                if start > 0 {
                    let word = &words[start];
                    if word.len() > 2 {
                        counter.remove(word);
                    }
                }

                if end < words.len() || i + self.half_para_len == words.len() {
                    let word = &words[end - 1];
                    if word.len() > 2 {
                        counter.add(word);
                    }
                }
            }

            if counter.total_count() < 3 || counter.num_items() < 2 {
                println!("skipping as not enough topics");
                break;
            }

            let topic_bigram = (counter.most_frequent(1).unwrap().0).to_owned()
                + " "
                + counter.most_frequent(2).unwrap().0;

            if topic_bigram != previous_topic_bigram {
                seq_num = 0;
                previous_topic_bigram = topic_bigram.clone();
            }

            let bigram = words[i].to_owned() + " " + words[i + 1];
            let next_bigram = words[i + 1].to_owned()
                + " "
                + if i + 2 >= words.len() {
                    "$"
                } else {
                    words[i + 2]
                };

            self.chain
                .entry(bigram)
                .or_insert(FxHashMap::default())
                .entry(topic_bigram)
                .or_insert(Vec::new())
                .push((seq_num, next_bigram));

            seq_num += 1;
        }
    }

    fn prune(&mut self, threshold: usize) {
        self.chain.retain(|_, v| v.len() >= threshold);
    }

    fn shrink_to_fit(&mut self) {
        self.chain.shrink_to_fit();
    }

    fn print_info(&self, print_mem: bool) {
        print!("{:010} entries", self.chain.len());

        if print_mem {
            println!(
                " using {:.3}GiB",
                malloc_size(&self.chain) as f64 / bytesize::GIB as f64
            );
        } else {
            println!();
        }
    }
}

fn main() -> std::io::Result<()> {
    let opts = Opts::parse();
    println!(
        "input: {}, output: {}, stop words: {}\n",
        opts.input,
        opts.output.clone().unwrap_or("none".to_string()),
        opts.stop_words
    );

    println!(
        "prune threshold: {}, prune period: {}",
        opts.prune_threshold, opts.prune_period
    );

    let re = Regex::new(r"[^\w\s]").unwrap();

    let stop_words = std::fs::read_to_string(opts.stop_words)?;
    let stop_words = re.replace_all(&stop_words, "").to_string();
    let stop_words = FxHashSet::<_>::from_iter(stop_words.split_ascii_whitespace());

    let input = File::open(opts.input)?;
    let reader = BufReader::new(input);

    let mut chain = Chain::new(opts.half_para_len);

    for (i, line) in reader.lines().enumerate() {
        let mut line = match line {
            Ok(line) => deunicode(&line),
            Err(_) => break,
        };

        line = re.replace_all(&line, "").to_string();

        line.make_ascii_lowercase();
        line = line.replace(" th ", " nth ");

        let words: Vec<&str> = line
            .split_ascii_whitespace()
            .filter(|s| !stop_words.contains(s))
            .collect();

        chain.update(&words);

        if i % opts.prune_period == 0 {
            chain.prune(opts.prune_threshold);
        }

        print!("{:07}: {} ... ", i + 1, &line[0..72]);
        chain.print_info(false);
    }

    chain.prune(opts.prune_threshold);
    chain.shrink_to_fit();

    println!();
    chain.print_info(true);

    if let Some(output) = opts.output {
        println!("writing to {}...", output);

        let output = File::create(output)?;
        let mut writer = BufWriter::new(output);

        serde_pickle::to_writer(&mut writer, &chain.chain, true).unwrap();

        println!(
            "{:.3}GiB written",
            writer.get_ref().metadata()?.len() as f64 / bytesize::GIB as f64
        );
    }

    Ok(())
}
