mod counter;

use clap::Clap;
use deunicode::deunicode;
use parity_util_mem::{malloc_size, MallocSizeOf};
use regex::Regex;
use rustc_hash::{FxHashMap, FxHashSet};

use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::iter::FromIterator;
use std::cmp;

#[derive(Clap)]
struct Opts {
    input: String,

    #[clap(short, long)]
    stop_words: String,

    #[clap(short, long, default_value = "64")]
    half_para_len: usize,
}


#[derive(MallocSizeOf)]
struct Chain {
    half_para_len: usize,
    chain: FxHashMap<String, Vec<(i32, String)>>,
}

impl Chain {
    fn new(half_para_len: usize) -> Self {
        let mut chain = Chain {
            half_para_len,
            chain: FxHashMap::default(),
        };

        chain.chain.reserve(30_000_000);
        chain
    }

    fn update(&mut self, words: &[&str]) {
        if words.len() < self.half_para_len {
            return;
        }

        let mut seq_num = 0;
        let mut previous_topic_bigram = String::new();

        let mut counter = counter::Counter::new();

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

            let key = bigram + " @ " + &topic_bigram;
            self.chain
                .entry(key)
                .or_insert(Vec::new())
                .push((seq_num, next_bigram));

            seq_num += 1;
        }
    }

    fn merge(&mut self, other: Chain) {
        for (k, mut v) in other.chain {
            self.chain.entry(k).or_insert(Vec::new()).append(&mut v);
        }
    }

    fn print_info(&self, print_mem: bool) {
        print!("{:010} entries", self.chain.len());

        if print_mem {
            println!(
                " using {:.3}GiB",
                malloc_size(self) as f64 / (1024.0 * 1024.0 * 1024.0)
            );
        } else {
            println!();
        }
    }
}

fn main() -> std::io::Result<()> {
    let opts = Opts::parse();
    println!(
        "input file: {}, stop words file: {}\n",
        opts.input, opts.stop_words
    );

    let stop_words = std::fs::read_to_string(opts.stop_words)?;
    let stop_words = FxHashSet::<_>::from_iter(stop_words.split("\n"));

    let file = File::open(opts.input)?;
    let reader = BufReader::new(file);

    let re1 = Regex::new(r"[^\w\s]").unwrap();
    let re2 = Regex::new(r"\s\s+").unwrap();

    let mut chain = Chain::new(opts.half_para_len);

    for (i, line) in reader.lines().enumerate() {
        let mut line = match line {
            Ok(line) => deunicode(&line).to_lowercase(),
            Err(_) => break,
        };

        line = re1.replace(&line, "").to_string();
        line = re2.replace(&line, " ").to_string();

        line = line.replace(" th ", " nth ");

        let words: Vec<&str> = line
            .split(" ")
            .filter(|s| !stop_words.contains(s))
            .collect();

        chain.update(&words);

        print!("{:07}: {} ... ", i + 1, &line[0..72]);
        chain.print_info(false);
    }

    chain.print_info(true);
    Ok(())
}
