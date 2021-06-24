#![feature(allocator_api)]

mod counter;

use bumpalo::Bump;
use clap::Clap;
use counter::Counter;
use deunicode::deunicode;
use hashbrown::{HashMap, HashSet};
use regex::Regex;
use smartstring::SmartString;

use std::cell::UnsafeCell;
use std::cmp;
use std::fs::File;
use std::io::{prelude::*, BufReader, BufWriter};
use std::iter::FromIterator;
use std::mem;
use std::ptr;
use std::time::Instant;

#[derive(Clap)]
struct Opts {
    input: String,

    #[clap(short, long)]
    output: Option<String>,

    #[clap(short, long)]
    stop_words: String,

    #[clap(long, default_value = "64")]
    half_para_len: usize,

    #[clap(long, default_value = "2.0")]
    prune_size_gib: f64,

    #[clap(long, default_value = "16")]
    prune_threshold: usize,
}

type BHashMap<'a, K, V> = HashMap<K, V, ahash::RandomState, &'a Bump>;
type BVec<'a, T> = Vec<T, &'a Bump>;

type Unigram = SmartString<smartstring::LazyCompact>;
type Bigram = (Unigram, Unigram);

type ChainMap<'a> = BHashMap<'a, Bigram, BHashMap<'a, Bigram, BVec<'a, (i32, Unigram)>>>;

struct Chain<'a> {
    half_para_len: usize,
    prune_size: usize,
    prune_threshold: usize,

    chain: ChainMap<'a>,

    pools: Vec<UnsafeCell<Bump>>,
    active_pool: usize,
}

impl<'a> Chain<'a> {
    fn new(half_para_len: usize, prune_size: usize, prune_threshold: usize) -> Self {
        let pools = vec![
            UnsafeCell::new(Bump::with_capacity((prune_size as f64 * 1.1) as usize)),
            UnsafeCell::new(Bump::with_capacity((prune_size as f64 * 1.1) as usize)),
        ];

        Chain {
            half_para_len,
            prune_size,
            prune_threshold,

            chain: BHashMap::with_capacity_in(prune_size / 1000, unsafe {
                mem::transmute(pools[0].get())
            }),

            pools,
            active_pool: 0,
        }
    }

    fn active_pool(&self) -> &'a Bump {
        unsafe { mem::transmute(self.pools[self.active_pool].get()) }
    }

    fn advance_pool(&mut self) -> &'a Bump {
        self.active_pool = (self.active_pool + 1) % self.pools.len();
        self.active_pool()
    }

    fn update(&mut self, words: &[&str]) {
        if words.len() < self.half_para_len {
            return;
        }

        let pool = self.active_pool();

        let mut seq_num = 0;
        let mut previous_topic_bigram = (Unigram::from(""), Unigram::from(""));

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

            let topic_bigram = (
                Unigram::from(counter.most_frequent(1).unwrap().0),
                Unigram::from(counter.most_frequent(2).unwrap().0),
            );

            if topic_bigram != previous_topic_bigram {
                seq_num = 0;
                previous_topic_bigram = topic_bigram.clone();
            }

            let next_unigram = if i + 2 >= words.len() {
                Unigram::from("$")
            } else {
                Unigram::from(words[i + 2])
            };

            self.chain
                .entry((Unigram::from(words[i]), Unigram::from(words[i + 1])))
                .or_insert(BHashMap::new_in(pool))
                .entry(topic_bigram)
                .or_insert(BVec::new_in(pool))
                .push((seq_num, next_unigram));

            seq_num += 1;
        }

        if self.allocated_bytes() > self.prune_size {
            self.prune();
        }
    }

    unsafe fn drop_unigram(u: &Unigram) {
        if u.is_inline() {
            return;
        }

        let cptr = u as *const Unigram;
        ptr::drop_in_place(cptr as *mut Unigram);
    }

    unsafe fn drop_bigram(b: &(Unigram, Unigram)) {
        Self::drop_unigram(&b.0);
        Self::drop_unigram(&b.1);
    }

    fn prune(&mut self) {
        let old_pool_id = self.active_pool;
        let new_pool = self.advance_pool();

        let new_size = (self.num_entries() as f64 * 1.4) as usize;

        let mut new_chain = BHashMap::with_capacity_in(new_size, new_pool);
        for (bigram, topic_map) in self
            .chain
            .iter()
            .filter(|(_, topic_map)| topic_map.len() >= self.prune_threshold)
        {
            let mut new_topic_map = BHashMap::with_capacity_in(topic_map.len(), new_pool);
            for (topic, unigrams) in topic_map.iter() {
                let mut new_unigrams = BVec::with_capacity_in(unigrams.len(), new_pool);

                for unigram in unigrams {
                    new_unigrams.push(unigram.clone());
                    unsafe { Self::drop_unigram(&unigram.1) }
                }

                new_topic_map.insert(topic.clone(), new_unigrams);
                unsafe { Self::drop_bigram(topic) }
            }

            new_chain.insert(bigram.clone(), new_topic_map);
            unsafe { Self::drop_bigram(bigram) }
        }

        mem::swap(&mut self.chain, &mut new_chain);
        mem::forget(new_chain);

        unsafe { self.reset_pool(old_pool_id) }
    }

    unsafe fn reset_pool(&mut self, id: usize) {
        self.pools[id].get_mut().reset();
    }

    fn num_entries(&self) -> usize {
        self.chain.len()
    }

    fn allocated_bytes(&self) -> usize {
        self.active_pool().allocated_bytes()
    }

    fn print_info(&self, print_mem: bool) {
        print!("{:010} entries", self.num_entries());

        if print_mem {
            println!(
                " using ~{:.3}GiB",
                self.allocated_bytes() as f64 / bytesize::GIB as f64
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
        "prune threshold: {}, prune size: {}GiB",
        opts.prune_threshold, opts.prune_size_gib
    );

    let re = Regex::new(r"[^\w\s]").unwrap();

    let stop_words = std::fs::read_to_string(opts.stop_words)?;
    let stop_words = re.replace_all(&stop_words, "").to_string();
    let stop_words = HashSet::<_>::from_iter(stop_words.split_ascii_whitespace());

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

        section_times.0 += section_start.elapsed().as_secs_f64();
        section_start = Instant::now();

        chain.update(&words);

        section_times.1 += section_start.elapsed().as_secs_f64();

        print!("{:07}: {} ... ", i + 1, &line[0..72]);
        chain.print_info(true);
    }

    let duration = start.elapsed();

    println!();

    println!(
        "finished in {:.3}s ({:.3}s, {:.3}s), cleaning up...",
        duration.as_secs_f64(),
        section_times.0,
        section_times.1,
    );

    chain.prune();
    chain.print_info(true);

    if let Some(output) = opts.output {
        println!("writing to {}...", output);

        let output = File::create(output)?;
        let mut writer = BufWriter::new(output);

        // serde_pickle::to_writer(&mut writer, &chain.chain, true).unwrap();

        println!(
            "{:.3}GiB written",
            writer.get_ref().metadata()?.len() as f64 / bytesize::GIB as f64
        );
    }

    Ok(())
}
