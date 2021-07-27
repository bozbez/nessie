use crate::counter::Counter;

use hashbrown::HashMap;
use smartstring::{LazyCompact, SmartString};

use postgres_types::ToSql;

use std::cmp::min;

pub type Unigram = String; // SmartString<LazyCompact>;

#[derive(Debug, ToSql, Hash, Clone, Eq, Ord, PartialEq, PartialOrd)]
#[postgres(name = "bigram")]
pub struct Bigram {
    first: Unigram,
    second: Unigram,
}

impl Bigram {
    pub fn new(first: Unigram, second: Unigram) -> Self {
        Bigram { first, second }
    }
}

#[derive(Debug, ToSql, Eq, Ord, PartialEq, PartialOrd)]
#[postgres(name = "seq_unigram")]
pub struct SeqUnigram {
    seq_num: i32,
    unigram: Option<Unigram>,
}

impl SeqUnigram {
    pub fn new(seq_num: i32, unigram: Option<Unigram>) -> Self {
        SeqUnigram { seq_num, unigram }
    }
}

pub type TopicMap = HashMap<Bigram, Vec<SeqUnigram>>;
pub type ChainMap = HashMap<Bigram, TopicMap>;

pub struct Chain {
    half_para_len: usize,
    chain: ChainMap,
}

impl Chain {
    pub fn new(half_para_len: usize) -> Self {
        Chain {
            half_para_len,
            chain: ChainMap::new(),
        }
    }

    pub fn num_entries(&self) -> usize {
        self.chain.len()
    }

    pub fn extract_chain_map(self) -> ChainMap {
        self.chain
    }

    pub fn update(&mut self, words: Vec<String>) {
        if words.len() < self.half_para_len {
            return;
        }

        let mut seq_num = 0;
        let mut previous_topic_bigram = Bigram::new(Unigram::new(), Unigram::new());

        let mut counter: Counter<&str> = Counter::new();

        for i in 0..(words.len() - 1) {
            let start = i.saturating_sub(self.half_para_len);
            let end = min(i.saturating_add(self.half_para_len), words.len());

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
                break;
            }

            let topic_bigram = Bigram::new(
                counter.most_frequent(1).unwrap().0.into(),
                counter.most_frequent(2).unwrap().0.into(),
            );

            if topic_bigram != previous_topic_bigram {
                seq_num = 0;
                previous_topic_bigram = topic_bigram.clone();
            }

            self.chain
                .entry(Bigram::new(words[i].clone().into(), words[i + 1].clone().into()))
                .or_insert(HashMap::new())
                .entry(topic_bigram)
                .or_insert(Vec::new())
                .push(SeqUnigram::new(
                    seq_num,
                    words.get(i + 2).map(|w| w.clone().into()),
                ));

            seq_num += 1;
        }
    }
}
