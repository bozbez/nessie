use deunicode::deunicode;
use hashbrown::HashSet;
use regex::Regex;

pub struct Opts {}

pub struct LineProcessor {
    special_chars_re: Regex,
    stop_words: HashSet<String>,
}

impl LineProcessor {
    pub fn new(stop_words: String) -> Self {
        let stop_words_clean = Regex::new(r"[^\w\s]")
            .unwrap()
            .replace_all(&stop_words, "")
            .to_string();

        LineProcessor {
            special_chars_re: Regex::new(r"[^\w\s]").unwrap(),
            stop_words: stop_words_clean
                .split_ascii_whitespace()
                .map(|s| s.into())
                .collect(),
        }
    }

    pub fn process(&self, line: &str) -> Vec<String> {
        let mut line = deunicode(line);

        line = self.special_chars_re.replace_all(&line, "").to_string();
        line.make_ascii_lowercase();

        line.split_ascii_whitespace()
            .map(|s| s.into())
            .filter(|s| !self.stop_words.contains(s))
            .collect::<Vec<String>>()
    }
}
