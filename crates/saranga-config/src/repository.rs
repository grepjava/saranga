//! Ported from `HierarchicalObjectRepository` and `Mergeable`.

use std::collections::HashMap;
use std::fmt;

use crate::Result;
use crate::matcher::Match;
use crate::wildcard::WildcardConfiguration;

/// `Mergeable`: a value that can take unset fields from a less specific one.
pub trait Mergeable: Sized {
    /// Returns a copy of `self` with every unset field filled from `other`.
    fn merge_copy(&self, other: &Self) -> Self;
}

/// `CompositeAddress.extractAddressName`: the address part of a fully
/// qualified queue name (`address::queue`), or the input unchanged.
pub fn extract_address_name(name: &str) -> &str {
    match name.find("::") {
        Some(i) => &name[..i],
        None => name,
    }
}

fn is_fully_qualified(name: &str) -> bool {
    name.contains("::")
}

type MatchModifier = Box<dyn Fn(&str) -> String + Send + Sync>;

/// Values stored against match strings (`#`, `queues.*`, `orders`), looked
/// up by address.
///
/// [`get_match`](Self::get_match) collects every entry whose match applies to
/// the address, orders them from most to least specific and merges them, so
/// that a field set on a more specific match wins and the rest are inherited.
/// Artemis also keeps a lookup cache; Saranga does not, as the lookup cost is
/// small next to what the broker caches per address anyway.
pub struct HierarchicalRepository<T> {
    wildcards: WildcardConfiguration,
    literal_markers: Option<(char, char)>,
    modifier: Option<MatchModifier>,
    /// In insertion order; Java keeps a `HashMap`, so ties in specificity
    /// have no defined order there.
    wildcard_matches: Vec<(Match, T)>,
    exact_matches: HashMap<String, (Match, T)>,
    literal_matches: HashMap<String, (Match, T)>,
    default: Option<T>,
}

impl<T: fmt::Debug> fmt::Debug for HierarchicalRepository<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HierarchicalRepository")
            .field("wildcards", &self.wildcards)
            .field("literal_markers", &self.literal_markers)
            .field("wildcard_matches", &self.wildcard_matches)
            .field("exact_matches", &self.exact_matches)
            .field("literal_matches", &self.literal_matches)
            .field("default", &self.default)
            .finish_non_exhaustive()
    }
}

impl<T> Default for HierarchicalRepository<T> {
    fn default() -> Self {
        Self::new(WildcardConfiguration::default())
    }
}

impl<T> HierarchicalRepository<T> {
    pub fn new(wildcards: WildcardConfiguration) -> Self {
        Self {
            wildcards,
            literal_markers: None,
            modifier: None,
            wildcard_matches: Vec::new(),
            exact_matches: HashMap::new(),
            literal_matches: HashMap::new(),
            default: None,
        }
    }

    /// Enables literal matches: a match wrapped in `start` and `end` (for
    /// example `(a.#)` with markers `()`) is stored without the markers and
    /// applies only to the address spelled exactly that way.
    pub fn with_literal_markers(mut self, start: char, end: char) -> Self {
        self.literal_markers = Some((start, end));
        self
    }

    /// `MatchModifier`: rewrites every match string on add and every address
    /// on lookup.
    pub fn with_match_modifier(
        mut self,
        f: impl Fn(&str) -> String + Send + Sync + 'static,
    ) -> Self {
        self.modifier = Some(Box::new(f));
        self
    }

    pub fn wildcards(&self) -> &WildcardConfiguration {
        &self.wildcards
    }

    fn modify(&self, s: &str) -> String {
        match &self.modifier {
            Some(f) => f(s),
            None => s.to_owned(),
        }
    }

    fn is_wildcard_entry(&self, m: &str) -> bool {
        self.wildcards.is_wild(m) && !is_fully_qualified(m)
    }

    /// `addMatch`. Replaces any value already stored for the same match.
    pub fn add_match(&mut self, match_str: &str, value: T) -> Result<()> {
        let mut text = match_str;
        let mut literal = false;
        if let Some((start, end)) = self.literal_markers {
            let mut chars = text.chars();
            literal = chars.next() == Some(start) && chars.next_back() == Some(end);
            if literal {
                text = &text[start.len_utf8()..text.len() - end.len_utf8()];
            }
        }
        let text = self.modify(text);
        Match::verify(&text, &self.wildcards)?;
        let m = Match::with_literal(&text, &self.wildcards, literal);
        if literal {
            self.literal_matches.insert(text, (m, value));
        } else if self.is_wildcard_entry(&text) {
            match self
                .wildcard_matches
                .iter_mut()
                .find(|(e, _)| e.as_str() == text)
            {
                Some(entry) => *entry = (m, value),
                None => self.wildcard_matches.push((m, value)),
            }
        } else {
            self.exact_matches.insert(text, (m, value));
        }
        Ok(())
    }

    /// `removeMatch`.
    pub fn remove_match(&mut self, match_str: &str) {
        let text = self.modify(match_str);
        if self.wildcards.is_wild(&text) {
            self.wildcard_matches.retain(|(m, _)| m.as_str() != text);
        } else {
            self.exact_matches.remove(&text);
            self.literal_matches.remove(&text);
        }
    }

    /// The value returned when nothing matches.
    pub fn set_default(&mut self, value: Option<T>) {
        self.default = value;
    }

    pub fn default_value(&self) -> Option<&T> {
        self.default.as_ref()
    }

    pub fn contains_exact_match(&self, match_str: &str) -> bool {
        self.exact_matches.contains_key(match_str)
    }

    pub fn contains_exact_wildcard_match(&self, match_str: &str) -> bool {
        self.wildcard_matches
            .iter()
            .any(|(m, _)| m.as_str() == match_str)
    }

    /// Every stored value: wildcard, then exact, then literal matches.
    pub fn values(&self) -> impl Iterator<Item = &T> {
        self.wildcard_matches
            .iter()
            .chain(self.exact_matches.values())
            .chain(self.literal_matches.values())
            .map(|(_, v)| v)
    }

    pub fn clear(&mut self) {
        self.wildcard_matches.clear();
        self.exact_matches.clear();
        self.literal_matches.clear();
    }

    /// Every match with its value, as it was added: the wildcard matches in
    /// the order they were added, then the exact and the literal ones by
    /// name, a literal one in its markers.
    pub fn entries(&self) -> Vec<(String, &T)> {
        let mut exact: Vec<(String, &T)> = self
            .exact_matches
            .iter()
            .map(|(k, (_, v))| (k.clone(), v))
            .collect();
        exact.sort_by(|a, b| a.0.cmp(&b.0));
        let mut literal: Vec<(String, &T)> = self
            .literal_matches
            .iter()
            .map(|(k, (_, v))| match self.literal_markers {
                Some((start, end)) => (format!("{start}{k}{end}"), v),
                None => (k.clone(), v),
            })
            .collect();
        literal.sort_by(|a, b| a.0.cmp(&b.0));
        self.wildcard_matches
            .iter()
            .map(|(m, v)| (m.as_str().to_owned(), v))
            .chain(exact)
            .chain(literal)
            .collect()
    }

    /// Every value that applies to `address`, most specific first: the exact
    /// match, then the literal match, then the wildcard matches ordered by
    /// [`Match::compare_specificity`].
    pub fn matches(&self, address: &str) -> Vec<&T> {
        self.named_matches(address)
            .into_iter()
            .map(|(_, v)| v)
            .collect()
    }

    /// [`Self::matches`] with each value's match, as [`Self::entries`]
    /// names it.
    pub fn named_matches(&self, address: &str) -> Vec<(String, &T)> {
        let address = self.modify(address);
        let mut out: Vec<(String, &T)> = Vec::new();
        if let Some((_, v)) = self.exact_matches.get(&address) {
            out.push((address.clone(), v));
        }
        if let Some((_, v)) = self.literal_matches.get(&address) {
            out.push(match self.literal_markers {
                Some((start, end)) => (format!("{start}{address}{end}"), v),
                None => (address.clone(), v),
            });
        }
        let mut wild: Vec<&(Match, T)> = self
            .wildcard_matches
            .iter()
            .filter(|(m, _)| m.matches(&address))
            .collect();
        // Insertion sort: stable, and unlike `sort_by` it cannot panic if the
        // ported comparator is not a strict total order.
        for i in 1..wild.len() {
            let mut j = i;
            while j > 0
                && Match::compare_specificity(
                    wild[j - 1].0.as_str(),
                    wild[j].0.as_str(),
                    &self.wildcards,
                ) == std::cmp::Ordering::Greater
            {
                wild.swap(j - 1, j);
                j -= 1;
            }
        }
        out.extend(wild.into_iter().map(|(m, v)| (m.as_str().to_owned(), v)));
        out
    }
}

impl<T> HierarchicalRepository<T> {
    /// `getMatch` for values that do not merge: the most specific value
    /// that applies, or the default.
    pub fn get_most_specific(&self, address: &str) -> Option<&T> {
        self.matches(address)
            .into_iter()
            .next()
            .or(self.default.as_ref())
    }
}

impl<T: Mergeable + Clone> HierarchicalRepository<T> {
    /// `getMatch`: merges every applicable value from most to least specific,
    /// or returns the default when nothing applies.
    pub fn get_match(&self, address: &str) -> Option<T> {
        let mut found = self.matches(address).into_iter();
        match found.next() {
            Some(first) => Some(found.fold(first.clone(), |acc, next| acc.merge_copy(next))),
            None => self.default.clone(),
        }
    }
}
