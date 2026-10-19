//! Content models, as Xerces's `CMBuilder` makes them: `XSAllCM` for an
//! `all` group, else the automaton of `XSDFACM`, whose states are the sets
//! of particle positions that may come next. Positions are numbered in the
//! order of the particles, which is the order Xerces tries them in and
//! lists them in `whatCanGoHere`.

use super::model::{Compositor, Particle, Schema, Term, Wildcard};

pub(super) enum ContentModel {
    All(AllCm),
    Dfa(Dfa),
}

/// What a child element matched.
pub(super) enum Matched {
    Element(usize),
    Wildcard(Wildcard),
}

pub(super) enum State {
    /// The positions that may come next; the end-of-content position
    /// included when the content may end here.
    Dfa(Vec<usize>),
    /// Whether any child was seen, and which elements were.
    All(bool, Vec<bool>),
}

pub(super) struct AllCm {
    elements: Vec<(usize, bool)>,
    optional: bool,
}

pub(super) enum Leaf {
    Element(usize),
    Wildcard(Wildcard),
}

pub(super) struct Dfa {
    leaves: Vec<Leaf>,
    follow: Vec<Vec<usize>>,
    start: Vec<usize>,
}

/// `nullable`, `firstpos` and `lastpos` of a part of the content model.
struct Frag {
    nullable: bool,
    first: Vec<usize>,
    last: Vec<usize>,
}

fn union(a: &mut Vec<usize>, b: &[usize]) {
    for &x in b {
        if let Err(i) = a.binary_search(&x) {
            a.insert(i, x);
        }
    }
}

impl ContentModel {
    /// `CMBuilder.getContentModel` for a particle.
    pub(super) fn build(p: &Particle) -> ContentModel {
        if let Term::Group(Compositor::All, particles) = &p.term {
            let elements = particles
                .iter()
                .map(|q| match q.term {
                    Term::Element(e) => (e, q.min == 0),
                    _ => panic!("all group of other than elements"),
                })
                .collect();
            return ContentModel::All(AllCm {
                elements,
                optional: p.min == 0,
            });
        }
        let mut dfa = Dfa {
            leaves: Vec::new(),
            follow: Vec::new(),
            start: Vec::new(),
        };
        let root = dfa.frag(p);
        let eoc = dfa.leaves.len();
        for &l in &root.last {
            union(&mut dfa.follow[l], &[eoc]);
        }
        dfa.start = root.first;
        if root.nullable {
            union(&mut dfa.start, &[eoc]);
        }
        ContentModel::Dfa(dfa)
    }

    pub(super) fn start(&self) -> State {
        match self {
            ContentModel::All(a) => State::All(false, vec![false; a.elements.len()]),
            ContentModel::Dfa(d) => State::Dfa(d.start.clone()),
        }
    }

    /// `oneTransition`: what the element matches and the state after it, or
    /// `None` for `FIRST_ERROR` (the state left as it was).
    pub(super) fn transition(
        &self,
        schema: &Schema,
        state: &mut State,
        ns: Option<&str>,
        local: &str,
    ) -> Option<Matched> {
        match (self, state) {
            (ContentModel::All(a), State::All(any, seen)) => {
                for (i, &(decl, _)) in a.elements.iter().enumerate() {
                    if seen[i] {
                        continue;
                    }
                    if let Some(m) = schema.matching_decl(ns, local, decl) {
                        *any = true;
                        seen[i] = true;
                        return Some(Matched::Element(m));
                    }
                }
                None
            }
            (ContentModel::Dfa(d), State::Dfa(next)) => {
                let eoc = d.leaves.len();
                for &p in next.iter().filter(|&&p| p != eoc) {
                    let matched = match &d.leaves[p] {
                        Leaf::Element(decl) => {
                            schema.matching_decl(ns, local, *decl).map(Matched::Element)
                        }
                        Leaf::Wildcard(w) => w.allows(ns).then(|| Matched::Wildcard(w.clone())),
                    };
                    if let Some(m) = matched {
                        *next = d.follow[p].clone();
                        return Some(m);
                    }
                }
                None
            }
            _ => unreachable!("state of another content model"),
        }
    }

    /// `endContentModel`.
    pub(super) fn can_end(&self, state: &State) -> bool {
        match (self, state) {
            (ContentModel::All(a), State::All(any, seen)) => {
                if a.optional && !any {
                    return true;
                }
                a.elements
                    .iter()
                    .zip(seen)
                    .all(|(&(_, optional), &seen)| optional || seen)
            }
            (ContentModel::Dfa(d), State::Dfa(next)) => next.contains(&d.leaves.len()),
            _ => unreachable!("state of another content model"),
        }
    }

    /// `whatCanGoHere`, as `expectedStr` prints it: `{a, b}`.
    pub(super) fn expected(&self, schema: &Schema, state: &State) -> Option<String> {
        let items: Vec<String> = match (self, state) {
            (ContentModel::All(a), State::All(_, seen)) => a
                .elements
                .iter()
                .zip(seen)
                .filter(|(_, seen)| !**seen)
                .map(|(&(e, _), _)| schema.describe_elem(e))
                .collect(),
            (ContentModel::Dfa(d), State::Dfa(next)) => next
                .iter()
                .filter(|&&p| p != d.leaves.len())
                .map(|&p| match &d.leaves[p] {
                    Leaf::Element(e) => schema.describe_elem(*e),
                    Leaf::Wildcard(w) => w.to_string(),
                })
                .collect(),
            _ => unreachable!("state of another content model"),
        };
        (!items.is_empty()).then(|| format!("{{{}}}", items.join(", ")))
    }
}

impl Dfa {
    fn leaf(&mut self, leaf: Leaf) -> Frag {
        self.leaves.push(leaf);
        self.follow.push(Vec::new());
        let p = self.leaves.len() - 1;
        Frag {
            nullable: false,
            first: vec![p],
            last: vec![p],
        }
    }

    fn frag(&mut self, p: &Particle) -> Frag {
        let mut f = match &p.term {
            Term::Element(e) => self.leaf(Leaf::Element(*e)),
            Term::Wildcard(w) => self.leaf(Leaf::Wildcard(w.clone())),
            Term::Group(Compositor::Sequence, ps) => {
                let mut acc = Frag {
                    nullable: true,
                    first: Vec::new(),
                    last: Vec::new(),
                };
                for q in ps {
                    let c = self.frag(q);
                    for &l in &acc.last {
                        union(&mut self.follow[l], &c.first);
                    }
                    if acc.nullable {
                        union(&mut acc.first, &c.first);
                    }
                    let mut last = c.last.clone();
                    if c.nullable {
                        union(&mut last, &acc.last);
                    }
                    acc.last = last;
                    acc.nullable &= c.nullable;
                }
                acc
            }
            Term::Group(Compositor::Choice, ps) => {
                let mut acc = Frag {
                    nullable: ps.is_empty(),
                    first: Vec::new(),
                    last: Vec::new(),
                };
                for q in ps {
                    let c = self.frag(q);
                    union(&mut acc.first, &c.first);
                    union(&mut acc.last, &c.last);
                    acc.nullable |= c.nullable;
                }
                acc
            }
            Term::Group(Compositor::All, _) => panic!("all group inside another group"),
        };
        if p.max.is_none() {
            for l in f.last.clone() {
                union(&mut self.follow[l], &f.first);
            }
        }
        if p.min == 0 {
            f.nullable = true;
        }
        f
    }
}
