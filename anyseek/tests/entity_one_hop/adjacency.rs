//! Executable contract for a derived one-hop consumer. The projection is only
//! an access path. An authorized, pinned view is the authority for each claim.

use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Cut<'a> {
    pub tenant: &'a str,
    pub collection: &'a str,
    pub generation: u64,
    pub sequence: u64,
}

impl Cut<'_> {
    fn follows(self, captured: Self) -> bool {
        self.sequence >= captured.sequence
            && Self {
                sequence: captured.sequence,
                ..self
            } == captured
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Revision<'a> {
    pub id: &'a str,
    pub version: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Edge<'a> {
    pub from: &'a str,
    pub kind: &'a str,
    pub to: &'a str,
}

pub(super) struct Projection<'a> {
    pub revision: Revision<'a>,
    pub entities: Vec<&'a str>,
    pub edges: Vec<Edge<'a>>,
}

#[derive(Clone, Copy)]
pub(super) enum Direction {
    Out,
    In,
}

#[derive(Clone, Copy)]
pub(super) enum Claim<'a> {
    Seed(&'a str),
    Edge(Edge<'a>),
    Target(&'a str),
}

#[derive(Clone, Copy)]
pub(super) struct Proof<'a> {
    pub revision: Revision<'a>,
    pub claim: Claim<'a>,
}

/// The caller binds a trusted principal, fixed cut and common predicates to
/// this view. Confirm the exact current revision AND canonical claim, with
/// visibility and authorization. Missing/denied/stale is false, unreadable is
/// an error. Request values and ordinary document fields confer no authority.
pub(super) trait ReadView {
    type Error;
    type Policy: Copy + Eq;

    fn cut(&self) -> Cut<'_>;
    /// Trusted identity of this request's principal and common predicates.
    fn policy(&self) -> Self::Policy;
    fn eligible(&self, proof: Proof<'_>) -> Result<bool, Self::Error>;
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum SearchError<E> {
    InvalidRequest,
    SnapshotChanged,
    ScanLimit,
    FanoutLimit,
    DependencyWithdrawn,
    View(E),
}

#[derive(Clone, Copy)]
pub(super) struct Limits {
    pub candidates: usize,
    pub visits: usize,
    pub edges_per_seed: usize,
}

pub(super) struct Request<'a> {
    pub seeds: &'a [&'a str],
    pub types: &'a [&'a str],
    pub direction: Direction,
    pub limits: Limits,
}

type Member<'a> = (&'a str, Revision<'a>);
type Declaration<'a> = (Edge<'a>, Revision<'a>);

pub(super) struct Adjacency<'a> {
    cut: Cut<'a>,
    members: Vec<Member<'a>>,
    outgoing: Vec<Declaration<'a>>,
    incoming: Vec<Declaration<'a>>,
}

struct Path<'a> {
    seed: Proof<'a>,
    edge: Proof<'a>,
    target: Proof<'a>,
}

pub(super) struct Selection<'a, P> {
    cut: Cut<'a>,
    policy: P,
    paths: Vec<Path<'a>>,
}

impl<'a, P: Copy + Eq> Selection<'a, P> {
    /// Provisional channel order for internal scoring. Delivery requires the
    /// verified subset and the caller's publication barrier.
    pub(super) fn candidates(&self) -> impl Iterator<Item = Revision<'a>> + '_ {
        self.paths.iter().map(|path| path.target.revision)
    }

    /// Check retained paths against a fresh trusted view before delivery. A
    /// withdrawn seed or edge invalidates the ranking. A withdrawn target only
    /// removes its hit. The caller still owns the shared publication fence and
    /// must deliver under its barrier, including the group/publication check.
    pub(super) fn revalidate<V: ReadView<Policy = P>>(
        self,
        view: &V,
    ) -> Result<Vec<Revision<'a>>, SearchError<V::Error>> {
        if !view.cut().follows(self.cut) || view.policy() != self.policy {
            return Err(SearchError::SnapshotChanged);
        }
        let mut hits = Vec::with_capacity(self.paths.len());
        for path in self.paths {
            if !eligible(view, path.seed)? || !eligible(view, path.edge)? {
                return Err(SearchError::DependencyWithdrawn);
            }
            if eligible(view, path.target)? {
                hits.push(path.target.revision);
            }
        }
        Ok(hits)
    }
}

impl<'a> Adjacency<'a> {
    /// Build the standalone reference projection from versioned Record rows.
    /// A later storage adapter can implement the same ordered range reads.
    pub(super) fn build(
        cut: Cut<'a>,
        rows: impl IntoIterator<Item = Projection<'a>>,
    ) -> Result<Self, &'static str> {
        let mut members = Vec::new();
        let mut outgoing = Vec::new();
        for row in rows {
            if row.revision.id.is_empty()
                || row.revision.version == 0
                || row.entities.len() > 32
                || row.edges.len() > 64
                || !row.entities.iter().all(|id| entity_id(id))
                || row.edges.iter().any(|edge| {
                    !entity_id(edge.from) || !entity_id(edge.to) || !token(edge.kind, 128)
                })
            {
                return Err("invalid entity projection");
            }
            members.extend(row.entities.into_iter().map(|id| (id, row.revision)));
            outgoing.extend(row.edges.into_iter().map(|edge| (edge, row.revision)));
        }
        members.sort_unstable();
        members.dedup();
        // Outgoing order is (from, kind, to, declaration revision). Reverse
        // ordering keeps the original directed triple for verification.
        outgoing.sort_unstable();
        outgoing.dedup();
        let mut incoming = outgoing.clone();
        incoming
            .sort_unstable_by_key(|(edge, revision)| (edge.to, edge.kind, edge.from, *revision));
        Ok(Self {
            cut,
            members,
            outgoing,
            incoming,
        })
    }

    fn members(&self, entity: &str) -> &[Member<'a>] {
        matching_range(&self.members, |row| row.0.cmp(entity))
    }

    fn edges(&self, seed: &str, kind: &str, direction: Direction) -> &[Declaration<'a>] {
        match direction {
            Direction::Out => matching_range(&self.outgoing, |(edge, _)| {
                (edge.from, edge.kind).cmp(&(seed, kind))
            }),
            Direction::In => matching_range(&self.incoming, |(edge, _)| {
                (edge.to, edge.kind).cmp(&(seed, kind))
            }),
        }
    }

    pub(super) fn select<V: ReadView>(
        &self,
        view: &V,
        request: Request<'_>,
    ) -> Result<Selection<'a, V::Policy>, SearchError<V::Error>> {
        if view.cut() != self.cut {
            return Err(SearchError::SnapshotChanged);
        }
        let Limits {
            candidates,
            mut visits,
            edges_per_seed,
        } = request.limits;
        if !(1..=16).contains(&request.seeds.len())
            || !request.seeds.iter().all(|id| entity_id(id))
            || !(1..=16).contains(&request.types.len())
            || !request.types.iter().all(|kind| token(kind, 128))
            || !(1..=128).contains(&candidates)
            || !(1..=4096).contains(&visits)
            || !(1..=256).contains(&edges_per_seed)
        {
            return Err(SearchError::InvalidRequest);
        }
        let seeds: BTreeSet<_> = request.seeds.iter().copied().collect();
        let types: BTreeSet<_> = request.types.iter().copied().collect();
        let mut paths = Vec::new();
        let mut seen = BTreeSet::new();
        for seed in seeds {
            let mut seed_proof = None;
            for member in self.members(seed) {
                charge(&mut visits, SearchError::ScanLimit)?;
                let &(entity, revision) = member;
                let proof = Proof {
                    revision,
                    claim: Claim::Seed(entity),
                };
                if eligible(view, proof)? {
                    seed_proof = Some(proof);
                    break;
                }
            }
            let Some(seed_proof) = seed_proof else {
                continue;
            };
            let mut fanout = edges_per_seed;
            for kind in &types {
                let mut expanded = None;
                for declaration in self.edges(seed, kind, request.direction) {
                    // Charge selected declarations even when hidden, self-loops,
                    // duplicates or beyond the retained candidate window.
                    charge(&mut fanout, SearchError::FanoutLimit)?;
                    charge(&mut visits, SearchError::ScanLimit)?;
                    let &(edge, revision) = declaration;
                    let neighbor = match request.direction {
                        Direction::Out => edge.to,
                        Direction::In => edge.from,
                    };
                    if neighbor == seed || expanded == Some(neighbor) || paths.len() == candidates {
                        continue;
                    }
                    let edge_proof = Proof {
                        revision,
                        claim: Claim::Edge(edge),
                    };
                    if !eligible(view, edge_proof)? {
                        continue;
                    }
                    expanded = Some(neighbor);
                    for member in self.members(neighbor) {
                        charge(&mut visits, SearchError::ScanLimit)?;
                        let &(entity, revision) = member;
                        let target = Proof {
                            revision,
                            claim: Claim::Target(entity),
                        };
                        if !seen.contains(revision.id) && eligible(view, target)? {
                            seen.insert(revision.id);
                            paths.push(Path {
                                seed: seed_proof,
                                edge: edge_proof,
                                target,
                            });
                            if paths.len() == candidates {
                                break;
                            }
                        }
                    }
                }
            }
        }
        Ok(Selection {
            cut: self.cut,
            policy: view.policy(),
            paths,
        })
    }
}

fn eligible<V: ReadView>(view: &V, proof: Proof<'_>) -> Result<bool, SearchError<V::Error>> {
    view.eligible(proof).map_err(SearchError::View)
}

fn charge<E>(remaining: &mut usize, error: SearchError<E>) -> Result<(), SearchError<E>> {
    *remaining = remaining.checked_sub(1).ok_or(error)?;
    Ok(())
}

fn matching_range<T>(rows: &[T], compare: impl Fn(&T) -> std::cmp::Ordering) -> &[T] {
    let start = rows.partition_point(|row| compare(row).is_lt());
    let end = rows[start..].partition_point(|row| compare(row).is_eq());
    &rows[start..start + end]
}

fn token(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && !value.chars().any(|c| c.is_control() || c.is_whitespace())
}

fn entity_id(value: &str) -> bool {
    token(value, 256)
        && value
            .split_once(':')
            .is_some_and(|(namespace, name)| !namespace.is_empty() && !name.is_empty())
}
