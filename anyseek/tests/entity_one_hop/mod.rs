//! Exercises the standalone adjacency consumer with records from the public SDK.

mod adjacency;

use adjacency::{
    Adjacency, Claim, Cut, Direction, Edge, Limits, Projection, Proof, ReadView, Request, Revision,
    SearchError,
};
use anyseek::{
    record::Record,
    sdk::{LocalEngine, OperationId},
};
use serde_json::{Value, json};
use std::{cell::Cell, path::Path};

const TENANT: &str = "tenant";
const COLLECTION: &str = "graph";
const A: &str = "app:A";
const B: &str = "app:B";
const C: &str = "app:C";
const D: &str = "app:D";
const LINK: &str = "link";
const RELATED: &str = "related";
const LIMITS: Limits = Limits {
    candidates: 8,
    visits: 64,
    edges_per_seed: 16,
};

struct View<'a> {
    cut: Cut<'a>,
    records: Vec<Record>,
    denied: &'a [&'a str],
    policy: &'static str,
    unreadable: Option<&'a str>,
    probes: Cell<usize>,
}

impl ReadView for View<'_> {
    type Error = &'static str;
    type Policy = &'static str;

    fn cut(&self) -> Cut<'_> {
        self.cut
    }
    fn policy(&self) -> Self::Policy {
        self.policy
    }

    fn eligible(&self, Proof { revision, claim }: Proof<'_>) -> Result<bool, Self::Error> {
        self.probes.set(self.probes.get() + 1);
        if self.unreadable == Some(revision.id) {
            return Err("record unreadable");
        }
        if self.denied.contains(&revision.id) {
            return Ok(false);
        }
        let Some(record) = self
            .records
            .iter()
            .find(|record| record.record_id == revision.id && record.version == revision.version)
        else {
            return Ok(false);
        };
        let document = record.document.as_ref().unwrap();
        let matches = match claim {
            Claim::Seed(entity) | Claim::Target(entity) => document["members"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value.as_str() == Some(entity)),
            Claim::Edge(edge) => document["edges"]
                .as_array()
                .unwrap()
                .contains(&edge_value(edge)),
        };
        Ok(matches)
    }
}

fn edge(from: &str, kind: &str, to: &str) -> Value {
    json!({"from":from,"type":kind,"to":to})
}

fn edge_value(edge_claim: Edge<'_>) -> Value {
    edge(edge_claim.from, edge_claim.kind, edge_claim.to)
}

fn row(id: &str, members: &[&str], edges: Vec<Value>) -> Value {
    // An ordinary field cannot override the fixture's separate authority.
    json!({"record_id":id,"document":{"members":members,"edges":edges,"authorized":"yes"}})
}

fn project(record: &Record) -> Projection<'_> {
    let doc = record.document.as_ref().unwrap();
    Projection {
        revision: Revision {
            id: &record.record_id,
            version: record.version,
        },
        entities: doc["members"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect(),
        edges: doc["edges"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| Edge {
                from: value["from"].as_str().unwrap(),
                kind: value["type"].as_str().unwrap(),
                to: value["to"].as_str().unwrap(),
            })
            .collect(),
    }
}

async fn open() -> (tempfile::TempDir, std::path::PathBuf, LocalEngine) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("catalog");
    let engine = LocalEngine::open(&root, true).await.unwrap();
    (directory, root, engine)
}

async fn create(engine: &LocalEngine, tenant: &str) {
    engine
        .create_collection(
            tenant,
            COLLECTION,
            OperationId::new(format!("create-{tenant}")),
            serde_json::from_value(json!({"index_name":COLLECTION,"schema":[
                {"field":"members","field_type":"StringSet","store":true},
                {"field":"edges","field_type":"Json","store":true},
                {"field":"authorized","field_type":"String","store":true}
            ]}))
            .unwrap(),
            None,
        )
        .await
        .unwrap();
}

async fn replace(engine: &LocalEngine, tenant: &str, group: &str, previous: u64, rows: Vec<Value>) {
    engine
        .replace_group(
            tenant,
            COLLECTION,
            serde_json::from_value(json!({
                "operation_id":OperationId::new(format!("{group}-{previous}")),
                "group_id":group, "expected_group_version":previous, "records":rows,
            }))
            .unwrap(),
        )
        .await
        .unwrap();
}

async fn cut<'a>(engine: &LocalEngine, root: &Path, tenant: &'a str) -> Cut<'a> {
    let info = engine.collection_info(tenant, COLLECTION).await.unwrap();
    // The SDK deliberately does not expose Catalog generation. The temporary
    // collection fixture has exactly one generation directory, as in local_sdk.
    let mut directories =
        std::fs::read_dir(root.join("collections").join(tenant).join(COLLECTION)).unwrap();
    let directory = directories.next().unwrap().unwrap().file_name();
    assert!(directories.next().is_none());
    let number = directory.to_str().unwrap();
    Cut {
        tenant,
        collection: COLLECTION,
        generation: u64::from_str_radix(number, 16).unwrap(),
        sequence: info["statistics"]["visible_seq"].as_u64().unwrap(),
    }
}

async fn capture<'a>(engine: &LocalEngine, root: &Path, tenant: &'a str, ids: &[&str]) -> View<'a> {
    let observed = cut(engine, root, tenant).await;
    let mut records = Vec::new();
    for id in ids {
        if let Some(record) = engine.get(tenant, COLLECTION, id).await.unwrap() {
            records.push(record);
        }
    }
    // Only quiescent fixture reads are used. Independent SDK point reads are
    // not a production authentication or publication-snapshot protocol.
    assert_eq!(observed, cut(engine, root, tenant).await);
    View {
        cut: observed,
        records,
        denied: &[],
        policy: "caller/default-filter",
        unreadable: None,
        probes: Cell::new(0),
    }
}

fn index<'a>(view: &'a View<'a>) -> Adjacency<'a> {
    Adjacency::build(view.cut, view.records.iter().map(project)).unwrap()
}

fn request<'a>(seeds: &'a [&'a str], types: &'a [&'a str], direction: Direction) -> Request<'a> {
    Request {
        seeds,
        types,
        direction,
        limits: LIMITS,
    }
}

fn outgoing() -> Request<'static> {
    request(&[A], &[LINK], Direction::Out)
}

fn ids(hits: Vec<Revision<'_>>) -> Vec<&str> {
    hits.into_iter().map(|hit| hit.id).collect()
}

fn recall<'a>(graph: &Adjacency<'a>, view: &View<'_>, request: Request<'_>) -> Vec<&'a str> {
    let selection = graph.select(view, request).unwrap();
    let ranked: Vec<_> = selection.candidates().collect();
    let verified = selection.revalidate(view).unwrap();
    assert_eq!(ranked, verified, "the fixed view has not changed");
    ids(verified)
}

#[tokio::test]
async fn stable_forward_reverse_one_hop() {
    let (_directory, root, engine) = open().await;
    create(&engine, TENANT).await;
    replace(
        &engine,
        TENANT,
        "graph",
        0,
        vec![
            row("a", &[A], vec![]),
            row("b", &[B, "app:B2"], vec![]),
            row("c", &[C], vec![]),
            row("d", &[D], vec![]),
            row("z", &[B], vec![]),
            row(
                "ab",
                &[],
                vec![edge(A, LINK, A), edge(A, LINK, B), edge(A, LINK, B)],
            ),
            row(
                "ab-other",
                &[],
                vec![edge(A, LINK, B), edge(A, RELATED, "app:B2")],
            ),
            row("bc", &[], vec![edge(B, LINK, C)]),
            row("db", &[], vec![edge(D, LINK, B)]),
            row("ac", &[], vec![edge(A, "ignored", C)]),
        ],
    )
    .await;
    let names = ["z", "db", "ac", "ab-other", "bc", "d", "c", "b", "a", "ab"];
    let view = capture(&engine, &root, TENANT, &names).await;
    let graph = index(&view);
    assert_eq!(recall(&graph, &view, outgoing()), ["b", "z"]);
    assert_eq!(
        recall(&graph, &view, request(&[B], &[LINK], Direction::In)),
        ["a", "d"]
    );
    assert!(recall(&graph, &view, request(&["app:a"], &[LINK], Direction::Out)).is_empty());
    let reordered = Adjacency::build(view.cut, view.records.iter().rev().map(project)).unwrap();
    for graph in [&graph, &reordered] {
        assert_eq!(
            recall(
                graph,
                &view,
                request(&[D, A, A], &[RELATED, LINK, LINK], Direction::Out)
            ),
            ["b", "z"]
        );
    }
    let capped = Request {
        limits: Limits {
            candidates: 1,
            ..LIMITS
        },
        ..outgoing()
    };
    assert_eq!(recall(&graph, &view, capped), ["b"]);

    let mut recheck = capture(&engine, &root, TENANT, &names).await;
    recheck.denied = &["ab"];
    assert_eq!(
        graph
            .select(&view, outgoing())
            .unwrap()
            .revalidate(&recheck),
        Err(SearchError::DependencyWithdrawn),
        "the retained path used the first declaration"
    );
    assert_eq!(recall(&graph, &recheck, outgoing()), ["b", "z"]);
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejects_hidden_forged_and_out_of_scope_proofs() {
    let (_directory, root, engine) = open().await;
    let rows = || {
        vec![
            row("a", &[A], vec![]),
            row("b", &[B], vec![]),
            row("edge", &[], vec![edge(A, LINK, B)]),
        ]
    };
    for tenant in [TENANT, "other"] {
        create(&engine, tenant).await;
        replace(&engine, tenant, "graph", 0, rows()).await;
    }
    let names = ["a", "b", "edge"];
    let view = capture(&engine, &root, TENANT, &names).await;
    let mut independent = capture(&engine, &root, TENANT, &names).await;
    let foreign = capture(&engine, &root, "other", &names).await;
    let graph = index(&view);
    assert_eq!(
        graph.select(&foreign, outgoing()).err(),
        Some(SearchError::SnapshotChanged)
    );
    assert_eq!(foreign.probes.get(), 0);
    independent.cut.generation += 1;
    assert_eq!(
        graph.select(&independent, outgoing()).err(),
        Some(SearchError::SnapshotChanged)
    );
    assert_eq!(independent.probes.get(), 0);
    independent.cut = view.cut;
    for denied in [&["a"][..], &["edge"][..], &["b"][..]] {
        independent.denied = denied;
        assert!(
            graph
                .select(&independent, outgoing())
                .unwrap()
                .revalidate(&independent)
                .unwrap()
                .is_empty()
        );
    }
    independent.denied = &[];
    independent.policy = "another-principal-or-filter";
    assert_eq!(
        graph
            .select(&view, outgoing())
            .unwrap()
            .revalidate(&independent),
        Err(SearchError::SnapshotChanged)
    );
    let forged = Adjacency::build(
        view.cut,
        view.records.iter().map(|record| {
            let mut posting = project(record);
            if posting.revision.id == "edge" {
                posting.edges[0].kind = RELATED;
            }
            posting
        }),
    )
    .unwrap();
    assert!(
        forged
            .select(&view, request(&[A], &[RELATED], Direction::Out))
            .unwrap()
            .revalidate(&view)
            .unwrap()
            .is_empty()
    );
    independent.unreadable = Some("edge");
    assert_eq!(
        graph.select(&independent, outgoing()).err(),
        Some(SearchError::View("record unreadable"))
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn revalidates_before_delivery_after_withdrawal() {
    let (_directory, root, engine) = open().await;
    create(&engine, TENANT).await;
    replace(&engine, TENANT, "seed", 0, vec![row("a", &[A], vec![])]).await;
    replace(&engine, TENANT, "target", 0, vec![row("b", &[B], vec![])]).await;
    replace(
        &engine,
        TENANT,
        "declarations",
        0,
        vec![row("edge", &[], vec![edge(A, LINK, B)])],
    )
    .await;
    let names = ["a", "b", "edge"];
    let view = capture(&engine, &root, TENANT, &names).await;
    let graph = index(&view);
    let selected = || graph.select(&view, outgoing()).unwrap();
    let expected = [Revision {
        id: "b",
        version: 1,
    }];
    assert!(selected().candidates().eq(expected));
    assert_eq!(selected().revalidate(&view).unwrap(), expected);

    let mut changed = capture(&engine, &root, TENANT, &names).await;
    for denied in [&["a"][..], &["edge"][..]] {
        changed.denied = denied;
        assert_eq!(
            selected().revalidate(&changed),
            Err(SearchError::DependencyWithdrawn)
        );
    }
    changed.denied = &["b"];
    assert!(selected().revalidate(&changed).unwrap().is_empty());

    replace(
        &engine,
        TENANT,
        "target",
        1,
        vec![row("b", &[B, "app:B2"], vec![])],
    )
    .await;
    let updated = capture(&engine, &root, TENANT, &names).await;
    assert_eq!(
        updated
            .records
            .iter()
            .find(|r| r.record_id == "b")
            .unwrap()
            .version,
        2
    );
    assert!(selected().revalidate(&updated).unwrap().is_empty());
    assert_eq!(
        graph.select(&updated, outgoing()).err(),
        Some(SearchError::SnapshotChanged)
    );

    replace(&engine, TENANT, "declarations", 1, vec![]).await;
    let withdrawn = capture(&engine, &root, TENANT, &names).await;
    assert!(withdrawn.records.iter().all(|r| r.record_id != "edge"));
    assert_eq!(
        selected().revalidate(&withdrawn),
        Err(SearchError::DependencyWithdrawn)
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn bounds_forward_reverse_fanout_and_rejected_postings() {
    let (_directory, root, engine) = open().await;
    create(&engine, TENANT).await;
    let mut rows = vec![row("a", &[A], vec![]), row("b", &[B], vec![])];
    let mut names = vec!["a".to_owned(), "b".to_owned()];
    for chunk in 0..5 {
        let id = format!("hot-{chunk}");
        let edges = (chunk * 64..((chunk + 1) * 64).min(257))
            .map(|ordinal| edge(A, LINK, &format!("app:F{ordinal:03}")))
            .collect();
        rows.push(row(&id, &[], edges));
        names.push(id);

        let id = format!("back-{chunk}");
        let edges = (chunk * 64..((chunk + 1) * 64).min(257))
            .map(|ordinal| edge(&format!("app:I{ordinal:03}"), LINK, B))
            .collect();
        rows.push(row(&id, &[], edges));
        names.push(id);
    }
    rows.push(row("other-type", &[], vec![edge(A, RELATED, B)]));
    names.push("other-type".into());
    replace(&engine, TENANT, "graph", 0, rows).await;
    let names: Vec<_> = names.iter().map(String::as_str).collect();
    let mut view = capture(&engine, &root, TENANT, &names).await;
    view.denied = &["hot-0", "hot-1", "hot-2", "hot-3", "hot-4"];
    let graph = index(&view);
    for (seeds, direction) in [(&[A][..], Direction::Out), (&[B][..], Direction::In)] {
        view.probes.set(0);
        let limits = Limits {
            candidates: 1,
            visits: 4096,
            edges_per_seed: 256,
        };
        let req = Request {
            limits,
            ..request(seeds, &[LINK], direction)
        };
        assert_eq!(
            graph.select(&view, req).err(),
            Some(SearchError::FanoutLimit)
        );
        assert_eq!(
            view.probes.get(),
            257,
            "first seed and at most 256 declarations are checked"
        );
    }
    view.probes.set(0);
    let req = Request {
        limits: Limits {
            visits: 3,
            ..LIMITS
        },
        ..outgoing()
    };
    assert_eq!(graph.select(&view, req).err(), Some(SearchError::ScanLimit));
    assert_eq!(
        view.probes.get(),
        3,
        "denied declarations consume the shared work budget"
    );
    view.probes.set(0);
    let other = request(&[A], &[RELATED], Direction::Out);
    assert_eq!(
        ids(graph
            .select(&view, other)
            .unwrap()
            .revalidate(&view)
            .unwrap()),
        ["b"]
    );
    assert_eq!(
        view.probes.get(),
        6,
        "an unselected type has no posting visits"
    );
    view.probes.set(0);
    let capped = Request {
        limits: Limits {
            visits: 2,
            ..LIMITS
        },
        ..request(&[A], &[RELATED], Direction::Out)
    };
    assert_eq!(
        graph.select(&view, capped).err(),
        Some(SearchError::ScanLimit)
    );
    assert_eq!(view.probes.get(), 2, "the target shares the scan budget");
    engine.shutdown().await.unwrap();
}
