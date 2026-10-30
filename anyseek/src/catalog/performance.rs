//! Opt-in cost investigation for #107; no latency assertions in normal CI.
use super::*;
use std::time::Instant;

#[tokio::test]
#[ignore = "manual optimized control-plane profile"]
async fn catalog_update_cost_profile() {
    let output = std::env::var("ANYSEEK_COST_OUTPUT").expect("output directory");
    let mut rows = Vec::new();
    for count in [16, 128, 1024, 4096] {
        let temporary = tempfile::tempdir().unwrap();
        let mut state = State::default();
        let mut seed = |command| {
            let n = state.seq + 1;
            let event = Applied { request: CatalogRequest {operation_id: OperationId::new(format!("seed-{n}")),command},
                receipt: CatalogReceipt {seq:n},observed_epoch:epoch() };
            apply(&mut state,&event).unwrap();
        };
        seed(CatalogCommand::AddTenant {tenant_id:"bench".into()});
        seed(CatalogCommand::AddKey {key_id:"bench-key".into(),tenant_id:"bench".into(),verifier:"a".repeat(64)});
        for i in 0..count {
            let name = format!("c{i}");
            seed(CatalogCommand::PrepareCollection {tenant_id:"bench".into(),collection_id:name.clone(),config:tests::config()});
            seed(CatalogCommand::ActivateCollection {tenant_id:"bench".into(),collection_id:name});
        }
        let c = Catalog::create_state(&temporary.path().join("catalog"),state).await.unwrap();
        let mut inner = c.inner.lock().await;
        let bytes_before = serde_json::to_vec(&inner.state).unwrap().len();
        let mut samples = Vec::new();
        for i in 0..30 {
            let start = Instant::now();
            let mut next = std::hint::black_box(&inner.state).clone();
            let cloned = Instant::now();
            let event = Applied {request: tests::request(&format!("update-{i}"),CatalogCommand::RevokeKey {key_id:"bench-key".into()}),
                receipt:CatalogReceipt {seq:next.seq+1},observed_epoch:epoch()};
            apply(&mut next,&event).unwrap();
            let applied = Instant::now();
            sqlite::validate_state(&next).unwrap();
            let validated = Instant::now();
            let Inner {store,state,..} = &mut *inner;
            store.persist(state,&next).unwrap();
            let persisted = Instant::now();
            inner.state = next;
            if i >= 5 {
                samples.push(serde_json::json!({"clone_us":(cloned-start).as_micros(),
                    "apply_us":(applied-cloned).as_micros(),"validate_us":(validated-applied).as_micros(),
                    "persist_us":(persisted-validated).as_micros(),"total_us":(persisted-start).as_micros()}));
            }
        }
        rows.push(serde_json::json!({"collections":count,"operations":inner.state.operations.len(),
            "state_bytes_before":bytes_before,"state_bytes_after":serde_json::to_vec(&inner.state).unwrap().len(),"samples":samples}));
    }
    std::fs::create_dir_all(&output).unwrap();
    std::fs::write(Path::new(&output).join("catalog.json"),serde_json::to_vec_pretty(&rows).unwrap()).unwrap();
}
