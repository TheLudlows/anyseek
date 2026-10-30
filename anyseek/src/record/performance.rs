//! Opt-in in-memory generation preparation profile for #107.
use super::*;
use std::time::Instant;

#[test]
#[ignore = "manual optimized control-plane profile"]
fn generation_renew_cost_profile() {
    let output = std::env::var("ANYSEEK_COST_OUTPUT").expect("output directory");
    let mut rows = Vec::new();
    for (count, controls) in [(100,1), (1000,1), (5000,1), (10000,1), (1000,16), (1000,64), (1000,256), (10000,128)] {
        let mut cfg = tests::config();
        cfg.max_records = count + controls * 2 + 32; cfg.max_operations = count * 2 + 128;
        cfg.max_journal_bytes = 16 * 1024 * 1024;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let epoch = now / 86_400_000;
        let mut state = State {lineage:"profile".into(),history:history::HistoryState {scope:"profile".into(),floor:0},..State::default()};
        let mut seed = |request:RecordWrite| {
            let event = Applied {request_digest:None,receipt:WriteReceipt {seq:state.seq+1,version:1,record_id:request.record_id.clone(),staged_only:false},
                request,observed_epoch:epoch,observed_time_ms:now};
            apply(&mut state,&event).unwrap();
        };
        seed(tests::source_write("source","s",&[]));
        for i in 0..count { seed(tests::write(&format!("r-{i}"),&format!("r{i}"),"alpha body",ExpectedVersion::Absent)); }
        for control in 0..controls {
            let member = format!("member-{control}");
            let mut stage = tests::generation_write(&format!("stage-{control}"),ExpectedVersion::Absent,tests::stage_generation("g",&[&member]));
            stage.record_id = format!("doc{control}");
            let prepared = generation::prepare(&state,&cfg,stage,epoch,now).unwrap();
            let mutation::PreparedWrite::Append {candidate:Some(next),..} = prepared else {panic!("expected generation state");};
            state = next;
        }
        state.admit_bytes(state_proof::length(&state).unwrap()).unwrap();
        let mut samples = Vec::new();
        for i in 0..30 {
            let start = Instant::now();
            let copy = state.try_clone().unwrap();
            let clone_us = start.elapsed().as_micros();
            std::hint::black_box(copy);
            let start = Instant::now();
            generation::validate_state(std::hint::black_box(&state),&cfg).unwrap();
            let validate_us = start.elapsed().as_micros();
            let mut request = tests::generation_write(&format!("renew-{i}"),ExpectedVersion::Match(1),GenerationCommand::Renew {generation_id:"g".into()});
            request.record_id = "doc0".into();
            let start = Instant::now();
            let prepared = generation::prepare(std::hint::black_box(&state),&cfg,request,epoch,now).unwrap();
            let prepare_us = start.elapsed().as_micros();
            std::hint::black_box(prepared);
            if i >= 5 {samples.push(serde_json::json!({"clone_us":clone_us,"validate_us":validate_us,"prepare_us":prepare_us}));}
        }
        rows.push(serde_json::json!({"records":state.records.len(),"operations":state.operations.len(),"generations":state.generations.len(),
            "state_bytes":state_proof::length(&state).unwrap(),"samples":samples}));
    }
    std::fs::create_dir_all(&output).unwrap();
    std::fs::write(Path::new(&output).join("generation.json"),serde_json::to_vec_pretty(&rows).unwrap()).unwrap();
}
