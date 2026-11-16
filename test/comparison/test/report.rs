use std::collections::BTreeSet;

fn report(name: &str, cache: bool) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(name);
    let doc = sxpb::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let records = doc.as_array().unwrap();
    assert_eq!(records.len(), if cache { 61 } else { 181 });
    let env = records[0].as_mesg().unwrap();
    assert_eq!(env.len(), 1);
    assert!(env.get("environment").unwrap().as_mesg().is_some());
    let mut seen = BTreeSet::new();
    for row in &records[1..] {
        let m = row.as_mesg().unwrap();
        assert_eq!(m.len(), 7);
        let text = |field: &str| m.get(field).unwrap().as_str().unwrap();
        let number = |field: &str| {
            m.get(field)
                .unwrap()
                .as_number()
                .unwrap()
                .to_u128()
                .unwrap()
        };
        let kind = text("kind");
        let backend = text("backend");
        let rows = number("rows");
        let payload = number("payload_bytes");
        let trial = number("trial");
        assert!([2, 16, 128, 1024, 8192].contains(&rows));
        assert!(trial < 3);
        assert!(number("elapsed_ns") > 0);
        let burst = if cache {
            assert!(["clocked-rooms-hot-no-expired", "clocked-rooms-expiry-burst"].contains(&kind));
            assert!(["vec", "ordered"].contains(&backend));
            assert_eq!(payload, 32);
            kind == "clocked-rooms-expiry-burst"
        } else {
            match kind {
                "full" | "reference" => {
                    assert!(["vec", "ordered"].contains(&backend));
                    assert!([32, 4096].contains(&payload));
                }
                "opening-worst" => {
                    assert!(["vec", "ordered"].contains(&backend));
                    assert_eq!(payload, 32);
                }
                "clocked-hot-no-expired" | "expiry-burst" => {
                    assert_eq!(backend, "ordered-only");
                    assert_eq!(payload, 32);
                }
                _ => panic!("unknown measurement kind: {kind}"),
            }
            kind == "expiry-burst"
        };
        assert_eq!(number("operations"), if burst { rows } else { 2000 });
        assert!(seen.insert((kind.to_owned(), rows, payload, trial, backend.to_owned())));
    }
    assert_eq!(seen.len(), records.len() - 1);
}

#[test]
fn benchmark_report_is_sxpb_with_complete_trials() {
    report("benchmark.sxpb", false);
}

#[test]
fn cache_benchmark_report_is_sxpb_with_complete_trials() {
    report("cache-benchmark.sxpb", true);
}
