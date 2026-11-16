use std::collections::BTreeSet;

#[test]
fn actual_http_report_has_every_trial_and_consistent_latency_fields() {
    let doc = sxpb::from_str(include_str!("http.sxpb")).unwrap();
    let records = doc.as_array().unwrap();
    assert_eq!(records.len(), 72);
    let mut seen = BTreeSet::new();
    for record in records {
        let m = record.as_mesg().unwrap();
        assert_eq!(m.len(), 10);
        let n = |room: &str| m.get(room).unwrap().as_number().unwrap().to_u128().unwrap();
        let distribution = m.get("distribution").unwrap().as_str().unwrap();
        assert!(["shared", "distinct"].contains(&distribution));
        let workers = n("workers");
        let clients = n("clients");
        let payload = n("payload_bytes");
        let trial = n("trial");
        assert!([1, 4].contains(&workers));
        assert!([1, 4, 16].contains(&clients));
        assert!([32, 4096].contains(&payload));
        assert!(trial < 3);
        assert_eq!(n("requests"), clients * 64);
        assert!(n("elapsed_ns") > 0 && n("p50_ns") > 0);
        assert!(n("p50_ns") <= n("p95_ns") && n("p95_ns") <= n("p99_ns"));
        assert!(seen.insert((workers, clients, distribution.to_owned(), payload, trial)));
    }
    assert_eq!(seen.len(), 72);
}
