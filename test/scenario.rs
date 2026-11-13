use rendezqueue::scenario;
use std::{fs, path::Path};

#[test]
fn server_scenarios() {
    let mut paths: Vec<_> =
        fs::read_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("test/scenario"))
            .unwrap()
            .map(|p| p.unwrap().path())
            .filter(|p| p.extension().is_some_and(|s| s == "sxpb"))
            .collect();
    paths.sort();
    assert!(!paths.is_empty());
    for path in paths {
        let count = scenario::run(&fs::read_to_string(&path).unwrap())
            .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        assert!(count > 0, "empty scenario: {}", path.display());
    }
}

#[test]
fn malformed_scenarios_fail_loudly() {
    for source in [
        "(()) (() (unknown +true))",
        "(()) (() (reset +false))",
        "(()) (() (req (sid 1)) (reject +true))",
        "(()) (() (req (sid \"a\") (value \"typo\")) (reject +true))",
        "(()) (() (req (sid \"a\")))",
        "(()) (() (req (sid \"a\")) (reject +false))",
        "(()) (() (req (sid \"a\")) (res (sid \"a\")) (reject +true))",
        "(()) (() (reset +true) (forget \"a\"))",
        "(()) (() (forget \"a\") (res (sid \"a\")))",
    ] {
        assert!(scenario::run(source).is_err(), "{source}");
    }
}
