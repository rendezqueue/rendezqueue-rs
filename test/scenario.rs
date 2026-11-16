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
    for (source, expected) in [
        ("(()) (() (unknown +true))", "unknown field: unknown"),
        ("(()) (() (reset +false))", "reset must be +true"),
        ("(()) (() (req (self 1)) (reject +true))", "expected string"),
        (
            "(()) (() (req (self \"a\") (value \"typo\")) (reject +true))",
            "unknown field: value",
        ),
        (
            "(()) (() (req (self \"a\")))",
            "request needs exactly one of res or reject",
        ),
        (
            "(()) (() (req (self \"a\")) (reject +false))",
            "reject must be +true",
        ),
        (
            "(()) (() (req (self \"a\")) (res (self \"a\")) (reject +true))",
            "request needs exactly one of res or reject",
        ),
        (
            "(()) (() (reset +true) (forget \"a\"))",
            "expected exactly one operation",
        ),
        (
            "(()) (() (forget \"a\") (res (self \"a\")))",
            "fault operation cannot have a response",
        ),
    ] {
        let error = scenario::run(source).unwrap_err();
        assert!(error.contains(expected), "{source}: {error}");
    }
}
