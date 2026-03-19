use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .expect("repo root should resolve")
}

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/normalization")
        .canonicalize()
        .expect("fixture dir should resolve")
}

fn fixture_paths() -> Vec<PathBuf> {
    let mut fixtures: Vec<_> = fs::read_dir(fixture_dir())
        .expect("fixture dir should be readable")
        .map(|entry| entry.expect("fixture entry should load").path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
        .collect();
    fixtures.sort();
    fixtures
}

fn run_oracle(fixture_path: &Path) -> Value {
    let root = repo_root();
    let python = {
        let venv_python = root.join(".venv/bin/python");
        if venv_python.exists() {
            venv_python
        } else {
            PathBuf::from("python3")
        }
    };
    let output = Command::new(&python)
        .args([
            "scripts/normalization-parity-oracle.py",
            fixture_path
                .to_str()
                .expect("fixture path should be valid unicode"),
        ])
        .current_dir(&root)
        .output()
        .expect("oracle command should start");

    assert!(
        output.status.success(),
        "oracle command failed for {}:\nstdout:\n{}\nstderr:\n{}",
        fixture_path.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    serde_json::from_slice(&output.stdout).expect("oracle stdout should be valid json")
}

#[test]
fn fixtures_match_python_normalization_oracle() {
    for fixture_path in fixture_paths() {
        let fixture: Value =
            serde_json::from_slice(&fs::read(&fixture_path).expect("fixture should be readable"))
                .expect("fixture should parse");

        let expected = fixture
            .get("expected")
            .expect("fixture should contain expected output");
        let actual = run_oracle(&fixture_path);

        assert_eq!(
            &actual,
            expected,
            "fixture output mismatch for {}",
            fixture_path.display()
        );
    }
}

#[test]
fn fixture_corpus_covers_all_sources_and_dead_letters() {
    let mut normalized_sources = Vec::new();
    let mut dead_letter_count = 0;

    for fixture_path in fixture_paths() {
        let fixture: Value =
            serde_json::from_slice(&fs::read(&fixture_path).expect("fixture should be readable"))
                .expect("fixture should parse");
        let raw_event = fixture
            .get("raw_event")
            .and_then(Value::as_object)
            .expect("fixture raw_event should be an object");
        let source = raw_event
            .get("source")
            .and_then(Value::as_str)
            .expect("fixture raw_event should include source");
        let expected = fixture
            .get("expected")
            .and_then(Value::as_object)
            .expect("fixture expected should be an object");
        match expected.get("kind").and_then(Value::as_str) {
            Some("normalized") => normalized_sources.push(source.to_string()),
            Some("dead_letter") => dead_letter_count += 1,
            other => panic!("unexpected fixture kind: {other:?}"),
        }
    }

    normalized_sources.sort();
    normalized_sources.dedup();
    assert_eq!(
        normalized_sources,
        vec!["github", "gworkspace", "okta", "snowflake"]
    );
    assert!(
        dead_letter_count >= 2,
        "expected at least two dead-letter fixtures"
    );
}
