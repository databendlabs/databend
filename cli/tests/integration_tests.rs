use assert_cmd::cargo::CommandCargoExt;
use std::process::Command;

fn init_fusectl() -> Command {
    let mut cmd = Command::cargo_bin("datafuse-cli").unwrap();
    cmd.current_dir("/tmp");
    cmd
}

fn raw_fusectl() -> Command {
    let cmd = init_fusectl();
    cmd
}

fn fusectl() -> assert_cmd::Command {
    assert_cmd::Command::from_std(raw_fusectl())
}

#[test]
fn basic() {
    fusectl()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicates::str::contains("Prints help information" ))
        .stderr("");
}
