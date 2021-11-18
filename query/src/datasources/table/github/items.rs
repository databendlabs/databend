use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct RepoMetaInfoSimple {
    name: String,
    full_name: String,
}

#[derive(Deserialize, Debug)]
pub struct License {
    key: String,
    name: String,
}

#[derive(Deserialize, Debug)]
struct RepoDetails {
    name: String,
    language: Option<String>,
    license: Option<License>,
    stargazers_count: u32,
    forks_count: u32,
    watchers_count: u32,
    open_issues_count: u32,
    subscribers_count: u32,
    created_at: String,
    updated_at: String,
}
