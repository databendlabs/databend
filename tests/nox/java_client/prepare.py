import argparse
from pathlib import Path
import requests
from requests.auth import HTTPBasicAuth
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("version")
    args = parser.parse_args()

    if args.version != "main":
        download_jdbc(args.version)
    download_testng()
    create_user()


def download_jdbc(version):
    filename = f"databend-jdbc-{version}.jar"
    target = Path(f"cache/jdbc/{filename}")
    if target.exists():
        print(f"{filename} exists")
    target.parent.mkdir(parents=True, exist_ok=True)

    print(f"start download {filename}")
    resp = requests.get(
        f"https://github.com/databendlabs/databend-jdbc/releases/download/v{version}/{filename}"
    )
    resp.raise_for_status()
    target.write_bytes(resp.content)


def exec(sql, port=8000):
    print(f"{port}: {sql}")
    print("----")
    resp = requests.post(
        f"http://localhost:{port}/v1/query/",
        auth=HTTPBasicAuth("root", ""),
        headers={"Content-Type": "application/json"},
        json={"sql": sql},
    )
    if resp.status_code != 200:
        print(f"error({resp.status_code}, {resp.reason}):{resp.text}")
        raise Exception()
    j = resp.json()
    if j["error"]:
        print(f"sql error: {j['error']}")
        raise Exception()
    print(j)
    print("====")


def create_user():
    exec("DROP USER IF EXISTS databend")
    exec(
        "CREATE USER databend IDENTIFIED BY 'databend' with default_role='account_admin'"
    )
    exec("GRANT ROLE account_admin TO USER databend")
    exec("SYSTEM FLUSH PRIVILEGES")


def download_testng():
    urls = [
        "https://repo.maven.apache.org/maven2/org/testng/testng/7.11.0/testng-7.11.0.jar",
        "https://repo1.maven.org/maven2/com/vdurmont/semver4j/3.1.0/semver4j-3.1.0.jar",
        "https://repo1.maven.org/maven2/org/jcommander/jcommander/1.83/jcommander-1.83.jar",
        "https://repo1.maven.org/maven2/org/locationtech/jts/jts-core/1.19.0/jts-core-1.19.0.jar",
        "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.16/slf4j-api-2.0.16.jar",
        "https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/2.0.13/slf4j-simple-2.0.13.jar",
        "https://repo1.maven.org/maven2/org/junit/platform/junit-platform-console-standalone/1.11.3/junit-platform-console-standalone-1.11.3.jar",
    ]

    for url in urls:
        splited = url.rsplit("/", 1)
        filename = splited[1]
        target = Path(f"cache/lib/{filename}")
        if target.exists():
            print(f"{filename} exists")
            continue
        target.parent.mkdir(parents=True, exist_ok=True)

        print(f"start download {filename}")
        resp = requests.get(url)
        resp.raise_for_status()
        target.write_bytes(resp.content)


if __name__ == "__main__":
    main()
