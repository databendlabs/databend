import argparse
from pathlib import Path
import requests


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("version")
    args = parser.parse_args()

    download_jdbc(args.version)
    download_testng()


def download_jdbc(version):
    filename = f"databend-jdbc-{version}.jar"

    target = Path(f"cache/jdbc/{filename}")
    if target.exists():
        return
    target.parent.mkdir(parents=True, exist_ok=True)

    resp = requests.get(
        f"https://github.com/databendlabs/databend-jdbc/releases/download/v{version}/{filename}"
    )
    resp.raise_for_status()
    target.write_bytes(resp.content)


def download_testng():
    urls = [
        "https://repo.maven.apache.org/maven2/org/testng/testng/7.11.0/testng-7.11.0.jar",
        "https://repo1.maven.org/maven2/com/vdurmont/semver4j/3.1.0/semver4j-3.1.0.jar",
        "https://repo1.maven.org/maven2/org/jcommander/jcommander/1.83/jcommander-1.83.jar",
        "https://repo1.maven.org/maven2/org/locationtech/jts/jts-core/1.19.0/jts-core-1.19.0.jar",
        "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.16/slf4j-api-2.0.16.jar",
    ]

    for url in urls:
        splited = url.rsplit("/", 1)
        print(splited)
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
