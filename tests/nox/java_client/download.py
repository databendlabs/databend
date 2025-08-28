import argparse
from pathlib import Path
import requests


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("version")
    args = parser.parse_args()

    download_jdbc(args.version)
    # download_testng()


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
    filename = f"testng-7.10.2.jar"

    target = Path(f"cache/lib/{filename}")
    if target.exists():
        return
    target.parent.mkdir(parents=True, exist_ok=True)

    resp = requests.get(
        f"https://repo1.maven.org/maven2/org/testng/testng/7.10.2/{filename}"
    )
    resp.raise_for_status()
    target.write_bytes(resp.content)


if __name__ == "__main__":
    main()
