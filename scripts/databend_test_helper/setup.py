#!/usr/bin/env python3

from setuptools import setup, find_packages

setup(
    name="databend-test-helper",
    version="0.1.0",
    description="Testing utilities for starting and stopping Databend processes",
    packages=find_packages(),
    package_data={
        "databend_test_helper": ["configs/*.toml"],
    },
    include_package_data=True,
    python_requires=">=3.7",
    install_requires=[
        "psutil>=5.0.0",
        "requests>=2.20.0",
        "toml>=0.10.0",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Testing",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)