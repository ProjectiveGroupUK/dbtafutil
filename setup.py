#!/usr/bin/env python
import os
import sys

if sys.version_info < (3, 8, 10):
    print("Error: dbtafutil is not supported for this version of Python.")
    print("Please upgrade to Python 3.8.10 or higher.")
    sys.exit(1)


from setuptools import setup

try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print("Error: dbtafutil requires setuptools v40.1.0 or higher.")
    print(
        'Please upgrade setuptools with "pip install --upgrade setuptools" '
        "and try again"
    )
    sys.exit(1)


this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md")) as f:
    long_description = f.read()


package_name = "dbtafutil"
package_version = "1.0.1"
description = """dbtafutil is dbt to Airflow Utility package.\
It can be used to generate Airflow Dags,\
from data pipelines developed in dbt.\
It uses relationship graph available in dbt to identify associated models\
of data pipeline."""


setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Nitin Garg",
    author_email="nitin.garg@dtsquared.co.uk",
    url="https://github.com/nitindt2/dbtafutil",
    packages=find_namespace_packages(include=["dbtafutil", "dbtafutil.*"]),
    include_package_data=True,
    test_suite="test",
    entry_points={
        "console_scripts": [
            "dbtafutil = dbtafutil.main:main",
        ],
    },
    install_requires=[
        "colorama==0.4.5",
    ],
    zip_safe=False,
    classifiers=[
        "Development Status :: 1 - Production/Stable",
        "License :: OSI Approved :: MIT",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.8.10",
)
