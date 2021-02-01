#!/usr/bin/env python

from distutils.core import setup
from os.path import abspath, dirname, join

from setuptools import find_packages

with open(
    join(dirname(abspath(__file__)), "pegasus_wrapper", "version.py")
) as version_file:
    exec(compile(version_file.read(), "version.py", "exec"))

setup(
    name="pegasus_wrapper",
    version=version,  # noqa
    author="Jacob Lichtefeld, Ryan Gabbard",
    author_email="jalichtefeld@gmail.com,ryan.gabbard@gmail.com",
    description="A higher-level API for ISI Pegasus, adapted to the quirks of the ISI Vista group",
    url="https://github.com/isi-vista/vista-pegasus-wrapper",
    packages=find_packages(),
    package_data={'': ['sites.xml', 'pegasus.conf']},
    # 3.6 and up, but not Python 4
    python_requires="~=3.6",
    install_requires=[
        "importlib-resources==1.4.0",
	    "vistautils>=0.21.0",
        "gitpython==3.1.12",
        "pegasus-wms.api==5.0.0",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
