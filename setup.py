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
    author_email="jalichtefeld@gmail.com,gabbard@isi.edu",
    description="A higher-level API for ISI Pegasus, adapted to the quirks of the ISI Vista group",
    url="https://github.com/isi-vista/vista-pegasus-wrapper",
    packages=['pegasus_wrapper', 'pegasus_wrapper.resources'],
    package_data={'': ['sites.xml', 'pegasus.conf']},
    # 3.6 and up, but not Python 4
    python_requires="~=3.6",
    install_requires=[
        "importlib-resources==1.4.0",
	"vistautils>=0.21.0"
    ],
    scripts=['scripts/multiply_by_x.py', 'scripts/sort_nums_in_file.py', 'scripts/nuke_checkpoints'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
