#!/usr/bin/env python

"""
Setup script for the metamist_infrastructure Python package.
We won't list any dependencies here, but it does require pulumi-gcp
"""

from setuptools import setup

PKG = 'metamist_infrastructure'

with open('README.md', encoding='utf-8') as f:
    readme = f.read()

setup(
    name=PKG,
    # not super important as we don't deploy this plugin
    version='1.0.0',
    description='Metamist infrastructure plugin for cpg-infrastructure',
    long_description=readme,
    long_description_content_type='text/markdown',
    url=f'https://github.com/populationgenomics/sample-metadata',
    license='MIT',
    packages=[
        'metamist_infrastructure',
        'metamist_infrastructure.etl',
        'metamist_infrastructure.etl.post',
        'metamist_infrastructure.etl.load',
    ],
    package_dir={
        # files in THIS directory are included as metamist_infrastructure
        'metamist_infrastructure': '.',
        # files in ../etl are included as metamist_infrastructure.etl
        'metamist_infrastructure.etl': '../etl',
        # files in ../etl/post are included as metamist_infrastructure.etl.post
        'metamist_infrastructure.etl.post': '../etl/post',
        # files in ../etl/load are included as metamist_infrastructure.etl.load
        'metamist_infrastructure.etl.load': '../etl/load',
    },
    package_data={
        # ensure bq_schema.json is included in etl
        'metamist_infrastructure.etl': ['*.json'],
        # ensure requirements.txt is included in etl.post
        'metamist_infrastructure.etl.post': ['*.txt'],
        # ensure requirements.txt is included in etl.load
        'metamist_infrastructure.etl.load': ['*.txt'],
    },
    install_requires=[],
    entry_points={
        # register this plugin
        'cpginfra.plugins': [
            'metamist = metamist_infrastructure:MetamistInfrastructure',
        ],
    },
    zip_safe=False,
    keywords='bioinformatics',
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
    ],
)
