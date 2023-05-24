#!/usr/bin/env python

"""
Setup script for the Python package. Dependencies for server are listed separately
in requirements.txt, dev dependencies are listed in requirements-dev.txt.
"""

from setuptools import setup

PKG = 'metamist_infrastructure'

with open('README.md', encoding='utf-8') as f:
    readme = f.read()

setup(
    name=PKG,
    # This tag is automatically updated by bump2version
    version='1.0.0',
    description='CPG Infrastructure plugin for Pulumi',
    long_description=readme,
    long_description_content_type='text/markdown',
    url=f'https://github.com/populationgenomics/sample-metadata',
    license='MIT',
    packages=[
        'metamist_infrastructure',
        'metamist_infrastructure.etl',
        'metamist_infrastructure.etl.endpoint',
    ],
    # set package_dir so that files in THIS directory are included as metamist_infrastructure
    package_dir={
        'metamist_infrastructure': '.',
        'metamist_infrastructure.etl': '../etl',
        'metamist_infrastructure.etl.endpoint': '../etl/endpoint',
    },
    package_data={
        'metamist_infrastructure.etl': ['*.json'],
        'metamist_infrastructure.etl.endpoint': ['*.txt'],
    },
    install_requires=[],
    entry_points={
        'cpginfra.plugins': [
            'metamist = metamist_infrastructure:MetamistInfrastructure',
        ],
    },
    # include_package_data=True,
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
