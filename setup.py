#!/usr/bin/env python
"""
Setup script for the analysis_runner python API
- Used for development setup with `pip install --editable .`
- Parsed by conda-build to extract version and metainfo
"""

from setuptools import setup, find_packages

PKG = 'sample_metadata'

all_packages = []
for hl_package in 'sample_metadata':
    all_packages.extend(
        f'{hl_package}.' + p for p in sorted(find_packages(f'./{hl_package}'))
    )

setup(
    name=PKG,
    # This tag is automatically updated by bump2version
    version='1.0.1',
    description='Python API for interacting with the Sample API system',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url=f'https://github.com/populationgenomics/{PKG}',
    license='MIT',
    packages=all_packages,
    requirements=[
        'google-auth',
        'urllib3 >= 1.25.3',
        'python-dateutil',
    ],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        # 'console_scripts': ['=analysis_runner.cli:main_from_args']
    },
    keywords='bioinformatics',
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
    ],
)
