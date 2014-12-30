# -*- coding: utf-8 -*-

#   Copyright (c) 2010-2014, MIT Probabilistic Computing Project
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='bayeslite',
    version='0.0.dev',
    packages=[
        'bayeslite',
        'bayeslite.plex',
    ],
    package_dir={
        'bayeslite.plex': 'external/plex/dist/Plex',
    },
    install_requires=[
        'scipy',
    ],
    license='Apache License, Version 2.0',
)
