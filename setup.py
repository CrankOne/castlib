# Copyright (c) 2016 Renat R. Dusaev <crank@qcrypt.org>
# Author: Renat R. Dusaev <crank@qcrypt.org>
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

from distutils.core import setup

setup(name = "castlib3",
    version = "0.3",
    description = "A toolset for file synchronization with CASTOR storage.",
    author = "Renat R. Dusaev",
    author_email = "renat.dusaev@cern.ch",
    url = "https://gitlab.cern.ch/P348/aux.scripts",
    packages = [ 'castlib3.sVbp',
                 'castlib3'],
    package_dir = {
            'castlib3.sVbp' : 'sVbp',
            'castlib3' : 'castlib3' },
    scripts = ['cstl3-run'],
    long_description = """\
The castlib3 package is a set of tools designed for file synchronization with
CERN's CASTOR storage. Toolkit was designed for needs of NA64 experiment.

The database and chunk file ORM can be further used for front-end applications.\
""")
