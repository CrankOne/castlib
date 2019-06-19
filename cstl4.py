#!/usr/bin/env python3
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

"""
Executable script providing various entry points to common routines of
castlib4 python package.
"""

import sys, argparse, yaml
import castlib4.models.filesystem as fs
import castlib4.backend.local as lbe
import castlib4.executives

import json  # XXX

if "__main__" == __name__:
    p = argparse.ArgumentParser(description=globals().__doc__)
    p.add_argument('-c', '--config', help='YAML config file used.'
                  , default='config.yaml')
    #p.add_argument('-l', '--location', help='')
    # ...
    args = p.parse_args()
    # Load config file:
    with open(args.config) as cf:
        cfg = yaml.load(cf, Loader=yaml.FullLoader)
    # Initialize database:
    castlib4.executives.initialize_database( cfg['database']['args']
                                           , engineCreateKWargs=cfg['database']['kwargs'])
    # -------------------------------------------------------------------------
    # Testing code: uses content of $(pwd)/castlib4/ to create a files index.
    lbe = lbe.LocalBackend()
    entries = lbe.ls_detailed( 'castlib4'
                             , recursive=True
                             , filePropertiesExclude=['adler32']
                             , omitEmptyDirs=True
                             , pattern='.*.py$' )
    # Uncomment to see a JSON dump
    #print( json.dumps( entries, indent=4) )
    #print( lbe.adler32(sys.argv[1]) )


