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
import castlib4.executives

if "__main__" == __name__:
    p = argparse.ArgumentParser(description=globals().__doc__)
    p.add_argument('-c', '--config', help='YAML config file used.'
                  , default='config.yaml')
    #p.add_argument('-l', '--location', help='')
    # ...
    args = p.parse_args()
    # Load config file and init castlib
    with open(args.config) as cf:
        cfg = yaml.load(cf, Loader=yaml.FullLoader)
    castlib4.executives.init_all(cfg)

