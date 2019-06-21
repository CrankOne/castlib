# -*- coding: utf-8 -*-
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

from __future__ import print_function

import datetime
from schema import Schema, And, Use, Optional, Or

fseSchema = Schema({ Optional('size') : Use(int)
                    , Optional('mtime') : Use(datetime.datetime.utcfromtimestamp)
                    , Optional('owner') : Or( {
                            'user' : (str, int),  # TODO: or list
                            'group' : (str, int)  # TODO: or list
                        }, int )
                    , Optional('adler32') : str  # TODO
                    , Optional('permissions') : int
                    , Optional('@content') : dict
                    })

def validate_fs_entries( entries ):
    es = {}
    for name, entry in entries.items():
        # TODO: check name for validity
        es[name] = fseSchema.validate( entry )
        if '@content' in entry.keys():
            es[name]['@content'] = validate_fs_entries(entry['@content'])
    return es

