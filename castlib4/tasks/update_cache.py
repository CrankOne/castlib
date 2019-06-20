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

import os, urllib, logging
import castlib4.backend \
     , castlib4.executives \
     , castlib4.backend.mixins

import yaml, json  # XXX

@castlib4.queue.task
def update_cache( uri
                , recursive=False
                , filePropertiesExclude=set({})
                , dirPropertiesExclude=set({})
                , omitEmptyDirs=False
                , pattern='*' ):
    """
    Performs [recursive] traversal of given path, retrieval of certain
    attributes from the files and folder and updates the cache of existing
    entries.
    """
    L = logging.getLogger(__name__)
    parsedURI = urllib.parse.urlparse( uri )
    be = castlib4.backend.registry.get_backend( parsedURI.scheme )
    lsdKWargs = { 'recursive' : recursive
                , 'filePropertiesExclude' : filePropertiesExclude
                , 'dirPropertiesExclude' : dirPropertiesExclude
                , 'omitEmptyDirs' : omitEmptyDirs
                , 'pattern' : pattern
                , 'scheme' : parsedURI.scheme
                , 'netloc' : parsedURI.netloc
                , 'path' : parsedURI.path
                , 'params' : parsedURI.params
                , 'query' : parsedURI.query
                , 'fragment' : parsedURI.fragment
            }
    if hasattr( be, 'ls_detailed' ):
        # Backend defines own ls_detailed() method
        entries = be.ls_detailed( **lsdKWargs )
    else:
        # Use default implementation of detailed listings
        entries = castlib4.backend.mixins.ls_detailed( **lsdKWargs )
    # TODO: update cache ...
    print( json.dumps( entries, indent=4) ) # XXX


if "__main__" == __name__:
    # Load config file and init castlib
    with open('config.yaml') as cf:
        cfg = yaml.load(cf, Loader=yaml.FullLoader)
    castlib4.executives.init_all(cfg)

    update_cache( 'file:///%s'%os.path.join(os.getcwd(), 'castlib4')
                , recursive=True
                , filePropertiesExclude=['adler32']
                , omitEmptyDirs=True
                , pattern='.*.py$')
