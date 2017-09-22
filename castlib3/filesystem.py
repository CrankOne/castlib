# -*- coding: utf-8 -*-
# Copyright (c) 2017 Renat R. Dusaev <crank@qcrypt.org>
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

import os, fnmatch
from urlparse import urlparse, urlunsplit
from castlib3.logs import gLogger

def split_path( path ):
    """
    Splits given path into list of tokens. Another approach (returning empty
    token for root): filter( lambda pt: pt, os.path.normpath(origPath).split( os.sep ) )
    """
    pToks = []
    while True:
        path, pTok = os.path.split(path)
        if pTok != '':
            pToks.append( pTok )
        else:
            if path != '':
                pToks.append(path)
            break
    pToks.reverse()
    return pToks

def discover_entries( entriesDict, backends={} ):
    """
    This function builds a filesystem entries dictionary for stages. It expects
    a dictionary in form:
    {
        <inner-entry-id> : {
            'node' : <node-id>,  # optional
            'localPath' : <local-path>,
            'ignore' : [ <wildcard1>, <wildcard2>, ... ],
            'only' : [ <wildcard1>, <wildcard2>, ... ],
            ... # stage-specific arguments (e.g. castorSync: <flag>)
        }
        ...
    }
    Where `ignore' and `only' wildcards lists are not mutually exclusive. When
    both given the `ignore' will be applied after `only' selector. The
    `inner-entry-id' is used for runtime operations and won't affect any
    persistent data.
    
    The folder value is a tuple of (realPath, virtualFolderName).
    The `files' entries are merely string filenames of files to be indexed.
    `directories' entry is a dictionaries following the same topology
    recursively.

    Note, that in case of deleted directory content, none of the internal
    directories will be removed from database. This routine designed for
    intermediate representation of synchronization parameters, prior to
    castlib3 `stages' execution, not for standalone use.

    localPaths are always a full paths while `files' are just
    filenames.
    """
    # Traverse directories content, forming the list without directories
    # resolution:
    ret = []
    if not entriesDict:
        return ret
    for innerFolderID, dirListPrescript in entriesDict.iteritems():
        uri = dirListPrescript.pop('localPath')
        lpp = urlparse(uri)
        backend = backends[lpp.scheme or 'file']
        gLogger.info( 'Listing contents of directory "%s" : %s...'%(
            innerFolderID, uri) )
        dirContent = backend.get_dir_content(
                uri,
                onlyPats=dirListPrescript.pop('only', None),
                ignorePats=dirListPrescript.pop('ignore', None),
                extra=dirListPrescript
            )
        if lpp.netloc:
            dirContent['node'] = lpp.netloc
        # Prepend folder path:
        pToks = split_path( os.path.split(lpp.path)[0] )
        while pToks:
            uri = urlunsplit((lpp.scheme or '', lpp.netloc or '', os.path.join(*pToks), '', ''))
            pToks.pop()
            dirContent = {
                    'folder' : uri,
                    'files' : [],
                    'subFolders' : [dirContent]
                }
            if lpp.netloc:
                dirContent['node'] = lpp.netloc
        ret.append( dirContent )
    return ret
        
