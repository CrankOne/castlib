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

import os, re
import castlib4

def ls_detailed_f( be, path, recursive=False
                 , filePropertiesExclude=set({})
                 , dirPropertiesExclude=set({})
                 , omitEmptyDirs=False
                 , pattern='*'
                 # This are usually shipped with URI
                 , scheme='file'
                 , netloc=None
                 , params=None
                 , query=None
                 , fragment=None
                 ):
    """
    Arguments:
        recursive -- contols whether or not to traverse sub-directories
    filePropertiesExclude -- set (or list) containing list of properties that
        have to be excluded from resulting dictionary
    dirPropertiesExclude -- set (or list) containing list of properties that
        have to be excluded from resulting dictionary
    omitEmptyDirs -- controls, whether to omit the empty directories from
        result
    regex -- regular expression that, if given, shall be applied against full
        path to approve inclusion of the file.

    NOTE: for globbing pattern that is easy to use, consider usage of
    fnmatch.translate() function: https://docs.python.org/3/library/fnmatch.html#fnmatch.translate
    """
    get_ = lambda mtd, p: getattr(be, mtd)(p) if hasattr(be, mtd) else None
    entries = {}
    files, dirs = be.ls(path)
    for f in files:
        fp = os.path.join(path, f)
        if '*' != pattern and not re.match(pattern, fp):
            continue
        entries[f] = { k: get_(p, fp) for (k, p) in [
            ('size',    'file_size'),
            ('adler32', 'adler32' ),
            ('mtime', 'get_mtime' ),
            ('owner', 'get_owner' ),
            ('permissions', 'get_permissions'),
            # ...
            ] if k not in filePropertiesExclude }
    for d in dirs:
        dp = os.path.join(path, d)
        ec = { k: get_(p, dp) for (k, p) in [
            ('mtime', 'get_mtime' ),
            ('owner', 'get_owner' ),
            ('permissions', 'get_permissions'),
            # ...
            ] if k not in dirPropertiesExclude }
        de = be.ls_detailed( dp
                           , recursive=True
                           , filePropertiesExclude=filePropertiesExclude
                           , dirPropertiesExclude=dirPropertiesExclude
                           , omitEmptyDirs=omitEmptyDirs
                           , pattern=pattern ) if recursive else None
        if de is None \
        or ( not de \
             and omitEmptyDirs ):
            continue
        ec['content'] = de
        entries[d] = ec
    return entries

class DetailedList(object):
    """
    Mixin for recursive traversal. Might be included in backends implementing
    standard "interface", which does not provide detailed retrieval of the
    filesystem attributes at once.
    """
    def ls_detailed(*args, **kwargs):
        return ls_detailed_f(*args, **kwargs)
