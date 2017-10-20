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

from __future__ import print_function

"""
This stage performs simple selection of filesystem entries by some elementary
filtering criteria.
"""

import os, datetime
from urlparse import urlparse, urlunsplit

from castlib3.stage import Stage, StageMetaclass
from castlib3.models.filesystem import Folder, File, StoragingNode, RemoteFolder
from castlib3.logs import gLogger
from castlib3.filesystem import split_path
from castlib3 import dbShim as DB

class SelectSimple( Stage ):
    __metaclass__ = StageMetaclass
    __castlib3StageParameters = {
        'name' : 'choose-simple',
        'description' : """
        Stage performing selection of files/directories by simple filtering
        criteria.
        """
    }

    def __init__(self, *args, **kwargs):
        super(SelectSimple, self).__init__(*args, **kwargs)

    def _V_call( self,
               , location=None
               , locations={} ):
        refLoc = get_location_by_path( referential )
        dstLoc = get_location_by_path( destination )
        fullPathRef = [ p.name for p in refLoc.mp.query_ancestors().all()]
        fullPathRef.append( refLoc.name )
        fullPathDst = [ p.name for p in dstLoc.mp.query_ancestors().all()]
        fullPathDst.append( dstLoc.name )
        gLogger.info( 'Reference: %s %s (id=%d), destination: %s : %s (id=%d)'%(
            referential, os.path.join(*fullPathRef), refLoc.id,
            destination, os.path.join(*fullPathDst), dstLoc.id ) )
        mmDct = {}
        for k in mismatches:
            mmDct[k] = [gComparators[k]( refLoc, dstLoc, truncateSeconds=truncateSeconds ), refLoc, dstLoc]
            gLogger.info( ' - %s : %d mismatches'%(k, mmDct[k][0].count()) )
        return mmDct


