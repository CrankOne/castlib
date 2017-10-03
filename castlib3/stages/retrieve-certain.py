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
This stage sends uses referenced location (that has to be generally a local
directory) to retrieve certain file(s) pointed out by input arguments. Has
various parameters to be configured in order to maintain such a storage in
a poper way: locations priority, automatic clean-up, etc.
"""

import os, json, urllib2

from castlib3.stage import Stage, StageMetaclass
from castlib3.models.filesystem import Folder, File
from castlib3.logs import gLogger
from castlib3 import dbShim as DB

from sqlalchemy import exists, and_
from os import path as P
from urlparse import urlparse

class Retrieve2TempLoc( Stage ):
    __metaclass__ = StageMetaclass
    __castlib3StageParameters = {
        'name' : 'temporary-retrieve',
        'description' : """
        Will retrieve a files, by given names from given locations and store
        them in temporary location.

        Staging arguments implies:
        - list of files (or directories) to be retrieved
        """
    }

    _allowedProcedures = {
            'retrieve' : None,
            'cleanup' : None,
            'update-expiration' : None,
        }

    def __init__(self, *args, **kwargs):
        super(MaintainTempLoc, self).__init__(*args, **kwargs)

    def _V_call( self
                , retrieveNames=[]
                , retrieveLocs=[]
                , validityPeriod=None
                , backends={}
                , sizeLimit='4Gb'
                , action='retrieve'
            ):
        if action not in _allowedProcedures.keys():
            raise RuntimeError( "Got unrecognized value for `action' "
                    "parameter: %s. Allowed ones "
                    "are: %s."%(action, ', '.join(_allowedProcedures.keys())) )
        # TODO ...
        raise NotImplementedError('Stage not implemented.')



