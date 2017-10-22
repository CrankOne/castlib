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
a proper way: locations priority, automatic clean-up, etc.
"""

import os, datetime

from castlib3.stage import Stage, StageMetaclass
from castlib3.models.filesystem import Folder, File, ExpiringEntry
from castlib3.logs import gLogger
from castlib3 import dbShim as DB
from castlib3.stages.select import get_location_by_path

from sqlalchemy import exists, and_
from os import path as P
from urlparse import urlparse

class Retrieve2TempLoc( Stage ):
    __metaclass__ = StageMetaclass
    __castlib3StageParameters = {
        'name' : 'retrieve',
        'description' : """
        Will retrieve a files selected by some previous stages.
        Note: if noCleanExpired is not set, will wipe out expired files.
        """
    }

    def __init__(self, *args, **kwargs):
        super(Retrieve2TempLoc, self).__init__(*args, **kwargs)

    def _V_call( self
                , nMaxEntries=0
                , results=None
                , from_=None
                , criterion='select'
                , sourceLocations=[]
                , validityPeriod=None
                , backends={}
                , locations={}
                , sizeLimit='4Gb'
                , destination=None
                , reporter=None
                , noCleanExpired=False
            ):
        if destination is None:
            raise RuntimeError( "Destinateion location (`destination' arg) "
                    "is not set. Unable to retrieve." )
        if ':/' not in destination:
            # The destination is probably reference to defined locations:
            destination = locations[destination]['URI']
        destinationLoc, scheme = self.resolve_dest_location( destination )
        backend = backends[ destLPP.scheme ]
        # Clean-up expired entries if need:
        if not noCleanExpired:
            self.clear_expired_at( destinationLoc, backend )
        # Retrieve selected files:
        for entry in results[from_]:
            self.retrieve_file( entry, destinationLoc )

    def resolve_dest_location(self, destination):
        destLPP = urlparse( destination )
        destinationLoc = get_location_by_path( destination )
        return destinationLoc, destLPP.scheme or 'file'

    def clear_expired_at(self, destinationLoc, backend ):
        q = DB.session.query( ExpiringEntry ).filter_by( parent=destinationLoc )
        for expiredE in q.filter_by( expiration > datetime.datetime.now() ):
            fileURI = expiredE.get_uri()
            gLogger.info('Deleting expired file "%s".'%(
                    fileURI ))  # ... (staled %s).
            backend.del_file( filePath )
            DB.session.delete( expiredE )

    def retrieve_file( self, originalEntry, destinationLoc
                     , backends={}
                     , destBackend=None ):
        origURI = originalEntry.get_uri()
        origLPP = urlparse( origURI )
        origBackend = backends[origLPP.scheme]
        if destBackend is None:
            destBackend = backends[destinationLoc.scheme or 'file']
        raise NotImplementedError('TODO: copying between back-ends.')

