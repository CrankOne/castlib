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
        Will retrieve a files, by given names from given locations and store
        them in temporary location.

        Staging arguments implies:
        - list of files to be retrieved
        """
    }

    #_allowedProcedures = {
    #        'retrieve' : None,
    #        'cleanup' : None,
    #        'update-expiration' : None,
    #    }

    def __init__(self, *args, **kwargs):
        super(Retrieve2TempLoc, self).__init__(*args, **kwargs)

    def _V_call( self
                , nMaxEntries=0
                , results=None
                , from_=None
                , criterion='select'
                , retrieveNames=[]
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
        if (retrieveNames and from_) \
        or (not retrieveNames and not from_):
            raise RuntimeError( 'The "from_" and "retrieveNames" arguments are '
                    'both set or both are not set.' )
        destLPP = urlparse( destination )
        backend = backends[destLPP.scheme]
        resolvedLocs = []
        destinationLoc = get_location_by_path( destination )
        for locStr in [destinationLoc,] + sourceLocations:
            resolvedLocs.append( get_location_by_path( locStr ) )
        q = DB.session.query( ExpiringEntry ).filter_by(parent=destinationLoc)
        # Clean-up expired entries:
        if not noCleanExpired:
            for expiredE in q.filter_by( expiration > datetime.datetime.now() ):
                fileURI = expiredE.get_uri()
                gLogger.info('Deleting expired file "%s".'%(
                        fileURI ))  # ... (staled %s).
                backend.del_file( filePath )
                DB.session.delete( expiredE )
        for eStr in retrieveNames:
            # Locate entry within given variants
            fileEntry = None
            for loc in [destinationLoc,] + resolvedLocs:
                fileEntry = DB.session.query( File ) \
                        .filter_by( name=eStr, parent=loc ).one_or_none()
                if fileEntry:
                    break
            if fileEntry is None:
                gLogger.error( 'Unable to locate "%s" in locations: %s'%(
                        eStr, ', '.join(sourceLocations) ) )
                continue  # NEXT
            if fileEntry.parent is destinationLoc:
                # TODO: update expiration
                gLogger.info( 'File "%s" is already migrated to "%s". '
                        'Expiration time updated.'%( eStr, destination ) )
                continue  # NEXT
            self.retrieve_file( fileEntry )
        for selectCriterion in results[from_].keys() if from_ else []:
            for selectTuple in results[from_][criterion]:
                self.retrieve_selection( *selectTuple )
        raise NotImplementedError('Stage not implemented.')


    def retrieve_file( self, originalEntry ):
        pass

    def retrieve_selection( self, query, refLoc, dstLoc=None ):
        pass

