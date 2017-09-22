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

import os
from uuid import uuid4
from castlib2.shell import invoke_util 
from castlib2.parsing import rxStagerQueryError, rxStagerSubrequesFailure
from castlib2.logs import gLogger
from castlib2.chunk import SynchronizableFile
import collections

"""
Implementation of CASTOR querying routines.
"""

class CastorQuery(object):
    """
    Base class representing the CASTOR query. Hides out the invokation of
    particular shell utilities managing the particular request.
    """

    # List of all queries created in current session.
    _queriesIndex = {}

    @staticmethod
    def new_tag( cls, self ):
        while True:
            newTag = uuid4()
            if newTag in cls._queriesIndex.keys():
                continue
            rc, s, e = invoke_util('stager/query',
                                    usertag=newTag,
                                    noexcept=True,
                                    applyRegexOn=None )
            # yes, stager_qry outputs errors to stdout and rc is set to 1,
            # not to error code:
            m = rxStagerQueryError.match( s )
            if 22 != int(m.group('errorCode')):
                gLogger.debug( 'stager/query returned unexpected result: '
                    'rc=%d, stdout=%s, stderr=%s.'%(rc, s, e) )
            if rc != 1:
                continue
            break
        cls._queriesIndex[newTag] = self
        return newTag

    @staticmethod
    def del_tag( cls, tag ):
        cls._queriesIndex.pop( tag )


    def __init__( self, queryType, usertag=None ):
        self._type = queryType
        self._rqID = None
        if usertag is None:
            self._usertag = type(self).new_tag(type(self), self)
            gLogger.info('New CASTOR query %s created.'%self._usertag )
        self._state = None
        self._filelist = None

    @property
    def state(self):
        """
        Query state or None if query has no usertag.
        """
        if not self.usertag:
            return None
        res = invoke_util( 'stager/query', usertag=self.usertag )
        return res['status']  # TODO: map!

    @property
    def rqID(self):
        """
        HSM request ID or None.
        """
        return self._rqID

    @property
    def usertag(self):
        """
        usertag or None.
        """
        return self._usertag

    @property
    def query_type(self):
        return self._type

    def __del__(self):
        # No analogue of stager_putdone.
        # Just remove itself from index:
        if hasattr(self, '_usertag') and self._usertag:
            self.del_tag( self._usertag )
            self._usertag = None



class CastorPutQuery(CastorQuery):
    """
    Class representing putting query.
    Wraps a `stager_put` util with combination.
    """
    def __init__(self, hsmFiles):
        self.results = {}  # < stores upload results
        # Initializes internals: unique usertag, filelist, etc.
        super(self.__class__, self).__init__( 'PUT' )
        rc, s, e = invoke_util( 'stager/put',
                                usertag=self.usertag,
                                hsmFiles=hsmFiles,
                                popenDry=False,
                                noexcept=True,
                                applyRegexOn=None )
        rqUUID = None
        self.stageOutFiles = []
        self.existing = []
        if 0 != rc:
            # Now we have one or more files with error:
            #   /castor/cern.ch/p348/cdr01008-002355.dat SUBREQUEST_FAILED 16 \
            #                     Another prepareToPut is ongoing for this file
            #   ...
            #   Stager request ID: edc34ca5-f49b-4fdb-bb7e-1cf088908b14
            # that probably means that stager expects file to be uploaded either by previously
            # malfunctional session or other software claiming `stager_put`
            # query. We need now extract the "Stager request ID" and refer to
            # it within this query.
            # For statuscodes see:
            #   https://gitlab.cern.ch/castor/CASTOR/blob/5755b618e278803e6091adb73d89428b2561b38d/castor/stager/SubRequestStatusCodes.hpp
            gLogger.debug( "stager/put rc=%d,\nstderr=%s,\nstdout=%s\n"%(rc, e, s) )
        matchFailed = False
        while not matchFailed:
            m = rxStagerSubrequesFailure.match(s)
            if not m:
                matchFailed = True
                continue
            for mit in rxStagerSubrequesFailure.finditer(s):
                if mit.group('StagerRequestUUID'):
                    if not rqUUID:
                        rqUUID = mit.group('StagerRequestUUID')
                    else:
                        matchFailed = True
                        continue
                elif mit.group('subreqStatPart'):
                    if 'FAILED' == mit.group('subreqStatPart') \
                    and mit.group('errorCode') \
                    and '16' == mit.group('errorCode'):
                        self.stageOutFiles.append(mit.group('filename'))
                        if not mit.group('filename') in self.existing:
                            self.existing.append(mit.group('filename'))
                    elif 'READY' == mit.group('subreqStatPart'):
                        self.stageOutFiles.append(mit.group('filename'))
                    else:
                        matchFailed = True
                        continue
            break
        if matchFailed:
            gLogger.error( 'stager/put results: rc={returnCode},\n'
                'stderr={stderr},\nstdout={stdout}'.format( returnCode=rc,
                    stderr=e, stdout=s))
            raise SystemError( 'Unable to interpret las stager/put '
                'invokation.' )
        self._rqID = rqUUID
        if not len(self.stageOutFiles):
            raise AssertionError('No files to stageout.')

    def put_one( self, fileInstance, overrideHSMDest=True, communicate=False ):
        """
        Perfroms an upload.
        """
        if isinstance( fileInstance, SynchronizableFile ):
            # A SynchronizableFile instance provided to put. If overrideHSMDest
            # is true the hsm_dir property of it will be overriden by current
            # one based on filename match. Otherwise its hsm_dir prop must be
            # set and match to one of the self.stageOutFiles.
            if not fileInstance.available:
                raise SystemError( 'File %s is not '
                    'available.'%fileInstance.filename )
            hsmDestLocation = None
            if overrideHSMDest:
                filename = fileInstance.filename
                for candidate in self.stageOutFiles:
                    if filename == os.path.basename( candidate ):
                        # check for collision. In this case multiple files with
                        # same basename are pushed into put queue. To avoid it,
                        # external code must explicitly specify desired dir_hsm
                        # for files.
                        if not hsmDestLocation is None:
                            raise KeyError('Filename \"%s\" has multiple '
                            'matches.'%filename )
                        hsmDestLocation = candidate
                if not hsmDestLocation:
                    raise SystemError( 'Empty destination for file '
                                                '%s.'%fileInstance.filename )
                fileInstance.hsm_dir = hsmDestLocation
            else:
                destLocation = os.path.join( fileInstance.dir_hsm,
                                             fileInstance.filename )
                if not destLocation in self.stageOutFiles:
                    raise KeyError( 'File %s is not related to this '
                                            'query.'%fileInstance.filename )
                hsmDestLocation = destLocation
            srcLocation = None
            # Get first availablel ocation:
            for loc in fileInstance.get_possible_locations():
                if loc:
                    srcLocation = loc
                    break
            if hsmDestLocation in self.existing:
                util = 'stager/re-upload'
            else:
                util = 'stager/re-upload'  # TODO: 'stager/upload'
            self.results[fileInstance.filename] = invoke_util(
                                util,
                                timeout='long',
                                srcLocation=srcLocation,
                                remLocation=hsmDestLocation,
                                communicate=communicate )
        else:
            raise NotImplementedError('Currently, only instances of '
                'SynchronizableFile class are supported.')

    def put( self, toPut, overrideHSMDest=True, communicate=False ):
        """
        Forwards collection or single entity to put_one().
        """
        if isinstance( toPut, collections.Iterable ):
            for one in toPut:
                self.put_one( one, overrideHSMDest=overrideHSMDest )
        else:
            self.put_one( toPut, overrideHSMDest=overrideHSMDest )


    def __del__(self):
        super(self.__class__, self).__del__()
        pass



class CastorGetQuery(CastorQuery):
    """
    Class representing CASTOR getting query.
    Wraps a `stager_get` util.
    """
    def __init__(self):
        super(self.__class__, self).__init__( 'GET' )
        res = invoke_util( 'stager/get',
                           usertag=self.usertag,
                           hsmFiles=hsmFiles )
        self._rqID = res[0]['requestID']

    def __del__(self):
        # No analogue of stager_putdone.
        # Just remove itself from index:
        super(self.__class__, self).__del__()


class CastorUpdateQuery(CastorQuery):
    """
    Class representing CASTOR updating query.
    Wraps a `stager_get` util.
    """
    def __init__(self):
        super(self.__class__, self).__init__( 'UPDATE' )

    def __del__(self):
        super(self.__class__, self).__del__()

