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
Main synchronization stage implementation module.
"""

import os, json

from castlib3.stage import Stage, StageMetaclass
from castlib3.logs import gLogger
from castlib3 import dbShim as DB
from urlparse import urlparse, urlunparse

from castlib3.rpc.simple import ReportingBar

class Sync( Stage ):
    __metaclass__ = StageMetaclass
    __castlib3StageParameters = {
        'name' : 'sync',
        'description' : """
        Stage performing various synchronization routines against the selected
        mismatches. Requires that one or more Select stages was (were)
        performed in pipeline prior to it to choose the particular mismatching
        entries.
        """
    }

    def __init__(self, *args, **kwargs):
        super(Sync, self).__init__(*args, **kwargs)

    def _V_call( self
                , backends={}
                , from_=None
                , nMaxEntries=0
                , mismatches=[]
                , results=None
                , commitEvery=0
                , truncateSeconds=False
                , extractChecksumOnUpload=True
                , validateChecksum=True
                , allowNULL=False
                , reporter=None ):
        if not results:
            raise RuntimeError('The Select stage instance has to find out the '
                    'mismatches prior to syncing stage.' )
        if not from_:
            raise RuntimeError('"from_" keyword argument is required for '
                    'sync stage to reference particular mismatch id.')
        if not mismatches or not len(mismatches):
            mismatches = results[from_].keys()
        for n, mismatchesName in enumerate(mismatches):
            n += 1
            if not hasattr(self, 'sync_' + mismatchesName):
                gLogger.warning( "Don't know how to fix `%s' mismatches. "
                        "Skipping %d/%d."%(mismatchesName, n, len(mismatches)) )
                continue
            if mismatchesName not in results[from_].keys():
                gLogger.warning( "The `%s' stage didn't locate any `%s' mismatches. "
                        "Skipping %d/%d."%(from_, mismatchesName, n, len(mismatches)) )
                continue
            if 0 == results[from_][mismatchesName][0].count():
                gLogger.info( "Skipping `\033[1m%s\033[0m' of `\033[1m%s\033[0m' "
                    "selection as all the entries matches. %d/%d"%(
                    mismatchesName, from_, n, len(mismatches)) )
                continue
            gLogger.info( "Syncing the `\033[1m%s\033[0m' of `\033[1m%s\033[0m' "
                    "query result %d/%d"%(
                mismatchesName, from_, n, len(mismatches)) )
            getattr(self, 'sync_' + mismatchesName)( *results[from_][mismatchesName]
                                                   , backends=backends
                                                   , nMaxEntries=nMaxEntries
                                                   , truncateSeconds=truncateSeconds
                                                   , extractChecksumOnUpload=extractChecksumOnUpload
                                                   , commitEvery=commitEvery
                                                   , allowNULL=allowNULL
                                                   , validateChecksum=validateChecksum
                                                   , reporter=reporter )

    def sync_modified( self
                     , mismatchQuery, refLoc, dstLoc
                     , nMaxEntries=0
                     , backends={}
                     , commitEvery=0
                     , truncateSeconds=False
                     , extractChecksumOnUpload=True
                     , validateChecksum=True
                     , allowNULL=False
                     , reporter=None ):
        n, nMax = 0, nMaxEntries or mismatchQuery.count()
        bar = ReportingBar( 'syncing modified timestamps'
                    , max=nMax
                    , suffix='%(index)d/%(max)d, %(prec_hr_time)s remains'
                    , reporter=reporter )
        for entry in (mismatchQuery if not nMaxEntries else mismatchQuery.limit(nMaxEntries)):
            n += 1
            trgF = entry[0]
            refF = entry[1]
            if refF.modified is None and not allowNULL:
                bar.next()
                gLogger.warning('Skipping %s since reference timestamp is NULL.'%(trgURI) )
                continue
            trgURI = trgF.get_uri()
            trgLPP = urlparse(trgURI)
            gLogger.debug( 'Setting the date of %s from %r to %r according to %r.'%(
                    trgURI, trgF.modified, trgF.modified, refF.get_uri() ) )
            backend = backends[trgLPP.scheme or 'file']
            backend.set_modified( trgURI, refF.modified )
            newTs = backend.get_modified( trgURI )
            refTs = refF.modified
            if truncateSeconds:
                newTs = newTs.replace(second=0, microsecond=0)
                refTs = refTs.replace(second=0, microsecond=0)
            if newTs != refTs:
                gLogger.warning('Failed to set timestamp for %s; requested: %r, '
                        'real: %r'%(trgURI, refTs, newTs) )
            trgF.modified = newTs
            DB.session.add(trgF)
            if commitEvery and 0 == n%commitEvery:
                DB.session.commit()
            bar.next()
        bar.finish()

    def sync_size( self
                 , mismatchQuery, refLoc, dstLoc
                 , nMaxEntries=0
                 , backends={}
                 , commitEvery=0
                 , truncateSeconds=False
                 , extractChecksumOnUpload=True
                 , allowNULL=False  # unused (todo?)
                 , validateChecksum=True
                 , reporter=None ):
        #bar = ReportingBar( 're-uploading files with mismatched size'
        #            , max=mismatchQuery.count()
        #            , suffix='%(index)d/%(max)d, %(eta)ds remains'
        #            , reporter=reporter )
        n, nMax = 0, nMaxEntries or mismatchQuery.count()
        for entry in (mismatchQuery if not nMaxEntries else mismatchQuery.limit(nMaxEntries)):
            n += 1
            trgF = entry[0]
            refF = entry[1]
            trgURI, refURI = trgF.get_uri(), refF.get_uri()
            trgLPP, refLPP = urlparse(trgURI), urlparse(refURI)
            gLogger.info( 'Re-uploading \033[1m%d/%d\033[0m : %s (size=%d) on location %s (size=%s).'%(
                    n, nMax,
                    refURI, refF.size, trgURI, trgF.size ) )
            trgBackend = backends[trgLPP.scheme or 'file']
            trgBackend.rewrite_file( refURI, trgURI, backends=backends )
            # Correct timestamp:
            trgBackend.set_modified( trgURI, refF.modified )
            # Verify upload:
            newTs = trgBackend.get_modified( trgURI )
            newSize = trgBackend.get_size( trgURI )
            if newTs != refF.modified:
                if not truncateSeconds \
                    and not newTs.replace(second=0, microsecond=0) \
                        == refF.modified.replace(second=0, microsecond=0):
                    gLogger.warning('Modified timestamp verification failed. Referential: %r, '
                        'real: %r'%( refF.modified, newTs ) )
            trgF.modified = newTs
            if not newSize == refF.size:
                gLogger.warning('Size verification failed. Referential: %d, '
                        'real: %d'%( refF.size, newSize ) )
            trgF.size = newSize
            DB.session.add( trgF )
            if commitEvery and 0 == n%commitEvery:
                DB.session.commit()
            #bar.next()
        #bar.finish()

    def sync_missing( self
                    , mismatchQuery, refLoc, dstLoc
                    , backends={}
                    , nMaxEntries=0
                    , commitEvery=0
                    , truncateSeconds=False
                    , extractChecksumOnUpload=True
                    , allowNULL=False  # pointless
                    , validateChecksum=True
                    , reporter=None ):
        n, nMax = 0, nMaxEntries or mismatchQuery.count()
        if nMax > 1e6:
            gLogger.warning('Too much entries to treat. Check selection criteria.')
            return
        for entry in (mismatchQuery if not nMaxEntries else mismatchQuery.limit(nMaxEntries)):
            n += 1
            srcF = entry[0]
            assert( entry[1] is None )  # Has to be size mismatch.
            filename = srcF.name
            srcURI, dstURI = srcF.get_uri(), os.path.join(dstLoc.get_uri(), filename)
            srcLPP = urlparse(srcURI)
            dstLPP = urlparse(dstURI)
            # Upload:
            a32 = None
            if extractChecksumOnUpload:
                gLogger.info('Computing adler32 checksum for %s'%srcURI)
                a32 = backends[srcLPP.scheme or 'file'].get_adler32( srcURI )
                gLogger.info('... %s'%a32 )
                srcF.adler32 = a32
                DB.session.add( srcF )
            dstBackend = backends[dstLPP.scheme or 'file']
            dstF = dstBackend.new_file( dstURI
                                      , name=srcF.name
                                      , parent=dstLoc
                                      , size=srcF.size
                                      , adler32=a32
                                      , modified=srcF.modified )
            gLogger.info( 'Uploading \033[1m%d/%d\033[0m : %s (size=%d) to location %s (adler32=%s, size=%d).'%(
                    n, nMax,
                    srcURI, srcF.size, dstURI,
                    a32 or '<unknown>', srcF.size ) )
            dstBackend.cpy_file( srcURI, dstURI, backends=backends )
            dstBackend.set_modified( srcURI, srcF.modified )
            newTs = dstBackend.get_modified( dstURI )
            newSize = dstBackend.get_size( dstURI )
            if newTs != srcF.modified:
                if not truncateSeconds \
                    and not newTs.replace(second=0, microsecond=0) \
                        == srcF.modified.replace(second=0, microsecond=0):
                    gLogger.warning('Modified timestamp verification failed. Referential: %r, '
                        'real: %r'%( srcF.modified, newTs ) )
            srcF.modified = newTs
            if not newSize == srcF.size:
                gLogger.warning('Size verification failed. Referential: %d, '
                        'real: %d'%( srcF.size, newSize ) )
            dstF.size = newSize
            dstF.modified = newTs
            assert(dstF.name)
            assert(srcF.name)
            DB.session.add(dstF)
            DB.session.add(srcF)
            if commitEvery and 0 == n%commitEvery:
                DB.session.commit()
                gLogger.info('Interim changes committed to DB.')

