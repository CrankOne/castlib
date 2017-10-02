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

import os

from castlib3.stage import Stage, StageMetaclass
from castlib3.models.filesystem import Folder, File, RemoteFolder, StoragingNode, FSEntry
from castlib3.logs import gLogger
from castlib3.backend import LocalBackend
from castlib3 import dbShim as DB

from sqlalchemy import exists, and_, not_
from sqlalchemy.orm.exc import MultipleResultsFound
from os import path as P
from urlparse import urlparse

from castlib3.rpc import ReportingBar

class Stats(object):
    """
    Aux class performing gathering of creation/updating statistics within the
    stage. Represents a dictionary indexed by py class instances.
    """
    def __init__(self):
        self.stats = {}

    def get_stats_for(self, cls):
        if cls in self.stats.keys():
            return self.stats[cls]
        self.stats[cls] = {'created' : 0, 'updated' : 0, 'deleted' : 0}
        return self.stats[cls]

    def upd_inc(self, cls):
        self.get_stats_for(cls)['updated'] += 1

    def crd_inc(self, cls):
        self.get_stats_for(cls)['created'] += 1

    def dlt_inc(self, cls):
        self.get_stats_for(cls)['deleted'] += 1

    def __str__(self):
        ret = []
        for k, v in self.stats.iteritems():
            s = []
            if v['created']:
                s.append('%d created'%v['created'])
            if v['updated']:
                s.append('%d updated'%v['updated'])
            if v['deleted']:
                s.append('%d deleted'%v['deleted'])
            ret.append('"%s" instances: %s'%(k.__name__, ', '.join(s)))
        ret = '; '.join(ret)
        if not ret or ret.isspace():
            ret = 'no changes'
        return ret


def index_directory( dirEntry, backend
                    , parent=None
                    , syncFields=[]
                    , report=None
                    , maxNewFiles=0
                    , reporter=None ):
    """
    This recursive function performs synchronization fielsystem entries
    (files and directories) against database entries.

    The function is usually called within the :class:`IndexDirectory` stage
    performing recursive traversal along the filesystem-like structure.

    TODO: currently, does not support deletion of directories!

    :param dirEntry:
        is the dictionary of special structure (usually returned by
        :ref:`discover_entries`) describing the local or remote directory
        to be considered.
    :param backend:
        the :class:`Backend` subclass implementing particular data access
        methods.
    :param parent:
        is the parent node, if exists.
    :param syncFields:
        a list contining names of file attributes to be updated/set on
        creation.
    :parame maxNewFiles:
        if set to non-zero value, only `maxNewFiles` new files may be added.
        When this limit will be reached, the no updating procedures will take
        place and function returns.
    """
    print('XXX', reporter)  # XXX
    # -------------------------------------------------------------------------
    # Querying the parent structures
    report = report or Stats()
    # Look-up for these files in database:
    localPath = dirEntry['folder']
    dirName = P.split( dirEntry['folder'] )[1] or '/'
    if not dirName:
        raise RuntimeError( 'Empty dir name for local path "%s".'%localPath )
    folderEntry = None
    node = dirEntry.get('node', None)
    folderUpdated = False
    folderCreated = False
    if node is None:
        # We're indexing local or not a first-level "folder" relatively to
        # node. Use ordinary folder entry. We have to exclude the remote ones:
        gLogger.debug( 'Looking for local folder entry %s'%dirName )
        folderEntryQ = DB.session.query(Folder).filter(
                      Folder.name == dirName
                    , Folder.parent == parent
                    , FSEntry.type == 'd' )
        try:
            folderEntry = folderEntryQ.one_or_none()
        except MultipleResultsFound as e:
            gLogger.warning('Details:')
            for e in folderEntryQ.all():
                fullp = [ p.name for p in e.mp.query_ancestors().all()]
                fullp.append( e.name )
                print( ' - %s (id=%d)'%(os.path.join(*fullp), e.id) )
            raise
        if not folderEntry:
            gLogger.info( "Folder `%s' (%s) was not cached yet..."%(dirName, localPath) )
            folderEntry = backend.new_folder( localPath
                                            , name=dirName
                                            , parent=parent )
            folderCreated = True
            DB.session.add( folderEntry )
        else:
            gLogger.info( 'Local %s folder entry found.'%dirName )
    else:
        # We're indexing a first-level "folder" relatively to node. Use
        # remote folder entry.
        gLogger.debug( "The `node' parameter is given for directory %s (%s). "
                "Considering it as a remote location."%( dirName, localPath ) )
        node, nodeCreated = DB.get_or_create( StoragingNode
                    , identifier=dirEntry['node']
                    , scheme=backend.scheme )
        if nodeCreated:
            report.crd_inc(StoragingNode)
        folderEntry, folderCreated = DB.get_or_create( RemoteFolder
                    , node=node
                    , name=dirName
                    , path=localPath
                    , parent=parent )
        if nodeCreated or folderCreated:
            DB.session.add(node)
            DB.session.flush()
    # -------------------------------------------------------------------------
    # Verification of existing caches
    if len(folderEntry.children):
        bar = None
        if len(folderEntry.children) > 10:
            bar = ReportingBar( '  veryfying cached files in %s'%dirEntry['folder']
                              , max=len(folderEntry.children)
                              , suffix='%(index)d/%(max)d, %(prec_hr_time)s remains'
                              , reporter=reporter )
        for cEntry in folderEntry.children.values():
            assert(issubclass(type(cEntry), FSEntry))
            if not type(cEntry) is File:
                continue
            if not cEntry.name in dirEntry['files']:
                DB.session.delete( cEntry )
                report.dlt_inc( File )
                continue
            upd = {}
            filePath = P.join( localPath, cEntry.name )
            for fName in syncFields:
                upd[fName] = getattr(backend, 'get_' + fName)(filePath)
            fileUpdated = cEntry.update_fields(**upd)
            if fileUpdated:
                DB.session.add(cEntry)
                report.upd_inc(File)
                folderUpdated = True
            dirEntry['files'].remove(cEntry.name)
            if bar:
                bar.next()
        if bar:
            bar.finish()
    else:
        gLogger.info( "Contents of folder `%s' (%s) was not cached before."%(dirName, localPath) )
    # -------------------------------------------------------------------------
    # Introducing new file entries
    bar = None
    if len((dirEntry['files'])) > 10:
        bar = ReportingBar( '  listing new entries at %s'%dirEntry['folder']
                          , max=( len((dirEntry['files'])) if 0 == maxNewFiles else maxNewFiles )
                          , suffix='%(index)d/%(max)d, %(prec_hr_time)s remains'
                          , reporter=reporter )
    for filename in dirEntry['files']:
        filePath = P.join( localPath, filename )
        fileEntry = backend.new_file( filePath
                                    , name=filename
                                    , syncFields=syncFields
                                    , parent=folderEntry )
        folderUpdated = True
        report.crd_inc(File)
        if bar:
            bar.next()
        if maxNewFiles > 0 \
            and report.get_stats_for(File)['created'] >= maxNewFiles:
            break  # maxNewFiles limit exceeded
    if bar:
        bar.finish()
    # -------------------------------------------------------------------------
    # Recursive invokation on sub-folders
    if folderUpdated:
        report.upd_inc(type(folderEntry))
    elif folderCreated:
        report.crd_inc(type(folderEntry))
    if folderUpdated or folderCreated:
        DB.session.add( folderEntry )
        gLogger.info('Flushing caches...')
        DB.session.flush()
    for subDir in dirEntry['subFolders']:
        # TODO: subdirs deletion
        index_directory( subDir
                       , backend
                       , parent=folderEntry
                       , syncFields=syncFields
                       , report=report
                       , maxNewFiles=maxNewFiles
                       , reporter=reporter )
    return folderEntry, report


class IndexDirectory( Stage ):
    __metaclass__ = StageMetaclass
    __castlib3StageParameters = {
        'name' : 'index-dir',
        'description' : """
        Will perform recursive indexing of given local directory content. For
        each file or directory met, the appropriate filesystem database entry
        will be created or updated according to local copy.
        """
    }

    def __init__(self, *args, **kwargs):
        super(IndexDirectory, self).__init__(*args, **kwargs)

    # nstages=1, stageNum=1, results=[]
    def _V_call(self
                , directory=None
                , backends={}
                , syncFields=[]
                , maxNewFiles=0
                , noCommit=False
                , reporter=None ):
        if directory is None:
            raise RuntimeError( 'Keyword argument directory= is mandatory'
                    'for this stage.' )
        backend = None
        pdURI = urlparse( directory['folder'] )
        if '' == pdURI.scheme or 'file' == pdURI.scheme:
            backend = backends['file']
        elif 'castor' == pdURI.scheme:
            backend = backends['castor']
        else:
            raise RuntimeError( "Can not determine backend for URI: \"%s\" (scheme \"%s\")"%(
                        directory['folder'][0], pdURI.scheme) )
        gLogger.info( 'Performing cache syncing procedure for '
                'attributes: %s.'%(', '.join(syncFields)) )
        if maxNewFiles > 0:
            gLogger.warning('The "maxNewFiles" limit is set! Only %d new '
                    'files may be introduced during the '
                    'single stage evaluation.'%maxNewFiles )
        fe, rep = index_directory( directory, backend, parent=None,
                                            syncFields=syncFields,
                                            maxNewFiles=maxNewFiles,
                                            reporter=reporter )
        gLogger.info( 'Sync stage stats: %s.'%( rep ) )

