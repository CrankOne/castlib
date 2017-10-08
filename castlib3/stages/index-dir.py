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
from castlib3.filesystem import discover_entries
from castlib3.models.filesystem import Folder, File, RemoteFolder, StoragingNode, FSEntry
from castlib3.logs import gLogger
from castlib3.backend import LocalBackend
from castlib3 import dbShim as DB

from sqlalchemy import exists, and_, not_
from sqlalchemy.orm.exc import MultipleResultsFound
from os import path as P
from urlparse import urlparse

from castlib3.rpc.simple import ReportingBar

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


def index_directory( dirEntry
                    , backend
                    , parent=None
                    , syncFields=[]
                    , report=None
                    , maxNewFiles=0
                    , reporter=None
                    , dirModified=None ):
    """
    This recursive function performs synchronization fielsystem entries
    (files and directories) against database entries.

    The function is usually called within the :class:`IndexDirectory` stage
    performing recursive traversal along the filesystem-like structure.

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
            fCreateKWargs = {
                        'name' : dirName,
                        'parent' : parent
                    }
            if dirModified is not None:
                fCreateKWargs['modified'] = dirModified
            folderEntry = backend.new_folder( localPath, **fCreateKWargs )
            folderCreated = True
            DB.session.add( folderEntry )
        else:
            gLogger.info( 'Local %s folder entry found.'%dirName )
            # TODO: check `modified' timestamp/datetime object
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
        fCreateKWargs = {
                    'name' : dirName,
                    'parent' : parent,
                    'node' : node,
                    'path' : localPath
                }
        if dirModified is not None:
            fCreateKWargs['modified'] = dirModified
        folderEntry, folderCreated = DB.get_or_create( RemoteFolder, **fCreateKWargs )
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
                doRetrieve = False
                if type(dirEntry['files']) is dict:
                    if fName in dirEntry['files'][cEntry.name].keys():
                        upd[fName] = dirEntry['files'][cEntry.name][fName]
                    else:
                        doRetrieve = True
                elif type(dirEntry['files']) is set:
                    doRetrieve = True
                else:
                    raise TypeError('Unexpected type of "files" entry: %s'%(type(dirEntry['files'])))
                if doRetrieve:
                    upd[fName] = getattr(backend, 'get_' + fName)(filePath)
            fileUpdated = cEntry.update_fields(**upd)
            if fileUpdated:
                DB.session.add(cEntry)
                report.upd_inc(File)
                folderUpdated = True
            if type(dirEntry['files']) is set:
                dirEntry['files'].remove(cEntry.name)
            else:
                del dirEntry['files'][cEntry.name]
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
    if type(dirEntry['files']) is set:
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
    elif type(dirEntry['files']) is dict:
        for filename, knownAttrs in dirEntry['files'].iteritems():
            filePath = P.join( localPath, filename )
            # NOTE: to avoid redundant queries, back-ends have to automatically
            # use the knownAttrs when they're given as keyword arguments.
            fileEntry = backend.new_file( filePath
                                        , name=filename
                                        , syncFields=syncFields
                                        , parent=folderEntry
                                        , **knownAttrs )
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
    # Subdirs deletion:
    if not folderCreated:
        for subdE in DB.session.query(Folder).filter( Folder.name == dirName
                                                    , Folder.parent == folderEntry ).all():
            if subdE.name not in dirEntry['subFolders']:
                DB.session.delete(subdE)
                report.dlt_inc( Folder )
    # Subdirs indexing:
    for subDir in dirEntry['subFolders']:
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
        Will perform recursive indexing of given directory(-ies) content. For
        each file or directory met, the appropriate filesystem database entry
        will be created or updated according to local copy.

        The targets argument may be either a string, a dictionary of special
        struct, or a list of string or dictionaries. In the former case, all
        the elements of the list will be treated in order. The string target
        has to refer to one of the entries given in `locations' dict.
        """
    }

    def __init__(self, *args, **kwargs):
        super(IndexDirectory, self).__init__(*args, **kwargs)

    # nstages=1, stageNum=1, results=[]
    def _V_call(self
                , targets=[]
                , locations={}
                , backends={}
                , syncFields=[]
                , maxNewFiles=0
                , noCommit=False
                , reporter=None ):
        if type(targets) is str \
        or type(targets) is dict:
            targets = [targets]
        for t in targets:
            if type( t ) is str:
                gLogger.info( 'Retrieving directory structure from aliased target "%s"...'%t )
                t = locations[t]
            else:
                gLogger.info( 'Retrieving directory structure from direct target "%s"...'%t['URI'] )
            lists = discover_entries( t, backends=backends )
            backend = None
            pdURI = urlparse( lists['folder'] )
            try:
                backend = backends[pdURI.scheme or 'file']
            except KeyError:
                raise RuntimeError( 'Target "%s" requires backend "%s" that is '
                        'unknown or disabled.'%( t['URI'], pdURI.scheme or 'file' ) )
            self._index_target( lists
                              , backend
                              , syncFields=syncFields
                              , maxNewFiles=maxNewFiles
                              , noCommit=noCommit
                              , reporter=reporter )

    def _index_target(self
                , lists
                , backend
                , syncFields=[]
                , maxNewFiles=0
                , noCommit=False
                , reporter=None ):
        gLogger.info( 'Synchronizing cache for attributes: %s.'%(', '.join(syncFields)) )
        if maxNewFiles > 0:
            gLogger.warning('The "maxNewFiles" limit is set! Only %d new '
                    'files may be introduced during the '
                    'single stage evaluation.'%maxNewFiles )
        fe, rep = index_directory( lists
                                 , backend
                                 , parent=None
                                 , syncFields=syncFields
                                 , maxNewFiles=maxNewFiles
                                 , reporter=reporter )
        gLogger.info( 'Cache updating statistics: %s.'%( rep ) )

