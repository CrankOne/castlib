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

"""
Various auxilliary subroutines ususally used by executable code.
"""

import os, dpath
from castlib3.logs import gLogger
from urlparse import urlparse

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from castlib3 import dbShim as DB
from castlib3.models import DeclBase
from castlib3.backend import gCastlibBackends
from castlib3.stage import Stages

from collections import OrderedDict

from sqlalchemy.engine import Engine
from sqlalchemy import event

try:
    from sVresources import yaml
except ImportError:
    import yaml

def initialize_database( engineCreateArgs,
                         engineCreateKWargs={},
                         autocommit=False,
                         autoflush=False):
    """
    Database initialization routine that shall be called prior to any database
    operation. Will configure CastLib package-wide database instance using
    given configuration. args/kwargs pair will be directly forwarded to 
    sessionmaker ctr. Other arguments in function signature is reserved for
    future use.
    """
    engine = create_engine(*engineCreateArgs, **engineCreateKWargs)
    dbS = scoped_session(sessionmaker(
                        bind=engine,
                        autocommit=autocommit,
                        autoflush=autoflush))
    DB.set_engine(engine)
    DB.set_session(dbS)
    DeclBase.metadata.create_all(engine)


def initialize_backends( schemes, cfg ):
    backends = {}
    for scheme in schemes:
        if scheme in cfg.keys():
            if cfg[scheme]:
                backends[scheme] = gCastlibBackends[scheme](**cfg[scheme])
            else:
                backends[scheme] = gCastlibBackends[scheme]()
    return backends


def ordered_load(stream, Loader=yaml.Loader, object_pairs_hook=OrderedDict):
    """
    YMAL loader function preserving order of dictionary entries. Usually used
    for task configs parsing (stages have to follow the order).
    """
    class OrderedLoader(Loader):
        pass
    def construct_mapping(loader, node):
        loader.flatten_mapping(node)
        return object_pairs_hook(loader.construct_pairs(node))
    OrderedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        construct_mapping)
    return yaml.load(stream, OrderedLoader)


def discover_locations(locs):
    """
    Takes either the path to YAML config describing locations, or just path
    to local directory. Returns directories dict suitable for further use
    within castlib3.filesystem.discover_entries().
    """
    directories = {}
    if type(locs) is not str:
        raise TypeError( 'Path argument referring to YAML file expected.' )
    lpp = urlparse(locs)
    if 'file' == lpp.scheme or '' == lpp.scheme:
        if os.path.isdir( locs ):
            directories[os.path.basename(locs)] = {
                    'localPath' : 'file:/' + os.path.abspath(locs),
                }
        elif os.path.isfile( locs ):
            with open(locs) as f:
                directories = yaml.load(f)
        else:
            raise RuntimeError('Argument \"%s\" is neither a file nor a directory " \
                "path.'%locs )
    else:
        raise NotImplementedError('Other than "file://" scheme are not '
                'supported yet.')
    return directories


def process_stages( stages
                  , directories=None
                  , noCommit=False
                  , backends={}
                  , reporter=None ):
    """
    Generic processing loop performing the processing using the given pipeline.
    XXX: deprecated
    """
    externalModules = stages.get('external-import', None)
    if externalModules:
        gLogger.info('Loading %d external import...'%len(externalModules))
        ms = map(__import__, externalModules)
    gLogger.debug('Initializeing stages pipeline...')
    stages = Stages( stages['stages'] )
    if directories:
        # Process directories:
        for directory in directories:
            gLogger.info("On \"%s\" -> \"%s\" dir:"%(
                directory['folder'], directory['folder'] ))
            stages( directory=directory, noCommit=noCommit, backends=backends, reporter=reporter )
    else:
        # If no directories given, run the pipeline once without the directory
        # parameter
        stages( noCommit=noCommit, backends=backends, reporter=reporter )
    return True  # TODO: used by external loop in listening mode

class TaskRegistry(object):
    """
    This object stores the index of pre-defined stages located in directories.
    Stores the known stages as a dictionary in following form:
        <task-label> : (<YAML-file-path>, <description>)
    """
    def __init__(self
                , paths=[] ):
        self.paths = paths
        self._knownTasks = {}
        self._discover_tasks( self.paths )

    def _discover_tasks(self, dPaths ):
        """
        Performs recursive lookup for available tasks, renewing the related
        information.
        """
        nStagesDiscovered = 0
        for path in dPaths:
            if not path:
                continue
            subdirs = []
            if not os.path.exists( path ) \
            or not os.path.isdir(path):
                gLogger.info( 'Omitting non-existing directory path "%s".'%path )
                continue
            for e in os.listdir(path):
                ePath = os.path.join(path, e)
                if os.path.isfile(ePath) and ( ePath.endswith('.yml') \
                                            or ePath.endswith('.yaml') ):
                    # Probably a stage file to be tracked
                    if self._cache_task_info(ePath):
                        nStagesDiscovered += 1
                elif os.path.isdir( ePath ):
                    nStagesDiscovered += self._discover_tasks( ePath )
        return nStagesDiscovered

    def _cache_task_info(self, path):
        """
        Will retrieve some info from YAML file and cache its path if it has the
        `stages' element. Will also try to extract the comment from the
        stages file.
        """
        with open(path) as f:
            taskContent = ordered_load(f)
        if taskContent.get('stages', None):
            taskDescription = taskContent.get('comment', '<description not available>')
            taskName = taskContent.get('label', None)
            if not taskName:
                taskName = os.path.splitext(os.path.basename(path))[0]
            taskName = self._get_unique_task_name_for( taskName )
            self._knownTasks[taskName] = ( path, taskDescription )
            return True
        return False

    def _get_unique_task_name_for( self, taskName ):
        """
        Forms unique name for pipeline according to the rules:
            - will return '<taskName>' if it was not used by any entry
            (i.e. is already unique) AND if there was no entry '<taskName>-1'.
            - will return '<taskName>-2' AND change the existing '<taskName>'
            key to '<taskName>-1' if there was one with name '<taskName>'.
            - will return '<taskName>-<n+1>' if all the names '<taskName>-<n>'
            were already taken by some entries.
        Appears to be a complication. One shall to, may be, re-think this part.
        """
        existingSingle = self._knownTasks.get( taskName, None )
        existingMultiple = self._knownTasks.get( taskName + '-1', None )
        if existingSingle is None and existingMultiple is None:
            gLogger.debug( 'Task name "%s" is unique.'%taskName )
            # Name is already unique, or corresponds to an empty object.
            return taskName
        elif existingSingle is not None and existingMultiple is None:
            self._knownTasks['%s-1'%taskName] = existingSingle
            del self._knownTasks[taskName]
            gLogger.debug( 'Task name "%s" is duplicating. '
                    'Existing renamed to "%s-1", "%s-2" is proposed.'%(
                        taskName, taskName, taskName) )
            return '%s-2'%taskName
        elif existingSingle is not None and existingMultiple is not None:
            raise RuntimeError('Bad state.')  # shall never happend
        #elif existingSingle is None and existingMultiple is not None:
        n = 1 if existingMultiple else 0
        newTaskName = '%s-%d'%(taskName, n)
        while newTaskName in self._knownTasks.keys():
            n += 1
        gLogger.debug( 'Task name "%s" is duplicating. '
                    '"%s" is proposed.'%( taskName, newTaskName ) )
        return newTaskName

    @property
    def predefinedTasks( self ):
        return self._knownTasks

    def reload_tasks( self ):
        self._knownTasks = {}
        self._discover_tasks()

    def __getitem__(self, key):
        return self.get_task(key)

    def get_task(self, key):
        fPath = self._knownTasks[key][0]
        with open(fPath) as f:
            task = ordered_load(f)
        return key
        #for oe in override:
        #    k, v = oe
        #    for dpath.set( task, k, v )
        #return Stages( task['stages'] )

def execute_task( taskDescr
                , omitStages=[]
                , onlyStages=[]
                , reporter=None
                , backends={}
                , locations={} ):
    if omitStages and onlyStages:
        raise RuntimeError( 'Both, omitStages and onlyStages arguments are '
                'given.' )
    # Omit stages:
    for k in omitStages:
        if k in taskDescr['stages'].keys():
            del taskDescr['stages'][k]
        else:
            gLogger.warning( 'No stages "%s" defined by task (can not be '
                'omitted).'%k )
    # Only stages:
    onlyStages = OrderedDict()  # TODO: XXX
    for k in onlyStages:
        if k in taskDescr['stages'].keys():
            onlyStages[k] = taskDescr['stages'][k]
        else:
            gLogger.warning( 'No stages "%s" defined by task (can not be '
                'included as "only" argument).'%k )
    if onlyStages:
        taskDescr['stages'] = onlyStages
    # Process:
    stages = Stages( taskDescr['stages'] )
    ret = stages( noCommit=taskDescr.get('no-commit', False)
                 , backends=backends
                 , reporter=reporter
                 , locations=locations )
    del stages
    return ret


def configure_cstl3( co
                , disabledBackends=[]
                , disabledLocations=[]
             ):
    backends = initialize_backends(
            filter( lambda e: e not in disabledBackends, co['backends'].keys() ),
            co['backends'] )
    #
    # Explicitly initialize database, if database config was provided within
    # config object:
    initialize_database( co['database'].pop('\\args'),
                         engineCreateKWargs=co['database'] )
    #
    # Disable locations if needed.
    for loc in disabledLocations:
        if loc in co['locations'].keys():
            del co['locations'][loc]

    return co, backends, TaskRegistry( paths=co['tasks-locations'] )

