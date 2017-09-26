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

from inspect import getargspec
from collections import OrderedDict
from sqlalchemy.exc import DatabaseError

from castlib3 import dbShim as DB
from castlib3.logs import gLogger
import castlib3.logs
import castlib3.utils

"""
Module declaring stage classes and management routines.
"""

# Global variable indexing subclasses of castlib3.stage
gCastlibStages = {}

class Stage(object):
    """
    The `Stage' class here represents a single synchronization procedure.
    Stages can be further stacked up in a pipelining sequence in user scripts.
    Class claims common infrastructural properties of such a stage: name and
    parameters management.
    """
    def __init__(self, *args, **kwargs):
        self._defaultArgs = list(args)
        self._defaultKWargs = dict(kwargs)

    def commit(self):
        try:
            DB.session.commit()
        except DatabaseError:
            DB.session.rollback()

    def __call__(self, **kwargs):
        result = None
        argspec = getargspec( self._V_call )
        formedKWargs = {}
        for n, k in enumerate(argspec.args[1:]):
            if k in kwargs.keys():
                formedKWargs[k] = kwargs.get(k)
            elif k in self._defaultKWargs.keys():
                formedKWargs[k] = self._defaultKWargs.get(k)
        result = self._V_call( *self._defaultArgs, **formedKWargs )
        return result

    def _V_call(self, *args, **kwargs):
        raise NotImplementedError('Abstract method called.')



class StageMetaclass(type):
    """
    Metaclass providing automated registry of declared stage classes.
    """
    def __new__(cls, clsname, bases, dct):
        global gCastlibStages
        baseCorrect = False
        for base in bases:
            if issubclass( base, Stage ):
                baseCorrect = True
                break
        if not baseCorrect:
            raise TypeError( '%s is not a subclass of Stage.'%clsname )
        dtp = dct.pop( '_' + clsname + '__castlib3StageParameters' )
        cls.stageName = dtp.pop( 'name' )
        cls.stageDescription = dtp.pop( 'description' )
        # Now, actually declare a class
        classInstance = super( StageMetaclass, cls ) \
            .__new__( cls, clsname, bases, dct )
        # Register class:
        if cls.stageName not in gCastlibStages.keys():
            gCastlibStages[cls.stageName] = classInstance
        else:
            raise IndexError("Stage name \"%s\" is already used. "
                    "Applicant: %s, holder: %s"%( cls.stageName
                                                , classInstance.__name__
                                                , gCastlibStages[cls.stageName].__name__))
        gLogger.debug('Stage "%s" <- %s registered.'%(cls.stageName, classInstance.__name__))
        return classInstance


class Stages(list):
    def __init__( self, *args ):
        """
        Stages constructor expects either an OrderedDict instance or a list.
        """
        stages = args[0]
        if type(stages) is OrderedDict:
            def _translate( inst ):
                ret = { 'class' : inst[0] }
                if inst[1] is not None:
                    ret.update( inst[1] )
                return ret
            list.__init__(self, map( _translate, stages.items() ))
        elif type(stages) is list:
            list.__init__(self, stages)
        else:
            raise RuntimeError( 'The first argument of Stages ctr '
                    'has to be either of type list, or OrderedDict. '
                    'Got %s.'%type(stages) )

    def _discover_stage_classes(self):
        """
        Will try to find out all stage classes. Raises RuntimeError if unknown
        class name is met.
        """
        def _discover_entry( entry ):
            if entry['class'] not in gCastlibStages.keys():
                raise RuntimeError( "Stage class \"%s\" is not loaded, or "
                    "was not induced from StageMetaclass."%entry['class'] )
            return gCastlibStages[entry['class']]
        for n, entry in enumerate(self):
            if 'classInstance' not in entry.keys():
                self[n]['classInstance'] = _discover_entry( entry )

    def __call__(self, *args, **kwargs):
        self._discover_stage_classes()
        results = {}
        doCommit=not kwargs.get('noCommit', False)
        for num, stageAttibutes in enumerate(self):
            stageInstance = stageAttibutes['classInstance']( **stageAttibutes )
            gLogger.debug('Stage instance %r:%r created.'%(stageInstance, stageAttibutes['classInstance']))
            stageName = stageAttibutes.get('id', None) \
                        if type(stageAttibutes) is dict \
                        or type(stageAttibutes) is OrderedDict else None
            if stageName is None:
                n = 1
                while True:
                    stageName = '%s-%d'%(stageAttibutes['class'], n)
                    if stageName not in results.keys():
                        break
            gLogger.info( "\033[1mStage %d of %d \033[0m (%s:%s)..."%(
                    num + 1, len(self),
                    stageName, stageAttibutes['classInstance'].__name__ ) )
            gLogger.debug('Stage instance %r:%r invokation:'%(stageInstance, stageAttibutes['classInstance']))
            results[stageName] = stageInstance(
                            *args,
                            stageNum=num,
                            nstages=len(self),
                            results=results,
                            **kwargs )
            gLogger.debug('Stage instance %r:%r done.'%(stageInstance, stageAttibutes['classInstance']))
            if doCommit:
                gLogger.info( "Updating local caching database..." )
                stageInstance.commit()
                gLogger.info( "\033[1mLocal cache updated.\033[0m" )
        return results

    def __str__(self):
        return str( [stage['class'] for stage in self] )

