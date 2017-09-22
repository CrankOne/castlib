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
This module contains Celery tasks that sV-resources package may invoke to perform
delayed operation. This routines are predominantly used by sV-resources server.
"""

from sVresources.utils.queueTools import CommonDelayedTask as sV_Task
from castlib3.logs import gLogger
from sVresources import apps as sVapps
from importlib import import_module
from castlib3.exec_utils import initialize_database, discover_locations

celery = sVapps.celery

class CastLibTask( sV_Task ):
    def __init__(self):
        #initialize_database(  )
        super(CastLibTask, self).__init__( gLogger )

@celery.task(bind=True, base=CastLibTask)
def castlib3_stages( self,
                        stageName,
                        stages={},
                        directories=None,
                        externalImport=[],
                        noCommit=False,
                        callbackAddress=None, task_id=None,  # XXX?
                        _cacheKey=None ):
    try:
        if externalImport:
            ms = map(import_module, externalImport)
        from castlib3 import dbShim as DB
        from castlib3.stage import Stages
        from castlib3.backend import LocalBackend
        from castlib3.castor.backend import CASTORBackend
        DB.set_session( self.db )
        backendClasses = {
                'file' : LocalBackend,
                'castor' : CASTORBackend
            }
        externalModules = stages.get('external-import', None)
        if externalModules:
            ms = map(__import__, externalModules)
        gLogger.info( 'Worker received %d stages list '
                'to perform: '%len(stages) + ', '.join(stages.keys()) + '.' )
        stages = Stages( stages )
        if directories:
            gLogger.info('Starting task on %d stages, on %d directories.'%(
                    len(stages), len(directories)) )
            # Process directories:
            for directory in directories:
                gLogger.info("On \"%s\" -> \"%s\" dir:"%(
                    directory['folder'][0], directory['folder'][1] ))
                stages( directory=directory, noCommit=noCommit, backendClasses=backendClasses )
        else:
            gLogger.info('Starting task on %d stages.'%len(stages) )
            # If no directories given, run the pipeline once without the directory
            # parameter
            stages( noCommit=noCommit, backendClasses=backendClasses )
        gLogger.info('All done (%d stages).'%len(stages) )
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as e:
        #gLogger.info( gdml )
        gLogger.exception(e)
        raise



