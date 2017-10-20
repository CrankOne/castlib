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

import socket, json, yaml, pickle, threading, copy
from castlib3.logs import gLogger, mh
from castlib3.backend import gCastlibBackends
from castlib3.syscfg import gConfig
import castlib3.stage

from .simple import send_msg, recv_msg, ReportingBar

# TODO: move reporter and views and stuff to dedicated module (rpc/ dir?)
gReportingViews = {}

class Reporter( threading.Thread ):
    """
    The Reporter object represents a listening thread (server) providing basic
    RPC of running cstl3 process with external users.
    """
    def __init__(self, *args, **kwargs):
        srvAddr = kwargs.pop('srvAddr')
        portNo = kwargs.pop('portNo')
        self.tr = kwargs.pop('taskRegistry')
        self.disabledViews = kwargs.pop('disabledViews', [])
        super(Reporter, self).__init__(*args, **kwargs)
        self.sock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
        self.sock.setsockopt( socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(( srvAddr, portNo ))
        self.stagesCV = threading.Condition()
        self.stages = {'stages':{}}
        self.progress = None
        self.alive = threading.Event()
        self.workState = 'Undefined.'

    def start(self, *args, **kwargs):
        self.alive.set()
        super(Reporter, self).start( *args, **kwargs )

    def run(self):
        while True and self.alive.is_set():
            self.sock.listen(1)
            conn, cliAddr = self.sock.accept()
            try:
                data = recv_msg( conn )
                if len(data) < 4:
                    send_msg( conn, 'json{"status":"Bad request."}' )
                else:
                    dataType = data[:4]
                    data = data[4:]
                    if dataType == 'json':
                        send_msg( conn, 'json' + json.dumps(self.treat_request( json.loads(data) )) )
                    elif dataType == 'yaml':
                        send_msg( conn, 'yaml' + yaml.dump(self.treat_request( json.loads(data) )) )
                    elif dataType == 'pckl':
                        send_msg( conn, 'pckl' + pickle.dumps(self.treat_request( pickle.loads(data) )) )
                    else:
                        send_msg( conn, 'json{"status":"Unrecognized request data type."}' )
            finally:
                conn.close()

    def treat_request( self, data ):
        resp = { 'status' : 'ok'
               , 'workState' : self.workState
               , 'errors' : [] }
        for k, v in data.iteritems():
            if k in gReportingViews.keys():
                if k in self.disabledViews:
                    resp['errors'].append('View "%s" is disabled.'%k)
                    continue
                try:
                    resp[k] = gReportingViews[k]( self, v )
                except Exception as e:
                    resp['errors'].append(
                            'Error %s: %r.'%( type(e), e ) )
            else:
                resp['errors'].append(
                        'Unknown/unsupported request field: "%s".'%k )
        return resp

    def take_stages(self):
        ret = copy.deepcopy(self.stages)
        self.stages = {'stages':{}}
        return ret

#
# Reporter views
###############

def _reporter_view__list_views( reporter, data ):
    """
    Returns object (dict) listing available views with their descriptions.
    """
    ret = {}
    for k in gReportingViews.keys():
        if k not in reporter.disabledViews:
            ret[k] = gReportingViews[k].__doc__
    return ret

gReportingViews['list-views'] = _reporter_view__list_views

def _reporter_view__stop( reporter, data ):
    """
    Returns object (dict) listing available views with their descriptions.
    """
    ret = {}
    for k in gReportingViews.keys():
        if k not in reporter.disabledViews:
            ret[k] = gReportingViews[k].__doc__
    return ret

gReportingViews['list-views'] = _reporter_view__list_views

#
# Generic status querying

def _reporter_view__query_progress( reporter, data ):
    """
    Returns the current business in form: (processed:int, max:int,
    estimated time:str).
    Returns (1, 1, 'unknown') if current activity is not homogeneous or can not
    be determined (complementary to query-progress-msg).
    """
    resp = {}
    if reporter.progress and issubclass(reporter.progress.__class__, ReportingBar):
        return ( reporter.progress.index
               , reporter.progress.max
               , reporter.progress.prec_hr_time )
    else:
        return (1, 1, 'unknown')
    return resp

gReportingViews['query-progress'] = _reporter_view__query_progress


def _reporter_view__query_progress_message( reporter, data ):
    """
    Returns str, name of current activity, or 'unknown activity' if current
    activity is not homogeneous or can not be determined (complementary to
    query-progress).
    """
    if reporter.progress and issubclass(reporter.progress.__class__, ReportingBar) and reporter.progress.message:
        return reporter.progress.message
    else:
        return 'unknown activity'

gReportingViews['query-progress-msg'] = _reporter_view__query_progress_message


def _reporter_view__query_latest_msgs( reporter, data ):
    """
    Returns list --- few (usually no more than 10) latest log messages.
    """
    return mh.get_records()

gReportingViews['query-latest-msgs'] = _reporter_view__query_progress

#
# Stages

def _reporter_view__run_stages( reporter, data ):
    """
    Master view running tasks. Just receives the stages description object
    from remote and performs it.
    """
    resp = {}
    if reporter.stagesCV.acquire(blocking=0):
        reporter.stages = {'stages':data}
        reporter.stagesCV.notify_all()
        reporter.stagesCV.release()
        resp['stage-accepted-status'] = 'Accepted.'
    else:
        resp['stage-accepted-status'] = 'Denied (worker busy).'
    return resp

gReportingViews['run-stages'] = _reporter_view__run_stages

def _reporter_view__list_stages( reporter, data ):
    """
    Returns object (dict) listing available stages.
    """
    ret = {}
    for n, clsT in castlib3.stage.gCastlibStages.iteritems():
        ret[n] = clsT.stageDescription
    return ret

gReportingViews['list-stages'] = _reporter_view__list_stages


#
# Tasks

def _reporter_view__list_tasks( reporter, data ):
    """
    Returns object (dict) listing available tasks.
    """
    return reporter.tr.predefinedTasks

gReportingViews['list-tasks'] = _reporter_view__list_tasks



def _reporter_view__run_task( reporter, data ):
    """
    Launches the pre-defined task.
    """
    resp = {}
    if reporter.stagesCV.acquire(blocking=0):
        task = reporter.tr.get_task(data)
        reporter.stages = {'stages':task['stages']}
        reporter.stagesCV.notify_all()
        reporter.stagesCV.release()
        resp['stage-accepted-status'] = 'Accepted.'
    else:
        resp['stage-accepted-status'] = 'Denied (worker busy).'
    return resp

gReportingViews['run-task'] = _reporter_view__run_task

#
# Backends

def _reporter_view__list_backends( reporter, data ):
    """
    Returns object (dict) listing enabled back-ends.
    """
    return [k for k in gCastlibBackends]

gReportingViews['list-backends'] = _reporter_view__list_backends

#
# Locations

def _reporter_view__list_locations( reporter, data ):
    """
    Returns object (dict) listing available locations.
    """
    return gConfig['locations']

gReportingViews['list-locations'] = _reporter_view__list_locations

