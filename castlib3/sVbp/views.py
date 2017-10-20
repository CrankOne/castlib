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

import os, re, json
from flask import Blueprint, render_template, request, url_for
from flask.views import MethodView as FlaskMethodView
from flask_api import status as HTTP_STATUS_CODES
from collections import OrderedDict

from castlib3.logs import gLogger
from castlib3.models.filesystem import Folder, File, FSEntry
from castlib3.exec_utils import TaskRegistry
from castlib3.rpc.mngr_super import WorkerManager_Supervisord

import sVresources.db.instance
from sVresources import apps, yaml
from sVresources.utils.contentType import expected_content_type
from sVresources.utils.queueTools import DelayedTaskViewMetaclass, CachedTaskView
from hashlib import md5

bp = Blueprint('cstl3', __name__,
               template_folder='templates',
               static_folder='static',
               url_prefix='/rawstat')

# Expected to be an instance of CastLib3.exec_utils.TaskRegistry.
# Set by configure().
gTaskRegistry = None
# Expected to be a dict of tuples. Names refers to particular hosts and tuples 
# contain number of ports. Set by configure().
gCstlNodes = {}

gWorkersMgmtBackends = {}

def configure( *args, **kwargs ):
    global gTaskRegistry
    global gCstlNodes
    global gWorkersMgmtBackends
    tasksCatalogues = kwargs.get( 'task-dirs', None )
    nodesList = kwargs.get('nodes', {})
    if tasksCatalogues is not None:
        gTaskRegistry = TaskRegistry( paths=tasksCatalogues )
        gLogger.info( "CastLib's TaskRegistry has discovered %d tasks for "
                "current instance."%len(gTaskRegistry.predefinedTasks) )
    else:
        gLogger.warning( "No \"cstlStagesDirs\" parameter is given." )
    if nodesList is not None:
        gCstlNodes = nodesList
        for nodeName, nodeProvides in gCstlNodes.iteritems():
            if 'supervisors' not in nodeProvides.keys():
                continue
            if type(nodeProvides) is not dict \
            and type(nodeProvides) is not OrderedDict:
                gLogger.error( 'Type of nodes:supervisors for node %s is %r, while '
                        'expected is a dict or OrderedDict.'%(nodeName, type(nodeProvides)) )
                continue
            gWorkersMgmtBackends[nodeName] = {}
            for spd in nodeProvides['supervisors']:
                if 'supervisord' == spd['type']:
                    gWorkersMgmtBackends[nodeName][spd['name']] = \
                            WorkerManager_Supervisord( nodeName
                                , int(spd['port'])
                                , username=(spd.get('username', None))
                                , password=(spd.get('password', None)) )
                # else ...
                else:
                    gLogger.warning( 'Unknown/unimplemented supervisor '
                            'type: "%s", on node "%s".'%(spd['type'], nodeName) )
        gLogger.info( "CastLib management view has been set to track "
                "%d nodes."%len(gCstlNodes) )
    else:
        gLogger.warning( "No \"cstlNodes\" parameter is given." )

def follow_path( path ):
    folderEntry = apps.db.query(Folder).filter( Folder.name=='$root' ).first()
    if not folderEntry:
        return None  # no root --- table is empty
    pToks = []
    path_ = path
    while True:
        path_, head = os.path.split(path_)
        pToks.append(head)
        if not head:
            break
    pToks.reverse()
    for pEntry in pToks[1:]:
        folderEntry = apps.db.query(Folder).filter(
                FSEntry.parent==folderEntry,
                FSEntry.name==pEntry).first()
    return folderEntry

#
# Filesystem

@bp.route('/observe/castlib3-files', methods=['GET'])
def observe_filesystem():
    """
    Web view returning browseable HTML page.
    """
    return render_template( 'pages/observe-files.html'
                , AJAXTreeAddr='castlib3-files-ajax'
                , AJAXGroupDetails=url_for('cstl3.folder_details')
                , AJAXItemDetails=url_for('cstl3.file_details')
                , AJAXParameters=['path']
                , groupFields=['path'] )

@bp.route('/observe/castlib3-files-ajax', methods=['POST'])
@expected_content_type
def filesystem_entries():
    """
    Returns castlib3 filesystem tree queries in form suitable for zTree.
    """
    path = request.form.get( 'path', '/' )
    folderEntry = follow_path( path )
    if not folderEntry:
        return [], HTTP_STATUS_CODES.HTTP_200_OK  # database is empty
    ret = []
    for entry in folderEntry.children.values():
        vPath = os.path.join( path if path else '/', entry.name )
        ret.append({
                'name' : entry.name,
                'isParent' : 'd' == entry.type,
                'path' : vPath
            })
    return ret, HTTP_STATUS_CODES.HTTP_200_OK

@bp.route('/observe/castlib3-file-details-ajax', methods=['GET'])
@expected_content_type
def file_details():
    path = request.args.get( 'path' )
    if not path:
        return {'details' : 'request did not submitted path'}, \
                HTTP_STATUS_CODES.HTTP_400_BAD_REQUEST
    folderPath, filename = os.path.split(path)
    folderEntry = follow_path( folderPath )
    if not folderEntry:
        return {}, HTTP_STATUS_CODES.HTTP_404_NOT_FOUND
    fileEntry = DB.session.query(File) \
            .filter( File.parent==folderEntry,
                     File.name==filename ).first()
    return fileEntry.as_dict(), HTTP_STATUS_CODES.HTTP_200_OK

@bp.route('/observe/castlib3-folder-details-ajax', methods=['GET'])
@expected_content_type
def folder_details():
    path = request.args.get( 'path' )
    if not path:
        return {'details' : 'request did not submitted path'}, \
                HTTP_STATUS_CODES.HTTP_400_BAD_REQUEST
    folderEntry = follow_path(path)
    if not folderEntry:
        return {}, HTTP_STATUS_CODES.HTTP_404_NOT_FOUND
    return folderEntry.as_dict(), HTTP_STATUS_CODES.HTTP_200_OK

#
# Management

@bp.route('/nodes/', methods=['GET'])
def manage_cstl3():
    extendedTasks = {}
    if gTaskRegistry:
        for k, v in gTaskRegistry.predefinedTasks:
            extendedTasks[k] = {
                          'comment' : v.get('comment', '<i>(No comment available.)</i>')
                        , 'runnable' : v.get('runnable', False)
                        , 'stagesJS' : json.dumps( v['stages'] )
                        , 'fileLocation' : v['_location']
                    }
    return render_template( 'pages/manage.html'
            , availableTasks=extendedTasks
            , AJAXTreeAddr='nodes-ajax'
            , AJAXGroupDetails=url_for('cstl3.node_overview')
            , AJAXItemDetails=url_for('cstl3.node_item_details')
            , AJAXParameters=['nodeName', 'portNo', 'shareName', 'category', 'sprvName', 'workerIndex']
            , groupFields=['category', 'nodeName'] )

@bp.route('/nodes/nodes-ajax', methods=['POST'])
@expected_content_type
def nodes_tree():
    """
    Returns castlib3 filesystem tree queries in form suitable for zTree.
    """
    category = request.form.get('category', None)
    if category is None:
        if not gCstlNodes:
            return [], HTTP_STATUS_CODES.HTTP_200_OK  # no nodes configured
        ret = []
        for nodeName, ports in gCstlNodes.iteritems():
            ret.append({
                    'name' : nodeName,
                    'isParent' : True,
                    'nodeName' : nodeName,
                    'category' : 'node'
                })
        return ret, HTTP_STATUS_CODES.HTTP_200_OK
    nodeName = request.form.get('nodeName')
    ret = []
    if 'node' == category:
        if nodeName not in gCstlNodes.keys():
            return { 'error' : 'Has no such node: "%s".'%nodeName }, HTTP_STATUS_CODES.HTTP_400_BAD_REQUEST
        if 'supervisors' in gCstlNodes[nodeName].keys():
            ret.append( { 'name' : 'Supervisors'
                        , 'isParent' : True
                        , 'nodeName' : nodeName
                        , 'category' : 'supervisors' } )
        if 'workers' in gCstlNodes[nodeName].keys():
            ret.append( { 'name' : 'Workers'
                        , 'isParent' : True
                        , 'nodeName' : nodeName
                        , 'category' : 'workers' } )
        if 'shares' in gCstlNodes[nodeName].keys():
            ret.append( { 'name' : 'Shares'
                        , 'isParent' : True
                        , 'nodeName' : nodeName
                        , 'category' : 'shares' } )
        return ret, HTTP_STATUS_CODES.HTTP_200_OK
    elif 'supervisors' == category:
        for sprv in gCstlNodes[nodeName]['supervisors']:
            ret.append( { 'name' : sprv['name']
                        , 'nodeName' : nodeName
                        , 'sprvName' : sprv['name']
                        , 'category' : 'supervisor' } )
        return ret, HTTP_STATUS_CODES.HTTP_200_OK
    elif 'workers' == category:
        for n, wrkr in enumerate(gCstlNodes[nodeName]['workers']):
            ret.append( { 'name' : '%r'%wrkr['id']
                        , 'workerIndex' : n
                        , 'nodeName' : nodeName
                        , 'category' : 'worker' } )
        return ret, HTTP_STATUS_CODES.HTTP_200_OK
    elif 'shares' == category:
        # ...
        return ret, HTTP_STATUS_CODES.HTTP_200_OK
    return { 'error' : 'Unknown category: "%s".'%category }, HTTP_STATUS_CODES.HTTP_400_BAD_REQUEST

@bp.route('/nodes/node_overview')
@expected_content_type
def node_overview():
    nodeName = request.args.get('nodeName')  # .form ?
    if nodeName not in gCstlNodes.keys():
        return { 'error' : 'Has no such node: "%s".'%nodeName }, HTTP_STATUS_CODES.HTTP_400_BAD_REQUEST
    # TODO: Find a better way?
    isUp = True if 0 == os.system("ping -c 1 %s > /dev/null"%nodeName) else False
    shares = gCstlNodes[nodeName].get('nfs-shares', {})
    # ...
    # TODO: Basically, we have to return generic information about host here.
    # May be latency/traceroute may be useful for further utilization within
    # balancers or task brokers. We have to cache this info as well.
    return { 'isUp': isUp
            , 'NFS-shares' : shares
            , 'nodeName' : nodeName} \
         , HTTP_STATUS_CODES.HTTP_200_OK

@bp.route('/nodes/item_details')
@expected_content_type
def node_item_details():
    ret = []
    category = request.args.get('category', None)
    nodeName = request.args.get('nodeName', None)
    if nodeName is not None and nodeName not in gCstlNodes.keys():
        return { 'error' : 'Has no such node: "%s".'%nodeName }, HTTP_STATUS_CODES.HTTP_400_BAD_REQUEST
    if 'worker' == category:
        workerIndex = request.args.get('workerIndex')
        if workerIndex is None:
            return { 'error' : 'Request did not submit worker num.' }, HTTP_STATUS_CODES.HTTP_400_BAD_REQUEST
        workerIndex = int(workerIndex)
        workerEntry = gCstlNodes[nodeName]['workers'][workerIndex]
        manager = gWorkersMgmtBackends[nodeName].get( workerEntry['supervisor'], None )
        if manager is None:
            return { 'error' : 'Worker config entry %s[%d] refers to '
                    'non-existing/unimplemented supervisor: "%s".'%(
                            nodeName, workerIndex, workerEntry['supervisor'] ) } \
                            , HTTP_STATUS_CODES.HTTP_400_BAD_REQUEST
        
        return manager.worker_status( gCstlNodes[nodeName]['workers'][workerIndex]['id'] ) \
                , HTTP_STATUS_CODES.HTTP_200_OK
    elif 'supervisor' == category:
        supervisorName = request.args.get('sprvName')
        try:
            ret = gWorkersMgmtBackends[nodeName][supervisorName].status()
        except Exception as e:
            gLogger.exception(e)
            return { 'available' : False, 'details' : str(e) }, HTTP_STATUS_CODES.HTTP_200_OK
        if ret[0]:
            return { 'available' : True, 'details' : ret[1] }, HTTP_STATUS_CODES.HTTP_200_OK
        else:
            return { 'available' : False, 'details' : None }, HTTP_STATUS_CODES.HTTP_200_OK
    else:
        return { 'error' : 'Unknown category: "%s".'%category }, HTTP_STATUS_CODES.HTTP_400_BAD_REQUEST
    return ret, HTTP_STATUS_CODES.HTTP_200_OK

#
# Long-running operations

class BaseCSTL3View(FlaskMethodView):
    """
    View class implementing generic castlib3 routines related to sV-resources
    server.
    """
    pass

class Castlib3TaskView( BaseCSTL3View, CachedTaskView ):
    """
    This view is responsible for delayed cstl3 launch.
    """
    __metaclass__ = DelayedTaskViewMetaclass
    __delayedTaskParameters = {
            'queue' : 'cstl3',
            'import' : 'castlib3.sVbp.tasks.castlib3_stages'
        }

    @staticmethod
    def args_digest(  stageName
                    , stages=[]
                    , externalImport=[]
                    , directories=None  # TODO: use after cpickle
                ):
        return md5(stageName).hexdigest()

    def get(self, stageName):
        stagesDescription = gTaskRegistry.get( stageName, None ) if gTaskRegistry else None
        if not stagesDescription:
            abort( HTTP_STATUS_CODES.HTTP_404_NOT_FOUND )
        return super( Castlib3TaskView, self).delayed_retrieve( self
                , stageName
                , stages=stagesDescription['stages']
                , externalImport=stagesDescription.get('external-import', [])
                , directories=None  # TODO
                )

    def post(self, stageName):
        stagesDescription = gTaskRegistry.get( stageName, None ) if gTaskRegistry else None
        if not stagesDescription:
            abort( HTTP_STATUS_CODES.HTTP_404_NOT_FOUND )
        pass

bp.add_url_rule( '/run/<regex("[-\w]+"):stageName>',
            view_func=Castlib3TaskView.as_view('run_stage') )

sVresources_Blueprint = bp
sVresources_ObservablePages = [
        # title, destination
          ('CASTOR entries', 'cstl3.observe_filesystem')
        , ('Manage CastLib tasks', 'cstl3.manage_cstl3')
        # ('API reference', 'cstl3.reference')  # TODO
    ]

