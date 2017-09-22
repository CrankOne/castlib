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

import os, re, json, yaml
from flask import Blueprint, render_template, request, url_for
from flask.views import MethodView as FlaskMethodView
from flask_api import status as HTTP_STATUS_CODES
from collections import OrderedDict

from castlib3.logs import gLogger
from castlib3.models.filesystem import Folder, File, FSEntry
from castlib3.exec_utils import initialize_database, ordered_load, discover_locations

import sVresources.db.instance
from sVresources import apps
from sVresources.utils.contentType import expected_content_type
from sVresources.utils.queueTools import DelayedTaskViewMetaclass, CachedTaskView
from hashlib import md5

bp = Blueprint('cstl3', __name__,
               template_folder='templates',
               static_folder='static',
               url_prefix='/rawstat')

gAvailableTasks = {}

def ordered_load(stream, Loader=yaml.Loader, object_pairs_hook=OrderedDict):
    class OrderedLoader(Loader):
        pass
    def construct_mapping(loader, node):
        loader.flatten_mapping(node)
        return object_pairs_hook(loader.construct_pairs(node))
    OrderedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        construct_mapping)
    return yaml.load(stream, OrderedLoader)

def append_tasks_dict( dct, key, content ):
    existingSingle = dct.get(key, None)
    existingMultiple = dct.get(key + '-1', None)
    if not (existingSingle or existingMultiple):
        dct[key] = content
        return
    # Task with similar name exists. Re-insert it incrementing index by one.
    if existingSingle:
        dct.pop(key)
        dct[key + '-1'] = existingSingle
        dct[key + '-2'] = content
        return
    # Multiple tasks with similar name:
    latestIdx = int(key.split('-')[-1:]) + 1
    dct[key + '_%d'%latestIdx] = content

def discover_stages( path ):
    ret = {}
    if not path:
        return ret
    subdirs = []
    for e in os.listdir(path):
        ePath = os.path.join( path, e )
        if os.path.isfile( ePath ) and re.match( "^.+\.yml$", e ):
            taskName = e[:-4]
            taskContent = None
            with open(ePath) as f:
                taskContent = ordered_load(f)
            if taskContent.get('stages', None):
                taskContent['_location'] = ePath
                append_tasks_dict( ret, taskName, taskContent )
        elif os.path.isdir( ePath ):
            subdirs.append( ePath )
    for e in subdirs:
        sRet = discover_stages( e )
        ret.update(sRet)
    return ret

def configure( *args, **kwargs ):
    tasksCatalogues=kwargs.get( 'cstlStagesDirs', None )
    if tasksCatalogues is not None:
        for e in tasksCatalogues:
            if os.path.exists(e) and os.path.isdir(e):
                gAvailableTasks.update( discover_stages( e ) )
                gLogger.info( "%d CastLib tasks have been discovered in \"%s\""%(
                                        len(gAvailableTasks), e) )
            else:
                gLogger.warning( 'Path "%s" is not reachable or does not refer '
                    'to a directory. Can not extract CastLib tasks from '
                    'there.'%e )
    else:
        gLogger.warning( "No \"cstlStagesDirs\" parameter is given." )

def follow_path( path ):
    folderEntry = apps.query(Folder).filter( Folder.name=='$root' ).first()
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
# Long-running operations

@bp.route('/manage', methods=['GET'])
def manage_cstl3():
    extendedTasks = {}
    for k, v in gAvailableTasks.iteritems():
        extendedTasks[k] = {
                      'comment' : v.get('comment', '<i>(No comment available.)</i>')
                    , 'runnable' : v.get('runnable', False)
                    , 'stagesJS' : json.dumps( v['stages'] )
                    , 'fileLocation' : v['_location']
                }
    return render_template('pages/manage.html'
            , availableTasks=extendedTasks)

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
        stagesDescription = gAvailableTasks.get( stageName, None )
        if not stagesDescription:
            abort( HTTP_STATUS_CODES.HTTP_404_NOT_FOUND )
        return super( Castlib3TaskView, self).delayed_retrieve( self
                , stageName
                , stages=stagesDescription['stages']
                , externalImport=stagesDescription.get('external-import', [])
                , directories=None  # TODO
                )

    def post(self, stageName):
        stagesDescription = gAvailableTasks.get( stageName, None )
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
        # ('API reference', 'extGDML.reference')  # TODO
    ]

