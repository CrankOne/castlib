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
This stage sends entries from local database to the given HTTP address within
POST request.
"""

import os, datetime
from urlparse import urlparse, urlunsplit

from castlib3.stage import Stage, StageMetaclass
from castlib3.models.filesystem import Folder, File, StoragingNode, RemoteFolder
from castlib3.logs import gLogger
from castlib3.filesystem import split_path
from castlib3 import dbShim as DB

from sqlalchemy import and_, or_
from sqlalchemy import select as sql_select
from sqlalchemy.orm import aliased
from sqlalchemy.sql import func

def _size_compare( refLoc, dstLoc, **kwargs ):
    sqRef = DB.session.query(File) \
                          .filter_by(parent=refLoc).subquery(with_labels=True)
    RefFile = aliased(File, sqRef)
    return DB.session.query( File, RefFile ) \
                    .filter( File.parent==dstLoc ) \
                    .join( RefFile,
                           and_( RefFile.name == File.name
                               , RefFile.size != File.size ) )

def _adler32_compare( refLoc, dstLoc, **kwargs ):
    sqRef = DB.session.query(File) \
                          .filter_by(parent=refLoc).subquery(with_labels=True)
    RefFile = aliased(File, sqRef)
    return DB.session.query( File, RefFile ) \
                    .filter_by( parent=dstLoc ) \
                    .join( RefFile,
                           and_( RefFile.name    == File.name
                               , RefFile.adler32 != File.adler32 ) )

def _modified_compare( refLoc, dstLoc, **kwargs ):
    truncateSeconds = kwargs.get('truncateSeconds', False)
    sqRef = DB.session.query(File) \
                          .filter_by(parent=refLoc).subquery(with_labels=True)
    RefFile = aliased(File, sqRef)
    if not truncateSeconds:
        return DB.session.query( File, RefFile ) \
                    .filter_by( parent=dstLoc ) \
                    .join( RefFile,
                           and_( RefFile.name    == File.name
                               , RefFile.modified != File.modified ) )
    else:
        # NOTE: the unix_timestamp() is defined only for MySQL. We have to
        # invoke bare SQL here to filter by time difference.
        # Truncate seconds:
        #raise NotImplementedError("Raw SQL should be placed here.")
        return DB.session.query( File, RefFile ) \
                    .filter_by( parent=dstLoc ) \
                    .join( RefFile,
                           and_( RefFile.name    == File.name \
        , func.abs( RefFile.modified - File.modified ) >= 1 ) )

def _missing_compare( refLoc, dstLoc, **kwargs ):
    #q = DB.session.query( File.id, RefFile.id ) \
    #                .filter_by( parent=dstLoc ) \
    #                .outerjoin( RefFile, File.name == RefFile.name ) \
    #                .filter( File.id == None )
    # ^^^ Doesn't work. Think about joins.
    sqDst = DB.session.query(File) \
                          .filter_by(parent=dstLoc).subquery(with_labels=True)
    DstFile = aliased( File, sqDst )
    return DB.session.query( File, DstFile ) \
                     .filter_by( parent=refLoc ) \
                     .outerjoin( DstFile, DstFile.name == File.name ) \
                     .filter( DstFile.id == None ).order_by( File.modified )

def _deleted_compare( refLoc, dstLoc, **kwargs ):
    sqRef = DB.session.query(File) \
                          .filter_by(parent=refLoc).subquery(with_labels=True)
    RefFile = aliased(File, sqRef)
    return DB.session.query( File, RefFile ) \
                    .filter_by( parent=dstLoc ) \
                    .outerjoin( RefFile,
                           RefFile.name == File.name ) \
                    .filter( File.id == None )

gComparators = {
    'size' : _size_compare,
    'adler32' : _adler32_compare,
    'modified' : _modified_compare,
    'missing' : _missing_compare,
    'deleted' : _deleted_compare
}

def get_location_by_path( path ):
    lpp = urlparse( path )
    pToks = split_path( lpp.path )
    folderEntry = None
    if lpp.netloc:
        node = DB.session.query(StoragingNode).filter(
                                 StoragingNode.identifier==lpp.netloc
                               , StoragingNode.scheme==lpp.scheme ).one()
        cNodePath = []
        for pTok in pToks:
            cNodePath.append(pTok)
            uri = urlunsplit((lpp.scheme or '', lpp.netloc or '', os.path.join(*cNodePath), '', ''))
            gLogger.debug( '<<#1 pTok=%r netloc=%r uri=%r name=%r'%(pTok, lpp.netloc, uri, os.path.split( uri )[1]) )
            folderEntry = DB.session.query( RemoteFolder ).filter_by(
                          node=node
                        , name=os.path.split( uri )[1] or '/'
                        , path=uri
                        , parent=folderEntry ).one()
            gLogger.debug( '>>#1 %r'%pTok )
    else:
        # To prevent remote root folders being considered as local one:
        discQuery = DB.session.query(RemoteFolder.id)
        cNodePath = []
        for pTok in pToks:
            cNodePath.append(pTok)
            uri = urlunsplit((lpp.scheme or '', lpp.netloc or '', os.path.join(*cNodePath), '', ''))
            gLogger.debug( '<<#2 pTok=%r uri=%r name=%r'%(pTok, uri, os.path.split( uri )[1]) )
            folderEntry = DB.session.query( Folder ).filter_by(
                          name=(os.path.split( uri )[1]) or '/'
                        , parent=folderEntry ) \
                    .filter( ~Folder.id.in_(discQuery) ) \
                    .one()
            gLogger.debug( '>>#2 %r'%pTok )
    return folderEntry


class Select( Stage ):
    __metaclass__ = StageMetaclass
    __castlib3StageParameters = {
        'name' : 'select-mismatches',
        'description' : """
        Stage performing selection of files/directories by mismatches. Returns
        a mismatches dictionary {<key> : <mismatch-query>} where key is
        mismatch name from mismatches list and <mismatch-query> is the
        SQLAlchemy query keeping pairs <dstFile>, <refFile>.
        """
    }

    def __init__(self, *args, **kwargs):
        super(Select, self).__init__(*args, **kwargs)

    def _V_call( self,
                mismatches=['size', 'adler32', 'modified', 'missing', 'deleted'],
                truncateSeconds=False,  # we had to introduce this since some backends don't support it
                referential=None,
                destination=None ):
        refLoc = get_location_by_path( referential )
        dstLoc = get_location_by_path( destination )
        fullPathRef = [ p.name for p in refLoc.mp.query_ancestors().all()]
        fullPathRef.append( refLoc.name )
        fullPathDst = [ p.name for p in dstLoc.mp.query_ancestors().all()]
        fullPathDst.append( dstLoc.name )
        gLogger.info( 'Reference: %s %s (id=%d), destination: %s : %s (id=%d)'%(
            referential, os.path.join(*fullPathRef), refLoc.id,
            destination, os.path.join(*fullPathDst), dstLoc.id ) )
        mmDct = {}
        for k in mismatches:
            mmDct[k] = [gComparators[k]( refLoc, dstLoc, truncateSeconds=truncateSeconds ), refLoc, dstLoc]
            gLogger.info( ' - %s : %d mismatches'%(k, mmDct[k][0].count()) )
        return mmDct

