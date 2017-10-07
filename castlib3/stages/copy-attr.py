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
This stage sends uses referenced location (that has to be generally a local
directory) to retrieve certain file(s) pointed out by input arguments. Has
various parameters to be configured in order to maintain such a storage in
a poper way: locations priority, automatic clean-up, etc.
"""

import os, json, urllib2

from castlib3.stage import Stage, StageMetaclass
from castlib3.models.filesystem import Folder, File
from castlib3.logs import gLogger
from castlib3 import dbShim as DB
from castlib3.stages.select import get_location_by_path

from sqlalchemy import exists, and_
from os import path as P
from urlparse import urlparse

class CopyAttributes( Stage ):
    __metaclass__ = StageMetaclass
    __castlib3StageParameters = {
        'name' : 'copy-attributes',
        'description' : """
        Copies attributes from files from one location to another without
        modifying them through back-end. Useful for setting attributes of some
        "virtual" (usually referential) sources for further retrieval and
        comparison.
        """
    }

    def __init__(self, *args, **kwargs):
        super(CopyAttributes, self).__init__(*args, **kwargs)

    def _V_call( self
                , from_=None
                , to_=None
                , onCollision='warn-deny'
                , attributes=[]
                , recursive=False
            ):
        allowedAttrs = ['error', 'deny', 'warn-deny', 'warn-accept', 'accept']
        if not from_ or not to_:
            raise RuntimeError( 'The "from_" and "to_" arguments have to be set.' )
        if onCollision not in allowedAttrs:
            raise RuntimeError( 'The onCollision="%s" argument value is not '
                    'recognized. Possible values are: %s'%(', '.join(allowedAttrs)) )

        frmLoc = get_location_by_path( from_ )
        dstLoc = get_location_by_path( to_ )
        fullPathFrm = [ p.name for p in frmLoc.mp.query_ancestors().all()]
        fullPathFrm.append( frmLoc.name )
        fullPathDst = [ p.name for p in dstLoc.mp.query_ancestors().all()]
        fullPathDst.append( dstLoc.name )
        gLogger.info( 'Copying attrs: %s, from %s %s (id=%d), to: %s : %s (id=%d), %s.'%(
            ', '.join( attributes ),
            from_, os.path.join(*fullPathFrm), frmLoc.id,
            to_, os.path.join(*fullPathDst), dstLoc.id,
            'recursively' if recursive else 'not recursively' ) )
        n = self._copy_attrs( frmLoc, dstLoc
                        , onCollision=onCollision
                        , attributes=attributes
                        , recursive=recursive )
        gLogger.info( 'Copying attributes done. %d entries updated.'%n )
        
    def __copy_attribute( self, attrName, src, dst, onCollision ):
        """
        Returns -1 on error, 1 if attribute was set and 0 if values
        match or `deny' was set for onCollision and collision occured.
        """
        sv = getattr( src, attrName )
        dv = getattr( dst, attrName )
        if sv == dv:
            return 0
        if dv is not None:
            # this case shall be considered as a collision:
            if 'error' == onCollision:
                return -1
            if 'warn-deny' == onCollision \
            or 'warn-accept' == onCollision:
                gLogger.warning('Mismatching attribute %s was already set '
                        'for %s: src=%r, dst=%r.'%(
                    attrName, src.name, sv, dv) )
            if 'warn-accept' == onCollision \
            or 'accept' == onCollision:
                setattr( dst, attrName, sv )
                DB.session.add(dst)
                return 1
            return 0
        else:
            setattr( dst, attrName, sv )
            DB.session.add(dst)
            return 1

    def _copy_attrs( self, frmLoc, dstLoc
                   , onCollision='warn-deny'
                   , attributes=[]
                   , recursive=False ):
        nUpdates = 0
        frmQ = DB.session.query( File ).filter_by( parent=frmLoc )
        for srcE in frmQ.all():
            dstE = DB.session.query( File ) \
                    .filter_by( parent=dstLoc
                              , name=srcE.name ).one_or_none()
            if dstE:
                for attrName in attributes:
                    rc = self.__copy_attribute( attrName, srcE, dstE, onCollision=onCollision )
                    if -1 == rc:
                        raise RuntimeError( "Attribute `%s' collision. "
                            "Source entry: %s, destination entry: %s, "
                            "src=%r, dst=%r." %(
                            attrName, srcE.name, dstE.name,
                            getattr(srcE, attrName),
                            getattr(dstE, attrName) ) )
                    elif 1 == rc:
                        nUpdates += 1
        if recursive:
            for sF in DB.session.query( Folder ).filter_by( parent=frmLoc ).all():
                dF = DB.session.query( Folder ).filter_by( parent=dstLoc ).one_or_none()
                nUpdates += self._copy_attrs( sF, dF
                                , onCollision=onCollision
                                , attributes=attributes
                                , recursive=recursive )
        return nUpdates


