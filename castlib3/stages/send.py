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

import os, json, urllib2

from castlib3.stage import Stage, StageMetaclass
from castlib3.models.filesystem import Folder, File
from castlib3.logs import gLogger
from castlib3.backend import LocalBackend
from castlib3 import dbShim as DB

from sqlalchemy import exists, and_
from os import path as P
from urlparse import urlparse

def file_to_entry( f ):
    dct = { 'n' : f.name }
    if f.modified:
        dct['m'] = f.modified
    if f.size is not None:
        dct['s'] = f.size
    if f.adler32 is not None:
        dct['a32'] = f.adler32
    return dct

class FSEntriesVisitor(dict):
    def __init__(self):
        self._size = 0
        self['byPaths'] = {}
    def append(self, path, fileEntry):
        if path not in self['byPaths'].keys():
            self['byPaths'][path] = [file_to_entry(fileEntry)]
            self._size += 1
        else:
            self['byPaths'][path].append(file_to_entry(fileEntry))
            self._size += 1
    def clear(self):
        self['byPaths'] = {}
        self._size = 0
    def size(self):
        return self._size
    def to_JSON(self):
        return json.dumps(self['byPaths'])

def recursive_sending( acc, folder, path='/', maxLength=10 ):
    for child in folder.children.values():
        if issubclass(type(child), File):
            acc.append( path, child )
            if acc.size() >= maxLength:
                yield acc
                acc.clear()
        elif issubclass(type(child), Folder):
            for subseq in recursive_sending( acc, child,
                    path=os.path.join(path, child.name), maxLength=maxLength ):
                if acc.size() >= maxLength:
                    yield acc
                    acc.clear()
        else:
            raise RuntimeError( 'Unexpected type of child entry: %s'%type(child) )
    if acc:
        yield acc
        #acc.clear()  # TODO: shall we?

class HTTPPostSend( Stage ):
    __metaclass__ = StageMetaclass
    __castlib3StageParameters = {
        'name' : 'rest-send',
        'description' : """
        Will perform selection entries from local database and sending them via
        the HTTP POST request to the remote host.
        """
    }

    def __init__(self, *args, **kwargs):
        super(HTTPPostSend, self).__init__(*args, **kwargs)

    # nstages=1, stageNum=1, results=[]
    def _V_call( self
                , maxEntriesPerRequest=300  # max entries to send per one request
                , address='http://0.0.0.0:5000/na64-mdat/import/files'
            ):
        rootF = DB.session.query(Folder).filter(Folder.parentID==None).first()
        if not rootF:
            raise RuntimeError('Failed to find root folder. Is DB empty?')
        acc = FSEntriesVisitor()
        for bunch in recursive_sending( acc, rootF,
                        path=os.path.join('/', rootF.name),
                        maxLength=maxEntriesPerRequest ):
            if bunch.size():
                print( 'Sending %d entries...'%(bunch.size()) )
                req = urllib2.Request(address)
                req.add_header('Content-Type', 'application/json')
                response = urllib2.urlopen(req, acc.to_JSON())
        #path = os.path.join('/', rootF.name)
        #for sF in rootF:
        #    retrieve_entries_for_sending()


