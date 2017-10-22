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

from castlib3.backend import AbstractBackend, BackendMetaclass
from castlib3.shell import invoke_util
from urlparse import urlparse, urlunparse
from urllib import urlencode
from castlib3.logs import gLogger
import requests, fnmatch, datetime, json

class EOSBackend(AbstractBackend):
    __metaclass__ = BackendMetaclass
    __backendAttributes__ = {
        'scheme' : 'eos'
    }

    def __init__(self
                , eosNetLoc='eospublic.cern.ch'
                , eosPath='/proc/user/'  # may be '/proc/admin' for some operations as well?
                , ruid=0, rgid=0
                , format_='json' ):
        self.eosNetLoc = eosNetLoc
        self.eosPath = eosPath
        self.ruid = ruid
        self.rgid = rgid
        self.format_ = format_

    def _run_cmd_on_path_REST(self, filePath, cmd):
        qs = urlencode({ 'mgm.cmd' : cmd
                    , 'mgm.path' : urlparse(filePath).path if ':/' in filePath else filePath
                    , 'mgm.ruid' : self.ruid
                    , 'mgm.rgid' : self.rgid
                    , 'mgm.format' : self.format_
            })
        uri = urlunparse(( 'http'           # scheme
                        , self.eosNetLoc    # netloc
                        , self.eosPath      # path
                        , ''                # params
                        , qs                # query
                        , ''                # fragment
            ))
        #print('xxx: %s'%uri)  # xxx
        ret = requests.get( uri ).json()
        if 'retc' in ret.keys() and 0 != ret['retc']:
            raise RuntimeError( 'Query: %s. REST error (%r): %s'%(
                uri, ret['retc'], ret['errormsg']) )
        return ret

    def get_adler32(self, path):
        fInfo = self._run_cmd_on_path_REST( path, 'fileinfo' )
        if 'checksumtype' not in fInfo.keys() \
                or 'checksumvalue' not in fInfo.keys():
            raise RuntimeError( 'REST response does not contain checksum.' )
        if fInfo['checksumtype'] != 'adler':
            raise RuntimeError( 'REST response does not contain adler32 '
                    'checksum (type is "%s").'%fInfo['checksumtype'] )
        return fInfo["checksumvalue"]

    def get_permissions(self, path):
        raise NotImplementedError('Permissions management is not yet implemented (EOS).')  # EOS-mod

    def get_size(self, path):
        fInfo = self._run_cmd_on_path_REST( path, 'fileinfo' )
        if 'size' not in fInfo.keys():
            raise RuntimeError( 'REST response does not contain size value.' )
        return fInfo["size"]
    
    def get_modified(self, path):
        fInfo = self._run_cmd_on_path_REST( path, 'fileinfo' )
        if 'mtime' not in fInfo.keys():
            raise RuntimeError( 'REST response does not contain modified timestamp.' )
        return datetime.datetime.fromtimestamp(fInfo["mtime"])

    def set_modified(self, path, dtObject):
        raise NotImplementedError('Modification of EOS data is not yet implemented within back-end.')

    def listdir(self, path):
        """
        EOS may return a HUGE data object (~16MB for 2e4 entries) that may take
        a lot of system resources to be processed.
        """
        dInfo = self._run_cmd_on_path_REST( path, 'fileinfo' )
        if 'children' not in dInfo.keys():
            raise RuntimeError( 'REST response does not contain children (is it a dir?).' )
        namesLst = map( lambda e: e['name'], dInfo['children'] )

    def exists(self, path):
        raise NotImplementedError('TODO: check existance of file/dir with EOS back-end.')  # TODO

    def isfile(self, path, followSymlink=False):
        """
        TODO: find a better way.
        """
        dInfo = self._run_cmd_on_path_REST( path, 'fileinfo' )
        return 'nndirectories' not in dInfo.keys()

    def isdir(self, path, followSymlink=False):
        """
        TODO: find a better way.
        """
        dInfo = self._run_cmd_on_path_REST( path, 'fileinfo' )
        return 'nndirectories' in dInfo.keys()

    def islink(self, path):
        raise NotImplementedError('Querying a symlink at EOS data is not yet implemented within back-end.')

    def del_file(self, path):
        raise NotImplementedError('File deletion is not yet implemented (EOS).')  # EOS-mod

    def cpy_file(self, srcURI, dstURI, backends={} ):
        raise NotImplementedError('File copying is not yet implemented (EOS).')  # EOS-mod

    def get_dir_content(self, uri, onlyPats=None, ignorePats=None, extra={} ):
        dInfo = self._run_cmd_on_path_REST( uri, 'fileinfo' )
        ret = {
                'folder' : uri,
                'modified' : datetime.datetime.fromtimestamp(dInfo['mtime']),
                'files' : {},
                'subFolders' : []
            }
        subds = []
        for e in dInfo['children']:
            if 'xattr' not in e.keys():  # that's a file
                ret['files'][e['name']] = {
                            'size' : e['size'],
                            'modified' : datetime.datetime.fromtimestamp(e['mtime']),
                        }
                if 'adler' == e['checksumtype']:
                    ret['files'][e['name']]['adler32'] = e['checksumvalue']
            else:  # that's a dir
                subds.append( e['name'] )
        for subd in subds:
            subd = subd['filename']
            ret['subFolders'].append(
                    self.get_dir_content( self.uri_from_path( os.path.join(dirPath, subd)),
                                        onlyPats=onlyPats,
                                        ignorePats=ignorePats ) )
        ret.update(extra)
        return ret

    def uri_from_path(self, path):
        lpp = urlparse( path )
        return 'eos://' + lpp.path


