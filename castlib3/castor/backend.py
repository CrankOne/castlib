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
from castlib3.castor.parsing import rxNSLS, obtain_rfstat_timestamps, rxsNSLS
from urlparse import urlparse, urlunparse
from castlib3.logs import gLogger
import fnmatch, datetime

class CASTORBackend(AbstractBackend):
    __metaclass__ = BackendMetaclass
    __backendAttributes__ = {
        'scheme' : 'castor'
    }

    def __init__(self, tmpDir=None, castorNetLoc='castorpublic.cern.ch'):
        #self.tmpDir = os.mkdir(tmpDir) if not exists
        # TODO: try to create temp dir if it doesn't exist
        self.castorNetLoc = castorNetLoc

    def get_adler32(self, path):
        lpp = urlparse(path)
        return invoke_util( 'nsls'
                          , remotePath=lpp.path
                          , regexToApply=rxNSLS)[0]['adler32']

    def get_permissions(self, path):
        raise NotImplementedError('Permissions management is not yet implemented (CASTOR).')

    def get_size(self, path):
        lpp = urlparse(path)
        return int(invoke_util( 'nsls-dir'
                              , dirPath=lpp.path
                              , regexToApply=rxNSLS)[0]['fileSize'])
    
    def get_modified(self, path):
        rfsOut = invoke_util( 'rfstat', remotePath=urlparse(path).path )[1]
        r = obtain_rfstat_timestamps( rfsOut )['modTimestamp']
        return datetime.datetime.fromtimestamp(r)

    def set_modified(self, path, dtObject):
        lpp = urlparse(path)
        return invoke_util( 'updtstamp'
                              , key='m'
                              , timestamp=dtObject.strftime('%Y%m%d%H%M')
                              , hsmDestFile=lpp.path )

    def listdir(self, path):
        lpp = urlparse(path)
        return map( lambda e: e['filename'],
                invoke_util( 'nsls'
                            , timeout='long'
                            , remotePath=lpp.path
                            , regexToApply=rxNSLS) )

    def isfile(self, path):
        lpp = urlparse(path)
        fb = invoke_util( 'nsls-dir'
                        , dirPath=lpp.path
                        , regexToApply=rxNSLS)['mode'][0][0]
        return '-' == fb or 'm' == fb

    def isdir(self, path):
        lpp = urlparse(path)
        fb = invoke_util( 'nsls-dir'
                        , dirPath=lpp.path
                        , regexToApply=rxNSLS)['mode'][0][0]
        return 'd' == fb

    def islink(self, path):
        fb = invoke_util( 'nsls-dir'
                        , dirPath=path
                        , regexToApply=rxNSLS)['mode'][0][0]
        return 'l' == fb

    def del_file(self, path):
        lpp = urlparse(path)
        return invoke_util('nsrmFile', hsmFile=lpp.path)

    def cpy_file(self, srcURI, dstURI, backends={} ):
        srcLPP = urlparse(srcURI)
        dstLPP = urlparse(dstURI)
        if srcLPP.scheme != 'file' and srcLPP.scheme != '':
            # todo: use self.tmpDir
            raise NotImplementedError('CASTOR backend currently does not '
                    'support copy from locations other than local.')
        assert( dstLPP.scheme == 'castor' )
        # The xrdcp expects URI being in other form that declared by RFC1630
        # for CASTOR resource:
        # - the actual scheme must be `root', not `castor'
        # - the (absolute) path has to start from one extra dash symbol after
        # host identifier. Example:
        #   root://castorpublic.cern.ch//castor/cern.ch/na64/data/cdr/
        dstURImod = urlunparse( ( 'root'                # scheme
                                , self.castorNetLoc     # netloc
                                , '/' + dstLPP.path     # path
                                , '', '', ''            # params, query, fragment
                                ) )
        return invoke_util( 'xrdcp'
                          , srcURI=srcURI
                          , dstURI=dstURImod
                          , timeout='long' )

    def get_dir_content(self, uri, onlyPats=None, ignorePats=None, extra={} ):
        # Get list of all files and sub-directories in current dir
        ppl = urlparse(uri)
        entries = invoke_util('nsls', timeout='long', remotePath=ppl.path, regexToApply=rxNSLS)
        gLogger.debug('Acquired contents list of "%s".'%ppl.path)
        # Get rid from the logically deleted files and symlinks. The only types
        # to remain are the files and directories ('-', 'm' and 'd').
        entries = filter( lambda e: e['mode'][0] in 'md-', entries )
        if onlyPats:
            if type(onlyPats) is str:
                onlyPats = [onlyPats,]
            contentLst_ = []
            for wcard in onlyPats:
                contentLst_.extend(
                    filter(
                        lambda e: fnmatch.fnmatch(e['filename'], wcard), 
                        entries ) )
            entries = contentLst_
        if ignorePats:
            if type(ignorePats) is str:
                ignorePats = [ignorePats,]
            for wcard in ignorePats:
                entries = list(filter( lambda e: \
                                    not fnmatch.fnmatch(e['filename'], wcard), entries ))
        subds = filter( lambda e: e['mode'][0] == '', entries )
        filesDict = {}
        for e in filter( lambda e: e['mode'][0] in '-m', entries ):
            filesDict[e.pop('filename')] = {
                        'size' : e['fileSize'],
                        'adler32' : e['adler32']
                    }
        ret = {
            'folder' : uri,
            'files' : filesDict,
            'subFolders' : []
        }
        for subd in subds:
            subd = subd['filename']
            ret['subFolders'].append(
                            self.get_dir_content( self.uri_from_path( os.path.join(dirPath, subd)),
                                        onlyPats=onlyPats, ignorePats=ignorePats) )
        ret.update(extra)
        return ret

    def uri_from_path(self, path):
        return 'castor://' + path

