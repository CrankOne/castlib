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

from abc import ABCMeta, abstractmethod, abstractproperty
import os, shlex, zlib, fnmatch, datetime
from urlparse import urlparse
from castlib3.models.filesystem import File, Folder

# Keeps all the declared backends classes indexed by their schemes
gCastlibBackends = {}

class BackendMetaclass(ABCMeta):
    def __new__(cls, clsname, bases, dct):
        global gCastlibBackends
        # Require new backend class to inherit the AbstractBackend:
        baseCorrect = clsname == 'AbstractBackend'
        if not baseCorrect:
            for base in bases:
                if issubclass(base, AbstractBackend):
                    baseCorrect = True
                    break
            if not baseCorrect:
                raise TypeError( '%s is not a subclass of AbstractBackend.'%clsname )
        if clsname != 'AbstractBackend':
            backendClsDetails = dct.pop( '__backendAttributes__' )
            scheme = backendClsDetails['scheme']
            dct['scheme'] = scheme  # set the class property
            if scheme in gCastlibBackends.keys():
                raise KeyError('Backend class for scheme "%s" had been '
                        'declared before.'%scheme )
            # ... other backend attributes?
        classInstance = super(BackendMetaclass, cls) \
                .__new__( cls, clsname, bases, dct )
        if clsname != 'AbstractBackend':
            gCastlibBackends[scheme] = classInstance
        return classInstance

class AbstractBackend(object):
    __metaclass__ = BackendMetaclass
    """
    An interfacing class defining how to handle common operations with
    filesystem entries within particular environment.
    """

    @abstractmethod
    def get_adler32(self, path):
        """Shall return the hexidecimal digest built from the file content."""
        pass

    @abstractmethod
    def get_size(self, path):
        """Returns size (in bytes) of given file. Directories aren't supported."""
        pass
    
    @abstractmethod
    def get_modified(self, path):
        """The return value is a number giving the number of seconds since the epoch."""
        pass

    @abstractmethod
    def set_modified(self, path, dtObject):
        """
        Must set modified timestamp of filesystem entry identified by given URI
        to given datetime object.
        """
        pass

    @abstractmethod
    def get_permissions(self, path):
        """Returns encoded permissions."""
        return os.stat( path ).st_mode & 0o777

    @abstractmethod
    def listdir(self, path):
        """
        Returns a list contining the names of the entries in the directory
        given by path.
        """
        pass

    @abstractmethod
    def isfile(self, path):
        """
        Return True if path is an existing regular file. This follows symbolic
        links, so both islink() and isfile() can be true for the same path.
        """
        pass

    @abstractmethod
    def isdir(self, path):
        """
        Return True if path is an existing directory. This follows symbolic
        links, so both islink() and isdir() can be true for the same path.
        """
        pass

    @abstractmethod
    def islink(self, path):
        """
        Return True if path refers to a directory entry that is a symbolic
        link. Always False if symbolic links are not supported by the Python
        runtime.
        """
        pass

    @abstractmethod
    def get_dir_content( dirPath, virtualPath, onlyPats=None, ignorePats=None, extra={} ):
        """
        Utility method returning list in form:
        [
            {
                'folder' : <pathPair>,
                'files' : [<filename1>, <filename2>, ...]
                'subFolders' : [ <dir1>, <dir2>, ... ],
                ...  # 
            },
            ...
        ]
        Will be invoked once by filesystem.discover_entries(). See additional
        information there.
        """
        pass

    @abstractmethod
    def uri_from_path(self, path, *args, **kwargs):
        pass

    @abstractmethod
    def del_file(self, path):
        """
        Has to delete file referenced by given URI.
        """
        pass

    @abstractmethod
    def cpy_file(self, srcURI, dstURI, backends={} ):
        """
        Has to copy file from srcURI location to dstURI location using given
        backends.
        """
        pass

    def rewrite_file(self, srcURI, dstURI, backends={}):
        """
        This method may be used for incremental appending/patching the file
        (in case of size or checksum mismatch). The default implementation
        does not rely on appending/patching, but just deletes the copy and
        copies original instead of it.
        """
        origLPP = urlparse( srcURI )
        assert( (origLPP.scheme or 'file') in backends.keys() )
        delResult = self.del_file( dstURI )
        return delResult, self.cpy_file( srcURI, dstURI, backends=backends )

    def new_file( self, path, **kwargs):
        """
        Shall return new file instance created using backend functions.
        Additional keyword arguments will be forwarded directly to underlying
        sqlalchemy ctr.  The keyword arguments takes precedence on the ones
        obtained using back-end.

        The check for keyword arguments is strongly desirable since it allows
        to avoid redundant querying! The back-end has to automatically
        use the knownAttrs when they're given as keyword arguments.
        """
        kwd = dict(kwargs)
        sf = kwd.pop('syncFields', ['modified', 'size'])
        for k in sf:
            if k in kwd.keys():
                # Attribute was explicitly set.
                continue
            kwd[k] = getattr(self, 'get_' + k)(path)
        #print( '\nXXX:', kwd )  # XXX
        return File( **kwd )

    def new_folder( self, path, **kwargs ):
        """
        Returns new folder database entry object created using the backend
        functions. The keyword arguments takes precedence on the ones obtained
        using back-end.
        """
        kwd = dict(kwargs)
        for k in kwd.pop('syncFields', ['modified']):
            if k in kwd.keys():
                # Attribute was explicitly set.
                continue
            kwd[k] = getattr(self, 'get_' + k)(path)
        return Folder( **kwd )

#
# Local backend

def adler32( filename, blocksize=65536 ):
    checksum = zlib.adler32("")
    with open(filename, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            checksum = zlib.adler32(block, checksum)
    return checksum & 0xffffffff

class LocalBackend(AbstractBackend):
    """
    This implementation implies local filesystem operations only.
    The particular backend will be selected basing on string identifier
    usually prefixing the URIs.
    """
    __metaclass__ = BackendMetaclass
    __backendAttributes__ = {
            'scheme' : 'file'
    }

    def get_adler32(self, path):
        """Shall return the hexidecimal digest built from the file content."""
        #raise NotImplementedError('Checksum calculation for %s algorithm '
        #            'is not being implemented.'%algo)
        lpp = urlparse(path)
        return '%x'%adler32( lpp.path )

    def get_size(self, path):
        """Returns size (in bytes) of given file. Directories aren't supported."""
        lpp = urlparse(path)
        return os.path.getsize(lpp.path)
    
    def get_modified(self, path):
        lpp = urlparse(path)
        mt = os.path.getmtime(lpp.path)
        return datetime.datetime.fromtimestamp(mt)

    def set_modified(self, path, dtObject):
        raise NotImplementedError()

    def get_permissions(self, path):
        """Returns encoded permissions."""
        lpp = urlparse(path)
        return os.stat( lpp.path ).st_mode & 0o777

    def listdir(self, path):
        """
        Returns a list contining the names of the entries in the directory
        given by path.
        """
        lpp = urlparse(path)
        return os.listdir(lpp.path)

    def isfile(self, path):
        """
        Return True if path is an existing regular file. This follows symbolic
        links, so both islink() and isfile() can be true for the same path.
        """
        lpp = urlparse(path)
        return os.path.isfile(lpp.path)

    def isdir(self, path):
        """
        Return True if path is an existing directory. This follows symbolic
        links, so both islink() and isdir() can be true for the same path.
        """
        lpp = urlparse(path)
        return os.path.isdir(lpp.path)

    def islink(self, path):
        """
        Return True if path refers to a directory entry that is a symbolic
        link. Always False if symbolic links are not supported by the Python
        runtime.
        """
        path = urlparse(path)
        return os.path.islink(path)

    def del_file(self, path):
        raise NotImplementedError()  # TODO

    def cpy_file(self, srcURI, dstURI, backends={} ):
        raise NotImplementedError()  # TODO

    def get_dir_content(self, dirPath, onlyPats=None, ignorePats=None, extra={} ):
        # Get list of all files and sub-directories in current dir
        contentLst = [f for f in self.listdir(dirPath) \
            if self.isfile(os.path.join(dirPath, f)) \
                or self.isdir(os.path.join(dirPath, f))]
        if onlyPats:
            if type(onlyPats) is str:
                onlyPats = [onlyPats,]
            contentLst_ = []
            for wcard in onlyPats:
                contentLst_.extend( fnmatch.filter(contentLst, wcard) )
            contentLst = contentLst_
        if ignorePats:
            if type(ignorePats) is str:
                ignorePats = [ignorePats,]
            for wcard in ignorePats:
                contentLst = list(filter( lambda nm : \
                                    not fnmatch.fnmatch(nm, wcard), contentLst ))
        files = [ f for f in contentLst if self.isfile(os.path.join(dirPath, f)) ]
        subds = [ d for d in contentLst if self.isdir(os.path.join(dirPath, d)) ]
        ret = {
            'folder' : dirPath,
            'files' : set(files),
            'subFolders' : []
        }
        for subd in subds:
            ret['subFolders'].append(
                        self.get_dir_content( os.path.join(dirPath, subd),
                                        onlyPats=onlyPats, ignorePats=ignorePats) )
        ret.update(extra)
        return ret

    def uri_from_path(self, path):
        return 'file://' + path

