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

#from castlib3.castor.config import gCastor2Config  # TODO
from itertools import izip_longest
import os, datetime

"""
Miscellaneous utility routines.
"""

# Types below are designed to be used adjointly with argparse module.
# Thanks to:
# http://stackoverflow.com/questions/11415570/directory-path-types-with-argparse

class DirectoryType( object ):
    def __init__(self, access_):
        flags = []
        for c in access_:
            if 'r' == c:
                flags.append( os.R_OK )
            elif 'w' == c:
                flags.append( os.W_OK )
            elif 'x' == c:
                flags.append( os.X_OK )
            else:
                raise RuntimeError('Unknown access qualifier: `%c`'%c)
        self.access = access_
        self.accessFlags = flags

    def __call__(self, prospective_dir):
        if not os.path.isdir(prospective_dir):
            raise Exception("readable_dir:{0} is not a valid path".format(prospective_dir))
        for flag in self.accessFlags:
            if os.access(prospective_dir, flag):
                return prospective_dir
        else:
            raise Exception("readable_dir:{0} is not a `{1}` dir".format(prospective_dir, self.access))

class CastorDirectoryType( object ):
    def __init__(self, access_):
        self.access = access_

    def __call__(self, prospective_dir):
        from .parsing import parse_castor_directory_info  # TODO
        p = timeout_util_run('nsls-dir', {'dirPath':args.d},
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             timeout=gCastor2Config['timeouts']['shortSec'] )
        outP, errP = p.communicate()
        if not p.returncode:
            raise argparse.ArgumentTypeError('CASTOR directory \"%s\" does not exist.'%prospective_dir)

class ExtantFilePath( object ):
    def __init__(self, access_):
        flags = []
        for c in access_:
            if 'r' == c:
                flags.append( os.R_OK )
            elif 'w' == c:
                flags.append( os.W_OK )
            elif 'x' == c:
                flags.append( os.X_OK )
            else:
                raise RuntimeError('Unknown access qualifier: `%c`'%c)
        self.access = access_
        self.accessFlags = flags

    def __call__(self, prospectiveFilePath):
        if not os.path.isfile(prospectiveFilePath):
            raise Exception("{0} is not a file".format(prospectiveFilePath))
        for flag in self.accessFlags:
            if os.access(prospectiveFilePath, flag):
                return prospectiveFilePath
        else:
            raise Exception("{0} is not a `{1}` file".format(prospectiveFilePath, self.access))

def grouper(iterable, n, fillvalue=None):
    # http://stackoverflow.com/questions/434287/what-is-the-most-pythonic-way-to-iterate-over-a-list-in-chunks
    args = [iter(iterable)] * n
    return izip_longest(*args, fillvalue=fillvalue)

class ClassPropertyDescriptor(object):
    """
    A "class property" implementation taken from:
    http://stackoverflow.com/questions/5189699/how-can-i-make-a-class-property-in-python
    """
    def __init__(self, fget, fset=None):
        self.fget = fget
        self.fset = fset

    def __get__(self, obj, klass=None):
        if klass is None:
            klass = type(obj)
        return self.fget.__get__(obj, klass)()

    def __set__(self, obj, value):
        if not self.fset:
            raise AttributeError("can't set attribute")
        type_ = type(obj)
        return self.fset.__get__(obj, type_)(value)

    def setter(self, func):
        if not isinstance(func, (classmethod, staticmethod)):
            func = classmethod(func)
        self.fset = func
        return self    

def classproperty(func):
    if not isinstance(func, (classmethod, staticmethod)):
        func = classmethod(func)
    return ClassPropertyDescriptor(func)

def pid_exists(pid):
    """
    Check whether pid exists in the current process table.
    UNIX only.
    """
    if pid < 0:
        return False
    if pid == 0:
        # According to "man 2 kill" PID 0 refers to every process
        # in the process group of the calling process.
        # On certain systems 0 is a valid PID but we have no way
        # to know that in a portable fashion.
        raise ValueError('invalid PID 0')
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            # ESRCH == No such process
            return [False, errno.ESRCH]
        elif err.errno == errno.EPERM:
            # EPERM clearly means there's a process to deny access to
            return [True, errno.EPERM]
        else:
            # According to "man 2 kill" possible error values are
            # (EINVAL, EPERM, ESRCH)
            raise
    else:
        return True

def truncate_timestamp( origTimestamp ):
    """
    Note: CASTOR ignores seconds, so we need here to truncate local timestamp
    to seconds.
    """
    return int(datetime.datetime.fromtimestamp(origTimestamp) \
                            .replace(second=0).strftime('%s'))

