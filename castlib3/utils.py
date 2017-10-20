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
import os, datetime, re
from dateutil.relativedelta import relativedelta
from dateutil import parser as datetimeParser


"""
Miscellaneous utility routines.
"""

timeDurationUnits = [
        r'second', r'minute', r'hour', r'day', r'week', r'month', r'year'
    ]

T_rxsTimeDurationRegex = r'((?P<%s>\d+)%ss?)'
rxTimeDurUnitsList = [re.compile(T_rxsTimeDurationRegex%(u, u)) for u in timeDurationUnits]

rxsTimeDurExpr = r'^((\d+\s*(%s)s?[,\s]*))+$'%('|'.join(timeDurationUnits))
rxTimeDurExpr = re.compile( rxsTimeDurExpr )

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


def human_readable_time( deltaSecs ):
    delta = relativedelta(seconds=deltaSecs)
    attrs = ['years', 'months', 'days', 'hours', 'minutes', 'seconds']
    return ', '.join( ['%d %s'%( getattr(delta, attr), getattr(delta, attr) > 1 and attr or attr[:-1] )
                        for attr in attrs if getattr(delta, attr)] )


def timedelta_from_human_readable( delta ):
    """
    Accepts time delta in form:
        "2weeks, 1day"
    Returns dateutil.relativedelta object. Possible units are listed above, in
    `timeDurationUnits' list and corresponds to dateutil.relativedelta ctr
    keyword arguments: seconds, minutes, hours, days, weeks, months, years.
    Both, plural and singular forms are supported (i.e. "2week" is equivalent
    to "2weeks").
    """
    # Tokenize string and trim whitespaces:
    deltaLst = [s.strip() for s in delta.split(',')]
    # Convert list of strings to list of list of matches:
    deltaLst = [ map( lambda rx : rx.match(e), rxTimeDurUnitsList ) for e in deltaLst ]
    # Keep only true matches:
    deltaLst = [ filter( lambda m : m, e ) for e in deltaLst ]
    # (diag) Check that all the tokens matches to single regex in list:
    erroneousTokens = filter( lambda ms : len(ms) != 1, deltaLst )
    if erroneousTokens:
        raise RuntimeError( 'Tokens can not be parsed within HR time '
                'delta: "%s".'%('", "'.join(erroneousTokens)) )
    deltaLst = map( lambda ms : ms[0], deltaLst )
    # Converge array of dicts into one dict:
    kwargs = reduce( lambda kw, m : kw.update(m.groupdict()) or kw
                   , [{},] + deltaLst )
    # (diag) Check that all the tokens refer to different time values (i.e.
    # there are no repetitions, for instance "1days, 2hours, 3days").
    if len(kwargs) != len(deltaLst):
        raise RuntimeError('Time delta string contains repeatitions: %r.'%delta)
    # Turn singular keys naming into the plural by adding 's' to the key string:
    kwargs = dict( (k + 's' if not k.endswith('s') else k, v) for k,v in kwargs.items() )
    return dateutil.relativedelta( **kwargs )


def expiration_to_datetime( expiration ):
    """
    Accepts values in form:
        ::= <int>
          | <int> ( 'seconds'|'minutes?'|'hours?'|'days?'|'months?'|'years?' )
          | <datetime.timedelta>
          | <datetime.datetime>
          ;
    returning datetime object. If argument is given as a time interval, returns
    the expiration time from now. If argument is of integer type, assumes it is
    being given in seconds.
    """
    if type(expiration) is datetime.datetime:
        return expiration

    if type(expiration) is str:
        if rxTimeDurExpr.match(expiration):
            # returns timedelta from HR interval expression (e.g. "1week, 2hours"):
            expiration = timedelta_from_human_readable( expiration )
        else:
            # returns particular time from HR expression:
            expiration = datetimeParser.parse( expiration )
    if type(expiration) is datetime.timedelta:
        expiration = datetime.datetime.now() + expiration
    if type(expiration) is int:
        expiration = datetime.datetime.fromtimestamp(expiration)
    if type(expiration) is not datetime.datetime:
        raise RuntimeError( 'Unable to convert %r object of type %s to '
                'datetime.'%( expiration, type(expiration) ) )
    return expiration

