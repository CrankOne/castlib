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

"""
File contains miscellaneous routines for working with system utils via shell.
"""

import os, subprocess, threading, signal
import re, select, sys
from string import Formatter
from castlib3.syscfg import gConfig
from castlib3.logs import gLogger

class TimeoutError( RuntimeError ):
    """
    Custom class to denote a timeout error.
    """
    def __init__(self, *args, **kwargs):
        super(TimeoutError, self).__init__(*args, **kwargs)


class TimeoutPopen(subprocess.Popen):
    """
    As we have not the timeout feature in Python 2.6 Popen(), we implemented it.
    """
    def __init__(self, *args, **kwargs):
        timeout = kwargs.pop('timeout', 'default')
        dry = kwargs.pop('popenDry', False)
        self.timeoutInterruptFlag = False
        if type(timeout) is str:
            if 'default' == timeout or 'short' == timeout:
                self.timeoutSecs = gConfig['timeouts']['shortSec']
            elif 'long' == timeout:
                self.timeoutSecs = gConfig['timeouts']['longSec']
            else:
                raise RuntimeError( "Couldn't interpret the timeout specifier "
                    "\"%s\"."%timeout )
        elif type(timeout) is int and timeout > 0:
            self.timeoutSecs = timeout
        else:
            raise TypeError('"timeout" argument must be either of string value '
                    '=[short|long] or integer.')
        if timeout:
            self.timer = threading.Timer(self.timeoutSecs,
                                         self.terminate_as_timeout_expired)
            self.timer.daemon = True
            self.timer.start()
        else:
            self.timer = None
        kwargs.update({'preexec_fn' : os.setsid})
        self.poArgs = list(args)
        self.poKwargs = dict(kwargs)
        gLogger.debug( 'subprocess.Popen() args: args=' + str(args) \
                        + 'kwargs=' + str(kwargs) )
        if not dry:
            super(self.__class__, self).__init__(self.poArgs, bufsize=0, **kwargs)

    def communicate(self, *args, **kwargs):
        out, err = super(TimeoutPopen, self).communicate(*args, **kwargs)
        if self.timeoutInterruptFlag:
            raise TimeoutError("Process timeout expired (%d sec)." \
                    "Arguments: args=%r, kwargs=%s" %(self.timeoutSecs, self.poArgs, self.poKwargs) )
        else:
            if self.timer:
                self.timer.cancel()
        return out, err

    def terminate_as_timeout_expired(self):
        self.timeoutInterruptFlag = True
        os.killpg(self.pid, signal.SIGKILL)


class CyclicFormatter( Formatter ):
    """
    Class performing the expansion of expressions of the form
        {#<name> ... # ... }
    Such an expression will cause formatter to look up among named format()
    arguments for a one named by given `name` and then substitute this
    expression with substring "... # ..." where `#` stands for item in a list.
    """
    cyclicSubstRx = re.compile(r'^#<(?P<key>\w+)>(?P<expr>.+)$')
    def get_field( self, field_name, *args, **kwargs ):
        m = self.cyclicSubstRx.match( field_name )
        if m:
            tList = args[1][m.group('key')]
            assert( type(tList) is list )
            # (obj, used_key)
            retStr = ''
            for listItem in tList:
                retStr += m.group('expr').replace('#', str(listItem) )
            return (retStr, m.group('key'))
        return super(self.__class__, self).get_field(field_name, *args, **kwargs)

def format_cmdline_args( fmt, *args, **kwargs ):
    """
    Basically, behaves just like standard python str.format() method with only
    difference for special expressions of form {#<name> ... # ... }.
    Expressions of such a form will be expanded by CyclicFormatter class.
    """
    assert(fmt)
    return CyclicFormatter().format( fmt, *args, **kwargs )

def invoke_util( name,
                 expectedReturnCode=0,
                 noexcept=False,
                 applyRegexOn='stdout',
                 regexToApply=None,
                 communicate=True,
                 *args, **kwargs ):
    """
    This routine will look up among gConfig['utils'] for name provided
    'n name parameter. If no entry found, the `name' will be splitted and
    forwarded to TimeoutPopen constructor as is. The *args and **kwargs
    parameters will be also forwarded except those listed below.

    If util will be found, the routine expects to find in config object either
    the command line template either the pair of template and regex to be
    applied on stdout.

    @param name --- name of shell command to be invoked.
    @param expectedReturnCode --- integer value to be expected upon shell
    process termination.
    @param noexcept --- if False is provided, exception will not be arised if
    expectedReturnCode does not match to process return code.
    @param applyRegex --- possible values is None, 'stdout', 'stderr'. If
    None is provided, the list of [returnCode, stdoutString, stderrString]
    will be returned.
    @param communicate --- if False, the child's stderr/stdout will be printed
    into terminal as well as stored to string objects.
    """
    popenArgs = []
    popenKwArgs = {}

    assert( not applyRegexOn \
            or 'stdout' == applyRegexOn \
            or 'stderr' == applyRegexOn )
    utilEntry = gConfig['utils'].get( name, None )
    if not utilEntry:
        # Treat it like it is ordinary popen call: just forward args/kwargs
        # to the popen call doing other things as usual.
        popenArgs = list(args)
        popenKwArgs = dict(kwargs)
    else:
        # That's a predefined castlib3' util:
        utilStr = None
        if type(utilEntry) is tuple:
            utilStr = utilEntry[0]
            if not regexToApply and applyRegexOn:
                regexToApply = utilEntry[1]
        elif type(utilEntry) is str:
            utilStr = utilEntry
        popenArgs = format_cmdline_args( utilStr, *args, **kwargs ).split()
        # popenKwArgs = {}
    popenDry = kwargs.get( 'popenDry', False )
    if popenDry:
        popenKwArgs['popenDry'] = True
    if 'timeout' in kwargs.keys():
        popenKwArgs['timeout'] = kwargs['timeout']
    gLogger.debug( 'popenArgs=%r popenKwArgs=%r'%(popenArgs, popenKwArgs) )
    p = None
    try:
        p = TimeoutPopen( stdout=subprocess.PIPE,
                      stderr=subprocess.PIPE,
                      *popenArgs, **popenKwArgs )
    except OSError as e:
        gLogger.exception(e)
        gLogger.error( 'TimeoutPopen arguments: args=%r, kwargs=%r'%(popenArgs, popenKwArgs) )
    if popenDry:
        return
    stdoutStr, stderrStr = '', ''
    if communicate:
        stdoutStr, stderrStr = p.communicate()
    else:
        # This code is kinda tricky. Performs real-time forwarding the child's
        # stdout/stderr to the terminal as well as saving it to strings.
        # tnx to: http://stackoverflow.com/questions/12270645/can-you-make-a-python-subprocess-output-stdout-and-stderr-as-usual-but-also-cap
        # TODO: does not work for single line output (like xrdcp's progressbar).
        # I believe it is because of line-buffering of pty. Suggest other
        # alternatives: http://stackoverflow.com/questions/12419198/python-subprocess-readlines-hangs
        while True:
            reads = [p.stdout.fileno(), p.stderr.fileno()]
            ret = select.select(reads, [], [])
            for fd in ret[0]:
                if fd == p.stdout.fileno():
                    read = p.stdout.read()  #p.stdout.readline()
                    sys.stdout.write( read )
                    stdoutStr += read
                if fd == p.stderr.fileno():
                    read = p.stderr.read()  #p.stderr.readline()
                    sys.stderr.write( read )
                    stderrStr += read
            if p.poll() != None:
                break
    returnCode = p.returncode
    gLogger.debug( 'Last subprocess.Popen.communicate() finished with rc=' + \
                    str(returnCode) )

    # Check result and raise exception if something unexpectedly went wrong:
    if expectedReturnCode != returnCode and not noexcept:
        s  = 'invoke_util(): code=%d!=%d, stderr="%s", stdout="%s"\n'%(returnCode,
                                                    expectedReturnCode,
                                                    stderrStr,
                                                    stdoutStr)
        s += 'last communicate() call: %s, %s\n'%(str(popenArgs),
                                                  str(popenKwArgs))
        raise RuntimeError( stderrStr + '\n' + s )

    # Otherwise, if there is nothing to preprocess return the resulting triplet
    if not applyRegexOn or not regexToApply:
        return returnCode, stdoutStr, stderrStr

    # Preprocess:
    if type(regexToApply) is str:
        regexToApply = re.compile(regexToApply)
    strcheck = stdoutStr
    if 'stderr' == applyRegexOn:
        strcheck = stderrStr
    return [m.groupdict() for m in regexToApply.finditer(strcheck)]

