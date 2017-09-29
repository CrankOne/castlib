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
Various auxilliary subroutines ususally used by executable code.
"""

import os

from urlparse import urlparse

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from castlib3 import dbShim as DB
from castlib3.models import DeclBase
from castlib3.backend import gCastlibBackends

from collections import OrderedDict

from sqlalchemy.engine import Engine
from sqlalchemy import event

try:
    from sVresources import yaml
except ImportError:
    import yaml

#@event.listens_for(Engine, "connect")
#def set_sqlite_pragma(dbapi_connection, connection_record):
#    cursor = dbapi_connection.cursor()
#    cursor.execute("pragma journal_mode=OFF")
#    cursor.execute("pragma synchronous=OFF")
#    cursor.execute("pragma cache_size=100000")
#    cursor.close()

def initialize_database( engineCreateArgs,
                         engineCreateKWargs={},
                         autocommit=False,
                         autoflush=False):
    """
    Database initialization routine that shall be called prior to any database
    operation. Will configure CastLib package-wide database instance using
    given configuration. args/kwargs pair will be directly forwarded to 
    sessionmaker ctr. Other arguments in function signature is reserved for
    future use.
    """
    engine = create_engine(*engineCreateArgs, **engineCreateKWargs)
    dbS = scoped_session(sessionmaker(
                        bind=engine,
                        autocommit=autocommit,
                        autoflush=autoflush))
    DB.set_engine(engine)
    DB.set_session(dbS)
    DeclBase.metadata.create_all(engine)

def initialize_backends( schemes, cfg ):
    backends = {}
    for scheme in schemes:
        if scheme in cfg.keys():
            if cfg[scheme]:
                backends[scheme] = gCastlibBackends[scheme](**cfg[scheme])
            else:
                backends[scheme] = gCastlibBackends[scheme]()
    return backends

def ordered_load(stream, Loader=yaml.Loader, object_pairs_hook=OrderedDict):
    """
    YMAL loader function preserving order of dictionary entries. Usually used
    for task configs parsing (stages have to follow the order).
    """
    class OrderedLoader(Loader):
        pass
    def construct_mapping(loader, node):
        loader.flatten_mapping(node)
        return object_pairs_hook(loader.construct_pairs(node))
    OrderedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        construct_mapping)
    return yaml.load(stream, OrderedLoader)

def discover_locations(locs):
    """
    Takes either the path to yaml config describing locations, or just path
    to local directory. Returns directories dict suitable for further use
    within castlib3.filesystem.discover_entries().
    """
    directories = {}
    lpp = urlparse(locs)
    if 'file' == lpp.scheme or '' == lpp.scheme:
        if os.path.isdir( locs ):
            directories[os.path.basename(locs)] = {
                    'localPath' : 'file:/' + os.path.abspath(locs),
                }
        elif os.path.isfile( locs ):
            with open(locs) as f:
                directories = yaml.load(f)
        else:
            raise RuntimeError('Argument \"%s\" is neither a file nor a directory " \
                "path.'%locs )
    else:
        raise NotImplementedError('Other than "file://" scheme are not '
                'supported yet.')
    return directories
