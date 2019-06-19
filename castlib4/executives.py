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
from castlib4.logs import gLogger

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from castlib4 import dbShim as DB
from castlib4.models import DeclBase
import yaml

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

