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

__version__ = '0.4.0'

import sys, logging

from celery import Celery
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from castlib4.models import DeclBase

def _initialize_database( engineCreateArgs,
                          engineCreateKWargs={'echo' : True} ):
    """
    The database initialization function that has to be invoked prior to any
    database operation. Sets up SQLAlchemy's machinery according to castlib2
    needs: creates the engine, composes tables metadata and initializes
    session.

    Note, that this function is somewhat default implementation intended for
    urgent initialization of the database, if no instance was initialized
    before. For orderly initialization process use initialize_database()
    function from the castlib4.executives module.
    """
    engine = create_engine(*engineCreateArgs, **engineCreateKWargs)
    dbS = scoped_session(sessionmaker(
                        bind=engine,
                        autocommit=False,
                        autoflush=False))
    DeclBase.metadata.create_all(engine)
    return engine, dbS

class _DatabaseShim(object):
    def __init__(self):
        self._engine = None
        self._session = None

    def __getattr__(self, key):
        if 'session' == key:
            return self.get_session()
        raise AttributeError("Class %s has no attribute \"%s\"."%(self.__class__, key))

    def get_session(self):
        L = logging.getLogger(__name__)
        if self._session is None:
            L.debug( "No extra configuration provided for database."
                    " Initializing instance with default parameters." )
            self._engine, self._session = _initialize_database(
                        ["sqlite:///castlib4-database.sqlite"],
                        engineCreateKWargs={
                                'encoding' : 'utf8',
                                'convert_unicode' : True,
                                'echo' : True
                            })
        return self._session

    def set_engine(self, engine):
        self._engine = engine

    def set_session(self, session):
        self._session = session

    # Note: (ret, ), = DB.session.query(exists().where( Folder.name==virtualDirName ))
    # is useful for fast check for entry to exist. See SO question:
    # https://stackoverflow.com/questions/6587879/how-to-elegantly-check-the-existence-of-an-object-instance-variable-and-simultan

    def get_or_create(self, model, **kwargs):
        session = kwargs.pop('_session', self.session)
        instance = session.query(model).filter_by(**kwargs).first()
        if instance:
            return instance, False
        else:
            instance = model(**kwargs)
            session.add(instance)
            session.commit()
            return instance, True

setattr( sys.modules[__name__], 'queue', Celery('cstl4') )
setattr( sys.modules[__name__], 'dbShim', _DatabaseShim() )

