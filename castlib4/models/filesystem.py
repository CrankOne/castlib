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

import os

from sqlalchemy import Column, ForeignKey, String, Integer, DateTime
from sqlalchemy.orm import relationship, Session
from sqlalchemy.orm.collections import attribute_mapped_collection

from castlib4.models import DeclBase
from castlib4.logs import gLogger

from urlparse import urlunsplit

# See for neat example of recursive adjacency list:
# https://github.com/zzzeek/sqlalchemy/blob/master/examples/adjacency_list/adjacency_list.py

class UpdatingMixin(object):
    def update_fields(self, **kwargs):
        updated = False
        mappings = kwargs.pop('_attrMappings', None)
        for k, v in kwargs.iteritems():
            if mappings and k in mappings.keys():
                k = mappings[k]
            av = getattr( self, k )
            tv = v
            if av is not None:
                if type(av) is not type(tv):
                    try:
                        tv = type(av)(tv)
                    except TypeError:
                        gLogger.error( 'Expected type: %s, given: %s.'%( type(av), type(tv) ) )
                        raise
            if av != tv:
                setattr(self, k, v)
                updated = True
        return updated

class FSEntry(DeclBase, UpdatingMixin):
    """
    Base filesystem entry
    """
    __tablename__ = 'sync_entries'
    __mp_manager__ = 'mp'
    __mp_parent_id_field__ = 'parentID'

    id = Column(Integer, primary_key=True)
    parentID = Column(Integer, ForeignKey('folders.id'))

    type = Column(String(1))
    name = Column(String, nullable=False)

    modified = Column(DateTime)

    __mapper_args__ = {
        'polymorphic_identity' : 'sync_entries',
        'polymorphic_on' : type
    }

    def __repr__(self):
        return "FSEntry(name=%r, id=%r, parent_id=%r)" % (
            self.name, self.id, self.parentID )

    def dump(self, indent_):
        return '| '*indent_ + '|' + repr(self)

    #def as_dict(self):
    #    # Consider this variant. Cleaner, but doesn't work as expected:
    #    #return {c.name: unicode(getattr(self, c.name)) for c in self.__table__.columns}
    #    ret = {}
    #    for k, v in self.__dict__.iteritems():
    #        if not k.startswith('_'):
    #            ret[k] = v
    #    return ret

    def get_uri(self):
        path = self.mp.query_ancestors().all()
        root = path[0]
        path = [p.name for p in path]
        path.append(self.name)
        scheme, netloc = None, None
        if type(root) is RemoteFolder:
            scheme = root.node.scheme
            netloc = root.node.identifier
        return urlunsplit((scheme or 'file', netloc or '', os.path.join(*path), '', ''))


class Folder(FSEntry, UpdatingMixin):
    """
    Model representing a local directory. Named `Folder' to avoid possible
    disambiguations.
    """
    __tablename__ = 'folders'
    id = Column(Integer, ForeignKey(FSEntry.id), primary_key=True)
    children = relationship(
            "FSEntry",
            backref='parent',
            cascade="all, delete-orphan",
            collection_class=attribute_mapped_collection('name'),
            primaryjoin='Folder.id==FSEntry.parentID'
        )

    __mapper_args__ = {
        'polymorphic_identity' : 'd',
        'inherit_condition': id == FSEntry.id
    }

    def __str__(self):
        return "%r {id=%r, parent_id=%r}" % (
            self.name, self.id, self.parentID )

    def dump(self, _indent=0):
        # todo: do it fancier
        s = "| "*_indent + '|~' + str(self) + "".join([
                    "\n" + c.dump(_indent + 1)
                    for c in self.children.values()
                ])
        return s


class File(FSEntry, UpdatingMixin):
    __tablename__ = 'files'
    #__mp_manager__ = 'mp'
    id = Column(Integer, ForeignKey('sync_entries.id'), primary_key=True)

    size = Column( Integer )
    adler32 = Column( String )
    # TODO: creation date?
    # TODO: last sync date?
    # TODO: local path?
    # TODO: md5 checksum?

    def __str__(self):
        return "-%r: {id=%r, parent_id=%r}" % (
            self.name, self.id, self.parentID )

    __mapper_args__ = {
        'polymorphic_identity' : 'f',
    }

class StoragingNode( DeclBase, UpdatingMixin ):
    """
    This model refers to particular remote instance, accessible or not from
    resources server. It may refer to localhost (file:// scheme),
    CASTOR (castor://), EOS (eos://) or whatever else. Reaching the particular
    file will be defined by back-end procedures.
    Is in the one-to-many relationship with RemoteFolder owning the
    locations.
    """
    __tablename__ = 'storaging_nodes'
    # Ususally a hostname
    identifier = Column(String, primary_key=True)
    # nfs, castor, file, whatever
    scheme = Column(String, nullable=False)

    locations = relationship( 'RemoteFolder', back_populates='node')

class RemoteFolder( Folder, UpdatingMixin ):
    """
    Model representing remote folder keeping a bunch of chunks. Always
    associated with single StoragingNode.
    """
    __tablename__ = 'remote_locations'

    id = Column(Integer, ForeignKey(Folder.id), primary_key=True)

    nodeID = Column(String, ForeignKey('storaging_nodes.identifier'))
    node = relationship( 'StoragingNode', back_populates='locations' )

    path = Column(String)

    __mapper_args__ = {
        'polymorphic_identity' : 'r',
    }

class ExpiringEntry( File ):
    """
    An (local) filesystem entries that have expiration date. They're usually
    a temporary ones.
    """
    __tablename__ = 'expiration'
    fsOriginalID = Column( Integer, ForeignKey(FSEntry.id) )
    expiration = Column( DateTime )

    __mapper_args__ = {
        'polymorphic_identity' : 'e',
    }

