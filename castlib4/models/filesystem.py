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

import os, logging, datetime

from sqlalchemy import Column, ForeignKey, String, Integer, DateTime
from sqlalchemy.orm import relationship, Session
from sqlalchemy.orm.collections import attribute_mapped_collection

from castlib4.models import DeclBase
from castlib4 import dbShim as DB

#from urlparse import urlunsplit
# See for neat example of recursive adjacency list:
# https://github.com/zzzeek/sqlalchemy/blob/master/examples/adjacency_list/adjacency_list.py

FS_PI_UNKNOWN = 0
FS_PI_FILE = 1
FS_PI_FOLDER = 2

class FSMappings(object):
    """
    Defines conversion rules from validated local filesystem entries data into
    the ones available for ORM during update with
    `UpdatingMixin.update_fields()'.
    """

    def __init__(self, node):
        self.node = node

    def owner(self, ownerDescr):
        group, groupCreated = DB.get_or_create( OwningGroup
                                              , name=ownerDescr['group'][0]
                                              , gid=ownerDescr['group'][1]
                                              , node=self.node )
        user, userCreated = DB.get_or_create( OwningUser
                                            , name=ownerDescr['user'][0]
                                            , uid=ownerDescr['user'][1]
                                            , group=group )
        if groupCreated:
            DB.session.add( group )
        if userCreated:
            DB.session.add( user )
        return user, groupCreated or userCreated

class UpdatingMixin(object):
    """
    Performs recursive updating of the fields within an entry w.r.t. given
    attributes.
    """
    def update_fields(self, **kwargs):
        L = logging.getLogger(__name__)
        updated = False
        # Try to retrieve the special "mappings" dictionary, matching the
        #   <given name> -> <model attribute>
        mappings = kwargs.pop('_transformAttributes', None)
        for k, v in kwargs.items():
            if k.startswith('@'): continue  # omit service fields
            try:
                # If conversion rule is defined, use it over the value.
                if hasattr(mappings, k):
                    v, updated = getattr(mappings, k)(v)
                # Get model's value:
                mv = getattr( self, k )
                if mv != v:
                    setattr(self, k, v)
                    updated |= True
            except Exception as e:
                L.error("While check on the \"%s\" property:"%k)
                L.exception(e)
            # TODO: check updating logic and refresh the `lastUpdateTime'
            # if necessary.
        return updated

class OwningGroup(DeclBase):
    """
    Remote or local user group identifier.
    """
    __tablename__ = 'fs_user_group'
    id = Column(Integer, primary_key=True)  # NOTE: this ID is NOT a GID
    name = Column(String)
    gid = Column(Integer)
    users = relationship('OwningUser', back_populates='group')
    nodeID = Column(String, ForeignKey('storaging_nodes.identifier'))
    node = relationship( 'Node', back_populates='ownerGroups' )

class OwningUser(DeclBase):
    """
    Remote or local user identifier.
    """
    __tablename__ = 'fs_user_entry'
    id = Column(Integer, primary_key=True)  # NOTE: this ID is NOT a UID
    name = Column(String)
    uid = Column(Integer)
    groupID = Column(Integer, ForeignKey('fs_user_group.id'))
    group = relationship('OwningGroup', back_populates='users')

    def as_dict(self):
        return {
                'group' : ( self.group.name, self.group.gid ),
                'user' : ( self.name, self.uid )
            }

class FSEntry(DeclBase, UpdatingMixin):
    """
    Baseic filesystem entry model.
    """
    __tablename__ = 'fs_sync_entries'
    __mp_manager__ = 'mp'
    __mp_parent_id_field__ = 'parentID'

    id = Column(Integer, primary_key=True)
    parentID = Column(Integer, ForeignKey('folders.id'))

    type = Column(Integer)
    name = Column(String, nullable=False)
    permissions = Column(Integer)

    ownerID = Column(Integer, ForeignKey('fs_user_entry.id'))
    owner = relationship("OwningUser")

    # corresponds to the modification time
    mtime = Column(DateTime)
    # when the entry was created in DB
    imposedTime = Column(DateTime, default=datetime.datetime.utcnow)
    # when was updated last time
    lastUpdateTime = Column(DateTime, default=datetime.datetime.utcnow)
    # when was synchronized last time
    lastSyncTime = Column(DateTime, default=datetime.datetime.utcnow)

    __mapper_args__ = {
        'polymorphic_identity' : FS_PI_UNKNOWN,
        'polymorphic_on' : type
    }

    def __repr__(self):
        return "FSEntry(name=%r, id=%r, parent_id=%r)" % (
            self.name, self.id, self.parentID )

    def dump(self, indent_):
        return '| '*indent_ + '|' + repr(self)

    def as_dict(self):
        return {
                'permissions' : self.permissions,
                'owner' : self.owner.as_dict() if self.owner else None,
                'mtime' : self.mtime.timestamp() if self.mtime else None,
                '@imposed' : self.imposedTime.timestamp() if self.imposedTime else None,
                '@updated' : self.lastUpdateTime.timestamp() if self.lastUpdateTime else None,
                '@synced' : self.lastSyncTime.timestamp() if self.lastSyncTime else None
            }

    def get_uri(self):
        path = self.mp.query_ancestors().all()
        root = path[0]
        path = [p.name for p in path]
        path.append(self.name)
        scheme, netloc = None, None
        if type(root) is RootFolder:
            raise NotImplementedError()  # TODO
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
        'polymorphic_identity' : FS_PI_FOLDER,
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

    def sync_substruct( self
                      , entries
                      , recursive=False
                      , _transformAttributes=None ):
        """
        General method for recursive syncing of the filesystem structure
        database image.
        """
        updated = False
        for entryName, entryDescr in entries.items():
            isDir = '@content' in entryDescr.keys()
            e, eCreated = DB.get_or_create( Folder if isDir else File
                                          , name=entryName
                                          , parent=self )
            updated = eCreated or e.update_fields(
                    _transformAttributes=_transformAttributes, **entryDescr )
            if isDir and recursive:
                updated |= e.sync_substruct( entryDescr['@content']
                                , recursive=recursive
                                , _transformAttributes=_transformAttributes )
        return updated

    def as_dict(self, recursive=False, emptyContent=False):
        ret = super(Folder, self).as_dict()
        if emptyContent:
            ret['@content'] = None
            return ret
        ret['@content'] = {}
        for se in self.children.values():
            if type(se) is File:
                ret['@content'][se.name] = se.as_dict()
            elif type(se) is Folder:
                if recursive:
                    ret['@content'][se.name] = se.as_dict(recursive=recursive)
                else:
                    ret['@content'][se.name] = se.as_dict(recursive=recursive, emptyContent=True)
        return ret

class File(FSEntry, UpdatingMixin):
    __tablename__ = 'files'
    #__mp_manager__ = 'mp'
    id = Column(Integer, ForeignKey('fs_sync_entries.id'), primary_key=True)

    size = Column( Integer )
    adler32 = Column( String )
    # TODO: other, more reliable hashsums?

    def __str__(self):
        return "-%r: {id=%r, parent_id=%r}" % (
            self.name, self.id, self.parentID )

    __mapper_args__ = {
        'polymorphic_identity' : FS_PI_FILE,
    }

    def as_dict(self):
        ret = super(File, self).as_dict()
        ret.update({
                'size' : self.size,
                'adler32' : self.size
            })
        return ret

class Node( DeclBase, UpdatingMixin ):
    """
    This model refers to particular remote instance, accessible or not from
    resources server. It may refer to localhost (file:// scheme),
    CASTOR (castor://), EOS (eos://) or whatever else.

    Is in the one-to-many relationship with RemoteFolder owning the
    locations.
    """
    __tablename__ = 'storaging_nodes'
    # Ususally a hostname
    identifier = Column(String, primary_key=True)
    locations = relationship( 'RootFolder', back_populates='node')
    ownerGroups = relationship( 'OwningGroup', back_populates='node' )

class RootFolder( Folder, UpdatingMixin ):
    """
    Starting point for filesystem subtree being monitored. Ususally, identified
    by some path on the particular node.
    """
    __tablename__ = 'remote_locations'
    id = Column(Integer, ForeignKey(Folder.id), primary_key=True)
    nodeID = Column(String, ForeignKey('storaging_nodes.identifier'))
    node = relationship( 'Node', back_populates='locations' )
    path = Column(String)
    __mapper_args__ = {
        'polymorphic_identity' : 'r',
    }

#class ExpiringEntry( File ):
#    """
#    An (local) filesystem entries that have expiration date. They're usually
#    a temporary ones.
#    """
#    __tablename__ = 'expiration'
#    fsOriginalID = Column( Integer, ForeignKey(FSEntry.id) )
#    expiration = Column( DateTime )
#
#    __mapper_args__ = {
#        'polymorphic_identity' : 'e',
#    }

