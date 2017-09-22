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
import unittest

from castlib3.models import DeclBase
from castlib3.models.filesystem import Folder, File

class TestFilesystemModel(unittest.TestCase):
    def setUp(self):
        from sqlalchemy import create_engine
        #engine = create_engine('sqlite:///test.sqlite')
        self.engine = create_engine('sqlite:///:memory:')
        from sqlalchemy.orm import Session
        self.session = Session(self.engine)
        #session.configure(bind=engine)
        DeclBase.metadata.create_all(self.engine)

    def test_orphans_deletion(self):
        """
        This will create the following struct:
            root
             |~ folder-1
             |~ folder-2
             |  |~ folder-3
             |  |  `- three
             |  |- two
             |  `- one
             `- one
        and store it at the DB.
        Then the `folder-2' will be deleted and the test will check that all
        its subsequent folders and files are removed as well.
        """
        rootNode = Folder(name='root')
        Folder(name='folder-1', parent=rootNode)
        Folder(name='folder-2', parent=rootNode)
        File(  name='one',    parent=rootNode )
        File(  name='two',    parent=rootNode.children['folder-2'] )
        File(  name='one',    parent=rootNode.children['folder-2'] )
        Folder(name='folder-3', parent=rootNode.children['folder-2'])
        File(  name='three',  parent=rootNode.children['folder-2'].children['folder-3'] )

        self.session.add(rootNode)
        self.session.commit()

        # Test association:
        folderNames = ['folder-1', 'folder-2', 'folder-3', 'root']
        for folder in self.session.query( Folder.name ).order_by(Folder.name):
            self.assertTrue( folder.name in folderNames )
        fileNames = ['one', 'two', 'three']
        for file_ in self.session.query( File.name ).order_by( File.name ):
            self.assertTrue( file_.name in fileNames )

        f3 = self.session.query(File).filter(File.name=='three').one()
        p3 = '/'.join([a.name for a in f3.mp.query_ancestors().all()])
        self.assertEquals( p3, 'root/folder-2/folder-3' )

        # Delete `folder-2'
        self.session.delete( rootNode.children['folder-2'] )

        self.session.add(rootNode)
        self.session.commit()

        # Test association:
        folderNames = ['folder-1', 'root']
        for folder in self.session.query( Folder ).order_by(Folder.name):
            self.assertTrue( folder.name in folderNames )
        fileNames = ['one']
        for file_ in self.session.query( File ).order_by( File.name ):
            self.assertTrue( file_.name in fileNames )

    def tearDown(self):
        self.session.close()

if __name__ == "__main__":
    unittest.main()

