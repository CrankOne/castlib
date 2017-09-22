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
from castlib3.models.na64 import Run, Chunk
from castlib3.models.filesystem import Folder, File
from castlib3.stages.rip_na64_html import parse_cdr_filename

exampleStrings = {
    ('cdr01001-002901.dat', 2901, 1),
    ('cdr01001-002900.dat', 2900, 1),
    ('cdr01003-002899.dat', 2899, 3),
    ('cdr01002-002871.dat', 2871, 2),
    ('cdr01001-002871.dat', 2871, 1)
}

class TestFilesystemModel(unittest.TestCase):
    def setUp(self):
        from sqlalchemy import create_engine
        #engine = create_engine('sqlite:///test.sqlite')
        self.engine = create_engine('sqlite:///:memory:')
        from sqlalchemy.orm import Session
        self.session = Session(self.engine)
        #session.configure(bind=engine)
        DeclBase.metadata.create_all(self.engine)

    def test_creation(self):
        """
        ...
        """
        rootNode = Folder(name='root')
        for nm in exampleStrings:
            runNo, chunkNo = parse_cdr_filename( nm[0] )
            self.assertEquals( runNo,   nm[1] )
            self.assertEquals( chunkNo, nm[2] )
            file_ = File(name=nm[0], parent=rootNode)
            run = self.session.query(Run).filter_by( id=runNo ).first()
            if not run:
                run = Run(id=runNo)
            chunk = Chunk(id=chunkNo, run=run, fileID=file_.id)
            self.session.add(run)
            self.session.add(chunk)
            self.session.add(file_)
            self.session.commit()
        #chunk1 = Chunk(id=17, run=run)
        #chunk2 = Chunk(id=22, run=run)
        #self.session.add(run)
        #self.session.commit()
        #print("Created...")
        #print(chunk1, chunk2)

    def tearDown(self):
        self.session.close()


