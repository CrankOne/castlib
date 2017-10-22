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

from castlib3.backend import CopyingRegistryBase

class TestFilesystemModel(unittest.TestCase):
    def test_copying_registry_base(self):
        self.cr = CopyingRegistryBase()
        self.cr[ ['file', ['castor', 'eos', 'whatever', 'file']] ] = {'file', 23}
        self.cr[ [['castor', 'eos'], ['file', 'eos']] ] = {'file', 32}
        self.cr[ ['file', 'file'] ] = {'file', 42}
        self.assertEquals( 23, self.cr['file', 'castor']['file'] )
        self.assertEquals( 23, self.cr['file', 'eos']['file'] )
        self.assertEquals( 23, self.cr['file', 'whatever']['file'] )
        self.assertEquals( 32, self.cr['castor', 'file']['file'] )
        self.assertEquals( 32, self.cr['eos', 'file']['file'] )
        self.assertEquals( 42, self.cr['file', 'file']['file'] )

if '__main__' == __name__:
    unittest.main()

