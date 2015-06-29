#!/usr/bin/python
#

# Copyright (C) 2014 n0fate
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

#import sqlite3
import struct
from ctypes import *

class _SQLiteDBHeader(BigEndianStructure):
    _fields_ = [
        ('signature', c_char*16),
        ('pagesize', c_uint16),
        ('unknown', c_char*6),
        ('filechangecounter', c_uint32),
        ('databasesize', c_uint32),
        ('freepagenumber', c_uint32),
        ('schemacookie', c_uint32),
        ('schemaformatver', c_uint32),
        ('cachesize', c_uint32),
        ('vaccumsetting', c_uint32),
        ('textencoding', c_uint32),
        ('userversion', c_uint32),
        ('increvaccummode', c_uint32)
    ]

def _memcpy(buf, fmt):
    return cast(c_char_p(buf), POINTER(fmt)).contents


class SQLITE():
    def __init__(self, buf):
        self.buf = buf

    def dbheader(self):
        self.header = _memcpy(self.buf, _SQLiteDBHeader)
        if self.header.signature[0:12] == 'SQLite format':
            return 1, self.header
        else:
            return 0, []

    def getschemata(self):
        CREATETABLE = 'CREATE TABLE'
        #print 'buf size : %d'%len(self.buf)
        columnsdic = {}
        for offset in range(0, len(self.buf)):
            tablename = ''
            if CREATETABLE == self.buf[offset:offset+len(CREATETABLE)]:
                columnlst = []
                tablename = str(self.buf[offset+len(CREATETABLE):].split('(')[0].replace(' ', ''))
                strcolumns = self.buf[offset+len(CREATETABLE):].split('(')[1].split(')')[0]
                #print strcolumns
                primary_key = 0
                for column in strcolumns.split(','):
                    columninfo = []
                    if len(column.split(' ')) >= 3:
                        columnname = column.split(' ')[1]
                        columntype = column.split(' ')[2].split('\x00')[0]
                        if (columntype == 'INTEGER') \
                            or (columntype == 'TEXT') \
                            or (columntype == 'BLOB'):
                            columninfo.append(columnname)
                            columninfo.append(columntype)
                    if columninfo.__len__() != 0:
                        columnlst.append(columninfo)
                columnlst.append(primary_key)
                columnsdic[tablename] = columnlst
        return columnsdic






def main():
    print 'test'





if __name__ == "__main__":
    main()
