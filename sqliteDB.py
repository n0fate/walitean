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

import sqlite3
import struct

# struct db{
#   char signature[16];
#   unsigned short pagesize;
#   char unknown[6];
#   unsigned int filechangecounter
#   unsigned int databasesize;
#   unsigned int freepageoffset;
#   unsigned int freepagenumber;
#   unsigned int schemacookie;
#   unsigned int shemaformatver;
#   unsigned int cachesize;
#   unsigned int vaccumsetting;
#   unsigned int textencoding;
#   unsigned int userversion;
#   unsigned int increvaccummode;
# }

SQLITEHEADER = '>16sH6xIIIIIIIIIII'

class SQLITE():
    def __init__(self, buf):
        self.buf = buf

    def dbheader(self):
        self.header = struct.unpack(self.buf, SQLITEHEADER)
        if self.header[0:12] == 'SQLite format':
            return 1, self.header
        else:
            return 0, []

    def getschemata(self):
        CREATETABLE = 'CREATE TABLE'
        print 'buf size : %d'%len(self.buf)
        columnsdic = {}
        for offset in range(0, len(self.buf)):
            tablename = ''
            if CREATETABLE == self.buf[offset:offset+len(CREATETABLE)]:
                columnlst = []
                tablename = str(self.buf[offset+len(CREATETABLE):].split('(')[0].replace(' ', ''))
                strcolumns = self.buf[offset+len(CREATETABLE):].split('(')[1].split(')')[0]
                print strcolumns
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