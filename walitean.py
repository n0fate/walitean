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

import argparse
import os
import sqlitePage
import sqliteDB
from operator import eq
from collections import defaultdict
from ctypes import *

class _WALFileHeader(BigEndianStructure):
    _fields_ = [
        ('Signature', c_uint32),
        ('Version', c_uint32),
        ('PageSize', c_uint32),
        ('SequenceNumber', c_uint32),
        ('Salt1', c_uint32),
        ('Salt2', c_uint32),
        ('CheckSum1', c_uint32),
        ('Checksum2', c_uint32)
    ]

class _WALFrameHeader(BigEndianStructure):
    _fields_ = [
        ('DBPageNumber', c_uint32),
        ('EndofTransaction', c_uint32),
        ('Salt1', c_uint32),
        ('Salt2', c_uint32),
        ('Checksum1', c_uint32),
        ('Checksum2', c_uint32)
    ]

def _memcpy(buf, fmt):
    return cast(c_char_p(buf), POINTER(fmt)).contents

class WAL_SQLITE():
    def __init__(self):
        self.fhandle = ''
        self.fbuf = ''
        self.pagesize = 0
        self.count = 0


    def open(self, filepath):
        try:
            self.fhandle = open(filepath, 'rb')
        except:
            print 'invalid input file'
            return 1
        self.fbuf = self.fhandle.read()
        self.fhandle.close()
        self.get_header()
        return self.fbuf

    ## get WAL File Header
    def get_header(self):
        fileheader = _memcpy(self.fbuf[:sizeof(_WALFileHeader)], _WALFileHeader)

        if (fileheader.Signature != 0x377f0682) and (fileheader.Signature != 0x377f0683):
            print 'invalid file format'
            return None
        self.pagesize = fileheader.PageSize
        self.cpseqnum = fileheader.SequenceNumber
        return fileheader

    def get_frame_list(self):
        frame_list = []
        frame_buf = self.fbuf[sizeof(_WALFileHeader):]
        count = 1
        for offset in range(0, len(frame_buf), self.pagesize + sizeof(_WALFrameHeader)):
            frame_element = []
            frameheader = _memcpy(frame_buf[offset:offset+sizeof(_WALFrameHeader)], _WALFrameHeader)
            frame_element.append(frameheader) #frame header
            frame_element.append(frame_buf[offset+sizeof(_WALFrameHeader):offset+sizeof(_WALFrameHeader)+self.pagesize]) # page
            frame_list.append(frame_element)

            count += 1
        self.count = count
        return frame_list

    def getscheme(self, rootpage):
        db = sqliteDB.SQLITE(rootpage)
        scheme = db.getschemata()
        #print scheme
        return scheme

    def process(self, framelist):
        # ref : http://stackoverflow.com/questions/5378231/python-list-to-dictionary-multiple-values-per-key
        d1 = defaultdict(list)

        total_list = []

        # searching cell list on each page
        for frame in framelist:
            sqlite_page = sqlitePage.SQLITE_PAGE(frame[1])
            if sqlite_page.isleaf() == 1:   # If it is leaf page
                celllst = sqlite_page.getcelloffset()   # Getting a list of cell offset

                for celloffset in celllst:
                    #print 'cell'
                    cellbuf = frame[1][celloffset:]
                    dataset = sqlite_page.getCellData(cellbuf)  # Getting a Cell Data(column type, data)

                    if len(dataset) == 0:
                        continue

                    record = EncodeColumn(dataset)

                    findit = 0
                    for prev_column, prev_record in total_list:
                        if all(map(eq, prev_record, record[1])):
                            findit = 1
                            break

                    if findit == 0:
                        total_list.append(record)

        self.rematchingcolumn(total_list)

        # extract key(column type) and value(records) and commit to dictionary type (for separating same column)
        for k, v in total_list:
            d1[k].append(v)

        # remove duplicate list on dictionary
        d = dict((k, tuple(v)) for k, v in d1.iteritems())

        return d

    def rematchingcolumn(self, total_list):
        count = 0
        for column, record in total_list:
            s = list(column)
            for prevcolumn, prevrecord in total_list:
                if len(column) == len(prevcolumn):
                    sametable = 1
                    for coloffset in xrange(len(column)):
                        if ord(column[coloffset]) != (ord(column[coloffset]) & ord(prevcolumn[coloffset])):
                            if 0 == ((column[coloffset] == 'U') or (prevcolumn[coloffset] == 'U')):
                                sametable = 0
                                break

                    if sametable:
                        for coloffset in range(0, len(column)):
                            if column[coloffset] == 'U':
                                if prevcolumn[coloffset] != 'U':
                                    s[coloffset] = prevcolumn[coloffset]

            str = "".join(s)
            total_list[count][0] = str
            count += 1

    def exportSqliteDB(self, dbname, newdbinfo):
        from exportdb import ExportSQLite
        export = ExportSQLite()
        if export.createDB(dbname) is False:
            print '[!] File is Exists'
            return

        for tbname, recordsinfo in newdbinfo.iteritems():
            if len(recordsinfo[0]) is 0:
                continue
            export.createTable(tbname, recordsinfo[0])

            for row in recordsinfo[1]:
                export.insertRecord(tbname, row)

        export.commit()
        export.close()

    def findvalidcolumninfo(self, schema, wallist):
        #print '[+] Find DB table schema'
        newdbinfo = {}
        for waltbname, walrecords in wallist.items():
            recordslist = []
            findit = False

            #columnname = columninfo[0]
            #columntype = columninfo[1]

            for tbname, columninfo in schema.items():
                columncount = len(columninfo)
                if columncount == len(waltbname):
                    print ' [-] %s is %s' % (waltbname, tbname)
                    recordslist.append(columninfo)
                    recordslist.append(walrecords)
                    recordslist.append(True)
                    newdbinfo[tbname] = recordslist
                    findit = True
                    break

            # round 2
            # if findit == False:
            #     for tbname, columninfo in schema.items():
            #         decColumn = DecodeColumn(waltbname)
            #         import difflib
            #         sm = difflib.SequenceMatcher(None, columninfo, decColumn)
            #         if sm.ratio() <= 0.90:
            #             print ' [-] %s is %s (%d%%)' % (waltbname, tbname, sm.ratio()*100)
            #             recordslist.append(columninfo)
            #             recordslist.append(walrecords)
            #             recordslist.append(True)
            #             newdbinfo[tbname] = recordslist
            #             findit = True
            #             break

            # round 3
            if findit == False:
                decColumn = DecodeColumn(waltbname)
                newcolumninfo = []
                for columnoffset in xrange(len(decColumn)):
                    dummy = []
                    dummy.append('unknown%d' % columnoffset)
                    dummy.append(decColumn[columnoffset])
                    newcolumninfo.append(dummy)
                print ' [-] Could not find table schema %s'%waltbname
                newdbinfo[waltbname] = [newcolumninfo, walrecords, False]

        return newdbinfo    # key is tablename, value is list(columninfo, records)





def comp(dbtbl, waltbl):
    tblcolumnlen = len(dbtbl)
    result = set(dbtbl) & set(waltbl)
    return ((len(result)/tblcolumnlen) * 100)


def EncodeColumn(dataset):
    column_hash = ''
    for type in dataset[0]:
        column_hash += type[0]  # if type[0] is 'i', type is 'int'

    record = []
    record.append(column_hash)
    record.append(dataset[1])
    return record


def DecodeColumn(dataset):
    column = []
    column_hash = dataset

    for columntype in column_hash:
        if columntype == 'I':
            column.append('INTEGER')
        elif columntype == 'F':
            column.append('FLOAT')
        elif columntype == 'B':
            column.append('BLOB')
        elif columntype == 'T':
            column.append('TEXT')
        elif columntype == 'U':
            column.append('UNKNOWN')

    return column


def main():
    parser = argparse.ArgumentParser(description='Written-Ahead Log Analyzer for SQLite by @n0fate')
    parser.add_argument('-f', '--file', nargs=1, help='WAL file(*-wal)', required=True)
    parser.add_argument('-x', '--exportfile', nargs=1, help='SQlite filename', required=True)
    parser.add_argument('-m', '--maindb', nargs=1, help='Main DB File (Optional)', required=False)
    
    args = parser.parse_args()

    inputfile = args.file[0]

    if os.path.exists(inputfile) is False:
        print '[!] File is not exists'
        parser.print_help()
        exit()
    
    wal_class = WAL_SQLITE()
    wal_class.open(inputfile)
    frame_list = wal_class.get_frame_list()
    DBSchema = {}
    print '[+] Check a root-page in WAL'
    for data in frame_list:
        if data[0].DBPageNumber == 1:
            DBSchema = wal_class.getscheme(data[1])
            if len(DBSchema):
                print ' [-] Find DB Schema at root-page'
                break

    if len(DBSchema) == 0 and args.maindb is not None:
        print '[*] Could not find root-page in WAL. Now we are checking a Main DB'
        opendb = open(args.maindb[0], 'rb')
        buf = opendb.read(4096)
        DBSchema = wal_class.getscheme(buf)

        if len(DBSchema):
            print ' [-] Find DB Schema at %s'%args.maindb[0]
        else:
            print ' [-] Could not find DB Schema at %s'%args.maindb[0]

    print '[*] Processing..'
    d = wal_class.process(frame_list)

    print '[*] Schema Matching..'
    newdbinfo = wal_class.findvalidcolumninfo(DBSchema, d)

    print '[*] Output Type : SQLite DB'
    print '[*] File Name : %s' % args.exportfile[0]
    wal_class.exportSqliteDB(args.exportfile[0], newdbinfo)

if __name__ == "__main__":
    main()
