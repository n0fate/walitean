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
from binascii import hexlify, unhexlify
from sys import argv, exit
import struct
import sqlitePage
import sqliteDB
from operator import eq
from collections import defaultdict
from tableprint import columnprint

#struct Write Ahead Log file header {
# uint signature; // 0x377f0682 or 0x377f0683
# uint version; // 0x002de218 -> 3007000
# uint pagesize;
# uint sequencenum; // starting at 0
# uint salt1; // incremented with every checkpoint
# uint salt2; // regenerated with every checkpoint
# uint checksum1; // checksum1
# uint checksum2; // checksum2
# }
FILE_HEADER = '>IIIIIIII'
FILE_HEADER_SIZE = 32

#struct Write Ahead Log frame header {
# uint dbpagenum; //
# uint endoftransaction;
# uint salt1; // incremented with every checkpoint
# uint salt2; // regenerated with every checkpoint
# uint checksum1; // checksum1
# uint checksum2; // checksum2
# }
FRAME_HEADER = '>IIIIII'
FRAME_HEADER_SIZE = 24

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
        if self.fbuf == '':
            return 1
        
        fileheader = struct.unpack(FILE_HEADER, self.fbuf[0:FILE_HEADER_SIZE])
        #print '%x'%fileheader[0]

        if (fileheader[0] != 0x377f0682) and (fileheader[0] != 0x377f0683):
            print 'invalid file format'
            return 1
        self.pagesize = fileheader[2]
        self.cpseqnum = fileheader[3]
        #print '[+] PageSize: %x'%self.pagesize 
        #print '[+] Checkpoint Sequence Number: %d'%fileheader[3]
        return struct.unpack(FILE_HEADER, self.fbuf[0:FILE_HEADER_SIZE])

    def get_frame_list(self):
        frame_list = []
        frame_buf = self.fbuf[FILE_HEADER_SIZE:]
        #print 'frame size: %x'%len(self.fbuf)
        count = 1
        for offset in range(0, len(frame_buf), self.pagesize+FRAME_HEADER_SIZE):
            #print '[+] frame %d, offset : %x'%(count, offset+FILE_HEADER_SIZE)
            frame_element = []
            frameheader = struct.unpack(FRAME_HEADER, frame_buf[offset:offset+FRAME_HEADER_SIZE])
            #print ' [-] Frame Number : %d, endoftransaction: %d'%(frameheader[0], frameheader[1])
            #print ' [-] Sequence Number : %d'%frameheader[2]
            #print ' [-] Sequence Number : %d'%frameheader[3]
            frame_element.append(frameheader) #frame header
            frame_element.append(frame_buf[offset+FRAME_HEADER_SIZE:offset+FRAME_HEADER_SIZE+self.pagesize]) # page
            frame_list.append(frame_element)
            #print ' [-] frame size : %x'%len(frame_buf[offset+FRAME_HEADER_SIZE:offset+FRAME_HEADER_SIZE+self.pagesize])

            #hexdump(frame_buf[offset+FRAME_HEADER_SIZE:offset+FRAME_HEADER_SIZE+self.pagesize])

            count += 1
        self.count = count
        return frame_list

    def getscheme(self, rootpage):
        db = sqliteDB.SQLITE(rootpage)
        scheme = db.getschemata()
        print scheme
        return scheme

    def process(self, framelist):
        # ref : http://stackoverflow.com/questions/5378231/python-list-to-dictionary-multiple-values-per-key
        d1 = defaultdict(list)

        total_list = []

        # searching cell list on each page
        for frame in framelist:
            sqlite_page = sqlitePage.SQLITE_PAGE(frame[1])
            if sqlite_page.isleaf() == 1:   # If it is leaf page
                #print 'leaf node : frame %i'%frame[0][0]
                #hexdump(frame[1])
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
        #print total_list
        count = 0
        for column, record in total_list:
            s = list(column)
            for prevcolumn, prevrecord in total_list:
                if len(column) == len(prevcolumn):
                    sametable = 1
                    for coloffset in range(0, len(column)):
                        if ord(column[coloffset]) != (ord(column[coloffset]) & ord(prevcolumn[coloffset])):
                            if 0 == ((column[coloffset] == 'U') or (prevcolumn[coloffset] == 'U')):
                                sametable = 0
                                break

                    if sametable:
                        #print column, prevcolumn
                        for coloffset in range(0, len(column)):
                            if column[coloffset] == 'U':
                                if prevcolumn[coloffset] != 'U':
                                    s[coloffset] = prevcolumn[coloffset]

            str = "".join(s)
            total_list[count][0] = str
            count += 1

    def exportSqliteDB(self, dbname, dic):
        from exportdb import ExportSQLite
        export = ExportSQLite()
        if export.createDB(dbname) is False:
            print '[!] File is Exists'
            return

        tablenum = 0
        for enccolumn, data in dic.iteritems():

            tablename = 'unknown%d'%tablenum
            decColumn = DecodeColumn(enccolumn)
            columnlist = []
            recordlist = []
            count = 0
            for doffset in xrange(len(decColumn)):
                column = ['', 'Unknown%d'%count, decColumn[doffset]]
                count += 1
                columnlist.append(column)

            tablenum += 1
            export.createTable(tablename, columnlist)

            for row in data:
                export.insertRecord(tablename, row)

        export.commit()
        export.close()


def comp(dbtbl, waltbl):
    tblcolumnlen = len(dbtbl)
    result = set(dbtbl) & set(waltbl)
    return ((len(result)/tblcolumnlen) * 100)


def EncodeColumn(dataset):

    column_hash = ''
    for type in dataset[0]:
        column_hash += type[0] # if type[0] is 'i', type is 'int'

    record = []
    record.append(column_hash)
    record.append(dataset[1])
    return record


def DecodeColumn(dataset):
    column = []
    column_hash = dataset

    for type in column_hash:
        if type == 'I':
            column.append('INTEGER')
        elif type == 'F':
            column.append('FLOAT')
        elif type == 'B':
            column.append('BLOB')
        elif type == 'T':
            column.append('TEXT')
        elif type == 'U':
            column.append('UNKNOWN')

    return column


def main():
    inputfile = ''
    

    parser = argparse.ArgumentParser(description='Written-Ahead Log Analyzer for SQLite by @n0fate')
    parser.add_argument('-f', '--file', nargs=1, help='WAL file(*-wal)', required=True)
    parser.add_argument('-x', '--exportfile', nargs=1, help='Export Filename(CSV format, Optional)', required=False)
    
    args = parser.parse_args()

    inputfile = args.file[0]

    if os.path.exists(inputfile) is False:
        print '[!] File is not exists'
        parser.print_help()
        sys.exit()

    
    wal_class = WAL_SQLITE()
    wal_class.open(inputfile)
    frame_list = wal_class.get_frame_list()     # get a list of wal frame
    d = wal_class.process(frame_list)

    if args.exportfile is None:
        print '[*] Output Type : Standard Output(Terminal)'
        wal_class.print_table(d)
    else:
        print '[*] Output Type : SQLite DB'
        print '[*] File Name : %s'%args.exportfile[0]
        wal_class.exportSqliteDB(args.exportfile[0], d)

if __name__ == "__main__":
    main()