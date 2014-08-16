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

import getopt
from sys import argv, exit, stdout, stdin, stderr
import struct
#import time
import sqlitePage
import sqliteDB
import sqlite3

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

# SOURCE: http://mwultong.blogspot.com/2007/04/python-hex-viewer-file-dumper.html
def hexdump(buf):
    offset = 0
    while offset < len(buf):
        buf16 = buf[offset:offset+16]
        buf16Len = len(buf16)
        if buf16Len == 0: break
        output = "%08X:  " % (offset)
        
        for i in range(buf16Len):
            if (i == 8): output += " "
            output += "%02X " % (ord(buf16[i]))
        
        for i in range( ((16 - buf16Len) * 3) + 1 ):
            output += " "
            if (buf16Len < 9):
                output += " "
        
        for i in range(buf16Len):
            if (ord(buf16[i]) >= 0x20 and ord(buf16[i]) <= 0x7E):
                output += buf16[i]
            else: output += "."
        
        offset += 16
        print output
        
    if offset == 0:
        print "%08X:  " %offset

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

            count = count + 1
        self.count = count
        return frame_list

    def write_csv_file(self, dataset, outputfile):
        if (len(dataset[0]) is 0) or (len(dataset[1]) is 0):
            return
        try:
            file_handler = open(outputfile, 'a')
        except:
            print 'outputfile open failed'
            return 1

        for count in range(0, len(dataset[0])):
            file_handler.write(dataset[0][count])
            file_handler.write('\t')
        file_handler.write('\n')
        for count in range(0, len(dataset[1])):
            if dataset[1][count] == 0:
                continue
            if dataset[0][count] == 'blob':
                #file_handler.write(buffer(dataset[1][count]))
                file_handler.write('')
            elif dataset[0][count] == 'text':
                try:
                    file_handler.write(str(dataset[1][count]).decode('utf8').replace('\n', ' ').encode('utf8'))
                except UnicodeEncodeError:
                    file_handler.write(str(dataset[1][count]).replace('\n', ''))
            else:
                #print dataset[1][count]
                file_handler.write(str(dataset[1][count]))
            file_handler.write('\t')
        file_handler.write('\n')
        file_handler.close()

    def createdb(self, dbname):
        tblset = []
        db = sqliteDB.SQLITE(dbname)
        tbllist = db.getDBTableList()
        for tblname in tbllist:
            dbdata = []

            columnlist = db.getDBTblInfoList(tblname)
            #print columnlist
            dbdata.append(tblname)
            dbdata.append(columnlist)
            tblset.append(dbdata)

        con = sqlite3.connect('%s-wal.db'%dbname)
        cursor = con.cursor()

        for tbl in tblset:
            query = u"CREATE TABLE "+tbl[0][0]+u"("
            count = len(tbl[1])
            for column in tbl[1]:
                count -= 1
                query += column[1]
                query += u" "
                query += column[2]
                if count != 0:
                    query += u", "
            query += u");"
            print query
            try:
                cursor.execute(query)
            except sqlite3.OperationalError, e:
                print e
        con.close()
        return tblset

    def write_sqlite_file(self, tblset, dataset, dbname):
        if len(dataset[0]) == 0:
            return

        con = sqlite3.connect('%s-wal.db'%dbname)
        cursor = con.cursor()

        #print dataset[0]
        # comparing two column list
        percentagelst = []
        for tbl in tblset:
            temp_list = []
            for column in tbl[1]: # matching list type
                temp_list.append(column[2])
            percentagelst.append(comp(temp_list, dataset[0]))

        m = max(percentagelst)
        result_lst = [i for i, j in enumerate(percentagelst) if j == m]
        #print tblset[result_lst[0]]
        query = u"INSERT INTO "+tblset[result_lst[0]][0][0]+u" VALUES ("
        count = len(tblset[result_lst[0]][1])
        for record in dataset[1]:
            #print record
            count -= 1
            try:
                query += str(record)
            except UnicodeDecodeError, e:
                print record
                query += record.decode('utf-8')
            #except TypeError, e:
            #    query += buffer(record)
            if count != 0:
                query += u", "
        query += u");"
        print query
        try:
            cursor.execute(query)
        except sqlite3.OperationalError, e:
            print e

        con.commit()
        con.close()


def comp(dbtbl, waltbl):
    tblcolumnlen = len(dbtbl)
    result = set(dbtbl) & set(waltbl)
    return ((len(result)/tblcolumnlen) * 100)

def usage():
    print 'python wal_parser.py [-i SQLITE WAL FILE] [-d SQLITE FILE]'

def main():
    inputfile = ''
    outputfile = ''
    
    try:
        option, args = getopt.getopt(argv[1:], 'i:d:')

    except getopt.GetoptError, err:
        usage()
        exit()
    
    #print option
    for op, p, in option:
        if op in '-i':
            inputfile = p
        elif op in '-d':
            outputfile = p
        else:
            print 'invalid option'
            exit()
    
    try:
        if inputfile == '' or (outputfile == ''):
            usage()
            exit()
    
    except IndexError:
        usage()
        exit()

    
    wal_class = WAL_SQLITE()
    wal_class.open(inputfile)
    header = wal_class.get_header()
    frame_list = wal_class.get_frame_list()

    table_list = {} # dictionary

    #tblset = wal_class.createdb(outputfile) # return table information

    for frame in frame_list:
        sqlite_page = sqlitePage.SQLITE_PAGE(frame[1])
        if(sqlite_page.isleaf()):
            #print 'leaf node : frame %i'%frame[0][0]
            #hexdump(frame[1])
            celllst = sqlite_page.getcelloffset()

            for celloffset in celllst:
                #print 'cell'
                cellbuf = frame[1][celloffset:]
                dataset = sqlite_page.getCellData(cellbuf)

                #table_list[dataset[0]] = dataset[1]
                #print dataset[0] #column name
                #print dataset[1] #data

                wal_class.write_csv_file(dataset, outputfile)
                #wal_class.write_sqlite_file(tblset, dataset, outputfile) # need to time ;-/

if __name__ == "__main__":
    main()