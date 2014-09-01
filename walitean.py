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
from sys import argv, exit
import struct
import sqlitePage
import sqliteDB
import sqlite3
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



    def write_tsv_file(self, dic, outputfile):

        try:
            file_handler = open(outputfile, 'a')

        except:
            print 'outputfile open failed'
            return 1

        for k, v in dic.iteritems():
            DecColumn = DecodeColumn(k)
            for colvalue in DecColumn:
                file_handler.write(colvalue)
                file_handler.write(',')
            file_handler.write('\n')

            for row in v:
                for l in range(0, len(DecColumn)):
                    if DecColumn[l] == 'blob':
                        file_handler.write('')
                    elif DecColumn[l] == 'text':
                        bufstr = ''
                        try:
                            bufstr = str(row[l]).decode('utf8').replace('\n', ' ').replace(',', ' ').encode('utf8')
                        except UnicodeEncodeError:
                            bufstr = str(row[l]).replace('\n', ' ').replace(',', ' ')

                        file_handler.write(bufstr)
                    else:
                        file_handler.write(str(row[l]))

                    file_handler.write(',')
                file_handler.write('\n')
            file_handler.write('\n')
        file_handler.close()

    # dictionary
    def print_table(self, dic):

        for k, v in dic.iteritems():
            decColumn = []
            mszlist = []    # optional max size list

            decColumn = DecodeColumn(k)
            max_len = len(decColumn)
            offsetlst = []

            #print decColumn

            count = 0
            # add column type
            for coltype in decColumn:
                if coltype == 'blob':   # for remove blob data on stdout
                    offsetlst.append(count) # add column number on offsetlst
                mszlist.append(-1)
                count += 1

            #print decColumn
            contentlist = []
            for row in v:
                line = []
                for l in range(0, max_len):
                    if decColumn[l] == 'int':
                        try:
                            line.append('%d'%row[l])
                        except TypeError:
                            line.append('%s'%row[l])
                    elif decColumn[l] == 'float':
                        line.append('%f'%row[l])
                    elif decColumn[l] == 'text':
                        sqlitetext = ''
                        try:
                            sqlitetext = str(row[l]).decode('utf8').replace('\n', ' ').encode('utf8')
                        except:
                            sqlitetext = str(row[l]).replace('\n', '')
                        line.append('%s'%sqlitetext)
                    else:   # blob
                        line.append('')

                contentlist.append(line)
                #print row

            columnprint(decColumn, contentlist, mszlist)
            print ''



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


def usage():
    print 'Copyright by n0fate (n0fate@n0fate.com)'
    print 'python walitean.py [-i SQLITE WAL FILE] [-f TYPE(raw, csv)] [-o FILENAME(if type is csv)]'

def main():
    inputfile = ''
    outputfile = ''
    outputtype = ''     # file type
    
    try:
        option, args = getopt.getopt(argv[1:], 'i:o:f:')

    except getopt.GetoptError, err:
        usage()
        exit()
    
    #print option
    for op, p, in option:
        if op in '-i':
            inputfile = p
        elif op in '-o':
            outputfile = p
        elif op in '-f':
            outputtype = p
        else:
            print 'invalid option'
            exit()
    
    try:
        if (inputfile == '') or (outputtype == ''):
            usage()
            exit()

        elif outputtype == 'csv' and outputfile == '':
            usage()
            exit()
    
    except IndexError:
        usage()
        exit()

    
    wal_class = WAL_SQLITE()
    wal_class.open(inputfile)
    frame_list = wal_class.get_frame_list()     # get a list of wal frame
    d = wal_class.process(frame_list)

    if outputtype == 'raw':
        print '[+] Output Type : Standard Output(Terminal)'
        wal_class.print_table(d)
    elif outputtype == 'csv':
        print '[+] Output Type : CSV(Comma Separated Values)'
        print '[+] File Name : %s'%outputfile
        wal_class.write_tsv_file(d, outputfile)

if __name__ == "__main__":
    main()