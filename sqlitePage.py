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


import struct
import _sqliteVarInt
import binascii

# record header
# typedef _rec_header {
# unsigned char pageflag; // if 0x0D, page is leaf node
#	unsigned short firstunallocoffset; // first unallocated block offset
#	unsigned short cellcount;
#	unsigned short firstcelloffset;
#	unsigned char overunallocblockcount
# }

REC_HEADER = ">BHHHB"
REC_HEADER_SIZE = 8

SIZEOFSHORT = 2  # sizeof(short)


# leaf cell header
# typedef _leafcellheader {
#	unsigned short lengthofrec;
#	unsigned short rowid;
#	unsigned short lengthofheader;
# }

CELL_HEADER = '>BBBBH'
SIZE_OF_CELL_HEADER = 6

# SOURCE: http://mwultong.blogspot.com/2007/04/python-hex-viewer-file-dumper.html
def hexdump(buf):
    offset = 0
    while offset < len(buf):
        buf16 = buf[offset:offset + 16]
        buf16Len = len(buf16)
        if buf16Len == 0: break
        output = "%08X:  " % (offset)

        for i in range(buf16Len):
            if (i == 8): output += " "
            output += "%02X " % (ord(buf16[i]))

        for i in range(((16 - buf16Len) * 3) + 1):
            output += " "
            if (buf16Len < 9):
                output += " "

        for i in range(buf16Len):
            if (ord(buf16[i]) >= 0x20 and ord(buf16[i]) <= 0x7E):
                output += buf16[i]
            else:
                output += "."

        offset += 16
        print output

    if (offset == 0):
        print "%08X:  " % (offset)


class SQLITE_PAGE():
    def __init__(self, buf):
        self.buf = buf
        self.header = self.rec_header()

    #print self.header

    def rec_header(self):
        if len(self.buf) == 0:
            return ''
        unpacked_header = struct.unpack(REC_HEADER, self.buf[0:REC_HEADER_SIZE])
        return unpacked_header

    def isleaf(self):
        if self.header[0] == 0x0D:
            return 1
        else:
            return 0

    def getcelloffset(self):
        lstCelloffset = []
        if self.header[2] == 0:
            return lstCelloffset

        for count in range(0, self.header[2]):
            lstCelloffset.append(struct.unpack('>H', self.buf[REC_HEADER_SIZE + (count * 2):REC_HEADER_SIZE + (
                count * 2) + SIZEOFSHORT])[0])

        return lstCelloffset

    # private
    def cell_header(self, cellbytes):
        header = []
        cell = binascii.hexlify(cellbytes)
        # Get total number of bytes of payload
        bytes_of_payload_tuple = _sqliteVarInt.parse_next_var_int(
            cell[:18])  # a variable integer can be maximum 9 byte (= 18 nibbles) long
        bytes_of_payload = bytes_of_payload_tuple[0]

        # Get row_id
        row_id_string = cell[((bytes_of_payload_tuple[1] * 2)):((bytes_of_payload_tuple[1] + 9) * 2)]
        row_id_tuple = _sqliteVarInt.parse_next_var_int(row_id_string)
        row_id = row_id_tuple[0]

        header.append(bytes_of_payload)
        header.append(row_id)
        #print 'offset: %x'%(bytes_of_payload_tuple[1] + row_id_tuple[1])
        header.append(struct.unpack('=H', cellbytes[(bytes_of_payload_tuple[1] + row_id_tuple[1]):(
            bytes_of_payload_tuple[1] + row_id_tuple[1] + SIZEOFSHORT)])[0])

        return header, (bytes_of_payload_tuple[1] + row_id_tuple[1] + SIZEOFSHORT)

    # private
    def getData(self, celldata, type, size, offset):
        data = celldata[offset:offset + size]

        if type == 'int':
            if size >= 6:
                return struct.unpack('=q', data)[0]  # signed int (6 or 8 bytes)
            elif size == 1:
                return struct.unpack('=b', data)[0]  # signed int (1 bytes)
            elif size == 2:
                return struct.unpack('=h', data)[0]  # signed int (2 bytes)
            elif size == 3:
                struct.unpack('=i', data+'\x00')[0]  # signed int (3 bytes)
            elif size == 4:
                return struct.unpack('=i', data)[0]  # signed int (4 bytes)

        elif type == 'float':
            return struct.unpack('=d', data)[0]
        elif type == 'blob':
            return data
        elif type == 'text':
            return struct.unpack('=%ss' % len(data), data)[0]  # string


    def getCellData(self, cell):
        #hexdump(cell)

        header, size = self.cell_header(cell)
        celltype = cell[size:size + header[2] - 2]
        celldata = cell[size + header[2] - 2:size + header[0]]
        #hexdump(celldata)

        #print 'size: %x, %x'%(size, size+header[2]-2)
        offset = 0
        dataset = []
        column = []
        record = []

        if header[0] == 0x00 or header[2] == 0x00 or len(celldata) == 0:
            if len(column) != 0 and len(record) != 0:
                dataset.append(column)
                dataset.append(record)
            return dataset

        typehexstr = binascii.hexlify(celltype)
        typelist = _sqliteVarInt.parse_all_var_ints(typehexstr)

        #print typelist

        for byte in typelist:
            typensize = self.check_type(byte)
            if typensize[0] != 'Reserved':
                #print 'type: %s, size: %i'%(typensize[0], typensize[1])
                data = self.getData(celldata, typensize[0], typensize[1], offset)
                offset += typensize[1]

                column.append(typensize[0])  # column list
                record.append(data)
        if len(column) != 0 and len(record) != 0:
            dataset.append(column)
            dataset.append(record)

        return dataset

    # return type : <TYPE(text), size(integer)>
    def check_type(self, byte):
        if (byte >= 1) and (byte <= 4):
            return 'int', byte
        elif byte == 5:
            return 'int', 6
        elif byte == 6:
            return 'int', 8
        elif byte == 7:
            return 'float', 8
        elif byte > 12 and (byte % 2) == 0:
            return 'blob', (byte - 12) / 2
        elif byte > 13 and (byte % 2):
            return 'text', (byte - 13) / 2
        else:
            return 'Reserved', 0



