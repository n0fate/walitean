import getopt
from sys import argv, exit, stdout, stdin, stderr
import struct
import time
import sqlitePage

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
            frame_element.append(frameheader) # frame header
            frame_element.append(frame_buf[offset+FRAME_HEADER_SIZE:offset+FRAME_HEADER_SIZE+self.pagesize]) # page
            frame_list.append(frame_element)
            #print ' [-] frame size : %x'%len(frame_buf[offset+FRAME_HEADER_SIZE:offset+FRAME_HEADER_SIZE+self.pagesize])

            #hexdump(frame_buf[offset+FRAME_HEADER_SIZE:offset+FRAME_HEADER_SIZE+self.pagesize])

            count = count + 1
        self.count = count
        return frame_list

    def write_sqlitedb_file(self, frame_list, outputfile):
        try:
            file_handler = open(outputfile, 'wba')
        except:
            print 'outputfile open failed'
            return 1

        for count in range(1, self.count):
            frame_temp = []
            for frame in frame_list:
                if frame[0][0] == count:
                    if len(frame_temp) == 0:
                        frame_temp.append(frame[0])
                        frame_temp.append(frame[1])
                    else: # transaction number
                        if frame_temp[0][1] < frame[0][1]:
                            frame_temp.pop()
                            frame_temp.pop()
                            frame_temp.append(frame[0])
                            frame_temp.append(frame[1])

            if len(frame_temp) == 0:
                file_handler.write('\x00'*0x1000)
            else:
                print 'find frame %d, transaction number: %d'%(count, frame_temp[0][1])
                file_handler.write(frame_temp[1])
                    

        file_handler.close()

    def write_csv_file(self, dataset, outputfile):
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
            if dataset[0][count] == 'blob':
                file_handler.write('')
            elif dataset[0][count] == 'text':
                try:
                    file_handler.write(str(dataset[1][count]).decode('utf-8'))
                except UnicodeEncodeError:
                    file_handler.write(str(dataset[1][count]))
            else:
                #print dataset[1][count]
                file_handler.write(str(dataset[1][count]))
            file_handler.write('\t')
        file_handler.write('\n')
        file_handler.close()


def usage():
    print 'python wal_parser.py [-i SQLITE WAL FILE] [-o OUTPUT FILE]'

def main():
    
    inputfile = ''
    outputfile = ''
    
    try:
        option, args = getopt.getopt(argv[1:], 'i:o:')

    except getopt.GetoptError, err:
        usage(argv)
        exit()
    
    #print option
    for op, p, in option:
        if op in '-i':
            inputfile = p
        elif op in '-o':
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
                #print dataset[0]
                #print dataset[1]

                wal_class.write_csv_file(dataset, outputfile)

if __name__ == "__main__":
    main()