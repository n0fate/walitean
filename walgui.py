import sys
from PyQt4 import QtGui, QtCore
import walitean


####################################################################
class MyListModel(QtCore.QAbstractListModel):
    def __init__(self, datain, parent=None, *args):
        """ datain: a list where each item is a row
        """
        QtCore.QAbstractListModel.__init__(self, parent, *args)
        self.listdata = datain

    def rowCount(self, parent=QtCore.QModelIndex()):
        return len(self.listdata)

    def data(self, index, role):
        if index.isValid() and role == QtCore.DisplayRole:
            return QtCore.QVariant(self.listdata[index.row()])
        else:
            return QtCore.QVariant()

class RecordModel(QtCore.QAbstractTableModel):
    def __init__(self, datain, parent=None, *args):
        """ datain: a list where each item is a row
        """
        QtCore.QAbstractTableModel.__init__(self, parent, *args)
        self.listdata = datain

    def CreateColumn(self):
        model = QtGui.QStandardItemModel(2, 3, self) # 2 = row number, 3 = column number

    #def CreateTable(self):

class WaliteanUI(QtGui.QWidget):
    def __init__(self):
        super(WaliteanUI, self).__init__()
        self.filename = ''
        self.initUI()   # init

    def initUI(self):

        QOpenBtn = QtGui.QPushButton('Open')
        self.connect(QOpenBtn, QtCore.SIGNAL('clicked()'), self.openfile)

        self.Qfilepath = QtGui.QLineEdit()
        #QOpenBtn.move(600, 50)
        #QAnBtn.move(600, 100)

        self.setGeometry(300, 300, 800, 600)
        self.move(300, 300)
        self.setWindowTitle('walitean - WAL Analyzer for SQLite by n0fate')

        # set layout
        mainLayout = QtGui.QVBoxLayout()    # main layout
        topLayout = QtGui.QHBoxLayout()
        topLayout.addWidget(QOpenBtn)
        topLayout.addWidget(self.Qfilepath)

        self.tablelabel = QtGui.QLabel('Table List')
        self.tablelabel.setFixedWidth(150)
        wallabel = QtGui.QLabel('File Info')
        wallabel.setFixedWidth(150)
        walinfo = QtGui.QTableView()
        walinfo.setFixedWidth(150)
        self.QTableList = QtGui.QListWidget()
        #QTableList.move(50, 100)
        #QTableList.setMinimumSize(50, 250)

        #list_ex = [1,2,3]
        #lv = MyListModel(list_ex)

        #self.QTableList.setModel(lv)

        self.QTableList.setFixedWidth(150)

        self.connect(self.QTableList, QtCore.SIGNAL('itemClicked(QListWidgetItem *)'), self.showRecords)
        self.connect(self.QTableList, QtCore.SIGNAL('itemSelectionChanged()'), self.showRecordskb)


        middleLayout = QtGui.QHBoxLayout()
        leftLayout = QtGui.QVBoxLayout()

        leftLayout.addWidget(self.tablelabel)
        leftLayout.addWidget(self.QTableList)
        leftLayout.addWidget(wallabel)
        leftLayout.addWidget(walinfo)

        rightLayout = QtGui.QVBoxLayout()
        self.recordlabel = QtGui.QLabel('Records')
        self.recordtable = QtGui.QTableWidget()

        #self.connect(self.recordtable, QtCore.SIGNAL('itemClicked(QListWidgetItem *)'), self.recorddump)
        #self.connect(self.recordtable, QtCore.SIGNAL('itemSelectionChanged()'), self.recorddumpkb)
        self.connect(self.recordtable, QtCore.SIGNAL('currentCellChanged(int,int,int,int)'), self.recorddump)

        rightLayout.addWidget(self.recordlabel)
        rightLayout.addWidget(self.recordtable)

        middleLayout.addLayout(leftLayout)
        middleLayout.addSpacing(3)
        middleLayout.addLayout(rightLayout)

        bottomLayout = QtGui.QHBoxLayout()
        btleftLayout = QtGui.QVBoxLayout()
        btrightLayout = QtGui.QVBoxLayout()


        self.Qcheckheader = QtGui.QCheckBox('header analysis')

        self.Qcheckheader.setFixedWidth(140)

        QAnBtn = QtGui.QPushButton('Analysis', self)
        QAnBtn.setFixedWidth(150)
        self.connect(QAnBtn, QtCore.SIGNAL('clicked()'), self.process)
        self.QProgressbar = QtGui.QProgressBar()
        self.QProgressbar.setFixedWidth(140)

        btleftLayout.addWidget(self.Qcheckheader)
        btleftLayout.addWidget(QAnBtn)
        btleftLayout.addWidget(self.QProgressbar)

        self.hexdump = QtGui.QTextBrowser()
        hexdumplabel = QtGui.QLabel('Hex Dump')
        btrightLayout.addWidget(hexdumplabel)
        btrightLayout.addWidget(self.hexdump)

        bottomLayout.addLayout(btleftLayout)
        middleLayout.addSpacing(3)
        bottomLayout.addLayout(btrightLayout)

        mainLayout.addLayout(topLayout)
        mainLayout.addLayout(middleLayout)
        mainLayout.addLayout(bottomLayout)

        self.setLayout(mainLayout)
        self.show()

    def openfile(self):
        self.filename = QtGui.QFileDialog.getOpenFileName(self, 'Open File', QtCore.QDir.homePath())
        self.Qfilepath.setText(self.filename)

    def headeran(self):
        print 'header an start'

    def process(self):

        # Clean-Up
        self.QTableList.clear()
        self.recordtable.clear()
        self.hexdump.clear()

        if self.Qcheckheader.isChecked():
            print 'Header analysis checked'
            self.headeran()

        if self.filename == '':     # file open error
            #msg = QtGui.QMessageBox()
            #msg.setWindowTitle('File Open Error')
            #msg.setInformativeText("File open error")
            #msg.show()
            return
        self.walite = walitean.WAL_SQLITE()
        self.walite.open(self.filename)
        framelist = self.walite.get_frame_list()
        self.d = self.walite.process(framelist)

        self.framecount = framelist.__len__()
        self.pagesize = self.walite.pagesize

        if(self.d.__len__() == 0):
            QtGui.QMessageBox('Error!', 'WAL Parsing Failed!')
        else:
            #self.showfileinfo()
            self.showtablelist()
            #self.showRecords()


    def showtablelist(self):
        self.QTableList.clear()
        tablenum = self.d.__len__()
        #print tablenum
        self.tablelabel.setText('Table List (%d)'%self.d.__len__())

        for key, value in self.d.iteritems():
            item = QtGui.QListWidgetItem("Table %s"%key)
            self.QTableList.addItem(item)

    def showRecordskb(self):    # keyboard interrupt handler
        #print self.QTableList.selectedItems()
        self.showRecords(self.QTableList.currentItem())


    def showRecords(self, item):
        self.recordtable.clear()
        #print 'Table Name is : %s'%str(item.text())
        encodedcolumn = str(item.text()).split(' ')[1]
        records = self.d[encodedcolumn]

        self.tablename = encodedcolumn # save tablename for hexdump

        # show column list
        columnlist = walitean.DecodeColumn(encodedcolumn)

        #print columnlist

        self.recordtable.setRowCount(len(records))
        #print len(records)
        self.recordtable.setColumnCount(len(columnlist))

        # print Column Name
        self.recordtable.setHorizontalHeaderLabels(QtCore.QStringList(columnlist))

        self.recordlabel.setText('Records (%d)'%records.__len__())

        rownum = 0
        for record in records:
            colnum = 0
            for value in record:
                if columnlist[colnum] == 'text':
                    try:
                        #print value
                        self.recordtable.setItem(rownum, colnum, QtGui.QTableWidgetItem(QtCore.QString("%1").arg(value.decode('utf8').replace('\n', ' '))))
                    except UnicodeEncodeError:
                        self.recordtable.setItem(rownum, colnum, QtGui.QTableWidgetItem(QtCore.QString("%1").arg(value.replace('\n', ' '))))
                elif columnlist[colnum] == 'int':
                    try:
                        self.recordtable.setItem(rownum, colnum, QtGui.QTableWidgetItem(QtCore.QString("%1").arg(int(value))))
                    except TypeError: # None Type
                        self.recordtable.setItem(rownum, colnum, QtGui.QTableWidgetItem(QtCore.QString("%1").arg('None')))
                else:
                    self.recordtable.setItem(rownum, colnum, QtGui.QTableWidgetItem(QtCore.QString("%1").arg(value)))
                colnum += 1
            rownum += 1

    # testing pharse
    def recorddump(self, curRow, curCol, preRow, preCol):
        if (curRow == preRow) and (curCol == preCol):
            return

        self.hexdump.clear()

        records = self.d[self.tablename]

        data = records[curRow][curCol]
        output = hexdump(data)
        self.hexdump.setText(output)

    #def showfileinfo(self):
        # showing wal file information

# SOURCE: http://mwultong.blogspot.com/2007/04/python-hex-viewer-file-dumper.html
def hexdump(buf):
    offset = 0
    output = ''
    while offset < len(buf):
        buf16 = buf[offset:offset + 16]
        buf16Len = len(buf16)
        if buf16Len == 0: break
        output += "%08X:  " % (offset)

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
        output += '\r'
    return output


def main():
    app = QtGui.QApplication(sys.argv)
    wUI = WaliteanUI()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()