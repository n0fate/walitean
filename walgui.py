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
        self.initUI()   # init

    def initUI(self):

        QOpenBtn = QtGui.QPushButton('Open')
        self.connect(QOpenBtn, QtCore.SIGNAL('clicked()'), self.openfile)

        QAnBtn = QtGui.QPushButton('Analysis', self)
        self.connect(QAnBtn, QtCore.SIGNAL('clicked()'), self.process)

        self.Qfilepath = QtGui.QLineEdit()
        self.QProgressbar = QtGui.QProgressBar()
        #QOpenBtn.move(600, 50)
        #QAnBtn.move(600, 100)

        self.setGeometry(300, 300, 800, 400)
        self.move(300, 300)
        self.setWindowTitle('walitean - WAL Analyzer for SQLite by n0fate')

        # set layout
        mainLayout = QtGui.QVBoxLayout()    # main layout
        topLayout = QtGui.QHBoxLayout()
        topLayout.addWidget(QOpenBtn)
        topLayout.addWidget(self.Qfilepath)
        topLayout.addWidget(QAnBtn)
        topLayout.addWidget(self.QProgressbar)

        tablelabel = QtGui.QLabel('Table List')
        tablelabel.setFixedWidth(150)
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


        bottomLayout = QtGui.QHBoxLayout()
        leftLayout = QtGui.QVBoxLayout()

        leftLayout.addWidget(tablelabel)
        leftLayout.addWidget(self.QTableList)
        leftLayout.addWidget(wallabel)
        leftLayout.addWidget(walinfo)

        rightLayout = QtGui.QVBoxLayout()
        recordlabel = QtGui.QLabel('Records')
        self.recordtable = QtGui.QTableWidget()

        rightLayout.addWidget(recordlabel)
        rightLayout.addWidget(self.recordtable)

        bottomLayout.addLayout(leftLayout)
        bottomLayout.addSpacing(3)
        bottomLayout.addLayout(rightLayout)

        mainLayout.addLayout(topLayout)
        mainLayout.addLayout(bottomLayout)

        self.setLayout(mainLayout)
        self.show()

    def openfile(self):
        self.filename = QtGui.QFileDialog.getOpenFileName(self, 'Open File', QtCore.QDir.homePath())
        self.Qfilepath.setText(self.filename)


    def process(self):
        self.walite = walitean.WAL_SQLITE()
        self.walite.open(self.filename)
        framelist = self.walite.get_frame_list()
        self.d = self.walite.process(framelist)

        if(self.d.__len__() == 0):
            QtGui.QMessageBox('Error!', 'WAL Parsing Failed!')
        else:
            #self.showfileinfo()
            self.showtablelist()
            #self.showRecords()


    def showtablelist(self):
        tablenum = self.d.__len__()
        #print tablenum

        for key, value in self.d.iteritems():
            item = QtGui.QListWidgetItem("Table %s"%key)
            self.QTableList.addItem(item)



    def showRecords(self, item):
        self.recordtable.clear()
        print 'Table Name is : %s'%str(item.text())
        encodedcolumn = str(item.text()).split(' ')[1]
        records = self.d[encodedcolumn]
        print records
        #print records.__len__()

        # show column list
        columnlist = walitean.DecodeColumn(encodedcolumn)

        print columnlist

        self.recordtable.setRowCount(len(records))
        #print len(records)
        self.recordtable.setColumnCount(len(columnlist))

        # print Column Name
        self.recordtable.setHorizontalHeaderLabels(QtCore.QStringList(columnlist))

        rownum = 0
        for record in records:
            colnum = 0
            for value in record:
                self.recordtable.setItem(rownum, colnum, QtGui.QTableWidgetItem(QtCore.QString("%1").arg(value)))
                colnum += 1
            rownum += 1







    #def showfileinfo(self):
        # showing wal file information



def main():
    app = QtGui.QApplication(sys.argv)
    wUI = WaliteanUI()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()