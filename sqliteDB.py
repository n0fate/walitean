#C:\Python27\python.exe

import sqlite3

class SQLITE():
    def __init__(self, filename):
        self.conn = sqlite3.connect(filename)
        self.cursor = self.conn.cursor()

    def getDBTableList(self):
        cursor = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tbllist = list(map(lambda x: x[0], cursor.description))
        return tbllist

    def getDBTblInfoList(self, tablename):
        cursor = self.conn.execute("PRAGMA table_info(%s)"%tablename)
        columnlist = list(map(lambda x: x[0], cursor))
        return columnlist

    def close(self):
        self.conn.close()

def main():
    db = SQLITE("sms.db")
    tbllist = db.getDBTableList()
    for tblname in tbllist:
        columnlist = db.getDBTblInfoList(tblname)
        print columnlist


if __name__ == "__main__":
    main()