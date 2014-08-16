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

class SQLITE():
    def __init__(self, filename):
        self.conn = sqlite3.connect(filename)
        self.cursor = self.conn.cursor()

    def getDBTableList(self):
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tbllist = self.cursor.fetchall()
        return tbllist

    def getDBTblInfoList(self, tablename):
        self.cursor.execute("PRAGMA table_info(%s)"%tablename)
        columnlist = self.cursor.fetchall()
        return columnlist

    def close(self):
        self.conn.close()

def main():
    tblset = []
    db = SQLITE("sms.db")
    tbllist = db.getDBTableList()
    for tblname in tbllist:
        dbdata = []
        #print tblname
        columnlist = db.getDBTblInfoList(tblname)
        #print columnlist
        dbdata.append(tblname)
        dbdata.append(columnlist)
        tblset.append(dbdata)

    con = sqlite3.connect('sms-wal.db')
    cursor = con.cursor()

    for tbl in tblset:
        query = "CREATE TABLE "+tbl[0][0]+"("
        count = len(tbl[1])
        for column in tbl[1]:
            count -= 1
            query += column[1]
            query += " "
            query += column[2]
            if count != 0:
                query += ", "
        query += ");"
        print query
        try:
            cursor.execute(query)
        except sqlite3.OperationalError, e:
            print e
    con.close()





if __name__ == "__main__":
    main()