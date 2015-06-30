import sqlite3
import os


def isExist(filename):
    return os.path.exists(filename)


class ExportSQLite:
    def __init__(self):
        self.conn = ''
        self.cursor = ''

    def createDB(self, filename='output.db'):
        if isExist(filename):
            return False
        self.conn = sqlite3.connect(filename)
        self.conn.text_factory = str
        self.cursor = self.conn.cursor()
        return True

    def createTable(self, tablename, columnlist):
        sql = 'CREATE TABLE IF NOT EXISTS ' + tablename
        sql += ' ('
        count = 0
        for column in columnlist:
            if column[0] is None or column[1] is None:
                continue
            sql += ' ' + column[0] + ' ' + column[1]  # column name
            count += 1

            if len(columnlist) > count:
                sql += ','

        sql += ' )'
        # print sql
        self.cursor.execute(sql)
        self.conn.commit()

    def insertRecord(self, tablename, record):
        sql = 'INSERT INTO %s VALUES' % tablename
        for i in xrange(len(record)):
            if i == 0:
                sql += '('
            sql += ' ?'
            if (i + 1) == len(record):
                sql += ')'
            else:
                sql += ','
        self.cursor.execute(sql, record)

    def commit(self):
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()


