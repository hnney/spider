#! /usr/bin/env python
#coding=utf-8
import MySQLdb
from chardet import detect

class DbMgr:
    def __init__(self, host, user, pwd, db, port = 3306):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
        self.port = port
        self.conn = MySQLdb.connect(host=host,
                user=user, passwd=pwd, db=db, port=port, charset='utf8')
        self._query_no_result("set interactive_timeout=24*3600")

    def _query_no_result(self, sql, param=()):
        c = self.conn.cursor()
        c.execute(sql, param)
        self.conn.commit()

    def _query(self, sql, param=()):
        c = self.conn.cursor()
        c.execute(ql, param)
        return c

    def record_url2title(self, url, title):
        try:
            s = title.decode(detect(title)["encoding"],"ignore")
            title = s.encode('UTF-8')
            t = MySQLdb.escape_string(title.strip(" \n\r\t"))
            u = MySQLdb.escape_string(url)
            sql = "insert into url2title(url,title) values('%s','%s') \
            ON DUPLICATE KEY UPDATE title='%s'" % (u, t, t)
            self._query_no_result(sql)
        except:
            pass

if __name__ == '__main__' :
    mysqldb = DbMgr("127.0.0.1", "root", "123", "robotrecord")
    mysqldb.record_url2title("www.baidu.com", "百度")