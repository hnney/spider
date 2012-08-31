#! /usr/bin/env python
#coding=utf-8
from bsddb import db
import os
import sys

class BDB:
    def __init__(self, db, env):
        self.env = env
        self.db = db
        self.cursor = None

    def __del__(self):
        try:
            self.close()
        except:
            pass

    @staticmethod
    def btopen(homedir, filename, dbname):
        if not os.path.exists(homedir) :
            os.makedirs(homedir)
        env = db.DBEnv()
        env.open(homedir,  \
                    db.DB_USE_ENVIRON | \
                    db.DB_INIT_LOCK | \
                    db.DB_INIT_MPOOL | \
                    db.DB_CREATE | \
                    db.DB_THREAD )
        bdb = db.DB(env)
        bdb.open(filename, dbname, db.DB_BTREE, \
                    db.DB_CREATE |
                    db.DB_THREAD )
        return BDB(bdb, env)

    def close(self):
        if self.cursor :
            self.cursor.close()

        self.db.close()
        self.env.close()

    def keys(self):
        pass

    def hasKey(self, key):
        return self.db.has_key(repr(key))

    def add(self, key, value):
        try:
            self.db[repr(key)] = repr(value)
        except Exception,e:
            sys.stderr.write("key=[%s] insert into db error:%s\n" %(str(key),str(e)))

    def delete(self, key):
        try:
            self.db.delete(repr(key))
            return True
        except:
            return False

    def get(self, key):
        return self.db[repr(key)]

    def sync(self):
        self.db.sync()

    def __len__(self):
        return len(self.db)

    def first(self):
        if self.cursor :
            self.cursor.close()
        self.cursor = self.db.cursor()
        return self.cursor.first()

    def last(self):
        return self.cursor.last()

    def next(self):
        return self.cursor.next()

    def prev(self):
        return self.cursor.prev()

    def pop(self):
        #if not len(self.db):
        #    return None, None
        cursor = self.db.cursor()
        result = cursor.first()
        cursor.close()
        if not result:
            return None
        self.db.delete(result[0])
       # self.db.sync()
        return result

if __name__ == '__main__':
    pass
