#! /usr/bin/env python
#coding=utf-8

import sys
import urllib2
import re
import urlparse
import time
from bdb import BDB
from mysql import DbMgr
from HTMLParser import HTMLParser
from chardet import detect

headers={
"User-Agent": "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9.1) Gecko/20090624 Firefox/3.5",
"Accept":"text/html,application/xhtml+xml,application/xml"
}

class URL:
    def __init__(self,url):
        up = urlparse.urlparse(url)
        self.url = None
        self.host = None
        try:
            self.url = up.geturl()
            self.host = up.netloc
            if self.host is None or len(self.host) == 0:
                self.url = None
        except:
            pass
        pass
    def get_url(self):
        return self.url

    def get_host(self):
        return self.host

class HtmlParse(HTMLParser):
    def __init__(self):
        self.links = set()
        self.title = ""
        self.reading_title = False
        HTMLParser.__init__(self)

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name,value in attrs:
                if name == "href":
                    self.links.add(value.strip(" \t\n\r"))

        elif tag == "title":
            self.reading_title = True

        elif tag == "meta":
            for name,value in attrs:
                index = value.find("url")
                if index >= 0:
                    index = value.find("http")
                if index == -1:
                    continue
                self.links.add(value[index:].strip(" \t\n\r"));

    def handle_data(self, data):
        if self.reading_title:
            self.title += data

    def handle_endtag(self, tag):
        if tag == "title":
            self.reading_title = False

    def get_title(self):
        return self.title

    def get_links(self):
        return self.links

def download_html(host, url):
    data = None
    try:
        headers['Host'] = host
        req = urllib2.Request(url, headers=headers)
        data = urllib2.urlopen(req, timeout=5.0).read()
    except Exception, e:
        sys.stderr.write("urllib2 error: %s\n" % (str(e)))
    return data

def check_link(host, url):
    list = [".doc", ".ppt", ".xls", ".rtf", "chm", "pdf"]
    if len(url) == 1 and (url[0]=='#' or url[0] == '/'):
        return False
    if url.startswith("http") and url.find(host) < 0:
        return False
    if url.startswith("javascript"):
        return False
    for l in list:
        if url.find(l) >= 0:
            return False
    return True

def main(main_url, taskbdb, recbdb, mysqldb):
    ret = 1
    last_time = time.time()
    cnt = 0
    while True:

        now = time.time()
        if int(now - last_time) < 0.5:
            time.sleep(1.0 - now + last_time)
        last_time = now
        if cnt++ % 10 == 0:
            time.sleep(3)

        keyvalue = taskbdb.pop()
        if keyvalue is None:
            sys.stdout.write(main_url.get_host() + " is over\n")
            ret = 0
            break
        url = URL(keyvalue[0])
        if url.get_url() is None:
            sys.stderr.write("pop url=[%s] is invalid\n" % (keyvalue[0]))
            continue

        data = download_html(url.get_url())
        ret = 0
        if data is None or len(data)==0:
            sys.stderr.write("download_html failed, url=[ %s ]\n" % (url.get_url()))
            ret = -1
            continue
        parser = HtmlParse()
        try:
            parser.feed(data)
        except Exception,e:
            ret = -1
            sys.stderr.write("[ %s ] parse html error: %s\n" % (url.get_url(), e))

        title = parser.get_title()
        if len(title) == 0:
            pass
        else:
            #insert into mysql
            mysqldb.record_url2title(url.get_url(), title)
            pass
        links = parser.get_links()
        for link in links:
            if not check_link(main_url.get_host(), link):
                continue

            if not link.startswith("http"):
                if link.find(main_url.get_host()) >= 0:
                    link = urlparse.urljoin("http://", link)
                else:
                    link = urlparse.urljoin(url.get_url(), link)

            link_tmp = URL(link)
            if link_tmp.get_url() is None:
                sys.stderr.write("parse url=[%s] is error\n" % (link))
                continue

            if not recbdb.hasKey(link_tmp.get_url()):
                recbdb.add(link_tmp.get_url(), "1")
                taskbdb.add(link_tmp.get_url(), "1")
    return ret

def usage(app):
    print "usage: "
    print "     %s index_url" % (app)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        usage(sys.argv[0])
        sys.exit(-2)

    url = sys.argv[1]
    if not url.startswith("http"):
        url = "http://" + url

    #mysqldb = None
    mysqldb = DbMgr("127.0.0.1", "root", "123", "robotrecord")
    main_url = URL(url)
    if main_url.get_url() is None:
        sys.stderr.write("the index url=[%s] is invalid\n" % (url))
        sys.exit(-2)

    dbname = main_url.get_host().replace(".", "_");
    taskbdb = BDB.btopen(".db/", dbname + ".db", "url2title")
    recbdb = BDB.btopen(".db/", dbname + "_rc.db", "url2title")

    if not recbdb.hasKey(main_url.get_url()):
        taskbdb.add(main_url.get_url(), "1")
        recbdb.add(main_url.get_url(), "1")

    ret = 1
    try:
        ret = main(main_url, taskbdb, recbdb, mysqldb)
    except Exception,e:
        sys.stderr.write("spider %s exit error: %s\n" % (url, str(e)))

    del taskbdb
    del recbdb

    sys.exit(ret)



"""
#!/bin/bash
if [ ! $# -eq 1 ]
then
    echo "usage:"
    echo "      $0 index_url "
    exit 1
fi

#!/bin/bash

if [ ! $# -eq 1 ]
then
    echo "usage:"
    echo "      $0 index_url "
    exit 1
fi

path=`pwd`

index_url=$1

cd spider
mkdir -p .db

touch ${path}/err.log
touch ${path}/out.log

while [ 1 ]
do
    python spider.py $index_url 2>>${path}/err.log 1>>${path}/out.log
    if [ $? -eq 0 ]
    then
        echo "over" >> ${path}/out.log
        break
    fi
    sleep 1
    echo "continue $index_url" >>${path}/err.log
done

cd ${path}

"""