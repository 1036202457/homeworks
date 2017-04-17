#!usr/bin/env
# -*-coding:utf-8 -*-
connection = False
config = {
    'bbs_urls_file': r'bbs_urls.txt',
}


# 根据文件路径读取文件内容
def getFileContent(filename):
    f = open(filename, 'r')
    return f.read()


# 根据url获取网页html源码，并智能分析encoding后解码返回
def getUrlContent(url):
    # 读取html
    from urllib import request
    req = request.Request(url)
    req.add_header("user-agent",
                   "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36")
    response = request.urlopen(req)
    content = response.read()
    return content

# 根据给定的内容，判断文本编码，并把解码后的内容返回
def getDecodedContent(content):
    # 智能分析encoding
    import chardet
    encoding = chardet.detect(content)
    print('url=' + url + '\nencoding=' + str(encoding))
    # 根据encoding解码返回
    return content.decode(encoding['encoding']).strip()

# 根据url查询数据库中是否有该url的网页html源码获取记录
def getPageFromDbByUrl(url):
    connection = getDbConnection()
    cur = connection.cursor()
    sql = 'select * from PAGE where URL = ?'
    executeResult = cur.execute(sql, (str(url),))
    if (len(cur.fetchall())>0):
        return cur.fetchone()
    return 0


# 根据id获取网页html源码获取记录
def getPageFromDbById(id):
    connection = getDbConnection()
    cur = connection.cursor()
    sql = 'select * from PAGE where id = ?'
    executeResult = cur.execute(sql, (str(id),))
    if (len(cur.fetchall()) > 0):
        return cur.fetchone()
    return 0


# 把网页html源码获取结果存入到数据库
def addPageToDb(url, content='',grabbed=0,decoded=0):
    connection = getDbConnection()
    try:
        # 获取当前时间戳
        import time
        timestamp = str(int(time.time()))
        # 生成记录内容
        argsTuple = (url, content, decoded,grabbed,timestamp, timestamp)
        # 生成sql语句
        sql = "insert into PAGE(`URL`,`CONTENT`,`DECODED`,`GRABBED`,`CREATEDTIME`,`UPDATEDTIME`) values(?,?,?,?,?,?)"
        cur = connection.cursor()
        # 执行操作
        cur.execute(sql, argsTuple)
        # 无错误发生，确认操作生效
        connection.commit()

    except Exception as err:
        # 发现错误，本次操作回滚
        connection.rollback()
        print(err)


# 把网页html源码获取结果存入到数据库
def updatePageInDb(id, content='',grabbed=0,decoded=0):
    connection = getDbConnection()
    try:
        # 获取当前时间戳
        import time
        timestamp = str(int(time.time()))
        # 生成记录内容
        argsTuple = (content, decoded,grabbed,timestamp,id)
        # 生成sql语句
        sql = "update PAGE set `CONTENT`= ? ,`GRABBED`= ?, `DECODED`=?,UPDATEDTIME=? where id = ?"
        cur = connection.cursor()
        # 执行操作
        cur.execute(sql, argsTuple)
        # 无错误发生，确认操作生效
        connection.commit()

    except Exception as err:
        # 发现错误，本次操作回滚
        connection.rollback()
        print(err)


# 获取数据库连接，并保存到全局变量connection中，减少连接建立数，做到连接重利用，加快处理效率
def getDbConnection():
    # 连接的最多尝试次数
    retryLimit = 2
    tryCount = 0
    global connection
    if(connection!=False):
        return connection
    try:
        import sqlite3
        while (retryLimit > tryCount and connection == False):
            # 连接数据库语句
            connection = sqlite3.connect('test.db')
            tryCount += 1
        if (connection == False):
            raise Exception('数据库连接失败')
    except Exception as err:
        print(err)
        exit()
    print ("Opened database successfully")

    print('checking table PAGE')
    cur = connection.cursor()
    tryLimit = 2
    tryCount = 0
    while (tryCount<tryLimit):
        try:
            tryCount += 1
            cur.execute("select * from PAGE")
            print('table PAGE OK')
            break;
        except  Exception as err:
            errStr = str(err)
            print(errStr)
            if(errStr.find('no such table',0)!=-1):
                print('trying to create table PAGE')
                connection.execute('''CREATE TABLE PAGE
                       (ID INTEGER PRIMARY KEY   NOT NULL,
                       URL           TEXT    NOT NULL,
                       CONTENT       TEXT    NOT NULL,
                       DECODED       INT     NOT NULL,
                       GRABBED       INT     NOT NULL,
                       CREATEDTIME   INT     NOT NULL,
                       UPDATEDTIME   INT     NOT NULL);''')
                connection.commit()
                print ("Table created successfully")
                break
            else:
                connection.close()
                exit()
    return connection


# 程序最后才调用该函数来关闭数据库连接
def closeDbConnection():
    if (connection):
        connection.close()
    print('database connection successfully closed')

def printSiteStrs():
    connection = getDbConnection()
    cur = connection.cursor()
    sql = 'select * from PAGE'
    cur.execute(sql)
    result = cur.fetchall()
    if (len(result)>0):
        for row in result:
            printSiteStr(row[2])
    return 0


def printSiteStr(html):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html)
    for str in soup.stripped_strings:
        print(str)


# 读取bbs_urls.txt文件，并根据其中的url获取html源码、中文编码encoding解码并存入到数据库
def getBbsUrlsToDb():
    global config
    # 读取bbs_urls.txt文件
    urls = getFileContent(config['bbs_urls_file'])
    # 解析其中的url
    urlList = urls.split(sep='\n')
    urlListLength = len(urlList)
    for key, url in enumerate(urlList):
        print('当前第' + str(key) + '条（共' + str(urlListLength) + '）url：' + url + ' ')
        # 判断是不是有效url
        if (url.strip() == ''):
            continue
        # 检查是否已获取过该url的源代码
        dbRecord = getPageFromDbByUrl(url)
        # dbRecord = getPageFromDbById(5)
        # 如果没获取过
        if (dbRecord == 0):
            try:
                # 根据url获取html源码并中文编码encoding解码
                print('未获取，下载中' + '.' * 10)
                content = getUrlContent(url)
                # 成功返回，但先检查返回的内容是否有效
                if (content != ''):
                    # 数据有效，存入到数据库
                    addPageToDb(url, content,1)
                    print('成功插入到数据库')
            except Exception as err:
                print('currentUrl:' + url + ' \nerrorInfo:' + str(err))
                addPageToDb(url)
        else:
            print('已获取，跳过')
        print('='*20)