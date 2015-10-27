#encoding=utf-8

import sqlite3
import jieba
import jieba.posseg as pseg

#connect db
cx = sqlite3.connect("./user.db")
cu = cx.cursor()

#init jieba
#jieba.load_userdict("c:/mydict.txt")

#parase single user
def analysis(session, userid):
    sql = "select * from msgs where (nick=? or userid=?) and session=?"

    result = cu.execute(sql, (userid, userid, session))
    msgs = result.fetchall()

    str = ""
    for i in range(len(msgs)):
        str += msgs[i][5]
        
    ts = tokens(str)
    dslist = []
    for key in ts.keys():
        dslist.append((session, userid, key, ts[key][0], ts[key][1],))

    cu.executemany("insert into tokens(sessionid, userid, token, count, flag) \
        values (?, ?, ?, ?, ?) ;", dslist)

def tokens(str):
    '''
       cut the str and return tokens map
    '''
    words = pseg.cut(str)

    result = {}
    for w in words:
        if w.word in result:
            result[w.word][0] += 1
        else:
            result[w.word] = [1, w.flag]

    return result

#analysis all by session id
sql = 'select distinct session from msgs'
ress = cu.execute(sql).fetchall()
count = 0

print("session count: ", len(ress))

for i in range(len(ress)):
    session = ress[i][0]

    #1. nomal msg
    if not session.startswith(u"我的QQ群"):
        sql = "select distinct nick from msgs where session = '%s'" % session
        sub_ress = cu.execute(sql).fetchall()

        for sub_i in range(len(sub_ress)):
            analysis(session, sub_ress[sub_i][0])
    #2. qun msg
    else:
        sql = "select distinct userid from msgs where session = '%s'" % session
        sub_ress = cu.execute(sql).fetchall()

        for sub_i in range(len(sub_ress)):
            analysis(session, sub_ress[sub_i][0])

    cx.commit()
    count += 1
    print('process session ', count, 'finished.')