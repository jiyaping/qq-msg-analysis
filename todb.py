# encoding=utf-8

import sqlite3
import re
import sys
import datetime

# connect db
cx = sqlite3.connect('./user.db')
cu = cx.cursor()

# init database
create_msg_sql = '''
CREATE TABLE if not exists "msgs" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
    "time" datetime, 
    "session" varchar, 
    "nick" varchar, 
    "userid" varchar, 
    "content" varchar
);'''

create_nlist_sql = '''
CREATE TABLE if not exists "nlists" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL
);'''


create_token_sql = '''
CREATE TABLE if not exists "tokens" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
    "userid" varchar, 
    "sessionid" varchar, 
    "token" varchar, 
    "flag" varchar, 
    "count" integer
);'''

cu.execute(create_msg_sql)
cu.execute(create_nlist_sql)
cu.execute(create_token_sql)

# init nlists data
result = cu.execute("select count(*) from nlists").fetchone()
if int(result[0]) <= 0:
    cu.executemany("insert into nlists(id) values(?)", [[i] for i in range(10000)])
    cx.commit()

# parse it
msg_pattern = '(\d{4}-\d{2}-\d{2}\s*\d{1,}:\d{2}:\d{2})\s{0,}\n?(.*?)((\(|<)(.*)(\)|>)){0,1}\n(.*)'

def get_session(str):
    matches = re.match(r'={60,}\n消息分组:(.*)\n={60,}\n消息对象:(.*)\n={60,}', str)

    if matches:
        return matches.group(1) + " - " + matches.group(2)

def msg2db(str, sessionid):
    if sessionid.startswith(u'最近联系人'): return

    matches = re.match(msg_pattern, str)
    if matches:
        if re.match('.*\d{1,}:\d{2}:\d{2}\s*', matches.group(2)): return

        sql = "insert into msgs(time, session, nick, userid, content) values(?, ?, ?, ?, ?)"
        cu.execute(sql, (datetime.datetime.strptime(matches.group(1), '%Y-%m-%d %H:%M:%S'),\
                        sessionid,\
                        matches.group(2),\
                        matches.group(5),\
                        matches.group(7)))

def parse(filename):
    curr_block = ''
    curr_node  = ''
    count      = 0

    try:
        file = open(filename, 'r', encoding='utf-8')
    except IOError as e:
        print("file open error:", e)
    else:
        while True:
            line = file.readline()
            if line == '':break

            if line.find(u'消息记录（此消息记录为文本格式，不支持重新导入') >= 0:
                continue
            if line.find('=' * 60) >= 0:
                curr_node = line
                # read 4 line
                curr_node += ''.join([file.readline() for _ in range(4)])
                curr_block = get_session(curr_node)
                curr_node = ''
                continue

            if line == '\n' and len(curr_node.strip()) != 0:
                msg2db(curr_node, curr_block)
                count += 1
                if count % 1000 == 0:
                    print("parse", count, 'msgs')
                curr_node = ''
            else:
                curr_node += line

# parsing
filename = sys.argv[1]
parse(filename)
cx.commit()