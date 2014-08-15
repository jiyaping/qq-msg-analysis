#encoding=utf-8

import sqlite3
import datetime

#connect db
cx = sqlite3.connect("./user.db")
cu = cx.cursor()

#output file
op = ""

def str2microsecond(str):
    dt = datetime.datetime.strptime(str, '%Y-%m-%d')
    return int((dt - datetime.datetime(1970, 1, 1, 8, 0, 0)).total_seconds() * 1000)

def microsecond2str(ptime):
    return datetime.datetime.fromtimestamp(ptime * 0.001)

#generate js data for use ,data example:
# datas = {session1:[{label1:"nickname",data:[[1222332323,21]]},{label1:"nickname",data:[[1222332323,21]]}]}
# habits = {session1:[{nick1:{token1:20,token3:40}},{nick1:{token1:20,token3:40}}],session1:[{nick1:{token1:20,token2:30,token3:40}}]}
def getHabit(session, nick):
    sql = '''
select * from (select tokens.token,
       count,
       tokens.flag,
       (case
         when tokens.flag like 'n%' then
          tokens.count * 2.0
         when tokens.flag like 'a%' then
          tokens.count * 0.9
         when tokens.flag like 'v%' then
          tokens.count * 1
         when tokens.flag like 'eng%' then
          tokens.count * 0.8
         when tokens.flag like 'r%' then
          tokens.count * 0
         when tokens.flag like 'x%' then
          0
         else
          tokens.count * 0.1
       end) newcount,random() rand
  from tokens
 where length(token) > 1
   and newcount > 3 and count > 3 
   and (token != '图片' and token != '表情')
   and tokens.sessionid = ?
   and tokens.userid = ?
 order by newcount desc limit ?
) temp order by rand desc
    ''' 
    #print sql
    _str = ""
    ress = cu.execute(sql,(session, nick, 100)).fetchall()
    for i in range(len(ress)):
        _token = ress[i][0]
        _count = ress[i][1]
        _str += "'%s':%s ," % (_token, _count)

    if nick is None:
        nick = "None"
    #print "{'" + nick + "': {" + _str + "}}"
    #print "----------"
    return "'" + nick + "': {" + _str + "}"

def getChartData(session, nick):
    # data gather timespan
    sql = '''
    select strftime('%Y-%m-%d', dr) ndr,
       (select count(*)
          from msgs
         where session = ?
           and (nick = ? or userid = ?)
           and  date(time) = dr) ncount
  from (select date(strftime('%Y-%m-%d',
(select min(time)
                   from msgs
                  where session = ?)
), '+' || id || ' day') as dr
           from nlists
          where id <(select julianday(date(max(time)))-julianday(date(min(time)))
                   from msgs
                  where session = ?)) dateranges;
    '''

    #print sql
    ress = cu.execute(sql,(session, nick, nick, session, session)).fetchall()
    _str = ""
    for i in range(len(ress)):
        _str += "[%s,%s]" % (str2microsecond(ress[i][0]), int(ress[i][1])) + ","

    return "{label:'%s',data:[%s]}" % (nick, _str)

def foreach_data():
    sql = "select distinct session from msgs"
    ress = cu.execute(sql).fetchall()
    count = 0

    habit_str = ""
    chart_str = ""
    for i in range(len(ress)):
        session = ress[i][0]
        habit_item_str = ""
        chart_item_str = ""
        #1. nomal msg
        if not session.startswith(u"我的QQ群"):
            sql = "select distinct nick from msgs where session = '%s'" % session
            sub_ress = cu.execute(sql).fetchall()

            for sub_i in range(len(sub_ress)):
                habit_item_str = habit_item_str + getHabit(session, sub_ress[sub_i][0]) + ","
                chart_item_str = chart_item_str + getChartData(session, sub_ress[sub_i][0]) + ","

        #2. qun msg
        else:
            sql = "select distinct userid from msgs where session = '%s'" % session
            sub_ress = cu.execute(sql).fetchall()

            for sub_i in range(len(sub_ress)):
                habit_item_str = habit_item_str + getHabit(session, sub_ress[sub_i][0]) + ","
                chart_item_str = chart_item_str + getChartData(session, sub_ress[sub_i][0]) + ","

        habit_str = habit_str + "'" + session + "':{" + habit_item_str + "},"
        chart_str = chart_str + "'" + session + "':[" + chart_item_str + "],"
        count += 1
        print count
        
    return "habits = {" + habit_str +"}" + ";" + "chartdata = {%s}" % chart_str

def save2disk():
    file_object = open('result/static/data.js', 'w')
    file_object.write(foreach_data().encode('utf-8','ignore'))
    file_object.close( )

#run and save data to disk
save2disk()
#print foreach_data().encode('utf-8','ignore')