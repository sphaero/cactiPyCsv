#!/usr/bin/python
# -*- coding: utf8 -*- 
# Handy:
# http://docs.cacti.net/_media/manual:087:cacti_database_schema_0.8.7i.pdf
#import xml.etree.ElementTree as ET
import MySQLdb
import argparse
import rrdtool
import csv
import time

LOGLEVEL=2
CURPID="-"
OUTPUT='notset.csv'

def debug(msg):
    if LOGLEVEL>1:
        print("%s" %"DBG: %s:" %CURPID),
        if isinstance(msg, str) or isinstance(msg, unicode):
            print(msg.encode('utf-8'))
        else:
            print(msg)

def warn(msg):
    if LOGLEVEL>0:
        print("%s" %"WRN: %s:" %CURPID),
        if isinstance(msg, str) or isinstance(msg, unicode):
            print(msg.encode('utf-8'))
        else:
            print(msg)

def log(msg):
    print("%s" %"LOG: %s:" %CURPID),
    if isinstance(msg, str) or isinstance(msg, unicode):
        print(msg.encode('utf-8'))
    else:
        print(msg)
    
def main():
    
    def addToCsv(name, data):
        global CURPID, OUTPUT
        CURPID="addToCsv"
        f = open(OUTPUT, 'ab')
        writer = csv.writer(f, delimiter=";", doublequote=True, quotechar="'")
        data.insert(0, name)
        writer.writerow(data)
        f.close()

    def writeCsvHeader():
        global CURPID, OUTPUT
        CURPID="writeCsvHeader"
        header = ['Name',
            'In Average',
            'In Max',
            'In 95%',
            'In Total',
            'Out Average',
            'Out Max',
            'Out 95%',
            'Out Total',
            'In/Out Max 95%',
            'In/Out Sum 95%',
            'In/Out Max n95%',
            'In/Out Sum n95%']
        f = open(OUTPUT, 'wb')
        writer = csv.writer(f, delimiter=";", doublequote=True, quotechar="'")
        writer.writerow(header)
        f.close()
    
    def getCactiTreeId(treeName='Default Tree'):
        global CURPID
        CURPID="getCactiTreeId"
        """
        +-----------+----------------------+------+-----+---------+----------------+
        | Field     | Type                 | Null | Key | Default | Extra          |
        +-----------+----------------------+------+-----+---------+----------------+
        | id        | smallint(5) unsigned | NO   | PRI | NULL    | auto_increment | 
        | sort_type | tinyint(3) unsigned  | NO   |     | 1       |                | 
        | name      | varchar(255)         | NO   |     |         |                | 
        +-----------+----------------------+------+-----+---------+----------------+
        """
        treeId = None
        q = "select `id` from `graph_tree` where `name`='%s'" %treeName
        rows = cur.execute(q)#, cgi.escape(treeName, True))
        print(q)
        print(rows)
        treeId = int(cur.fetchone()[0])
        debug("treeId is %s" %treeId)
        return treeId
    
    def getDescriptionFromLocalDataId(localDataId):
        global CURPID
        CURPID="getDescriptionFromLocalDataId"

        q = "select `host_id`, `snmp_query_id`, `snmp_index` from `data_local` where `id`=%s" %localDataId
        cur.execute(q)
        hostId, snmpQueryId, snmpIndex = cur.fetchone()
        q = "select `field_value` from `host_snmp_cache` where `host_id`=%s and `snmp_query_id`=%s and `snmp_index`=%s and `field_name`='ifAlias'" %(hostId, snmpQueryId, snmpIndex)
        cur.execute(q)
        descr = cur.fetchone()[0]
        #debug(descr)
        return descr
        
    def getNameFromLocalGraphId(localGraphId):
        global CURPID
        CURPID="getNameFromLocalGraphId"
        """
        +-------------------------------+-----------------------+------+-----+---------+----------------+
        | Field                         | Type                  | Null | Key | Default | Extra          |
        +-------------------------------+-----------------------+------+-----+---------+----------------+
        | id                            | mediumint(8) unsigned | NO   | PRI | NULL    | auto_increment | 
        | local_graph_template_graph_id | mediumint(8) unsigned | NO   |     | 0       |                | 
        | local_graph_id                | mediumint(8) unsigned | NO   | MUL | 0       |                | 
        | graph_template_id             | mediumint(8) unsigned | NO   | MUL | 0       |                | 
        | t_image_format_id             | char(2)               | YES  |     | 0       |                | 
        | image_format_id               | tinyint(1)            | NO   |     | 0       |                | 
        | t_title                       | char(2)               | YES  |     | 0       |                | 
        | title                         | varchar(255)          | NO   |     |         |                | 
        | title_cache                   | varchar(255)          | NO   | MUL |         |                | 
        | t_height                      | char(2)               | YES  |     | 0       |                | 
        | height                        | mediumint(8)          | NO   |     | 0       |                | 
        | t_width                       | char(2)               | YES  |     | 0       |                | 
        | width                         | mediumint(8)          | NO   |     | 0       |                | 
        | t_upper_limit                 | char(2)               | YES  |     | 0       |                | 
        | upper_limit                   | varchar(20)           | NO   |     | 0       |                | 
        | t_lower_limit                 | char(2)               | YES  |     | 0       |                | 
        | lower_limit                   | varchar(20)           | NO   |     | 0       |                | 
        | t_vertical_label              | char(2)               | YES  |     | 0       |                | 
        | vertical_label                | varchar(200)          | YES  |     | NULL    |                | 
        | t_slope_mode                  | char(2)               | YES  |     | 0       |                | 
        | slope_mode                    | char(2)               | YES  |     | on      |                | 
        | t_auto_scale                  | char(2)               | YES  |     | 0       |                | 
        | auto_scale                    | char(2)               | YES  |     | NULL    |                | 
        | t_auto_scale_opts             | char(2)               | YES  |     | 0       |                | 
        | auto_scale_opts               | tinyint(1)            | NO   |     | 0       |                | 
        | t_auto_scale_log              | char(2)               | YES  |     | 0       |                | 
        | auto_scale_log                | char(2)               | YES  |     | NULL    |                | 
        | t_scale_log_units             | char(2)               | YES  |     | 0       |                | 
        | scale_log_units               | char(2)               | YES  |     | NULL    |                | 
        | t_auto_scale_rigid            | char(2)               | YES  |     | 0       |                | 
        | auto_scale_rigid              | char(2)               | YES  |     | NULL    |                | 
        | t_auto_padding                | char(2)               | YES  |     | 0       |                | 
        | auto_padding                  | char(2)               | YES  |     | NULL    |                | 
        | t_base_value                  | char(2)               | YES  |     | 0       |                | 
        | base_value                    | mediumint(8)          | NO   |     | 0       |                | 
        | t_grouping                    | char(2)               | YES  |     | 0       |                | 
        | grouping                      | char(2)               | NO   |     |         |                | 
        | t_export                      | char(2)               | YES  |     | 0       |                | 
        | export                        | char(2)               | YES  |     | NULL    |                | 
        | t_unit_value                  | char(2)               | YES  |     | 0       |                | 
        | unit_value                    | varchar(20)           | YES  |     | NULL    |                | 
        | t_unit_exponent_value         | char(2)               | YES  |     | 0       |                | 
        | unit_exponent_value           | varchar(5)            | NO   |     |         |                | 
        +-------------------------------+-----------------------+------+-----+---------+----------------+
        """
        rows = cur.execute("select `title_cache` from `graph_templates_graph` where local_graph_id=%s", localGraphId)
        name = cur.fetchone()[0]
        return(name)
        
    def getCactiRrdFilesFromLocalGraphId(localGraphId):
        global CURPID
        CURPID="getCactiRrdFilesFromLocalGraphId"
        q = 'select data_template_rrd.id, data_template_rrd.local_data_id \
        from graph_templates_item left join data_template_rrd on \
        (graph_templates_item.task_item_id=data_template_rrd.id) \
        left join graph_templates_graph on \
        (graph_templates_graph.local_graph_id = graph_templates_item.local_graph_id) \
        where (graph_templates_item.local_graph_id=%s and local_data_id <> "") \
        group by data_template_rrd.id order by sequence;' %localGraphId
        rows = cur.execute(q)
        res = cur.fetchall()
        firstId = res[0][1]
        for id, localDataId in res:
            if not localDataId == firstId:
                debug("Weird I've found multiple local_data_id's %s and and %s" %(localDataId, firstId))
            q = 'select rrd_name, rrd_path from poller_item \
                left join graph_templates_item on graph_templates_item.task_item_id=%s \
                where local_data_id=%s;' %(localDataId, firstId)
        rows = cur.execute(q)
        #print(rows)
        rrdPath = set() 
        for name, file in cur.fetchall():
            #debug(name),
            #debug(file)
            rrdPath.add(file)

        descr = getDescriptionFromLocalDataId(localDataId)
         
        if len(rrdPath) > 1:
            debug("Weird I've found multiple rrd files: %s" %rrdPath)
        return (rrdPath.pop(), descr)

    def parseCactiTree(treeId):
        global CURPID
        CURPID="parseCactiTree"
        """
        +--------------------+-----------------------+------+-----+---------+----------------+
        | Field              | Type                  | Null | Key | Default | Extra          |
        +--------------------+-----------------------+------+-----+---------+----------------+
        | id                 | mediumint(8) unsigned | NO   | PRI | NULL    | auto_increment | 
        | graph_tree_id      | smallint(5) unsigned  | NO   | MUL | 0       |                | 
        | local_graph_id     | mediumint(8) unsigned | NO   | MUL | 0       |                | 
        | rra_id             | smallint(8) unsigned  | NO   |     | 0       |                | 
        | title              | varchar(255)          | YES  |     | NULL    |                | 
        | host_id            | mediumint(8) unsigned | NO   | MUL | 0       |                | 
        | order_key          | varchar(100)          | NO   | MUL | 0       |                | 
        | host_grouping_type | tinyint(3) unsigned   | NO   |     | 1       |                | 
        | sort_children_type | tinyint(3) unsigned   | NO   |     | 1       |                | 
        +--------------------+-----------------------+------+-----+---------+----------------+
        """
        # only return where local_graph_id is not 0 since that's another branch of the tree
        q = "select `local_graph_id` from `graph_tree_items` where graph_tree_id=%s and not local_graph_id = 0"
        rows = cur.execute(q, treeId)
        for id in cur.fetchall():
            #print(id[0])
            rrdFilePath, name = getCactiRrdFilesFromLocalGraphId(id[0])
            if name == "":
                name = getNameFromLocalGraphId(id[0])
            debug((rrdFilePath, name))
            values = rrdFilePathname = getNameFromLocalGraphId(id[0])
            #values = rrdFilePath
            values = runRrd(rrdFilePath)
            if values:
                addToCsv(name, values[2])
            
    
    def runRrd(rrdFilePath):
        global CURPID#, startT, endT
        CURPID="runRrd"
        """
        execute rrd lib
        """
        try: 
            ret = rrdtool.graph('test3.png',
            '--imgformat', 'PNG' ,
            '--start', str(startT) ,
            '--end', str(endT) ,
            'DEF:in=%s:traffic_in:AVERAGE' %rrdFilePath ,
            'DEF:out=%s:traffic_out:AVERAGE' %rrdFilePath ,
            'VDEF:inlast=in,LAST' ,
            'VDEF:inmax=in,MAXIMUM' ,
            'VDEF:inavg=in,AVERAGE' ,
            'VDEF:inmin=in,MINIMUM' ,
            'VDEF:intot=in,TOTAL' ,
            'VDEF:inpct=in,95,PERCENT' ,
            'VDEF:outlast=out,LAST' ,
            'VDEF:outmax=out,MAXIMUM' ,
            'VDEF:outavg=out,AVERAGE' ,
            'VDEF:outmin=out,MINIMUM' ,
            'VDEF:outtot=out,TOTAL' ,
            'VDEF:outpct=out,95,PERCENT' ,
            'CDEF:inbits=in,8,*' ,
            'CDEF:outbits=out,8,*' ,
            'VDEF:inbitslast=inbits,LAST' ,
            'VDEF:inbitsmax=inbits,MAXIMUM' ,
            'VDEF:inbitsavg=inbits,AVERAGE' ,
            'VDEF:inbitsmin=inbits,MINIMUM' ,
            'VDEF:inbitstot=inbits,TOTAL' ,
            'VDEF:inbitspct=inbits,95,PERCENT' ,
            'VDEF:outbitslast=outbits,LAST' ,
            'VDEF:outbitsmax=outbits,MAXIMUM' ,
            'VDEF:outbitsavg=outbits,AVERAGE' ,
            'VDEF:outbitsmin=outbits,MINIMUM' ,
            'VDEF:outbitstot=outbits,TOTAL' ,
            'VDEF:outbitspct=outbits,95,PERCENT' ,
            'PRINT:inbitsavg:"%lf"'  ,
            'PRINT:inbitsmax:"%lf"'  ,
            'PRINT:inbitspct:"%lf"' ,
            'PRINT:inbitstot:"%lf"' ,
            'PRINT:outbitsavg:"%lf"'  ,
            'PRINT:outbitsmax:"%lf"'  ,
            'PRINT:outbitspct:"%lf"' ,
            'PRINT:outbitstot:"%lf"' ,
            'CDEF:maxbits=inbits,outbits,MAX' ,
            'CDEF:sumbits=inbits,outbits,+' ,
            'VDEF:maxbitspct=maxbits,95,PERCENT' ,
            'VDEF:sumbitspct=sumbits,95,PERCENT' ,
            'PRINT:maxbitspct:"%lf"' ,
            'PRINT:sumbitspct:"%lf"' )
            #,
            #'VDEF:maxbitspctn=maxbits,95,PERCENTNAN' ,
            #'VDEF:sumbitspctn=sumbits,95,PERCENTNAN' ,
            #'PRINT:maxbitspctn:"%lf"' ,
            #'PRINT:sumbitspctn:"%lf"')
        except rrdtool.error, e:
             print(e)
        else:
             return(ret)    
        
    parser = argparse.ArgumentParser(description='Get monthly values from Cacti tree graph')    
    parser.add_argument("-x", "--month", dest='month', help='month to get data from host', default=1, type=int, nargs='?')
    parser.add_argument("-m", "--mysql", dest='mysql', help='mysql host', default='127.0.0.1', nargs='?')
    parser.add_argument("-u", "--user", dest='user', help='mysql user', default='cacti', nargs='?')
    parser.add_argument("-p", "--pwd", dest='pwd', help='mysql password', default='sssh', nargs='?')
    parser.add_argument("-d", "--db", dest='db', help='mysql database', default='cacti', nargs='?')
    parser.add_argument("-v", "--loglevel", dest='logLevel', help='loglevel', default=2, type=int, nargs='?')
    parser.add_argument("-t", "--tree", dest='tree', help='Cacti tree name', default='Default Tree', nargs='?')
    parser.add_argument("-o", "--output", dest='output', help='output csv file', default='output.csv', nargs='?')
    args = parser.parse_args()

    # yes, it's ugly
    global LOGLEVEL, OUTPUT
    LOGLEVEL = args.logLevel
    OUTPUT = args.output
    startT = int(time.mktime((2013, args.month, 1, 0, 0, 0, 0, 0, 0)))
    endT = int(time.mktime((2013, args.month+1, 1, 0, 0, 0, 0, 0, 0)))
 
    #start mysql connection
    db = MySQLdb.connect(args.mysql,args.user,args.pwd,args.db)
    # prepare a cursor object using cursor() method
    cur = db.cursor()
    
    treeId = getCactiTreeId(args.tree)
    writeCsvHeader()
    print treeId
    parseCactiTree(treeId)
    
    # disconnect from server
    cur.close()
    db.close()
    
if __name__ == '__main__':
    main()
