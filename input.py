import MySQLdb

conn = MySQLdb.connect(
    host = '10.12.22.160',
    port = 3306,
    user = 'root',
    passwd = 'cloudfs',
    db = 'cloudfs'
)

cur = conn.cursor()




class loginfo(object):
    mkey = None
    inode = None
    IO_type = None
    time = None
    timens = None
    duration = None
    IO_offset = None
    IO_len = None
    def __init__(self, input_str):
        input_list = input_str[48:].split('\t')
        if len(input_list)!=6:
            print "wrong line"
            raise Exception("wrong line")
        #self.mkey = input_list[2]
        self.inode = input_list[0]
        self.IO_type = input_list[1]
        self._fix_time(input_list[2])
        self.duration = input_list[3]
        self.IO_offset = input_list[4]
        self.IO_len = input_list[5]


    def _fix_time(self,input):
        tmp = input.split(":")
        self.time = int(tmp[0])
        self.timens = int(tmp[1])


    @property
    def gen_input_sql(self):
        newsql = "replace into trace (inode, iotype, time, timens, duration,io_offset,io_len )values('%s','%s','%s','%s','%s','%s','%s')"\
                 % (self.inode,self.IO_type,self.time,self.timens, self.duration, self.IO_offset,self.IO_len)
        return newsql

def handle_line(line):
    record = loginfo(line)
    cur.execute(record.gen_input_sql)


if __name__ == "__main__":
    path = "/home/devdeploy/trace/nfvm_trace_71_68.log"
    with open(path, 'r') as file:
        for line in file:
            handle_line(line)
            conn.commit()
    cur.close()
    conn.close()
