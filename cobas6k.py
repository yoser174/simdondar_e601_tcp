###############################
# cobas6k driver
#
# auth: Yoserizal
# date: 8 Mei 2017
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
####
# fix:
# 2017-05-31    frame setiap STX+1 diremove sebelumnya STX saja, +1 untuk record No.
# 2017-06-03    Fix kirim sekali banyak
# 2017-08-22    Tambahkan konfigurasi Greyzone kode 0, reaktif=1, nonreaktif=-1
# 2017-09-26    Insert otomatis ke MySQL SIM setelah insert ke SQLite
# 2019-07-05    insert row data to logging and change connection to TCP
# 2019-07-06    data parsing message berdasarkan struktur STX+data..+EOT atau data yang dikirim hanya EOT
# 2019-11-29    tambahkan table SAMPLE_SENT jika sudah sukses send ke alat maka tidak usah reply test lagi


import configparser
import serial
import time
import logging
import sqlite3
import MySQLdb
from datetime import datetime
import socket


DRIVER_NAME = 'cobas6k'
DRIVER_VERSION = '0.0.9'

OUT_PATH = 'C:\\Dev\\Interfacee601\\out\\'


ENCODING = 'latin-1'
NULL = b'\x00'
STX = b'\x02'
ETX = b'\x03'
EOT = b'\x04'
ENQ = b'\x05'
ACK = b'\x06'
NAK = b'\x15'
ETB = b'\x17'
LF  = b'\x0A'
CR  = b'\x0D'
CRLF = CR + LF
RECORD_SEP    = b'\x0D' # \r #
FIELD_SEP     = b'\x7C' # |  #
REPEAT_SEP    = b'\x5C' # \  #
COMPONENT_SEP = b'\x5E' # ^  #
ESCAPE_SEP    = b'\x26' # &  #
BUFFER_SIZE = 1024

MY_DB = 'lis_pmi'
MY_TABLE = 'cobas6000'
MY_USER = 'xxxx'
MY_PASS = 'xxxx'


config = configparser.ConfigParser()
config.read('run_driver.ini')
MY_DB = config.get('General','MY_DB')
MY_TABLE = config.get('General','MY_TABLE')
MY_USER = config.get('General','MY_USER')
MY_PASS = config.get('General','MY_PASS')

class cobas6k(object):

    def __init__(self,tcp_host = '127.0.0.1', tcp_port = '5000',server='127.0.0.1',db_offline=True,baudrate=9600,timeout=10):
        self.message=''
        self.my_server = server
        self.db_offline = db_offline
        logging.info('Setup SIMDONDAR ke:%s' % self.my_server)
        logging.info( DRIVER_NAME+' - '+DRIVER_VERSION+' loaded.')
        self.tcp_host = tcp_host
        self.tcp_port = int(tcp_port)
        logging.info('MySQL server [%s]' % str(self.my_server))

    def send_enq(self):
        logging.info('>>ENQ')
        self.conn.send(ENQ)
        time.sleep(0.01)
        
    def send_eot(self):
        logging.info('>>EOT')
        self.conn.send(EOT)
        time.sleep(0.01)

    def send_ack(self):
        logging.info('>>ACK')
        self.conn.send(ACK)
        time.sleep(0.01)

    def send_msg(self,msg):
        logging.info('>>%s'%msg)
        data = b''.join((str(1 % 8).encode(), msg, CR, ETX))
        data_tx = b''.join([STX, data, self.make_checksum(data), CR, LF])
        logging.info('data to TX:%s' % data_tx)
        self.conn.send(data_tx)
        time.sleep(0.01)
        

    def listen(self):
        logging.info('listening..')
        data = ''
        while data =='':
            data = self.conn.recv(BUFFER_SIZE)
        return data

    def  make_checksum(self,message):
        if not isinstance(message[0], int):
            message = map(ord, message)
        return hex(sum(message) & 0xFF)[2:].upper().zfill(2).encode()

    def checksum_verify(self,message):
        if not (message.startswith(STX) and message.endswith(CRLF)):
            logging.error('Malformed ASTM message. Expected that it will started'
                         ' with %x and followed by %x%x characters. Got: %r'
                         ' ' % (ord(STX), ord(CR), ord(LF), message))
            return False
        stx, frame_cs = message[0], message[1:-2]
        frame, cs = frame_cs[:-2], frame_cs[-2:]
        ccs = self.make_checksum(frame)
        if cs == ccs:
            logging.info( 'Checksum is OK')
            return True
        else:
            logging.warning('Checksum failure: expected %r, calculated %r' % (cs, ccs))
            return False

    def decode(self,data):
        if not isinstance(data, bytes):
            logging.error ('bytes expected, got %r' % data)
        if data.startswith(STX):  # may be decode message \x02...\x03CS\r\n
            records = self.decode_message(data)
            return records
        byte = data[:1].decode()
        if  byte.isdigit():
            records = self.decode_frame(data)
            return records
        return [self.decode_record(data)]

    def decode_message(self,message):
        if not isinstance(message, bytes):
            logging.error('bytes expected, got %r' % message)
        if not (message.startswith(STX) and message.endswith(CRLF)):
            logging.error('ERROR Malformed ASTM message. Expected that it will started with %x and followed by %x%x characters. Got: %r' % (ord(STX), ord(CR), ord(LF), message))
        stx, frame_cs = message[0], message[1:-2]
        frame, cs = frame_cs[:-2], frame_cs[-2:]
        ccs = self.make_checksum(frame)
        assert cs == ccs, 'Checksum failure: expected %r, calculated %r' % (cs, ccs)
        records = self.decode_frame(frame)
        return records

    def decode_frame(self,frame):
        if not isinstance(frame,bytes):
            logging.error('bytes expected, got %r' % frame)
        if frame.endswith(CR + ETX):
            frame = frame[:-2]
        elif frame.endswith(ETB):
            frame = frame[:-1]
        else:
            logging.warning('Incomplete frame data %r. Expected trailing <CR><ETX> or <ETB> chars' % frame)
        seq = frame[:1].decode()
        if not seq.isdigit():
            logging.warning('Malformed ASTM frame. Expected leading seq number %r' % frame)
        seq, records = int(seq), frame[1:]
        return  [self.decode_record(record)
                 for record in records.split(RECORD_SEP)]

    def decode_record(self,record):
        fields = []
        for item in record.split(FIELD_SEP):
            if REPEAT_SEP in item:
                item = self.decode_repeated_component(item)
            elif COMPONENT_SEP in item:
                item = self.decode_component(item)
            else:
                item = item
            fields.append([None, item][bool(item)])
        return fields

    def decode_component(self,field):
        return [[None, item][bool(item)]
                for item in field.split(COMPONENT_SEP)]
    
    def decode_repeated_component(self,component):
        return [self.decode_component(item)
            for item in component.split(REPEAT_SEP)]


    def encode_message(self,seq,records):
        data = RECORD_SEP.join(self.encode_record(record)
                               for record in records)
        data = b''.join((str(seq % 8).encode(), data, CR, ETX))
        return b''.join([STX, data, self.make_checksum(data), CR, LF])

    def encode_record(self,record):
        fields = []
        _append = fields.append
        for field in record:
            if isinstance(field, bytes):
                _append(field)
            elif isinstance(field, unicode):
                _append(field.encode(encoding))
            elif isinstance(field, Iterable):
                _append(encode_component(field, encoding))
            elif field is None:
                _append(b'')
            else:
                _append(unicode(field).encode(encoding))
        return FIELD_SEP.join(fields)

    def make_chunks(self,s, n):
        iter_bytes = (s[i:i + 1] for i in range(len(s)))
        return [b''.join(item)
                for item in izip_longest(*[iter_bytes] * n, fillvalue=b'')]

    def split(self,msg, size):
        stx, frame, msg, tail = msg[:1], msg[1:2], msg[2:-6], msg[-6:]
        assert stx == STX
        assert frame.isdigit()
        assert tail.endswith(CRLF)
        assert size is not None and size >= 7
        frame = int(frame)
        chunks = make_chunks(msg, size - 7)
        chunks, last = chunks[:-1], chunks[-1]
        idx = 0
        for idx, chunk in enumerate(chunks):
            item = b''.join([str((idx + frame) % 8).encode(), chunk, ETB])
            yield b''.join([STX, item, make_checksum(item), CRLF])
        item = b''.join([str((idx + frame + 1) % 8).encode(), last, CR, ETX])
        yield b''.join([STX, item, make_checksum(item), CRLF])
        
    
    def db_query(self,sql):
        conn = sqlite3.connect('cobas6k.db')
        cursor = conn.cursor()
        logging.info('Query - %s ' % sql)
        cursor.execute(sql)
        r = cursor.fetchall()
        conn.close()
        logging.info('Return = %s' % r)
        return r

    def db_delexists(self,sid,tesno):
        logging.info('deleting existing data')
        conn = sqlite3.connect('cobas6k.db')
        cursor = conn.cursor()
        cursor.execute("DELETE FROM GUI_RESULTS WHERE SID = '"+str(sid)+"' AND TestNo = '"+str(tesno)+"' ")
        conn.commit()
        conn.close()
        
        
    def db_insert(self,sql,resdata):
        sid = resdata[0] or ''
        tesno = resdata[1] or ''
        if sid!='' and tesno!='':
            logging.info('delete existing data (%s,%s)' % (sid,tesno))
            self.db_delexists(sid,tesno)
        
        conn = sqlite3.connect('cobas6k.db')
        cursor = conn.cursor()
        logging.info('Insert - %s data:%s' % (sql,resdata))
        cursor.execute(sql,resdata)
        conn.commit()
        conn.close()

    def db_insert_raw(self,sql,resdata):
        conn = sqlite3.connect('cobas6k.db')
        cursor = conn.cursor()
        logging.info('Insert - %s data:%s' % (sql,resdata))
        cursor.execute(sql,resdata)
        conn.commit()
        conn.close()

    def my_insert(self,sql):
        logging.info('IP MySQL [%s]' % self.my_server)
        try:
            conn = MySQLdb.connect(host= self.my_server,
                      user=MY_USER,
                      passwd=MY_PASS,
                      db=MY_DB)
            cursor = conn.cursor()        
            cursor.execute(sql)
            conn.commit()
        except MySQLdb.Error as e:
            logging.error(e)
            conn.rollback()
        conn.close()

    def get_testdesc(self,testno):
        logging.info('getting test description..')
        tesdesc = self.db_query("SELECT Desc FROM ALL_SET_TESTS WHERE TestNo = '"+testno+"' ")
        return str(tesdesc[0][0])


    def save_raw(self,direction,msg):
        logging.info('saving raw data [%s,%s]' % (direction,msg))
        sql = 'INSERT INTO RAW_DATA (direction,message) VALUES (?,?)'
        dt = [str(direction),str(msg)]
        self.db_insert_raw(sql,dt)
        return True
        
        

    def handleTSReq(self,msg):
        logging.info('handle TS Request..')
        q_sid = ''
        q_seq = ''
        q_rackid = ''
        q_posno = ''
        q_racktype = ''
        q_conttype = ''
        sql = ''
        is_sent = False
        for line in msg:
            logging.info(line)
            if line[0]=='Q':
                logging.info(' Q - Getting SID..')
                q_sid = str(line[2][2]).strip() or ''
                logging.info(' SID is "%s"' % q_sid)
                # cek apakah sudah pernah kirim
                sql = "SELECT count(id) jum FROM SAMPLE_SENT WHERE sample_no = '"+str(q_sid)+"'"
                res = self.db_query(sql)
                
                logging.info(res[0][0])
                if int(res[0][0])>0:
                    logging.info('Sudah pernah dikirim.')
                    is_sent = True
                
                
                q_seq = line[2][3] or ''
                q_rackid = line[2][4] or ''
                q_posno = line[2][5] or ''
                q_racktype = line[2][7] or ''
                q_conttype = line[2][8] or ''
                logging.info('  -> seq:%s RackNo-Pos: (%s - %s) Racktype-ContainerType:(%s-%s)' % (q_seq,q_rackid,q_posno,q_racktype,q_conttype))

        
        logging.info(' getting config name TS..')
        sql = "SELECT char_value FROM config WHERE name = 'TS'"
        res = self.db_query(sql)
        if str(res[0][0])=='ALL_SET_TESTS':
            """
            H|\^&|||Host^1|||||Modular|TSDWN^REPLY|P|1
P|1||PatID|||||M||||||40^Y
O|1|SampleID|Seq^Rack^Pos^^S1^SC|^^^1^1\^^^2^1|R||20170508204635||||A||||1||||||||||O
C|1|L|Comm1^Comm2^Comm3^Comm4^Comm5|G
L|1|N
"""
            # parsing Header
            ts = datetime.utcnow().strftime('%Y%m%d%H%M%S')
            ts_reply = 'H|\^&|||Host^1|||||cobas6000|TSDWN^REPLY|P|1\r'
            ts_reply += 'P|1|||||||U||||||^\r'
            
            comm1 = ''
            comm2 = ''
            comm3 = ''
            comm4 = ''
            comm5 = ''
            logging.info('ALL_SET_TESTS : get SET TEST and send it to instrument')
            sql = 'SELECT TestNo,Dilution FROM ALL_SET_TESTS WHERE Active = 1'
            res = self.db_query(sql)
            tes_dump = ''
            if not is_sent:
                rec = 0
                for tes in res:
                    if rec==0:
                        tes_dump +='^^^'+str(tes[0])+'^'+str(tes[1])
                    else:
                        tes_dump +='\^^^'+str(tes[0])+'^'+str(tes[1])
                    rec += 1
            logging.info(tes_dump)
            ts_reply += 'O|1|'+str(q_sid).rjust(22)+'|'+str(q_seq)+'^'+str(q_rackid)+'^'+str(q_posno)+'^^'+str(q_racktype)+'^'+str(q_conttype)+'|'+str(tes_dump)+'|R||'+str(ts)+'||||A||||1||||||||||O\r'
            logging.info(' -> Generate comment field')
            sql = "SELECT char_value FROM config WHERE name = 'TS_ALL_SET_TESTS_COMM1'"
            res = self.db_query(sql)
            comm1 = res[0][0] or ''
            sql = "SELECT char_value FROM config WHERE name = 'TS_ALL_SET_TESTS_COMM2'"
            res = self.db_query(sql)
            comm2 = res[0][0] or ''
            sql = "SELECT char_value FROM config WHERE name = 'TS_ALL_SET_TESTS_COMM3'"
            res = self.db_query(sql)
            comm3 = res[0][0] or ''
            sql = "SELECT char_value FROM config WHERE name = 'TS_ALL_SET_TESTS_COMM4'"
            res = self.db_query(sql)
            comm4 = res[0][0] or ''
            sql = "SELECT char_value FROM config WHERE name = 'TS_ALL_SET_TESTS_COMM5'"
            res = self.db_query(sql)
            comm5 = res[0][0] or ''
            # comment
            ts_reply += 'C|1|L|'+str(comm1)+'^'+str(comm2)+'^'+str(comm3)+'^'+str(comm4)+'^'+str(comm5)+'|G\r'
            ts_reply +='L|1|N'
            logging.info('TS REPLY:%s' % ts_reply)
            self.save_raw('OUT',ts_reply)
            self.send_enq()
            data = self.listen()
            if data==ACK:
                self.send_msg(ts_reply)
                data = self.listen()
                if data == ACK:
                    self.send_eot()
                    logging.info('insert sample_no yang sudah berhasil dikirim.')
                    # insert SAMPLE_SENT
                    sql = 'INSERT INTO SAMPLE_SENT (sample_no) VALUES (?)'
                    logging.info('data [%s]' % str(q_sid))
                    sidno_data = [str(q_sid)]
                    self.db_insert_raw(sql,sidno_data)
                    
                                
        return True






    def handlemsg(self,msg):
        self.save_raw('IN',str(msg))
        o_sid = '' # sample ID
        o_time = '' # specimen colection date\time
        r_testno = '' # test No
        r_dilut = '' # dilution
        r_predilut = '' # pre-dilut
        r_tes = '' # dummy tes
        r_result = '' # measurement data
        r_unit = '' # unit
        r_flag = '' # flag A=Abnormal L=Below normal H=Higher than normal high N=Normal
                    # LL=Lower than panic low HH=higer than panic high
        r_status = '' # status result C= crr. of prev.trasm.results F=Final results
        r_operator = '' # operator identification
        r_insid = '' # instrument identification
        o_lastmod = '' # last modified date result
        
        for line in msg:
            logging.info(line)
            if line[0]=='H':
                logging.info('=> Header message')
                try:
                    h_instname = str(line[4][0]+'.'+line[4][1]).strip() or ''
                    logging.info(line[10])
                    if line[10][0]=='RSUPL' and (line[10][1]=='REAL' or line[10][1]=='BATCH'):
                        logging.info('Message is RESULT UPLOAD')
                    elif line[10][0]=='TSREQ' and line[10][1]=='REAL':
                        logging.info('Message is TEST SELECTION request.')
                        self.handleTSReq(msg)
                        return True                                 
                    else:
                        logging.info('!!! Mesage not expeted, skip it.')
                        return False
                except Exception as e:
                    logging.info('error pasing H segment [%s][%s]' % (str(line),str(e)))
            elif line[0]=='P':
                logging.info('=> Patient message')
            elif line[0]=='C':
                logging.info('=> Comment message')
            elif line[0]=='L':
                logging.info('=> Terminate message')
            elif line[0]=='O':
                logging.info('=> Order message')
                try:
                    o_sid = str(line[2]).strip() or ''
                    o_reslastmoddate = str(line[22]).strip() or ''
                    o_lastmod = str(line[22]).strip() or ''
                    o_sampletype = str(line[11]).strip() or ''
                except Exception as e:
                    logging.error('Failed parsing O segment [%s][%s]' % (str(line),str(e)))
                    
            elif line[0]=='R':
                logging.info('=> Test result message')
                try:                    
                    r_tes = str(line[2][3]).split('/')
                    r_testno = r_tes[0] or ''
                    r_dilut = r_tes[1] or ''
                    r_predilut = r_tes[2] or ''
                    r_result = line[3] or ''
                    r_unit = line[4] or ''
                    r_flag = line[6] or ''
                    r_status = line[8] or ''
                    r_operator = line[10] or '' 
                    r_insid = line[13] or ''
                except Exception as e:
                    logging.error('Failed parsing O segment [%s][%s]' % (str(line),str(e)))

                res_num = r_result[1]
                # Insert ke DB offline
                if self.db_offline:
                    logging.info('Insert ke offline DB..')
                    sql = 'INSERT INTO GUI_RESULTS (SID,TestNo,TestDesc,Dilution,Flag,Operator,ResNum,ResStr) VALUES (?,?,?,?,?,?,?,?)'
                    res_char = r_result[0]
                    if str(res_char)=='-1':
                        res_char='Non Reaktif'
                    elif str(res_char)=='1':
                        res_char='Reaktif'
                    elif str(res_char)=='0':
                        res_char='Greyzone'
                    else:
                        res_char=res_char

                    resdata = [str(o_sid),str(r_testno),str(self.get_testdesc(r_testno)),str(r_dilut),str(r_flag),str(r_operator).strip(),str(res_num),str(res_char)]
                    self.db_insert(sql,resdata)

                # disini insert ke MySQL
                s = o_lastmod
                o_reslastmoddate = s[0:4]+'-'+s[4:6]+'-'+s[6:8]+' '+s[8:10]+':'+s[10:12]+':'+s[12:14]
                logging.info('Inserting ke MySQL...')
                parname = self.get_testdesc(r_testno)
                par_no = ''
                if parname == 'HBSAGII':
                    par_no = '1'
                if parname == 'A-HCV II':
                    par_no = '2'
                if parname == 'HIVCOMP':
                    par_no = '3'
                if parname == 'Syphilis':
                    par_no = '4'

                if o_sampletype == 'N':
                    o_sampletype = 'S'
                elif o_sampletype == 'Q':
                    o_sampletype = 'C'
                else:
                    o_sampletype = o_sampletype

                sql = "INSERT INTO cobas6000 (instrument_name,run_time,sample_id,parameter_name,parameter_no,sample_type,quantitative,qualitative,operator) VALUES ('"+str(h_instname)+"','"+str(o_reslastmoddate)+"','"+str(o_sid)+"','"+str(parname)+"','"+str(par_no)+"','"+str(o_sampletype)+"','"+str(res_num)+"','"+str(r_result[0])+"','"+str(r_operator)+"') "
                self.my_insert(sql)
                

                
            else:
                logging.info('Unkown message.')

        return True

    def clean_msg(self,s):
        logging.info('clean...')
        t = ''
        a = 1
        while a > 0:
            try:
                a = s.find(ETB)
                logging.info('got rec [%s]' % a)
                t =  s[a:a+6]
                logging.info(t)
                s = str(s).replace(t,'')
                logging.info(s)
            except:
                a = 0
                pass


        logging.info('result [%s]' % str(s))

        return s
                
                    
    def open(self):
        
        while 1:
            logging.info('connection to [%s:%s] ...' % (self.tcp_host,self.tcp_port))
            self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.conn.connect((self.tcp_host, self.tcp_port))
            logging.info('Ready.')
            self.message = ''
            while 1:
                data = self.conn.recv(BUFFER_SIZE)
                if not data: break
                if isinstance(data, unicode):
                    data = data.encode('utf-8')
                logging.info("Data: [{}]".format(data))
                if data!='':
                    self.send_ack()
                    #if len(data)>4:
                    #    data = str(data[2:-4]).replace(ETB,'')
                    self.message = self.message + data
                    logging.info('message [%s]' % str(self.message))
                    if (str(self.message).startswith(STX) and str(self.message).endswith(EOT)) or (str(self.message).startswith(ENQ) and str(self.message).endswith(EOT)) or str(data)==EOT :
                        if len(self.message)>0:
                            logging.info('start processing message [%s]' % str(self.message))
                            msg = self.message
                            msg = str(msg).replace(ENQ,'')
                            msg = str(msg).replace(ETX,'')
                            msg = str(msg).replace(EOT,'')
                            msg = str(msg).replace(STX,'')
                            # clean ETB
                            logging.info('clean ETB.')
                            msg = self.clean_msg(msg)
                            logging.info('cleaned message is [%s]' % str(msg))
                            msg = msg[1:-3]
                            logging.info('cleaned message 2 is [%s]' % str(msg))
                            msg = self.decode('1'+str(msg)+CR+ETX)
                            logging.info('decoded to [%s]' % str(msg))
                            self.handlemsg(msg)
                            self.message = ''
                            
                            
        
        
            
        
