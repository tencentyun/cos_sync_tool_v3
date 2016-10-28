#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import json
import ast
import stat
import sqlite3
import hashlib
import threading
import time
import qcloud_cos
import threadpool
import logging
import logging.handlers
from datetime import date
from qcloud_cos import CosClient
from qcloud_cos import CosConfig
from qcloud_cos import UploadFileRequest
from qcloud_cos import StatFileRequest
from qcloud_cos import StatFolderRequest
from qcloud_cos import DelFileRequest
from qcloud_cos import DelFolderRequest
from qcloud_cos import CreateFolderRequest

from sys import getfilesystemencoding

fs_encoding = getfilesystemencoding()

class CosSyncLog(object):
    @classmethod
    def set_log(cls, level=logging.INFO, log_name='./log/cos_sync.log'):
        logger = logging.getLogger('tencent_cos_sync')
        Rthandler = logging.handlers.RotatingFileHandler(log_name,
                maxBytes=100*1024*1024, backupCount=20)
        Rthandler.setLevel(level)
        formatter = logging.Formatter('''%(asctime)s %(filename)s 
        [line:%(lineno)d] %(levelname)s %(message)s''')
        Rthandler.setFormatter(formatter)
        logger.addHandler(Rthandler)
        logger.setLevel(logging.INFO) 
        return logger

cos_sync_logger = CosSyncLog.set_log()

class CosSyncConfig(object):

    def __init__(self, config_path=u'./conf/cos_sync_config.json'):
        if (not os.path.isfile(config_path)):
            self.init_config_err = u'config file %s not exist' % (config_path)
            self.init_config_flag = False
            return

        config_file = open(config_path)
        try:
            config_str = config_file.read()
            self.config_json = json.loads(config_str)
        except:
            self.init_config_err = u'config file is not valid json'
            self.init_config_flag = False
            return
        finally:
            config_file.close()


        valid_config_key_arr = [u'appid', u'secret_id', u'secret_key', u'bucket', 
                u'timeout', u'thread_num', u'local_path', u'cos_path', 
                u'delete_sync', u'daemon_mode', u'daemon_interval', u'enable_https']

        for config_key in valid_config_key_arr:
            if (config_key not in self.config_json.keys()):
                self.init_config_err = u'config file not contain %s' % (config_key)
                self.init_config_flag = False
                return
            config_value = self.config_json[config_key]                            
            if not isinstance(config_value, unicode):                               
                self.init_config_err = u'config %s value must be unicode str' % (config_key)
                self.init_config_flag = False
                return
            
        try:
            int(self.config_json[u'appid'])
        except:                                                                     
            self.init_config_err = u'appid is illegal'
            self.init_config_flag = False                                          
            return 

        if not os.path.isdir(self.config_json[u'local_path']):
            self.init_config_err = u'local_path is not effective dir path'
            self.init_config_flag = False
            return


        self.config_json[u'local_path'] = \
                os.path.abspath(self.config_json[u'local_path']) \
                + os.path.sep.decode('utf8')

        if self.config_json[u'cos_path'][0] != u'/':
            self.init_config_err = u'cos_path must start with bucket root /'
            self.init_config_flag = False
            return

        cos_path = self.config_json[u'cos_path']
        if cos_path[len(cos_path) - 1] != u'/':
            cos_path += u'/'
            self.config_json[u'cos_path'] = cos_path

        try:
            int(self.config_json[u'timeout'])
        except:                                                                     
            self.init_config_err = u'timeout is illegal'                             
            self.init_config_flag = False                                          
            return 

        try:
            int(self.config_json[u'thread_num'])
        except:                                                                     
            self.init_config_err = u'thread_num is illegal'                             
            self.init_config_flag = False                                          
            return 

        try:
            int(self.config_json[u'delete_sync'])
        except:                                                                     
            self.init_config_err = u'delete_sync is illegal'                             
            self.init_config_flag = False                                          
            return 

        try:
            int(self.config_json[u'daemon_mode'])
        except:                                                                     
            self.init_config_err = u'daemon_mode is illegal'                             
            self.init_config_flag = False                                          
            return 

        try:
            int(self.config_json[u'daemon_interval'])
        except:                                                                     
            self.init_config_err = u'daemon_interval is illegal'                             
            self.init_config_flag = False                                          
            return 

        try:
            int(self.config_json[u'enable_https'])
        except:                                                                     
            self.init_config_err = u'enable_https is illegal'                             
            self.init_config_flag = False                                          
            return 

        self.config_json[u'db_path'] = u'./db/db_rec.db'

        self.init_config_flag = True
        self.init_config_err = u''

    def is_valid_config(self):
        return self.init_config_flag

    def get_err_msg(self):
        return self.init_config_err

    def get_appid(self):
        return self.config_json[u'appid']

    def get_secret_id(self):
        return self.config_json[u'secret_id']

    def get_secret_key(self):
        return self.config_json[u'secret_key']

    def get_bucket(self):
        return self.config_json[u'bucket']

    def get_local_path(self):
        local_path = self.config_json[u'local_path']
        return local_path

    def get_cos_path(self):
        cos_path = self.config_json[u'cos_path']
        return cos_path

    def get_timeout(self):
        return int(self.config_json[u'timeout'])

    def get_thread_num(self):
        return int(self.config_json[u'thread_num'])

    def get_delete_sync(self):
        return int(self.config_json[u'delete_sync'])

    def get_daemon_mode(self):
        return int(self.config_json[u'daemon_mode'])

    def get_daemon_interval(self):
        return int(self.config_json[u'daemon_interval'])

    def get_enable_https(self):
        return int(self.config_json[u'enable_https'])

    def get_db_path(self):
        return self.config_json[u'db_path']

class FileStat(object):
    def __init__(self, file_path=None):
        if file_path is None:
            self.valid = False
            return

        self.valid = True

        file_stat = os.stat(file_path)
        self.file_path = file_path
        self.file_mode = file_stat[stat.ST_MODE]
        self.file_size = file_stat[stat.ST_SIZE]
        self.mtime = file_stat[stat.ST_MTIME]
        self.md5 = u''

    def is_valid(self):
        return self.valid

    def build_md5(self):
        if (stat.S_ISDIR(self.file_mode)):
            self.md5 = u''
            return

        md5_hash = hashlib.md5()
        file_obj = open(self.file_path, 'rb')
        try:
            while True:
                byte_in = file_obj.read(4096)
                if not byte_in:
                    break
                md5_hash.update(byte_in)
        except:
            return ""
        finally:
            file_obj.close()
        return md5_hash.hexdigest()

    @classmethod
    def build_stat(cls, file_path=u'', file_size=u'', file_mode=u'', mtime=u'',
            md5=u''):
        file_stat = FileStat()
        file_stat.file_path = file_path
        file_stat.file_size = file_size
        file_stat.file_mode = file_mode
        file_stat.mtime = mtime
        file_stat.md5 = md5
        file_stat.valid = True
        return file_stat

    def compare(self, file_stat):
        if (stat.S_ISDIR(self.file_mode)):
            return 0
        if (self.mtime == file_stat.mtime):
            return 0
        if (self.file_size != file_stat.file_size):
            return 1
        self.md5 = self.build_md5()
        if (self.md5 != file_stat.md5):
            return 1
        return 0

class LocalFileDirInfo(object):
    def __init__(self, local_path = None):
        self.file_stat_list = []
        self.dir_stat_list  = []
        if local_path is None:
            return

        if os.path.isfile(local_path):
            self.file_stat_list.append(FileStat(local_path))
            return

        str_local_path = local_path.encode(fs_encoding)
        wrong_path_file = open('./log/error_path.log', 'wb')
        for parent, dirnames, filenames in os.walk(str_local_path):
            for filename in filenames:
                full_path = os.path.join(parent, filename)
                full_path = os.path.abspath(full_path)
                try:
                    full_path.decode(fs_encoding).encode('utf8')
                    os.stat(full_path)  # try to invoke system function
                except :
                    print '[illeagl utf8 local path, please rename it] %s' % repr(full_path)
                    wrong_path_file.write(repr(full_path))
                    wrong_path_file.write('\n')
                    continue

                full_path = full_path.decode(fs_encoding)
                file_stat = FileStat(full_path)
                if not file_stat.valid:
                    print "wrong file_stat, full_path:", repr(full_path)
                else:
                    self.file_stat_list.append(file_stat)

            for dirname in dirnames:
                full_path = os.path.join(parent, dirname)
                full_path = os.path.abspath(full_path) + os.path.sep
                try:
                    full_path = full_path.decode(fs_encoding)
                except:
                    print '[illeagl utf8 local path, please rename it] %s' % repr(full_path)
                    wrong_path_file.write(repr(full_path))
                    wrong_path_file.write('\n')
                    continue


                file_stat = FileStat(full_path)
                if not file_stat.valid:
                    print "wrong file_stat, full_path:", repr(full_path)
                else:
                    self.dir_stat_list.append(file_stat)
        wrong_path_file.close()

class DbRecord(object):
    def __init__(self, db_path, appid, bucket, local_path, cos_path):
        # connect sqlite db
        self.cx = sqlite3.connect(db_path, check_same_thread = False)
        temp_md5_path = local_path.encode('utf-8') + cos_path.encode('utf-8')
        md5_hash = hashlib.md5()
        md5_hash.update(temp_md5_path)
        md5_digest = md5_hash.hexdigest()

        self.table_name = u'cos_sync_table_' + appid + u'_' + bucket + u'_' + md5_digest

        sql_create_table = u'create table if not exists ' + self.table_name + u''' 
        (`id` INTEGER PRIMARY key autoincrement,
        `file_path` varchar(1024) default '',
        `file_size` bigint not null default 0,
        `file_mode` bigint not null default 0,  
        `mtime` bigint not null,
        `md5` char(32) default '',                                                  
        unique (`file_path`))'''

        self.cx.execute(sql_create_table)
        self.cx.commit()
        self.file_stat_dict = {}
        self.dir_stat_dict = {}
        sql_select_all = u'select * from ' + self.table_name
        cursor = self.cx.execute(sql_select_all)
        for element in cursor:
            file_path = element[1]
            file_size = element[2]
            file_mode = element[3]
            mtime     = element[4]
            md5       = element[5]
            if (stat.S_ISREG(file_mode)):
                self.file_stat_dict[file_path] = FileStat.build_stat(file_path,
                        file_size, file_mode, mtime, md5)
            else:
                self.dir_stat_dict[file_path] = FileStat.build_stat(file_path,
                        file_size, file_mode, mtime, md5)

        self.db_lock = threading.Lock()

    def __del__(self):
        if self.db_lock.acquire():
            self.cx.close()
            self.db_lock.release()

    def update_record(self, file_stat):
        if (len(file_stat.md5) == 0):
            file_stat.md5 = file_stat.build_md5()

        record_tuple = (file_stat.file_path, file_stat.file_size,
                file_stat.file_mode, file_stat.mtime, file_stat.md5)
        sql_update_record = u'''
        replace into %s 
        (file_path, file_size, file_mode, mtime, md5) 
        values 
        (?, ?, ?, ?, ?)''' % self.table_name

        if self.db_lock.acquire():
            self.cx.execute(sql_update_record, record_tuple)
            self.cx.commit()
            self.db_lock.release()

    def del_record(self, file_stat):
        del_tuple = (file_stat.file_path, )
        sql_delete_record = u"delete from %s where file_path = ?" % self.table_name
        if self.db_lock.acquire():
            self.cx.execute(sql_delete_record, del_tuple)
            self.cx.commit()
            self.db_lock.release()

class CosTaskStatics(object):
    def __init__(self):
        self.lock                   = threading.Lock()
        self.start_time             = time.time()
        self.end_time               = time.time()
        self.create_folder_sum_cnt  = 0
        self.create_folder_ok_cnt   = 0
        self.create_folder_fail_cnt = 0
        self.upload_file_sum_cnt    = 0
        self.upload_file_ok_cnt     = 0
        self.upload_file_fail_cnt   = 0
        self.del_folder_sum_cnt     = 0
        self.del_folder_ok_cnt      = 0
        self.del_folder_fail_cnt    = 0
        self.del_file_sum_cnt       = 0
        self.del_file_ok_cnt        = 0
        self.del_file_fail_cnt      = 0

    def add_create_folder_ok(self):
        if self.lock.acquire():
            self.create_folder_sum_cnt += 1
            self.create_folder_ok_cnt += 1
            self.lock.release()

    def add_create_folder_fail(self):
        if self.lock.acquire():
            self.create_folder_sum_cnt += 1
            self.create_folder_fail_cnt += 1
            self.lock.release()

    def add_upload_file_ok(self):
        if self.lock.acquire():
            self.upload_file_sum_cnt += 1
            self.upload_file_ok_cnt += 1
            self.lock.release()

    def add_upload_file_fail(self):
        if self.lock.acquire():
            self.upload_file_sum_cnt += 1
            self.upload_file_fail_cnt += 1
            self.lock.release()

    def add_del_folder_ok(self):
        if self.lock.acquire():
            self.del_folder_sum_cnt += 1
            self.del_folder_ok_cnt += 1
            self.lock.release()

    def add_del_folder_fail(self):
        if self.lock.acquire():
            self.del_folder_sum_cnt += 1
            self.del_folder_fail_cnt += 1
            self.lock.release()

    def add_del_file_ok(self):
        if self.lock.acquire():
            self.del_file_sum_cnt += 1
            self.del_file_ok_cnt += 1
            self.lock.release()

    def add_del_file_fail(self):
        if self.lock.acquire():
            self.del_file_sum_cnt += 1
            self.del_file_fail_cnt += 1
            self.lock.release()

    def begin_collect_statics(self):
        if self.lock.acquire():
            self.start_time             = time.time()
            self.create_folder_sum_cnt  = 0
            self.create_folder_ok_cnt   = 0
            self.create_folder_fail_cnt = 0
            self.upload_file_sum_cnt    = 0
            self.upload_file_ok_cnt     = 0
            self.upload_file_fail_cnt   = 0
            self.del_folder_sum_cnt     = 0
            self.del_folder_ok_cnt      = 0
            self.del_folder_fail_cnt    = 0
            self.del_file_sum_cnt       = 0
            self.del_file_ok_cnt        = 0
            self.del_file_fail_cnt      = 0
            self.lock.release()

    def end_collect_statics(self):
        if self.lock.acquire():
            self.end_time = time.time()
            self.cx = sqlite3.connect("./db/op_record.db")
            sql_create_op_table = u'''
            create table if not exists op_record 
            (`id` INTEGER PRIMARY key autoincrement,
            `op_date` INTEGER,
            `start_time` INTEGER, 
            `end_time` INTEGER, 
            `op_sum_cnt` INTEGER, 
            `op_ok_cnt` INTEGER, 
            `op_fail_cnt` INTEGER, 
            `op_status`  varchar(20) NOT NULL DEFAULT 'ALL_OK',
            `create_folder_sum_cnt` INTEGER, 
            `create_folder_ok_cnt` INTEGER, 
            `create_folder_fail_cnt` INTEGER, 
            `upload_file_sum_cnt` INTEGER, 
            `upload_file_ok_cnt` INTEGER, 
            `upload_file_fail_cnt` INTEGER, 
            `del_folder_sum_cnt` INTEGER,
            `del_folder_ok_cnt` INTEGER,
            `del_folder_fail_cnt` INTEGER,
            `del_file_sum_cnt` INTEGER,
            `del_file_ok_cnt` INTEGER,
            `del_file_fail_cnt` INTEGER
            )'''

            self.cx.execute(sql_create_op_table)
            self.cx.commit()
            op_sum_cnt = self.create_folder_sum_cnt + self.del_folder_sum_cnt \
                    + self.upload_file_sum_cnt + self.del_file_sum_cnt
            op_ok_cnt = self.create_folder_ok_cnt + self.del_folder_ok_cnt \
                    + self.upload_file_ok_cnt + self.del_file_ok_cnt
            op_fail_cnt = self.create_folder_fail_cnt + self.del_folder_fail_cnt \
                    + self.upload_file_fail_cnt + self.del_file_fail_cnt
            if (op_sum_cnt == op_ok_cnt):
                op_status = 'ALL_OK'
            elif (op_sum_cnt == op_fail_cnt):
                op_status = 'ALL_FAIL'
            else:
                op_status = 'PART_OK'
            op_date = int(date.fromtimestamp(self.start_time).strftime('%Y%m%d'))

            record_tuple = (op_date, int(self.start_time), int(self.end_time), 
                    op_sum_cnt, op_ok_cnt, op_fail_cnt, op_status,
                    self.create_folder_sum_cnt, self.create_folder_ok_cnt, self.create_folder_fail_cnt,
                    self.upload_file_sum_cnt, self.upload_file_ok_cnt,
                    self.upload_file_fail_cnt, self.del_folder_sum_cnt, self.del_folder_ok_cnt,
                    self.del_folder_fail_cnt, self.del_file_sum_cnt, self.del_file_ok_cnt,
                    self.del_file_fail_cnt)
            sql_update_op_table = u'''
            insert into op_record
            (op_date, start_time, end_time, op_sum_cnt, op_ok_cnt, op_fail_cnt, 
            op_status, create_folder_sum_cnt, create_folder_ok_cnt,
            create_folder_fail_cnt, upload_file_sum_cnt, upload_file_ok_cnt, 
            upload_file_fail_cnt, del_folder_sum_cnt, del_folder_ok_cnt, 
            del_folder_fail_cnt, del_file_sum_cnt, del_file_ok_cnt,
            del_file_fail_cnt
            ) values (%d, %d, %d, %d, %d, %d, '%s', %d, %d, %d, %d, %d, %d, %d,
            %d, %d, %d, %d, %d)
            ''' % record_tuple
            if (op_sum_cnt != 0):
                self.cx.execute(sql_update_op_table)
                self.cx.commit()

            print '\n\nsync over! op statistics:'
            print '%30s : %s' % ('op_status', op_status)
            print '%30s : %s' % ('create_folder_ok', self.create_folder_ok_cnt)
            print '%30s : %s' % ('create_folder_fail', self.create_folder_fail_cnt)
            print '%30s : %s' % ('del_folder_ok', self.del_folder_ok_cnt)
            print '%30s : %s' % ('del_folder_fail', self.del_folder_fail_cnt)
            print '%30s : %s' % ('upload_file_ok', self.upload_file_ok_cnt)
            print '%30s : %s' % ('upload_file_fail', self.upload_file_fail_cnt)
            print '%30s : %s' % ('del_file_ok', self.del_file_ok_cnt)
            print '%30s : %s' % ('del_file_fail', self.del_file_fail_cnt)
            print '%30s : %s' % ('start_time', time.strftime('%Y%m%d %H:%M:%S',
                time.localtime(self.start_time)))
            print '%30s : %s' % ('end_time', time.strftime('%Y%m%d %H:%M:%S',
                time.localtime(self.end_time)))
            print '%30s : %s s' % ('used_time', int(self.end_time) -
                    int(self.start_time))
            self.lock.release()

cos_task_statics = CosTaskStatics()

def convert_op_ret_str(op_ret):
    op_ret_str = '{'
    first_element = True
    for key, value in op_ret.items():
        if not first_element:
            op_ret_str += ', '
        if (type(value).__name__ == 'unicode'):
            element_str = "'%s':'%s'" % (key, value.encode("utf-8"))
        else:
            element_str = "'%s':'%s'" % (key, value)
        op_ret_str += element_str
        first_element = False

    op_ret_str += '}'
    return op_ret_str

def cos_op_success(cos_ret_dict):
    if cos_ret_dict.has_key(u'code'):
        if cos_ret_dict[u'code'] == 0 or cos_ret_dict[u'code'] == -4018 \
                or cos_ret_dict[u'code'] == -178:
            return True
    return False

def is_upload_server_error(cos_ret_dict):
    if cos_ret_dict.has_key(u'code'):
        if cos_ret_dict[u'code'] == -4024:
            return True
        else:
            return False

def print_op_ok(op, op_ret, local_path):
    global cos_sync_logger
    op_ret_str = convert_op_ret_str(op_ret)
    cos_sync_logger.info("[ok]   [%13s] [%20s] [%s]" % (op, op_ret_str, local_path))
    print '[ok]   [%13s] [%s]' % (op, local_path)

def print_op_fail(op, op_ret, local_path):
    global cos_sync_logger
    op_ret_str = convert_op_ret_str(op_ret)
    cos_sync_logger.info("[fail] [%13s] [%20s] [%s]" % (op, op_ret_str, repr(local_path)))
    print '[fail]   [%13s] [%s]' % (op, local_path)

def create_folder_if_not_exist(cos_client, bucket, cos_dir):
    global cos_task_statics
    request = StatFolderRequest(bucket, cos_dir)
    op_ret = cos_client.stat_folder(request)
    if cos_op_success(op_ret):
        return True
    request = CreateFolderRequest(bucket, cos_dir)
    op_ret = cos_client.create_folder(request)
    if cos_op_success(op_ret):
        cos_task_statics.add_create_folder_ok()
        print_op_ok("createTargetFolder", op_ret, cos_dir)
        return True
    else:
        cos_task_statics.add_create_folder_fail()
        print_op_fail("createTargetFolder", op_ret, cos_dir)
        return False

def create_folder(cos_client, bucket, cos_path, db, file_stat):
    global cos_task_statics
    local_path = file_stat.file_path
    request = CreateFolderRequest(bucket, cos_path)
    op_ret = cos_client.create_folder(request)
    if cos_op_success(op_ret):
        db.update_record(file_stat)
        cos_task_statics.add_create_folder_ok()
        print_op_ok("createFolder", op_ret, local_path)
        return True
    else:
        cos_task_statics.add_create_folder_fail()
        print_op_fail("createFolder", op_ret, local_path)
        return False

def delete_folder(cos_client, bucket, cos_path, db, file_stat):
    global cos_task_statics
    local_path = file_stat.file_path
    request = DelFolderRequest(bucket, cos_path)
    op_ret = cos_client.del_folder(request)
    if cos_op_success(op_ret):
        db.del_record(file_stat)
        cos_task_statics.add_del_folder_ok()
        print_op_ok("deleteFolder", op_ret, local_path)
        return True
    else:
        cos_task_statics.add_del_folder_fail()
        print_op_fail("deleteFolder", op_ret, local_path)
        return False

def upload_file(cos_client, bucket, cos_path, db, file_stat):
    global cos_task_statics
    local_path = file_stat.file_path

    if not isinstance(local_path, unicode):
        local_path = local_path.decode(fs_encoding)

    retry_index = 0
    retry_max_cnt = 3
    while retry_index < retry_max_cnt:
        request = UploadFileRequest(bucket, cos_path, local_path)
        request.set_insert_only(0) 
        op_ret = cos_client.upload_file(request)
        if cos_op_success(op_ret):
            db.update_record(file_stat)
            cos_task_statics.add_upload_file_ok()
            print_op_ok("uploadFile", op_ret, local_path)
            return True
        if is_upload_server_error(op_ret):
            del_request = DelFileRequest(bucket, cos_path)
            cos_client.del_file(del_request)
        retry_index += 1

    if retry_index == retry_max_cnt:
        cos_task_statics.add_upload_file_fail()
        print_op_fail("uploadFile", op_ret, local_path)
        return False

def delete_file(cos_client, bucket, cos_path, db, file_stat):
    global cos_task_statics
    local_path = file_stat.file_path
    request = DelFileRequest(bucket, cos_path)
    op_ret = cos_client.del_file(request)
    if cos_op_success(op_ret):
        db.del_record(file_stat)
        cos_task_statics.add_del_file_ok()
        print_op_ok("deleteFile", op_ret, local_path)
        return True
    else:
        cos_task_statics.add_del_file_fail()
        print_op_fail("deleteFile", op_ret, local_path)
        return False

class CosSync(object):
    def __init__(self, config):
        self.config = config
        self.db = DbRecord(config.get_db_path(), config.get_appid(), 
                config.get_bucket(), config.get_local_path(), config.get_cos_path())

    def _local_path_to_cos(self, local_path):


        cos_dir   = self.config.get_cos_path()
        local_dir = self.config.get_local_path()
        cos_path  = cos_dir + local_path[len(local_dir):]
        cos_path  = cos_path.replace(u'\\', u'/')
        return cos_path

    def _create_cos_target_dir(self, cos_client, bucket):
        cos_dir = self.config.get_cos_path()
        return create_folder_if_not_exist(cos_client, bucket, cos_dir)

    def _del_cos_sync_file(self, cos_client, bucket, del_file_dict):
        global delete_file
        if (len(del_file_dict) == 0):
            return
        if self.config.get_delete_sync() == 0:
            return

        thread_worker_num = self.config.get_thread_num()
        thread_pool = threadpool.ThreadPool(thread_worker_num)
        for del_file_path in del_file_dict.keys():
            cos_path = self._local_path_to_cos(del_file_path)
            del_args = [cos_client, bucket, cos_path, self.db,
                    del_file_dict[del_file_path]]
            args_tuple = (del_args, None)
            args_list = [args_tuple]
            requests = threadpool.makeRequests(delete_file, args_list)
            for req in requests:
                thread_pool.putRequest(req)
        thread_pool.wait()
        thread_pool.dismissWorkers(thread_worker_num, True)

    def _del_cos_sync_dir(self, cos_client, bucket, del_dir_dict):
        global delete_folder
        if (len(del_dir_dict) == 0):
            return
        if self.config.get_delete_sync() == 0:
            return

        for del_dir_path in sorted(del_dir_dict.keys(), reverse=True):
            cos_path = self._local_path_to_cos(del_dir_path)
            delete_folder(cos_client, bucket, cos_path, self.db, 
                    del_dir_dict[del_dir_path])

    def _create_cos_sync_file(self, cos_client, bucket, create_file_dict):
        global upload_file
        if (len(create_file_dict) == 0):
            return
        thread_worker_num = self.config.get_thread_num()
        thread_pool = threadpool.ThreadPool(thread_worker_num)
        for create_file_path in create_file_dict.keys():
            cos_path = self._local_path_to_cos(create_file_path)
            upload_args = [cos_client, bucket, cos_path, self.db,
                    create_file_dict[create_file_path]]
            args_tuple = (upload_args, None)
            args_list = [args_tuple]
            requests = threadpool.makeRequests(upload_file, args_list)
            for req in requests:
                thread_pool.putRequest(req)

        thread_pool.wait()
        thread_pool.dismissWorkers(thread_worker_num, True)

    def _create_cos_sync_dir(self, cos_client, bucket, create_dir_dict):
        global create_folder
        if (len(create_dir_dict) == 0):
            return
        for create_dir_path in sorted(create_dir_dict.keys(), reverse=False):
            cos_path = self._local_path_to_cos(create_dir_path)
            create_folder(cos_client, bucket, cos_path, self.db,
                    create_dir_dict[create_dir_path])

    def sync(self):
        local_file_dir_info = LocalFileDirInfo(self.config.get_local_path())

        del_file_dict = self.db.file_stat_dict
        del_dir_dict  = self.db.dir_stat_dict

        create_file_dict = {}
        create_dir_dict  = {}

        for local_file_stat in local_file_dir_info.file_stat_list:
            local_file_path = local_file_stat.file_path
            if (self.db.file_stat_dict.has_key(local_file_path)):
                if (FileStat.compare(local_file_stat,
                    self.db.file_stat_dict[local_file_path]) == 0):
                    del self.db.file_stat_dict[local_file_path]
                else:
                    create_file_dict[local_file_path] = local_file_stat
            else:
                create_file_dict[local_file_path] = local_file_stat
        
        for local_dir_stat in local_file_dir_info.dir_stat_list:
            local_dir_path = local_dir_stat.file_path
            if (self.db.dir_stat_dict.has_key(local_dir_path)):
                del self.db.dir_stat_dict[local_dir_path]
            else:
                create_dir_dict[local_dir_path] = local_dir_stat

        appid      = int(self.config.get_appid())
        secret_id  = self.config.get_secret_id()
        secret_key = self.config.get_secret_key()
        bucket     = self.config.get_bucket()
        timeout    = self.config.get_timeout()
        cos_client = CosClient(appid, secret_id, secret_key)
        cos_config = CosConfig()
        cos_config.set_timeout(timeout)
        if self.config.get_enable_https() == 1:
            cos_config.enable_https()
        cos_client.set_config(cos_config)

        self._create_cos_target_dir(cos_client, bucket)

        self._del_cos_sync_file(cos_client, bucket, del_file_dict)

        self._del_cos_sync_dir(cos_client, bucket, del_dir_dict)

        self._create_cos_sync_dir(cos_client, bucket, create_dir_dict)

        self._create_cos_sync_file(cos_client, bucket, create_file_dict)

if __name__ == '__main__':
    config = CosSyncConfig('./conf/config.json')
    if not config.is_valid_config():
        print 'wrong config: ' + config.get_err_msg()
        sys.exit(1)
    while True:
        cos_task_statics.begin_collect_statics()
        cos_sync = CosSync(config)
        cos_sync.sync()
        cos_task_statics.end_collect_statics()
        if config.get_daemon_mode() == 1:
            daemon_interval = config.get_daemon_interval()
            time.sleep(daemon_interval)
        else:
            break;

