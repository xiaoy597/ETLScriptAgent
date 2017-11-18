#!/bin/env python
# -*- coding: UTF-8 -*-

import sys
import traceback
import logging
import logging.handlers
import os
import datetime

hive_cmd = '''hive -f %s -hivevar DATA_DT="%s" -hivevar YST_DATA_DT="%s" -hivevar DATA_DT_ISO="%s" -hivevar YST_DATA_DT_ISO="%s" -hivevar DATA_YEAR="%s" -hivevar DATA_DM="%s" -hivevar DATA_DM_ISO="%s" -hivevar DATA_DT_8="%s" 1>logs/%s 2>&1'''
hql_directory = "job-scripts"

logger = None


def run_command_func(command):
    logger.info("begin to run command[%s]" % command)
    for i in range(0, 3):
        print command
        ret_code = os.system(command)
        if ret_code != 0:
            if i == 2:
                error_message = "run command[%s] fail" % command
                logger.error(error_message)
                raise IOError(error_message)
            else:
                continue
        else:
            break


def excute_func(tablename):
    try:
        run_command_func(hive_cmd % \
                         (get_hql(tablename), get_data_dt_iso(tablename), get_prev_data_dt_iso(tablename),
                          get_data_dt_iso(tablename), get_prev_data_dt_iso(tablename), get_year(tablename),
                          get_data_dm_iso(tablename), get_data_dm_iso(tablename), get_data_dt(tablename),
                          get_logname(tablename))
                         )
    except Exception as e:
        warning_message = traceback.format_exc()
        logger.error(warning_message)
        exit(-1)


def get_hql(tablename):
    try:
        # DWP_JYJX_T_SCHOOL_INFO_20170208.dir
        tmp_ls = tablename.split(".")[0].split("_")
        hql_file_name = "_".join(x.capitalize() for x in tmp_ls[1:-1])
        # load_date = tmp_ls[-1]

        return hql_directory + '/pdata.' + hql_file_name + '.sql'
    except:
        warning_message = traceback.format_exc()
        logger.error(warning_message)
        sys.exit(-1)

def get_logname(tablename):
    try:
        # DWP_JYJX_T_SCHOOL_INFO_20170208.dir
        tmp_ls = tablename.split(".")[0].split("_")
        log_file_name = "_".join(x.capitalize() for x in tmp_ls[1:-1])
        # load_date = tmp_ls[-1]

        return log_file_name + '_' + get_data_dt(tablename) + '.log'
    except:
        warning_message = traceback.format_exc()
        logger.error(warning_message)
        sys.exit(-1)


def get_data_dt(tablename):
    # tmp_ls = tablename.split(".")[0].split("_")
    # return tmp_ls[-1]
    if os.path.exists('etc/workdate'):
        f = open('etc/workdate')
        workdate = f.readline().strip()
        f.close()
        return workdate
    else:
        return datetime.datetime.now().strftime('%Y%m%d')


def get_prev_data_dt(tablename):
    data_dt = get_data_dt(tablename)
    target_datetime = datetime.datetime.strptime(data_dt, "%Y%m%d")
    return (target_datetime - datetime.timedelta(days=1)).strftime("%Y%m%d")


def get_data_dt_iso(tablename):
    data_dt = get_data_dt(tablename)
    target_datetime = datetime.datetime.strptime(data_dt, "%Y%m%d")
    return target_datetime.strftime("%Y-%m-%d")


def get_prev_data_dt_iso(tablename):
    data_dt = get_data_dt(tablename)
    target_datetime = datetime.datetime.strptime(data_dt, "%Y%m%d")
    return (target_datetime - datetime.timedelta(days=1)).strftime("%Y-%m-%d")


def get_year(tablename):
    tmp_ls = tablename.split(".")[0].split("_")
    return tmp_ls[-1][:4]


def get_data_dm(tablename):
    tmp_ls = tablename.split(".")[0].split("_")
    return tmp_ls[-1][:6]


def get_data_dm_iso(tablename):
    data_dt = get_data_dt(tablename)
    target_datetime = datetime.datetime.strptime(data_dt, "%Y%m%d")
    return target_datetime.strftime("%Y-%m")


def main():
    if len(sys.argv) == 2:
        logger.info('Execute job %s ...' % sys.argv[1])
        excute_func(sys.argv[1])
    else:
        logger.error("parameter error!")
        sys.exit(-1)


if __name__ == '__main__':
    log_file = 'logs/job-executor.log'
    handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=1024 * 1024, backupCount=5)
    fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
    formatter = logging.Formatter(fmt)
    handler.setFormatter(formatter)

    logger = logging.getLogger('job-executor')
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    main()
