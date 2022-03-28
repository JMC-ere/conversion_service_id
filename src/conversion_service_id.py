# -*- coding: utf-8 -*-
import datetime
import multiprocessing
import os
import sys
import time
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import json
import logging.handlers
import pymysql
import cx_Oracle
import qry.db_qry
from multiprocessing import Pool

state = 'PRD'
charset = 'utf-8'

def list_chunk(lst, n):
    return [lst[i:i + n] for i in range(0, len(lst), n)]


def make_dict_factory(cursor):
    column_names = [d[0] for d in cursor.description]

    def create_row(*args):
        return dict(zip(column_names, args))

    return create_row


def make_log():
    log_name = "conversion_service_id.log"

    log_handler = logging.handlers.TimedRotatingFileHandler(
        filename='logs/' + log_name,
        when='midnight',
        interval=1,
        backupCount=30,
        encoding='utf-8',
        atTime=datetime.time(0, 0, 0, 0)
    )

    # 추가
    log_handler.suffix = 'log-%Y.%m.%d'

    logger = logging.getLogger(__file__)
    formatter = logging.Formatter('[%(levelname)s|%(filename)s:%(lineno)s] %(asctime)s > %(message)s')
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
    logger.setLevel(logging.INFO)

    return logger


log_txt = make_log()


def read_config():
    if state == 'PRD' or state == 'STG':
        with open('config/info.json', 'r') as f:
            info = json.load(f)
            info = info[state]
    else:
        log_txt.info(f'WRONG STATE : {state}')
        quit()

    return info


def connect_oracle():
    # oracle client 관련
    # LOCATION = r"C:/instantclient_19_10"
    # os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"]  # 환경변수 등록

    o_info = read_config()['SERVICE_ID_DB']
    print(o_info)
    try:
        if state == 'PRD':
            dns_str = f"""(DESCRIPTION=
                                (FAILOVER=on)
                                (ADDRESS_LIST=
                                    (ADDRESS=(PROTOCOL=tcp)(HOST=192.168.5.67)(PORT=1521))
                                    (ADDRESS=(PROTOCOL=tcp)(HOST=192.168.5.68)(PORT=1521))
                                )
                                (CONNECT_DATA=(SERVICE_NAME=PBTVDB))
                            )"""

            oracle_conn = cx_Oracle.connect(
                user=o_info['USER'],
                password=o_info['PASSWORD'],
                dsn=dns_str,
                encoding="UTF-8"
            )
        elif state == 'STG':
            oracle_conn = cx_Oracle.connect(
                user=o_info['USER'],
                password=o_info['PASSWORD'],
                dsn=o_info['HOST'],
                encoding="UTF-8"
            )
        else:
            oracle_conn = ''
            log_txt.info(f'WRONG STATE')

        return oracle_conn

    except Exception as err:
        log_txt.info(f'ORACLE CONNECT ERROR : {err}')
        quit()


def connect_mysql():
    m_info = read_config()['NUDGE_ADMIN_DB']
    try:
        mysql_conn = pymysql.connect(
            host=m_info['HOST'],
            user=m_info['USER'],
            password=m_info['PASSWORD'],
            db=m_info['DB_NAME'],
            port=m_info['PORT']
        )

        return mysql_conn

    except Exception as err:
        log_txt.info(f'CONNECT ERROR : {err}')
        quit()


m_conn = connect_mysql()
m_curs = m_conn.cursor(pymysql.cursors.DictCursor)



def get_service_id():
    sql = qry.db_qry.NUDGEADMIN_SERVICE_ID_1
    m_curs.execute(sql)

    m_rows = m_curs.fetchall()
    log_txt.info(f'FIRST CONVERSION SERVICE ID COUNT : {len(m_rows)}건')

    m_service_num = []

    for row in m_rows:
        m_service_num.append(str(row['service_num']))

    return m_service_num


def mapping_service_id(service_nums, count):
    sql = qry.db_qry.GET_STB_ID
    try:
        service_nums = ",".join(service_nums)
        log_txt.info(f'START MAPPING')
        o_curs.execute(sql % service_nums)
        o_curs.rowfactory = make_dict_factory(o_curs)
        list_result = (o_curs.fetchall())
        log_txt.info(f'END MAPPING')
        log_txt.info(f'START UPDATE')
        m_curs.executemany(qry.db_qry.UPDATE_STB_ID, list_result)
        m_conn.commit()
        count['count'] += len(list_result)
        log_txt.info(f"UPDATE COUNT : {count['count']}건")
        log_txt.info(f'END UPDATE')
    except Exception as err:
        log_txt.info(f'UPDATE ERROR : {err}')
        log_txt.info(f'ERROR SERVICE NUM : {service_nums}')


if __name__ == '__main__':
    try:
        global o_conn, o_curs
        o_conn = connect_oracle()
        o_curs = o_conn.cursor()
        manager = multiprocessing.Manager()
        mul_dict = manager.dict()
        mul_dict['count'] = 0
        log_txt.info(f'START CONVERSION SERVICE ID')
        pool = Pool(processes=8)
        list_service_num = list_chunk(get_service_id(), 500)
        log_txt.info(f'LOOP COUNT : {len(list_service_num)}건')
        time.sleep(2)
        for i in list_service_num:
            pool.apply_async(mapping_service_id, args=(i, mul_dict))
            time.sleep(1)

        pool.close()
        pool.join()
        m_conn.close()
        o_conn.close()
        log_txt.info(f'END CONVERSION SERVICE ID')
    except Exception as err:
        log_txt.info(f'ERROR : {err}')
