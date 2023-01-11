import os, time
import psycopg2
from psycopg2.extras import RealDictCursor

from config import *


class GPConnect:
    def __init__(self):
        for n in range(CON_ATTEMPT_COUNT):
            try:
                self.connection = psycopg2.connect(
                    database = DATABASE,
                    user = USERNAME,
                    password = PASSWORD,
                    host = HOSTNAME,
                    port = PORT)
                self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
                LOGGER.info('connection established')
                break
            except Exception as ex:
                LOGGER.warning(f'connection attempt failed, {CON_ATTEMPT_COUNT-n} attempts left.')
                LOGGER.debug(ex)
                if n == CON_ATTEMPT_COUNT-1:
                    LOGGER.error('failed to establish connection, exit...')
                    os._exit(0)
                time.sleep(CON_ATTEMPT_TIMEOUT)


    def commit_and_close(self):
        self.connection.commit()
        self.connection.close()


    def get_query_result(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()


    def get_row_from_history_table(self, content, task_conf):
        self.cursor.execute('SELECT * FROM public.democratizator_history WHERE task_name=%s AND usename=%s AND procpid=%s AND current_query=%s', (task_conf['task_name'], content['usename'], content['procpid'], content['current_query']))
        return self.cursor.fetchall()        


    def insert_or_update_history_table(self, content, task_conf):
        try:
            row_in_history_table = self.get_row_from_history_table(content, task_conf)
            if row_in_history_table:
                if len(row_in_history_table) == 1:
                    self.cursor.execute('UPDATE public.democratizator_history SET last_value=%s, last_datetime=now(), hit_count = hit_count+1 WHERE task_name=%s AND usename=%s AND procpid=%s AND current_query=%s', (content['value'], task_conf['task_name'], content['usename'], content['procpid'], content['current_query']))
                    self.connection.commit()
                    LOGGER.info(f'update row for {content["usename"]} with p{content["procpid"]} in history table')
                else:
                    LOGGER.warning(f'duplicate row for {content["usename"]} with p{content["procpid"]} in history table')
            else:
                self.cursor.execute('INSERT INTO public.democratizator_history VALUES (%s, %s, %s, %s, %s, now(), %s, now(), 1, null)', (task_conf['task_name'], content['usename'], content['procpid'], content['current_query'], content['value'], content['value']))
                self.connection.commit()
                LOGGER.info(f'insert new row for {content["usename"]} with p{content["procpid"]} in history table')
        except Exception as ex:
            self.connection.rollback()
            LOGGER.error(f'error when insert or update row in history table \n{ex}')


    def update_send_mail_time(self, content, task_conf):
        try:
            self.cursor.execute('UPDATE public.democratizator_history SET mail_sent_datetime=now() WHERE task_name=%s AND usename=%s AND procpid=%s AND current_query=%s', (task_conf['task_name'], content['usename'], content['procpid'], content['current_query']))
            self.connection.commit()
            LOGGER.info(f'update send mail time for {content["usename"]} with p{content["procpid"]} in history table')
        except Exception as ex:
            self.connection.rollback()
            LOGGER.error(f'error when update send mail time row in history table \n{ex}')
