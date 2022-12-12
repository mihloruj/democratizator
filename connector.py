import psycopg2
from psycopg2.extras import RealDictCursor

from config import *


class SQLConnect:
    def __init__(self):
        self.connection = psycopg2.connect(
            database = DATABASE,
            user = USERNAME,
            password = PASSWORD,
            host = HOSTNAME,
            port = PORT)
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)


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
                    self.cursor.execute('UPDATE public.democratizator_history SET last_value=%s, last_datetime=now(), hit_count = hit_count+1 WHERE task_name=%s AND usename=%s AND procpid=%s AND current_query=%s', (content['size'], task_conf['task_name'], content['usename'], content['procpid'], content['current_query']))
                    self.connection.commit()
                    LOGGER.info(f'update row for {content["usename"]} with p{content["procpid"]} in history table')
                else:
                    LOGGER.warning(f'duplicate row for {content["usename"]} with p{content["procpid"]} in history table')
            else:
                self.cursor.execute('INSERT INTO public.democratizator_history VALUES (%s, %s, %s, %s, %s, now(), %s, now(), 1, null)', (task_conf['task_name'], content['usename'], content['procpid'], content['current_query'], content['size'], content['size']))
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
