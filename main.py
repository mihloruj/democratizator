from config import *
import json, time
from glob import glob
import schedule
from connector import *
from mail import send_message


def get_tasks_configurations():
    try:
        tasks = []
        all_pathes_to_tasks = glob(PATH_TO_TASKS)
        for path_to_task in all_pathes_to_tasks:
            with open(path_to_task, encoding='utf-8') as file:
                tasks.append(json.load(file))
        return tasks
    except Exception as ex:
        LOGGER.error('error when loading tasks configurations \n{ex}')


def check_condition(value, limit):
    try:
        return eval(f'{value} {limit}')
    except Exception as ex:
        LOGGER.error(f'error when check condition \n{ex}')


def run_task(task_configuration):
    LOGGER.info(f'task {task_configuration["task_name"]} running')
    conn = SQLConnect()
    query_result = conn.get_query_result(task_configuration["query"])
    if query_result:
        for row in query_result:
            if check_condition(row['size'], task_configuration['limit']):
                conn.insert_or_update_history_table(row, task_configuration)
                history_row = conn.get_row_from_history_table(row, task_configuration)[0] 
                if history_row['hit_count'] >= COUNT_TO_EMAIL and history_row['mail_sent_datetime'] is None:
                    try:
                        send_message(history_row, task_configuration)
                        LOGGER.info('email send to admins')
                        conn.update_send_mail_time(row, task_configuration)
                    except Exception as ex:
                        LOGGER.error(f'email for {history_row["usename"]} with p{history_row["procpid"]} not be sent\n{ex}')
    conn.commit_and_close()


if __name__ == "__main__":
    LOGGER.info('start working now')
    tasks_configurations = get_tasks_configurations()
    if tasks_configurations:
        LOGGER.info(f'loaded {len(tasks_configurations)} tasks.')
        for task_conf in tasks_configurations:
            schedule.every(INTERVAL).minutes.do(lambda: run_task(task_conf))
        while True:
            schedule.run_pending()
            time.sleep(1)   
    else:
        LOGGER.info(f'no tasks found in {PATH_TO_TASKS}')
