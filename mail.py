import smtplib
from email.message import EmailMessage
from config import SMTP_IP


def send_message(content, task_conf):
    with smtplib.SMTP(SMTP_IP) as smtp:
        message = EmailMessage()
        message['From'] = 'incv_test@rt.ru'
        message['To'] = ['m.a.ruzheynikov@rt.ru', 'Nikolay.Koshelkov@rt.ru']
        message['Subject'] = 'Демократизатор'
        html = f"""\
            <html>
            <head></head>
            <body>
                <p>Здравствуйте!<br>
                Ваш запрос на кластере GP PROD достиг лимита по правилу '{task_conf["email_rule_text"]} {task_conf["limit"]}'. Пока это предупреждение, но в дальнейшем запрос может быть автоматически прерван, а учетная запись заблокирована.<br>
                </p>
                <p>
                Детальная информация:<br>
                Имя УЗ: <b>{content['usename']}</b><br>
                Procpid: <b>{content['procpid']}</b><br>
                Последнее значение: <b>{content['last_value']}</b><br>
                Текст запроса приложен к письму - bad_query.txt<br>  
                </p>
                <p>
                В этой <a href="https://confluence.rt.ru/pages/viewpage.action?pageId=231412071">статье</a> описан процесс реагирования на данное письмо. 
                </p>
                <small>Это письмо отправлено автоматически</small>
            </body>
            </html>
            """
        message.set_content(html, subtype='html')
        message.add_attachment(content['current_query'].encode('utf-8'), maintype='text',subtype='plain',filename="bad_query.txt")
        smtp.send_message(message)