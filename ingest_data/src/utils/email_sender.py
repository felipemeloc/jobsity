from email.mime.text import MIMEText
import smtplib
from tabulate import tabulate
import os
from dotenv import load_dotenv

load_dotenv()

def email_sender(df_status):
    """email_sender: function to send an email to notify the process status

    Args:
        rows (int): rows added in the process
    """
    sender_email = os.getenv('SENDER_EMAIL')
    rec_email = os.getenv('REC_EMAIL')
    password = os.getenv('EMAIL_PASSWORD')
    df_status.set_index('file',inplace=True)
    df_status['date'] = df_status['date'].dt.strftime('%Y-%m-%d %H:%M')
    df= tabulate(df_status, headers='keys', tablefmt='psql')
    msg = MIMEText(f'Summary ingest process: \n {df} ')
    msg['Subject'] = 'Ingest automatic process notification'
    msg['From'] = sender_email
    msg['To'] = rec_email

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(sender_email, password)
    print("Login success")
    server.sendmail(sender_email, rec_email, msg.as_string())
    print("Email has been sent to ", rec_email)