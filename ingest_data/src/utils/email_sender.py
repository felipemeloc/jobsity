from email.mime.text import MIMEText
import smtplib
import os
from dotenv import load_dotenv

load_dotenv()

def email_sender(rows):
    """email_sender: function to send an email to notify the process status

    Args:
        rows (int): rows added in the process
    """
    sender_email = os.getenv('SENDER_EMAIL')
    rec_email = os.getenv('REC_EMAIL')
    password = os.getenv('EMAIL_PASSWORD')

    msg = MIMEText(f'The ingest process has been successful. {rows} new records were added')
    msg['Subject'] = 'Ingest automatic process notification'
    msg['From'] = sender_email
    msg['To'] = rec_email

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(sender_email, password)
    print("Login success")
    server.sendmail(sender_email, rec_email, msg.as_string())
    print("Email has been sent to ", rec_email)