"""
Mail utilities.
Отвечает за отправку email через SMTP.
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from os.path import basename


def send_email(settings, to_emails, subject, body, attachments=None):
    """
    Отправить email.
    
    Args:
        settings: словарь с настройками SMTP (smtp_host, smtp_port, smtp_user, smtp_password, smtp_tls, from_name)
        to_emails: список email адресов получателей
        subject: тема письма
        body: текст письма (может быть HTML)
        attachments: список путей к файлам для вложений (опционально)
    
    Returns:
        tuple: (success: bool, message: str)
    """
    if not settings:
        return False, "Настройки почты не найдены"
    
    smtp_host = settings.get('smtp_host')
    smtp_port = settings.get('smtp_port', 587)
    smtp_user = settings.get('smtp_user')
    smtp_password = settings.get('smtp_password')
    smtp_tls = settings.get('smtp_tls', True)
    from_name = settings.get('from_name', 'Report Builder')
    
    if not smtp_host or not smtp_user or not smtp_password:
        return False, "Настройки SMTP неполные"
    
    try:
        msg = MIMEMultipart('alternative')
        msg['From'] = f"{from_name} <{smtp_user}>"
        msg['To'] = ', '.join(to_emails) if isinstance(to_emails, list) else to_emails
        msg['Subject'] = subject
        
        # Добавляем текстовую и HTML версии
        text_part = MIMEText(body, 'plain', 'utf-8')
        html_part = MIMEText(body, 'html', 'utf-8')
        msg.attach(text_part)
        msg.attach(html_part)
        
        # Добавляем вложения
        if attachments:
            for filepath in attachments:
                with open(filepath, 'rb') as f:
                    part = MIMEApplication(f.read(), Name=basename(filepath))
                    part['Content-Disposition'] = f'attachment; filename="{basename(filepath)}"'
                    msg.attach(part)
        
        # Подключаемся к SMTP серверу
        server = smtplib.SMTP(smtp_host, smtp_port)
        if smtp_tls:
            server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)
        server.quit()
        
        return True, f"Email отправлен на {', '.join(to_emails)}"
    
    except smtplib.SMTPException as e:
        return False, f"SMTP ошибка: {str(e)}"
    except Exception as e:
        return False, f"Ошибка отправки: {str(e)}"


def send_test_email(test_email=None):
    """
    Отправить тестовое письмо.
    
    Args:
        test_email: email получателя (если None, используется smtp_user)
    
    Returns:
        tuple: (success: bool, message: str)
    """
    from db_settings import get_mail_settings
    
    settings = get_mail_settings()
    if not settings:
        return False, "Настройки почты не найдены"
    
    recipient = test_email or settings.get('smtp_user')
    if not recipient:
        return False, "Email получателя не указан"
    
    subject = "Тестовое письмо - Report Builder"
    body = """
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <h2 style="color: #4F46E5;">✅ Тестовое письмо</h2>
        <p>Это тестовое письмо от <strong>Report Builder</strong>.</p>
        <p>Если вы получили это письмо, значит настройки почты работают корректно!</p>
        <hr style="border: 1px solid #eee; margin: 20px 0;">
        <p style="color: #666; font-size: 12px;">
            Отправлено: """ + __import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """
        </p>
    </body>
    </html>
    """
    
    return send_email(settings, [recipient], subject, body)
