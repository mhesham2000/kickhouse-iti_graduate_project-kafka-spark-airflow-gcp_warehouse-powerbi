import smtplib

server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login('sportsdb90@gmail.com', 'hpiupmsmncnfggzn')
server.sendmail('sportsdb90@gmail.com', 'shinetym@gmail.com', 'Subject: Test\n\nThis is a test email')
server.quit()