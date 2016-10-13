# Import smtplib for the actual sending function
import smtplib,email
from datetime import datetime,timedelta

# Here are the email package modules we'll need
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

filename = './output/top10_weekly_' + datetime.now().strftime('%Y%m%d') +'.xlsx'

##############################
# Define Sender / Recipients
##############################
sender = 'Devon Johnson <djohnson@guidespark.com>'
receiver = ['Devon Johnson <djohnson@guidespark.com>',
			'Linda Lam <llam@guidespark.com>'] 
#receiver = ['Devon Johnson <djohnson@guidespark.com>'] 

###############################################
# Create the container (outer) email message.
###############################################
msg = MIMEMultipart()
msg['Subject'] = 'Top-10 Customers Weekly ... ' + datetime.now().strftime('%Y-%m-%d')  
msg['From'] = sender 
msg['To'] = ','.join(receiver) # join allows for multiple emails 
text = "Top-10 Customers Weekly Report"

part1 = MIMEText(text,'plain')

###############################
# Prep xlsx file for attachment
###############################
fp = open(filename, 'rb')
file1=email.mime.base.MIMEBase('application','vnd.ms-excel')
file1.set_payload(fp.read())
fp.close()
email.encoders.encode_base64(file1)
file1.add_header('Content-Disposition','attachment;filename=' + filename.split('/')[len(filename.split('/'))-1])

##############################################################################
# Attach parts into message container.
# According to RFC 2046, the last part of a multipart message, in this case
# the HTML message, is best and preferred.
##############################################################################
msg.attach(part1)
msg.attach(file1)

#############
# Send email 
#############
try:
	s = smtplib.SMTP('localhost')
	s.sendmail(sender, receiver, msg.as_string())
	s.quit()
	print "Successfully sent email"
except smtplib.SMTPException:
	print "Error: unable to send email"

