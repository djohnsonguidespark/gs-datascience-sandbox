# Import smtplib for the actual sending function
import smtplib,email
from datetime import datetime,timedelta

# Here are the email package modules we'll need
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

filename = 'reset_ranking'

##############################
# Define Sender / Recipients
##############################
sender = 'Devon Johnson <djohnson@guidespark.com>'
receiver = ['Devon Johnson <djohnson@guidespark.com>'] 

###############################################
# Create the container (outer) email message.
###############################################
msg = MIMEMultipart()
msg['Subject'] = 'Ranking Reset Complete ... ' + datetime.now().strftime('%Y-%m-%d')  
msg['From'] = sender 
msg['To'] = ','.join(receiver) # join allows for multiple emails 
text = "Ranking Reset Complete"

part1 = MIMEText(text,'plain')

###############################
# Prep xlsx file for attachment
###############################
fp = file(filename)
file1 = MIMEText(fp.read())
file1.add_header('Content-Disposition','attachment;filename=' + filename)

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

