import imaplib
import email
import pandas as pd
from bs4 import BeautifulSoup
import re
import time
import random
from sqlalchemy import create_engine
import subprocess
import sys
import os
import pymysql
import pymysql.cursors
import datetime
from dateutil.parser import parse
    
path = os.getcwd()
os.chdir(path)    
host = 'imappro.zoho.com'
user = 'intern_kjones@quadratyx.com'
password = 'daZXbVDKtCXV'
column_names = ['CaseNo','From', 'To', 'Date', 'Subject', 'Content','Attachment','Status','time_insert']
df = pd.DataFrame(columns=column_names)
subject = ""
engine = create_engine('mysql+pymysql://username:password@databaseIP:port/databasename')
con12 = engine.connect()

def time_extract(k):
    dt = parse(k)
    return dt.strftime('%Y-%m-%d %H:%M:%S')     
        
def get_body(msg):
    if msg.is_multipart():
        return get_body(msg.get_payload(0))
    else:
        return msg.get_payload(None, True)
        
        
def parse_content(data):
    soup = BeautifulSoup(data, "html.parser")
    text = soup.get_text()
    c = ' '.join(w for w in re.split(r"\W", text) if w)
    return c
    
def rand_x_digit_num(x, leading_zeroes=True):
    if not leading_zeroes:
        return random.randint(10**(x-1), 10**x-1)  
    else:
        if x > 6000:
            return ''.join([str(random.randint(0, 9)) for i in xrange(x)])
        else:
            return '{0:0{x}d}'.format(random.randint(0, 10**x-1), x=x)
    
           
con = imaplib.IMAP4_SSL(host)
con.login(user, password)
con.select("INBOX")
result, data = con.uid('search', None, "ALL")
inbox_item_list = data[0].split()
    
for item in inbox_item_list:
    attach = 'N'
    result2, data = con.uid('fetch', item, '(RFC822)')
    raw = email.message_from_string(data[0][1].decode('utf-8'))
    to_ = raw['TO']
    from_ = raw['FROM']
    sub = raw['SUBJECT']
    date = raw['DATE']
    caseno = rand_x_digit_num(8)
    for part in raw.walk():
        if part.get_content_maintype() == 'multipart':
            continue
        if part.get('Content-Disposition') is None:
            continue
        attach = 'Y'
    content = get_body(raw).decode("utf-8")
    w = parse_content(content)  
    df=df.append({'CaseNo':caseno,'To': to_, 'From': from_, 'Date': date, 'Subject': sub, 
                      'Content':w,'Attachment':attach,'Status':'unprocessed','time_insert':time_extract(date)}, ignore_index=True)
    result2, data = con.store(item,'FLAGS','\\Seen')
    print(type(date),date)
    break
df.to_sql(name='Testing2020', con=con12, if_exists='append',
                                     index=False)



