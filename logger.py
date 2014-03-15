#!/usr/bin/env python
# -*- coding: utf8 -*-

# created by ErshKUS
# stapio logger

import datetime
from os import uname
import stapio_config as conf

default_file_log = 'stapio_logger.log'
timer={}
isAuto = False

def sendmail(text, head="message from Stapio"):
  import smtplib
  sender = conf.email_from
  receivers = [conf.email_to]
  message = 'From: %s\r\nTo: %s\r\nContent-Type: text/plain; charset="utf-8"\r\nSubject: %s\r\n\r\n'
  message = message % (conf.email_from, conf.email_to, head)
  message += text

  try:
    smtpObj = smtplib.SMTP(conf.smtp_server)
    if conf.email_pass <> '':
      smtpObj.login(conf.email_from, conf.email_pass)
    smtpObj.sendmail(sender, receivers, message)
  except SMTPException:
    add("Error: unable to send email")


def add(text, level=0, file=default_file_log, finish=False):
  outtext = ''
  nowtime = datetime.datetime.now()
  if timer.has_key(level+1):
    outtext = '#  timer = ' + str(nowtime-timer[level+1]) + '\n'
    timer.pop(level+1)
  if timer.has_key(level):
    pre='#  '
    if outtext != '':
      pre = (' '*16) + (' '*(2+2*level)) + '#  '
    outtext += pre + 'timer = ' + str(nowtime-timer[level]) + '\n'
  outtext = outtext[:-1]
  outtext += '\n' + (str(nowtime.strftime("%Y-%m-%d %H:%M:%S")) +(' '*(2+2*level))+ text)
  timer[level] = nowtime
  print outtext,
  if conf.write_in_file:
    file_log_d = open(conf.logdir + file,'a')
    file_log_d.write(outtext+'\n')
    file_log_d.close()
  if conf.write_in_singlefile:
    file_log_d = open(conf.logdir + conf.singlefile_log,'a')
    file_log_d.write(outtext+'\n')
    file_log_d.close()
  if conf.email_send:
    hostname = uname()[1]
    if text[0] == '!':
      sendmail(text=text, head=conf.email_head_error + ' , hostname='+hostname)
    if finish:
      sendmail(text=outtext, head=conf.email_head_finish + ' , hostname='+hostname)