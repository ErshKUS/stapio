#!/usr/bin/env python
# -*- coding: utf8 -*-

import argparse, datetime, ConfigParser, psycopg2, gzip, traceback, os
import urllib, urllib2
import subprocess
import ershkus_addr
import ershkus_poi
import stapio_config as conf
import logger as log


file_log = 'stapio_run.log'

def install(loglevel=0):
  import stapio_install
  
  stapio_install.install()


def cmdrun(cmd, errorText='', loglevel=0):
  PIPE = subprocess.PIPE
  p1 = subprocess.Popen(cmd, shell = True)
  returncode = p1.wait()
  if returncode <> 0:
    log.add (text=errorText, level=loglevel, file=file_log)
    raise Exception('returncode: ' + str(returncode) + ', text error: ' + str(errorText))


def load(update, today=False, loglevel=0):
  log.add (('load start (update=%s)' % update), level=loglevel, file=file_log)
  file={'temp':conf.tempdir, 'authFileOsmosis':conf.authFileOsmosis}
  if not os.path.exists('data'):
    os.mkdir('data')
  i=0
  if update:
    file['name_d'] = conf.workdir + 'data/load%s.osc.gz'
    file['name_e'] = conf.workdir + 'data/load%se.osc'
    file['url_list'] = conf.urlmaskosc
    file['osmosis_read'] = 'read-xml-change'
    file['osmosis_merge'] = 'merge-change --sort-change'
    file['osmosis_write'] = 'write-xml-change'
    file['osmosis_writedb'] = 'write-pgsql-change'

    # загрузим предыдущую позицию
    f = open(conf.workactual + 'upd_date.dat','r')
    file['date_s'] = datetime.datetime.strptime(f.readline(), conf.format_datetime)
    f.close()
    
    file['date_e'] = file['date_s'] + datetime.timedelta(days=1)
    file['daystart'] = file['date_s'].strftime("%y%m%d")
    file['dayend'] = file['date_e'].strftime("%y%m%d")
  else:
    file['name_d'] = conf.workdir + 'data/load%s.pbf'
    file['name_e'] = conf.workdir + 'data/load%se.pbf'
    file['url_list'] = conf.urlpbf
    file['osmosis_read'] = 'read-pbf'
    file['osmosis_merge'] = 'merge --sort'
    file['osmosis_write'] = 'write-pbf'
    file['osmosis_writedb'] = 'write-pgsql'

  info = {'load':False, 'next_load':True}
  # отключил meta
	# if not update:
    # urllib.urlretrieve(conf.urlpbfmeta, conf.workdir + "data/load.pbf.meta")
  while info['next_load']:
    if update:
      log.add ('load date at ' + file['date_s'].strftime(conf.format_datetime), level=loglevel, file=file_log)
    for url_file in file['url_list']:
      i += 1
      file['end'] = (file['name_e'] % i)
      log.add (('load, i=%s' % i), level=loglevel+1, file=file_log)
      file['now'] = (file['name_d'] % i)
      url_file = url_file % file
      try:
        asock = urllib2.urlopen(url_file)
      except urllib2.HTTPError, e:
        if e.code == 404:
          info['next_load'] = False
          file['date_e'] = file['date_s'] - datetime.timedelta(days=1)
          break
        log.add (('! error download (code=%s)' % e.code), level=loglevel+1, file=file_log)
        raise e
      print url_file
      urllib.urlretrieve(url_file, file['now'])
      if update:
        log.add ('decompress', level=loglevel+1, file=file_log)
        cmdrun(cmd=('gzip -df '+file['now']), errorText=('! error decompress, i=%s' % i), loglevel=loglevel+1)
        file['now'] = file['now'][:-3]
      if i == 1:
        file['in'] = file['now']
        continue
      log.add (('merge, i=%s' % i), level=loglevel+1, file=file_log)
      file['n'] = file['now']
      cmd  = 'osmosis -quiet --%(osmosis_read)s file=%(in)s '
      cmd += '--%(osmosis_read)s file=%(n)s  --%(osmosis_merge)s '
      cmd += '--%(osmosis_write)s file=%(end)s'
      cmd = cmd % file
      #print cmd  #  #  #  #  #  #  #  #  #  #  #  #  #  #
      cmdrun(cmd, errorText=('! error merge, i=%s' % i), loglevel=loglevel+1)

      file['in'] = file['end']
      
    if info['next_load']:
      info['load'] = True
    if update:
      file['date_s'] = file['date_e']
      file['date_e'] = file['date_s'] + datetime.timedelta(days=1)
      file['daystart'] = file['date_s'].strftime("%y%m%d")
      file['dayend'] = file['date_e'].strftime("%y%m%d")
    else:
      info['next_load'] = False
      
  if not info['load']:
    raise Exception('no load from pbf/osc')
  
  conn = psycopg2.connect(host=conf.addrfull_host, database=conf.addrfull_database, user=conf.addrfull_user, password=conf.addrfull_password)
  if not update:
		# отключил meta
    # pbfmeta = ConfigParser.RawConfigParser()
    # pbfmeta.read(conf.workdir + 'data/load.pbf.meta')
    # file['date_e'] = datetime.datetime.strptime(pbfmeta.get('DEFAULT', 'version'), '%Y-%m-%d %H:%M:%S')
    # log.add ('pbf at ' + file['date_e'].strftime(conf.format_datetime), level=loglevel, file=file_log)
    log.add ('clear db', level=loglevel, file=file_log)
    cur = conn.cursor()
    cur.execute("""
      TRUNCATE TABLE nodes;
      TRUNCATE TABLE relation_members;
      TRUNCATE TABLE relations;
      TRUNCATE TABLE users;
      TRUNCATE TABLE way_nodes;
      TRUNCATE TABLE ways;
      TRUNCATE TABLE deleted_entries;
    """)
    conn.commit()

  log.add ('load in db', level=loglevel, file=file_log)
  
  if conf.osmosisExport <> '':
    cmd = 'export ' + conf.osmosisExport + ' && '
  else:
    cmd = ''
  
  cmd += 'osmosis -quiet --%(osmosis_read)s file=%(in)s '
  cmd += '--%(osmosis_writedb)s authFile=%(authFileOsmosis)s'
  cmd = cmd % file
  # log.add ('cmd: ' + cmd, level=loglevel, file=file_log)
  cmdrun(cmd, errorText='! error load in db', loglevel=loglevel)

  # сохраним текущую позицию
  # отключил meta
  # log.add ('save date', level=loglevel, file=file_log)
  # f = open(conf.workactual + 'upd_date.dat','w')
  # f.write(file['date_e'].strftime(conf.format_datetime))
  # f.close()

  log.add ('load complite', level=loglevel, file=file_log)


def controlAuto(isEnd=False, isError=False, loglevel=0):
  if isEnd:
    if not isError:
      if os.path.exists(conf.workactual + 'work.dat'):
        os.remove(conf.workactual + 'work.dat')
    
    dir = conf.workdir + 'data'
    if dir[-1] == os.sep: dir = dir[:-1]
    files = os.listdir(dir)
    for file in files:
      if file == '.' or file == '..': continue
      os.remove(dir + os.sep + file)
    
    if conf.runAfter:
      cmdrun(cmd=conf.runAfter, errorText='! error run after', loglevel=loglevel)
    
  else:
    if os.path.exists(conf.workactual + 'work.dat'):
      texterror = '! previous requests can not be completed or if an error, exists "work.dat"'
      log.add (text=texterror, level=loglevel, file=file_log)
      raise Exception(texterror)
  
    file_log_d = open(conf.workactual + 'work.dat','w')
    file_log_d.write(str(datetime.datetime.now().strftime(conf.format_datetime)))
    file_log_d.close()
 
  
def insert(loglevel=0, noLoad=False, onlyAddr=False, onlyPOI=False):
  if not noLoad:
    load(update = False, loglevel = loglevel+1)

  conn = psycopg2.connect(host=conf.addrfull_host, database=conf.addrfull_database, user=conf.addrfull_user, password=conf.addrfull_password)

  if not onlyPOI:
    log.add ('insert addr', level=loglevel, file=file_log)
    ershkus_addr.insertAddr(conn, loglevel=loglevel+1)

  if not onlyAddr:
    log.add ('insert poi', level=loglevel, file=file_log)
    ershkus_poi.insertPOI(conn, loglevel=loglevel+1)

  if conf.sphinx_reindex:
    log.add ('update sphinx index', level=loglevel, file=file_log)
    cmdrun(cmd=conf.cmdindexrotate, errorText='! error update sphinx index', loglevel=loglevel)
    log.add ('update sphinx index complite', level=loglevel, file=file_log)


def update(loglevel=0, noLoad=False, onlyAddr=False):

  if not noLoad:
    load(update = True, loglevel = loglevel+1)

  conn = psycopg2.connect(host=conf.addrfull_host, database=conf.addrfull_database, user=conf.addrfull_user, password=conf.addrfull_password)

  log.add ('update addr', level=loglevel, file=file_log)
  ershkus_addr.updateAddr(conn, loglevel=loglevel+1)

  if not onlyAddr:
    log.add ('update poi', level=loglevel, file=file_log)
    ershkus_poi.updatePOI(conn, loglevel=loglevel+1)

  if conf.sphinx_reindex:
    log.add ('update sphinx index', level=loglevel, file=file_log)
    cmdrun(cmd=conf.cmdindexrotate, errorText='! error update sphinx index', loglevel=loglevel)
    log.add ('update sphinx index complite', level=loglevel, file=file_log)


def testStep(loglevel=0):
  fileLog = 'test.log'
  conn = psycopg2.connect(host=conf.addrfull_host, database=conf.addrfull_database, user=conf.addrfull_user, password=conf.addrfull_password)
  cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
  cur.execute("""SELECT min(id) as min, max(id) as max FROM ershkus_search_addr;""")
  limit = cur.fetchone()
  
  
  limit['max'] = limit['min']+300000
  step = 300000

  log.add ('start round, step='+str(step)+'  range='+str(limit['max']-limit['min']), level=loglevel, file=fileLog) # # # # #
  for n in range(limit['min'], limit['max'], step):
    if limit['min'] >= limit['max']:
      break
      
    log.add ('step = '+str(n)+' / '+str(int((float(n)-limit['min'])/(limit['max']-limit['min'])*100))+'%', level=loglevel+1, file=fileLog)
    
    cur = conn.cursor()
    cur.execute("""
      UPDATE ershkus_search_addr AS search1
        SET
          country=search2.country, country_id=search2.country_id,
          region=search2.region, region_id=search2.region_id,
          district=search2.district, district_id=search2.district_id,
          city=search2.city, city_id=search2.id
        FROM ershkus_search_addr AS search2
        WHERE ((search2.geom && search1.geom) AND ST_Covers(search2.geom, search1.geom))
          AND search2.city is not null
          --AND search1.city is null
          --AND (search1.street is not null or search1.housenumber is not null)
          AND search2.addr_type_id = 20 --city
          AND (ST_IsValid(search2.geom))
          AND (ST_IsValid(search1.geom))
          AND (search1.id>=%(min)s AND search1.id<%(max)s)
          AND (search1.member_role = 'outer' OR search1.member_role is null);
    """, {'min': n, 'max': n+step})
      
  conn.rollback()
  log.add ('end round', level=loglevel, file=fileLog) # # # # #
      
      

def main():
  parser = argparse.ArgumentParser(add_help=True, version='0.1')
  parser.add_argument("--noLoad",action="store_true", help="Не загружать файлы и не грузить osmosis-ом",default=False)
  parser.add_argument("--onlyAddr",action="store_true", help="Обработка только адресной информации",default=False)
  parser.add_argument("--onlyPOI",action="store_true", help="Обработка только POI",default=False)
  parser.add_argument("--isAuto",action="store_true", help="Проверка предыдущего завершения, отказ если были ошибки",default=False)
  parser.add_argument('action', metavar='action', type=str, choices=['insert', 'update', 'load-insert', 'load-update', 'install', 'test'], help='action operations `insert` or `update` or `load-insert` or `load-update` or `install`')
  args = parser.parse_args()

  try:
    log.add ('start main', file=file_log)
    
    if args.isAuto:
      controlAuto(loglevel=1)
    
    if args.action == 'insert':
      insert(loglevel = 1, noLoad=args.noLoad, onlyAddr=args.onlyAddr, onlyPOI=args.onlyPOI)
    elif args.action == 'update':
      update(loglevel = 1, noLoad=args.noLoad, onlyAddr=args.onlyAddr)
    elif args.action == 'load-insert':
      load(update = False, loglevel = 1)
    elif args.action == 'load-update':
      load(update = True, loglevel = 1)
    elif args.action == 'install':
      install(loglevel = 1)
    elif args.action == 'test':
      print 'test'
      testStep(loglevel = 1)
    
    controlAuto(isEnd=True, loglevel=1)
    
    log.add ('end main', file=file_log, finish=True)
  except :
    controlAuto(isEnd=True, isError=True, loglevel=1)
    log.add ('! error: '+str(traceback.format_exc()), file=file_log)


if __name__ == '__main__':
  main()