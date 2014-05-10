#!/usr/bin/env python
# -*- coding: utf8 -*-

import argparse
import datetime
import ConfigParser
import psycopg2
import traceback
import os
import urllib
import urllib2
import subprocess
import stapio_addr
import stapio_poi
import stapio_config as conf
import logger as log


file_log = 'stapio_run.log'


def do_query_commit_and_print_result(connection, text, query):
  print '%s...  ' % text,
  cursor = connection.cursor()
  cursor.execute(query)
  connection.commit()
  print 'done'


def create_index(connection, table, column, using=None):
  index_name = table + '_' + column + '_idx'
  text = "creating index '%s'" % index_name
  substitute = {'table': table, 'index_name': index_name, 'using': using}
  query = "DO $$ BEGIN IF NOT EXISTS ( " \
          "SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " \
          "WHERE  c.relname = '{index_name}' AND n.nspname = 'public' ) THEN " \
          "CREATE INDEX {index_name} ON {table}"
  if using:
    query += " USING {using}"
  query += "; END IF; END$$;"

  do_query_commit_and_print_result(connection, text, query.format(**substitute))


def create_function(connection, function_name, parameter, function_body):
  substitute = {'function_name': function_name, 'parameter': parameter, 'function_body': function_body}
  query = "CREATE OR REPLACE FUNCTION {function_name}({parameter}) " \
          "RETURNS geometry AS " \
          "$BODY$ " \
          "BEGIN " \
          "RETURN ({function_body}); " \
          "EXCEPTION " \
          "WHEN SQLSTATE 'XX000' THEN " \
          "RETURN null; " \
          "WHEN SQLSTATE '21000' THEN " \
          "RETURN null; " \
          "END; " \
          "$BODY$ " \
          "LANGUAGE plpgsql VOLATILE " \
          "COST 100; " \
          "GRANT EXECUTE ON FUNCTION {function_name}({parameter}) TO public"
  do_query_commit_and_print_result(connection, "creating function '%s'" % function_name, query.format(**substitute))


def add_column_updated_at(connection, table_name):
  query = "DO $$ " \
          "BEGIN " \
          "BEGIN " \
          "ALTER TABLE %s ADD COLUMN updated_at TIMESTAMP WITHOUT TIME ZONE; " \
          "EXCEPTION " \
          "WHEN duplicate_column THEN RETURN; " \
          "END; " \
          "END; " \
          "$$" % table_name
  do_query_commit_and_print_result(connection, "altering table '%s'" % table_name, query)


def install():
  import getpass
  from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

  print('')
  print('')
  print('- - - INSTALL - - -')
  print('')

  try:

    db_host = conf.db_host
    db_name = conf.db_name

    default_db_superuser = 'postgres'

    db_superuser = raw_input('insert db user (access to create tables and users) [%s]: ' % default_db_superuser)
    db_superuser_password = getpass.getpass()

    if not db_superuser:
      db_superuser = default_db_superuser

    db_user = conf.db_user
    db_password = conf.db_password

    print 'connect...  ',
    try:
      conn = psycopg2.connect(host=db_host, database='postgres',
                              user=db_superuser, password=db_superuser_password)
    except:
      print 'Connection error'
      return
    print 'done'

    print 'is user exists...  ',
    cur = conn.cursor()
    cur.execute("SELECT usename FROM pg_shadow WHERE usename = '%s'" % db_user)
    if not cur.fetchone():
      print 'not_exists'
      query = "CREATE ROLE %s LOGIN " \
              "PASSWORD '%s' " \
              "NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION" \
              % (db_user, db_password)
      do_query_commit_and_print_result(conn, "creating '%s' user" % db_user, query)
    else:
      print 'exists'

    print 'check if DB is exists...  ',
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = '%s'" % db_name)
    if not cur.fetchone():
      print 'not exists'

      conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

      query = "CREATE DATABASE %s OWNER %s" % (db_name, db_user)
      do_query_commit_and_print_result(conn, 'trying create DB', query)

    else:
      print 'exists'

    print 'reconnect to DB...  ',
    conn = psycopg2.connect(host=db_host, database=db_name,
                            user=db_superuser, password=db_superuser_password)
    print 'done'

    do_query_commit_and_print_result(conn, 'creating extension postgis', "CREATE EXTENSION IF NOT EXISTS postgis")
    do_query_commit_and_print_result(conn, 'creating extension hstore', "CREATE EXTENSION IF NOT EXISTS hstore")

    print 'check install pgsnapshot_schema_0.6...  ',
    cur = conn.cursor()
    cur.execute("SELECT * FROM pg_tables WHERE tablename='ways'")

    if not cur.fetchone():
      print 'not installed'

      pgsnapshot_root = None
      schema_file = 'pgsnapshot_schema_0.6.sql'
      schema_linestring_file = 'pgsnapshot_schema_0.6_linestring.sql'

      for root, subFolders, files in os.walk('/usr/share'):
        for _file in files:
          if _file in [schema_file, schema_linestring_file]:
            pgsnapshot_root = root
            break
        if pgsnapshot_root:
          break

      if not pgsnapshot_root:
        print "cannot find pg snapshot schema. Please install package 'osmosis'"
        return

      print 'trying to load schema files...  '
      conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
      cur.execute(open(pgsnapshot_root + '/' + schema_file, 'r').read())
      cur.execute(open(pgsnapshot_root + '/' + schema_linestring_file, 'r').read())

      for tbl in ['relations', 'relation_members', 'ways', 'way_nodes', 'nodes', 'users', 'schema_info']:
        query = "ALTER TABLE %s OWNER TO %s" % (tbl, db_user)
        do_query_commit_and_print_result(conn, '  set table\'s owner', query)

    conn = psycopg2.connect(host=db_host, database=db_name,
                            user=db_user, password=db_password)

    cur.execute("SELECT * FROM pg_tables WHERE tablename='way_tags'")
    if cur.fetchone():
      print 'error, it`s pgsimple_schema_0.6'
      return
    print 'done'

    table = conf.addr_table

    substitute = {'table': table}
    query = "CREATE TABLE IF NOT EXISTS {table} ( " \
            "id bigserial NOT NULL, " \
            "id_link_n bigint[], " \
            "id_link_w bigint[], " \
            "id_link_r bigint[], " \
            "full_name text, " \
            "region text, " \
            "region_id bigint, " \
            "district text, " \
            "district_id bigint, " \
            "city text, " \
            "city_id bigint, " \
            "suburb text, " \
            "village text, " \
            "village_id bigint, " \
            "street text, " \
            "housenumber text, " \
            "member_role text, " \
            "addr_type text, " \
            "index_name text, " \
            "addr_type_id integer, " \
            "geom geometry, " \
            "c_geom geometry, " \
            "modify boolean NOT NULL DEFAULT false, " \
            "osm_id text[], " \
            "postcode text, " \
            "country text, " \
            "country_id bigint, " \
            "street_id bigint, " \
            "name text, " \
            "CONSTRAINT pk_{table} PRIMARY KEY (id ), " \
            "CONSTRAINT enforce_dims_c_geom CHECK (st_ndims(c_geom) = 2), " \
            "CONSTRAINT enforce_dims_geom CHECK (st_ndims(geom) = 2), " \
            "CONSTRAINT enforce_geotype_c_geom CHECK (geometrytype(c_geom) = 'POINT'::text OR c_geom IS NULL), " \
            "CONSTRAINT enforce_srid_c_geom CHECK (st_srid(c_geom) = 4326), " \
            "CONSTRAINT enforce_srid_geom CHECK (st_srid(geom) = 4326) ) " \
            "WITH ( OIDS=FALSE )"

    do_query_commit_and_print_result(conn, "creating '%s' table" % table, query.format(**substitute))

    column = 'addr_type_id'
    create_index(conn, table, column, 'btree (' + column + ')')
    column = 'geom'
    create_index(conn, table, column, 'gist (' + column + ')')
    column = 'osm_id'
    create_index(conn, table, column, 'gin (%s COLLATE pg_catalog."default")' % column)

    table = conf.addr_p_table
    substitute = {'table': table}
    query = "CREATE TABLE IF NOT EXISTS {table} ( " \
            "id bigserial NOT NULL, " \
            "id_link_n bigint[], " \
            "id_link_w bigint[], " \
            "id_link_r bigint[], " \
            "full_name text, " \
            "region text, " \
            "region_id bigint, " \
            "district text, " \
            "district_id bigint, " \
            "city text, " \
            "city_id bigint, " \
            "suburb text, " \
            "village text, " \
            "village_id bigint, " \
            "street text, " \
            "housenumber text, " \
            "member_role text, " \
            "addr_type text, " \
            "addr_type_id integer, " \
            "index_name text, " \
            "geom geometry, " \
            "c_geom geometry, " \
            "osm_id text[], " \
            "postcode text, " \
            "country text, " \
            "country_id bigint, " \
            "name text, " \
            "CONSTRAINT pk_{table} PRIMARY KEY (id), " \
            "CONSTRAINT enforce_dims_c_geom CHECK (st_ndims(c_geom) = 2), " \
            "CONSTRAINT enforce_dims_geom CHECK (st_ndims(geom) = 2), " \
            "CONSTRAINT enforce_geotype_c_geom CHECK (geometrytype(c_geom) = 'POINT'::text OR c_geom IS NULL), " \
            "CONSTRAINT enforce_srid_c_geom CHECK (st_srid(c_geom) = 4326), " \
            "CONSTRAINT enforce_srid_geom CHECK (st_srid(geom) = 4326) ) " \
            "WITH ( OIDS=FALSE )"

    do_query_commit_and_print_result(conn, "creating '%s' table" % table, query.format(**substitute))

    column = 'addr_type_id'
    create_index(conn, table, column, 'btree (' + column + ')')
    column = 'geom'
    create_index(conn, table, column, 'gist (' + column + ')')

    table = conf.poi_table
    substitute = {'table': table}
    query = "CREATE TABLE IF NOT EXISTS {table} ( " \
            "id bigserial NOT NULL, " \
            "class text NOT NULL, " \
            "tags hstore, " \
            "name_ru text DEFAULT ''::text, " \
            "operator text, " \
            "tags_ru hstore, " \
            "opening_hours text, " \
            "class_ru text, " \
            "addr_region text, " \
            "addr_district text, " \
            "addr_city text, " \
            "addr_full_name text, " \
            "index_name text, " \
            "addr_region_id bigint, " \
            "addr_district_id bigint, " \
            "addr_city_id bigint, " \
            "addr_house_id bigint, " \
            "addr_in_id bigint, " \
            "addr_in_type integer, " \
            "brand text, " \
            "phone text, " \
            "fax text, " \
            "website text, " \
            "addr_village text, " \
            "addr_street text, " \
            "addr_house text, " \
            "c_geom geometry, " \
            "addr_country text, " \
            "email text, " \
            "description text, " \
            "wikipedia text, " \
            "osm_id text, " \
            "CONSTRAINT pk_{table} PRIMARY KEY (id), " \
            "CONSTRAINT enforce_dims_c_geom CHECK (st_ndims(c_geom) = 2), " \
            "CONSTRAINT enforce_geotype_c_geom CHECK (geometrytype(c_geom) = 'POINT'::text OR c_geom IS NULL), " \
            "CONSTRAINT enforce_srid_c_geom CHECK (st_srid(c_geom) = 4326) ) " \
            "WITH ( OIDS=FALSE )"
    do_query_commit_and_print_result(conn, "creating '%s' table" % table, query.format(**substitute))

    column = 'addr_in_type'
    create_index(conn, table, column, 'btree (%s)' % column)

    column = 'osm_id'
    create_index(conn, table, column, 'btree (%s COLLATE pg_catalog."default", id)' % column)

    table = conf.addr_upd_table
    substitute = {'table': table}
    query = "CREATE TABLE IF NOT EXISTS {table} ( " \
            "id bigserial NOT NULL, " \
            "osm_id text, " \
            "street_name text, " \
            "country text, " \
            "country_id bigint, " \
            "region text, " \
            "region_id bigint, " \
            "district text, " \
            "district_id bigint, " \
            "city text, " \
            "city_id bigint, " \
            "suburb text, " \
            "village text, " \
            "village_id bigint, " \
            "street text, " \
            "postcode text, " \
            "geom geometry, " \
            "CONSTRAINT pk_{table} PRIMARY KEY (id), " \
            "CONSTRAINT enforce_dims_geom CHECK (st_ndims(geom) = 2), " \
            "CONSTRAINT enforce_srid_geom CHECK (st_srid(geom) = 4326) ) " \
            "WITH ( OIDS=FALSE )"
    do_query_commit_and_print_result(conn, "creating '%s' table" % table, query.format(**substitute))

    table = conf.deleted_entries_table
    substitute = {'table': table}
    query = "CREATE TABLE IF NOT EXISTS {table} ( " \
            "id bigserial NOT NULL, " \
            "type character(1) NOT NULL, " \
            "osm_id bigint NOT NULL, " \
            "deleted_at timestamp without time zone DEFAULT now(), " \
            "CONSTRAINT pk_{table} PRIMARY KEY (id ) ) " \
            "WITH ( OIDS=FALSE )"
    do_query_commit_and_print_result(conn, "creating '%s' table" % table, query.format(**substitute))

    column = 'deleted_at'
    create_index(conn, table, column, 'btree (%s, id)' % column)

    create_function(conn, 'stapio_fn_buildarea', 'geometry', 'SELECT ST_BuildArea($1)')
    create_function(conn, 'stapio_fn_geompoly', 'geometry[]',
                    "SELECT ST_BuildArea(ST_GeomFromText(ST_AsText(ST_Collect(c1)),4326)) as c2 "
                    "FROM (SELECT unnest($1) as c1) as t1")

    add_column_updated_at(conn, 'ways')
    add_column_updated_at(conn, 'relations')
  except:
    print ""
    print ""
    print "  ! ! ! INSTALL ERROR ! ! !"
    print ""
    raise


def cmd_run(cmd, error_text='', log_level=0):
  print '!>> ' + cmd
  p1 = subprocess.Popen(cmd, shell=True)
  return_code = p1.wait()
  if not return_code == 0:
    log.add(text=error_text, level=log_level, file=file_log)
    raise Exception('return code: ' + str(return_code) + ', text error: ' + str(error_text))


def load(to_update, log_level=0):
  log.add(('load start (to_update=%s)' % to_update), level=log_level, file=file_log)

  file_info = {'temp': conf.tempdir, 'authFileOsmosis': conf.authFileOsmosis}

  if not os.path.exists('data'):
    os.mkdir('data')

  i = 0

  if to_update:
    file_info['name_d'] = conf.workdir + '/data/load%s.osc.gz'
    file_info['name_e'] = conf.workdir + '/data/load%se.osc'
    file_info['url_list'] = conf.urlmaskosc
    file_info['osmosis_read'] = 'read-xml-change'
    file_info['osmosis_merge'] = 'merge-change --sort-change'
    file_info['osmosis_write'] = 'write-xml-change'
    file_info['osmosis_write_db'] = 'write-pgsql-change'

    # загрузим предыдущую позицию
    f = open(conf.workactual + 'upd_date.dat', 'r')
    file_info['date_s'] = datetime.datetime.strptime(f.readline(), conf.format_datetime)
    f.close()

    file_info['date_e'] = file_info['date_s'] + datetime.timedelta(days=1)
    file_info['day_start'] = file_info['date_s'].strftime("%y%m%d")
    file_info['day_end'] = file_info['date_e'].strftime("%y%m%d")
  else:
    file_info['name_d'] = conf.workdir + '/data/load%s.pbf'
    file_info['name_e'] = conf.workdir + '/data/load%se.pbf'
    file_info['url_list'] = conf.urlpbf
    file_info['osmosis_read'] = 'read-pbf'
    file_info['osmosis_merge'] = 'merge --sort'
    file_info['osmosis_write'] = 'write-pbf'
    file_info['osmosis_write_db'] = 'write-pgsql'

  info = {'load': False, 'next_load': True}

  if not to_update:
    urllib.urlretrieve(conf.urlpbfmeta, conf.workdir + "/data/load.pbf.meta")
  while info['next_load']:
    if to_update:
      log.add('load date at ' + file_info['date_s'].strftime(conf.format_datetime),
              level=log_level,
              file=file_log)
    for url_file in file_info['url_list']:
      i += 1
      file_info['end'] = (file_info['name_e'] % i)
      log.add(('load, i=%s' % i), level=log_level + 1, file=file_log)
      file_info['now'] = (file_info['name_d'] % i)
      url_file = url_file % file_info
      try:
        urllib2.urlopen(url_file)
      except urllib2.HTTPError, e:
        if e.code == 404:
          info['next_load'] = False
          file_info['date_e'] = file_info['date_s'] - datetime.timedelta(days=1)
          break
        log.add(('! error download (code=%s)' % e.code), level=log_level + 1, file=file_log)
        raise e
      print url_file
      urllib.urlretrieve(url_file, file_info['now'])
      if to_update:
        log.add('decompress', level=log_level + 1, file=file_log)
        cmd_run(cmd=('gzip -df ' + file_info['now']), error_text=('! error decompress, i=%s' % i),
                log_level=log_level + 1)
        file_info['now'] = file_info['now'][:-3]
      if i == 1:
        file_info['in'] = file_info['now']
        continue
      log.add(('merge, i=%s' % i), level=log_level + 1, file=file_log)
      file_info['n'] = file_info['now']
      cmd = 'osmosis -quiet --%(osmosis_read)s file=%(in)s '
      cmd += '--%(osmosis_read)s file=%(n)s  --%(osmosis_merge)s '
      cmd += '--%(osmosis_write)s file=%(end)s omitmetadata=true'
      cmd = cmd % file_info
      #print cmd  #  #  #  #  #  #  #  #  #  #  #  #  #  #
      cmd_run(cmd, error_text=('! error merge, i=%s' % i), log_level=log_level + 1)

      file_info['in'] = file_info['end']

    if info['next_load']:
      info['load'] = True
    if to_update:
      file_info['date_s'] = file_info['date_e']
      file_info['date_e'] = file_info['date_s'] + datetime.timedelta(days=1)
      file_info['day_start'] = file_info['date_s'].strftime("%y%m%d")
      file_info['day_end'] = file_info['date_e'].strftime("%y%m%d")
    else:
      info['next_load'] = False

  if not info['load']:
    raise Exception('no load from pbf/osc')

  conn = psycopg2.connect(host=conf.addrfull_host, database=conf.addrfull_database, user=conf.addrfull_user,
                          password=conf.addrfull_password)
  if not to_update:
    pbf_meta = ConfigParser.RawConfigParser()
    pbf_meta.read(conf.workdir + '/data/load.pbf.meta')
    file_info['date_e'] = datetime.datetime.strptime(pbf_meta.get('DEFAULT', 'version'), '%Y-%m-%d %H:%M:%S')
    log.add('pbf at ' + file_info['date_e'].strftime(conf.format_datetime), level=log_level, file=file_log)
    log.add('clear db', level=log_level, file=file_log)
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

  log.add('load in db', level=log_level, file=file_log)
  cmd = 'osmosis -quiet --%(osmosis_read)s file=%(in)s '
  cmd += '--%(osmosis_write_db)s authFile=%(authFileOsmosis)s'
  cmd = cmd % file_info
  cmd_run(cmd, error_text='! error load in db', log_level=log_level)

  # сохраним текущую позицию
  log.add('save date', level=log_level, file=file_log)
  f = open(conf.workactual + 'upd_date.dat', 'w')
  f.write(file_info['date_e'].strftime(conf.format_datetime))
  f.close()

  log.add('load complete', level=log_level, file=file_log)


def control_auto(is_end=False, is_error=False, log_level=0):
  if is_end:
    if not is_error:
      if os.path.exists(conf.workactual + 'work.dat'):
        os.remove(conf.workactual + 'work.dat')

    data_dir = conf.workdir + '/data'
    if data_dir[-1] == os.sep:
      data_dir = data_dir[:-1]
    files = os.listdir(data_dir)
    for _file in files:
      if _file == '.' or _file == '..':
        continue
      os.remove(data_dir + os.sep + _file)

    if conf.runAfter:
      cmd_run(cmd=conf.runAfter, error_text='! error run after', log_level=log_level)

  else:
    if os.path.exists(conf.workactual + 'work.dat'):
      text_error = '! previous requests can not be completed or if an error, exists "work.dat"'
      log.add(text=text_error, level=log_level, file=file_log)
      raise Exception(text_error)

    file_log_d = open(conf.workactual + 'work.dat', 'w')
    file_log_d.write(str(datetime.datetime.now().strftime(conf.format_datetime)))
    file_log_d.close()


def insert(log_level=0, load_files=True, only_addr=False, only_poi=False):
  if load_files:
    load(to_update=False, log_level=log_level + 1)

  conn = psycopg2.connect(host=conf.addrfull_host, database=conf.addrfull_database, user=conf.addrfull_user,
                          password=conf.addrfull_password)

  if not only_poi:
    log.add('insert addr', level=log_level, file=file_log)
    stapio_addr.insert_addr(conn, log_level=log_level + 1)

  if not only_addr:
    log.add('insert poi', level=log_level, file=file_log)
    stapio_poi.insert_poi(conn, log_level=log_level + 1)

  if conf.sphinx_reindex:
    log.add('update sphinx index', level=log_level, file=file_log)
    cmd_run(cmd=conf.cmdindexrotate, error_text='! error update sphinx index', log_level=log_level)
    log.add('update sphinx index complete', level=log_level, file=file_log)


def update(log_level=0, load_files=True, only_addr=False):
  if load_files:
    load(to_update=True, log_level=log_level + 1)

  conn = psycopg2.connect(host=conf.addrfull_host, database=conf.addrfull_database, user=conf.addrfull_user,
                          password=conf.addrfull_password)

  log.add('update addr', level=log_level, file=file_log)
  stapio_addr.update_addr(conn, log_level=log_level + 1)

  if not only_addr:
    log.add('update poi', level=log_level, file=file_log)
    stapio_poi.update_poi(conn, log_level=log_level + 1)

  if conf.sphinx_reindex:
    log.add('update sphinx index', level=log_level, file=file_log)
    cmd_run(cmd=conf.cmdindexrotate, error_text='! error update sphinx index', log_level=log_level)
    log.add('update sphinx index complete', level=log_level, file=file_log)


def test_step(log_level=0):
  log_file = 'test.log'
  conn = psycopg2.connect(host=conf.addrfull_host, database=conf.addrfull_database, user=conf.addrfull_user,
                          password=conf.addrfull_password)
  cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
  cur.execute("""SELECT min(id) as min, max(id) as max FROM %(table)s;""", {'table': conf.addr_table})
  limit = cur.fetchone()

  limit['max'] = limit['min'] + 300000
  step = 300000

  log.add('start round, step=' + str(step) + '  range=' + str(limit['max'] - limit['min']), level=log_level,
          file=log_file)  # # # # #
  for n in range(limit['min'], limit['max'], step):
    if limit['min'] >= limit['max']:
      break

    log.add('step = ' + str(n) + ' / ' + str(
      int((float(n) - limit['min']) / (limit['max'] - limit['min']) * 100)) + '%',
            level=log_level + 1,
            file=log_file)

    cur = conn.cursor()
    cur.execute("""
      UPDATE %(table)s AS search1
        SET
          country=search2.country, country_id=search2.country_id,
          region=search2.region, region_id=search2.region_id,
          district=search2.district, district_id=search2.district_id,
          city=search2.city, city_id=search2.id
        FROM %(table)s AS search2
        WHERE ((search2.geom && search1.geom) AND ST_Covers(search2.geom, search1.geom))
          AND search2.city is not null
          --AND search1.city is null
          --AND (search1.street is not null or search1.housenumber is not null)
          AND search2.addr_type_id = 20 --city
          AND (ST_IsValid(search2.geom))
          AND (ST_IsValid(search1.geom))
          AND (search1.id>=%(min)s AND search1.id<%(max)s)
          AND (search1.member_role = 'outer' OR search1.member_role is null);
    """, {'table': conf.addr_table, 'min': n, 'max': n + step})

  conn.rollback()
  log.add('end round', level=log_level, file=log_file)  # # # # #


def main():
  parser = argparse.ArgumentParser(add_help=True, version='0.1')
  parser.add_argument("--load", action="store_true", help="Загружать файлы и грузить osmosis-ом",
                      default=False)
  parser.add_argument("--onlyAddr", action="store_true", help="Обработка только адресной информации", default=False)
  parser.add_argument("--onlyPOI", action="store_true", help="Обработка только POI", default=False)
  parser.add_argument("--isAuto", action="store_true", help="Проверка предыдущего завершения, отказ если были ошибки",
                      default=False)
  parser.add_argument('action', metavar='action', type=str,
                      choices=['insert', 'update', 'load-insert', 'load-update', 'install', 'test'],
                      help='action operations `insert` or `update` or `load-insert` or `load-update` or `install`')
  args = parser.parse_args()

  try:
    log.add('start main', file=file_log)

    if args.isAuto:
      control_auto(log_level=1)

    action = args.action

    if action == 'insert':
      insert(log_level=1, load_files=args.load, only_addr=args.onlyAddr, only_poi=args.onlyPOI)
    elif action == 'update':
      update(log_level=1, load_files=args.load, only_addr=args.onlyAddr)
    elif action == 'load-insert':
      load(to_update=False, log_level=1)
    elif action == 'load-update':
      load(to_update=True, log_level=1)
    elif action == 'install':
      install()
    elif action == 'test':
      print 'test'
      test_step(log_level=1)

    control_auto(is_end=True, log_level=1)

    log.add('end main', file=file_log, finish=True)
  except:
    control_auto(is_end=True, is_error=True, log_level=1)
    log.add('! error: ' + str(traceback.format_exc()), file=file_log)


if __name__ == '__main__':
  main()
