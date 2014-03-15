#!/usr/bin/env python
# -*- coding: utf8 -*-

import psycopg2
import getpass

def install(loglevel=0):
  
  
  p = {}
  
  print('')
  print('')
  print('- - - INSTALL - - -')
  print('')
  
  try:
    p['db_host'] = raw_input('insert db host: ')
    p['db_database'] = raw_input('insert name database: ')
    p['db_user'] = raw_input('insert db user (access to create tables and users): ')
    p['db_password'] = getpass.getpass()
    print 'connect...  ',
    try:
      conn = psycopg2.connect(host=p['db_host'], database=p['db_database'], user=p['db_user'], password=p['db_password'])
    except:
      print 'error'
      return
    print 'done'
    
    print 'check install pgsnapshot_schema_0.6...  ',
    cur = conn.cursor()
    cur.execute("select * from pg_tables where tablename='ways';")
    if not cur.fetchone():
      print 'error'
      return
    cur.execute("select * from pg_tables where tablename='way_tags';")
    if cur.fetchone():
      print 'error, it`s pgsimple_schema_0.6'
      return
    print 'done'
    
    print 'create users...  ',
    cur = conn.cursor()
    cur.execute("SELECT usename FROM pg_shadow WHERE usename = 'ershkus';")
    if not cur.fetchone():
      cur.execute("""
        CREATE ROLE ershkus LOGIN
          ENCRYPTED PASSWORD 'md52d069f00700241a1c5448e888717dddf'
          NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
      """)
      conn.commit()
    cur = conn.cursor()
    cur.execute("SELECT usename FROM pg_shadow WHERE usename = 'ershkus_work';")
    if not cur.fetchone():
      cur.execute("""
        CREATE ROLE ershkus_work LOGIN
          ENCRYPTED PASSWORD 'md516a4587a0354db6e4eecca5426d520da'
          NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
      """)
      conn.commit()
    print 'done'
    
    
    print "create table...  ",
    cur = conn.cursor()
    cur.execute("select * from pg_tables where tablename='ershkus_search_addr';")
    if not cur.fetchone():
      cur.execute("""
        CREATE ROLE ershkus_work LOGIN
          ENCRYPTED PASSWORD 'md516a4587a0354db6e4eecca5426d520da'
          NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
      """)
      conn.commit()
    
    
    
  except:
    print ""
    print ""
    print "  ! ! ! INSTALL ERROR ! ! !"
    print ""
    raise


tab_Addr = """
  CREATE TABLE ershkus_search_addr
  (
    id bigserial NOT NULL,
    id_link_n bigint[],
    id_link_w bigint[],
    id_link_r bigint[],
    full_name text,
    region text,
    region_id bigint,
    district text,
    district_id bigint,
    city text,
    city_id bigint,
    village text,
    village_id bigint,
    street text,
    housenumber text,
    member_role text,
    addr_type text,
    index_name text,
    addr_type_id integer,
    geom geometry,
    c_geom geometry,
    modify boolean NOT NULL DEFAULT false,
    osm_id text[],
    postcode text,
    country text,
    country_id bigint,
    street_id bigint,
    name text,
    CONSTRAINT pk_ershkus_search_addr PRIMARY KEY (id),
    CONSTRAINT enforce_dims_c_geom CHECK (st_ndims(c_geom) = 2),
    CONSTRAINT enforce_dims_geom CHECK (st_ndims(geom) = 2),
    CONSTRAINT enforce_geotype_c_geom CHECK (geometrytype(c_geom) = 'POINT'::text OR c_geom IS NULL),
    CONSTRAINT enforce_srid_c_geom CHECK (st_srid(c_geom) = 4326),
    CONSTRAINT enforce_srid_geom CHECK (st_srid(geom) = 4326)
  )
  WITH (
    OIDS=FALSE
  );
  ALTER TABLE ershkus_search_addr
    OWNER TO ershkus;
  GRANT ALL ON TABLE ershkus_search_addr TO ershkus;
  GRANT SELECT, UPDATE ON TABLE ershkus_search_addr TO ershkus_work;

  -- Index: addr_type_id_idx

  -- DROP INDEX addr_type_id_idx;

  CREATE INDEX addr_type_id_idx
    ON ershkus_search_addr
    USING btree
    (addr_type_id);

  -- Index: ershkus_search_addr_geom_idx

  -- DROP INDEX ershkus_search_addr_geom_idx;

  CREATE INDEX ershkus_search_addr_geom_idx
    ON ershkus_search_addr
    USING gist
    (geom);

  -- Index: ershkus_search_addr_osm_id_idx

  -- DROP INDEX ershkus_search_addr_osm_id_idx;

  CREATE INDEX ershkus_search_addr_osm_id_idx
    ON ershkus_search_addr
    USING gin
    (osm_id COLLATE pg_catalog."default");
"""

