#!/usr/bin/env python
# -*- coding: utf8 -*-

import json, psycopg2, os, datetime, sys, traceback
import psycopg2.extras
import argparse
import codecs
import stapio_config as conf
import stapio_utils as utils
import logger as log

# addr_type_id:
  # 5-country
  # 10-region
  # 15-district
  # 20-city
  # 25-village
  # 30-street
  # 35-house
  # 40-houseadd
  # 70-poi
#   old
  # 1-country
  # 2-region
  # 3-district
  # 4-city
  # 5-village
  # 6-street
  # 7-housenumber	house

localy='ru'
fupd_time = 'upd_time_addr.json'
file_log = 'addr.log'

def insertAddrSave(conn, loglevel=0):
  log.add ('start insertAddrSave', level=loglevel, file=file_log)
  
  log.add ('clear ershkus_addr_save', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  cur.execute("TRUNCATE TABLE ershkus_addr_save;")
  conn.commit()
  
  log.add ('insert country', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  cur.execute("""
    INSERT INTO
      ershkus_addr_save(
      addr_type,
      addr_type_id,
      osm_id,
      full_name,
      country,
      name,
      geom)
    SELECT
      'country' as addr_type,
      5 as addr_type_id,
      t1.osm_id as osm_id,
      t1.country as full_name,
      t1.country as country,
      t1.country as name,
      t1.geom as geom
    FROM
        (SELECT
          Array[relations.id] AS id_link_R,
          Array['r' || relations.id] AS osm_id,
          coalesce(relations.tags->'name:ru',relations.tags->'name',relations.tags->'place_name') AS country,
          (((ershkus_fn_geompoly(array_agg(ways.linestring))))) as geom
        FROM
          relations,
          relation_members,
          ways
        WHERE
          relations.id=relation_members.relation_id
          AND relation_members.member_id=ways.id
          AND relations.tags @> 'admin_level=>2'
          AND (relations.tags @> 'type=>boundary' OR relations.tags @> 'type=>multipolygon')
          AND (relation_members.member_role = 'outer' OR relation_members.member_role = 'inner')
        GROUP BY
          id_link_R,
          osm_id,
          country) as t1
    WHERE
      t1.geom is not null;
  """)
  conn.commit()

  log.add ('insert region', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  cur.execute("""
    INSERT INTO
      ershkus_addr_save(
      addr_type,
      addr_type_id,
      osm_id,
      region,
      name,
      geom)
    SELECT
      'region' as addr_type,
      10 as addr_type_id,
      t1.osm_id as osm_id,
      t1.region as region,
      t1.region as name,
      t1.geom as geom
    FROM
        (SELECT
          Array[relations.id] AS id_link_R,
          Array['r' || relations.id] AS osm_id,
          coalesce(relations.tags->'name:ru',relations.tags->'name',relations.tags->'place_name') AS region,
          (((ST_BuildArea(ST_Collect(ways.linestring))))) as geom
        FROM
          relations,
          relation_members,
          ways
        WHERE
          relations.id=relation_members.relation_id
          AND relation_members.member_id=ways.id
          AND relations.tags @> 'admin_level=>4'
          AND (relations.tags @> 'type=>boundary' OR relations.tags @> 'type=>multipolygon')
          --AND relations.tags->'addr:country' = 'RU'
          AND (relation_members.member_role = 'outer' OR relation_members.member_role = 'inner')
        GROUP BY
          id_link_R,
          osm_id,
          region) as t1
    WHERE
      t1.geom is not null;
  """)
  conn.commit()
  
  
  log.add ('end insertAddrSave', level=loglevel, file=file_log)


def insertFromSimpleOSM(conn, whereTime={}, loglevel=0):
  log.add ('start insertFromSimpleOSM', level=loglevel, file=file_log)

  if whereTime == {}:
    wheretimeN=""
    wheretimeALL=""
  else:
    wheretimeN = "AND ((tstamp>=%(lupd)s AND tstamp<=%(nupd)s)"
    wheretimeALL = wheretimeN + "OR (updated_at>=%(lupd)s AND updated_at<=%(nupd)s))"
    wheretimeN = wheretimeN + ")"

  curWay = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
  curWay.execute("""SELECT min(id) as min, max(id) as max FROM ways;""")
  limitWay = curWay.fetchone()
  log.add ('limit way: '+str(limitWay), level=loglevel+1, file=file_log)

  curNode = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
  curNode.execute("""SELECT min(id) as min, max(id) as max FROM nodes;""")
  limitNode = curNode.fetchone()
  log.add ('limit node: '+str(limitNode), level=loglevel+1, file=file_log)

  if conf.use_addr_save:
    if whereTime == {}:
      log.add ('load from ershkus_addr_save', level=loglevel+1, file=file_log) # # # # #
      cur = conn.cursor()
      cur.execute("""
        INSERT INTO ershkus_search_addr(
                    osm_id, addr_type, addr_type_id, full_name, country, country_id,
                    region, region_id, geom, name)
        SELECT osm_id, addr_type, addr_type_id, full_name, country, country_id, 
               region, region_id, geom, name
          FROM ershkus_addr_save;
      """)
      conn.commit()

  else:
    if conf.use_country:
      log.add ('insert country', level=loglevel+1, file=file_log) # # # # #
      cur = conn.cursor()
      cur.execute("""
        INSERT INTO
          ershkus_search_addr(
          addr_type,
          addr_type_id,
          id_link_R,
          osm_id,
          full_name,
          country,
          name,
          member_role,
          geom)
        SELECT
          'country' as addr_type,
          5 as addr_type_id,
          t1.id_link_R as id_link_R,
          t1.osm_id || t2.osm_id as osm_id,
          t1.country as full_name,
          t1.country as country,
          t1.country as name,
          t1.member_role as member_role,
          (CASE (t2.geom is null)
            WHEN true THEN t1.geom
            ELSE ST_Difference(t1.geom,t2.geom)
            END) as geom

        FROM
            (SELECT
              Array[relations.id] AS id_link_R,
              Array['r' || relations.id] AS osm_id,
              coalesce(relations.tags->'name:ru',relations.tags->'name',relations.tags->'place_name') AS country,
              relation_members.member_role as member_role,
              (((ershkus_fn_geompoly(array_agg(ways.linestring))))) as geom
            FROM
              relations,
              relation_members,
              ways
            WHERE
              relations.id=relation_members.relation_id
              AND relation_members.member_id=ways.id
              AND relations.tags @> 'admin_level=>2'
              AND (relations.tags @> 'type=>boundary' OR relations.tags @> 'type=>multipolygon')
              --AND relations.tags->'addr:country' = 'RU'
              AND relation_members.member_role = 'outer'
              """+wheretimeALL.replace('tstamp', 'relations.tstamp').replace('updated_at','relations.updated_at')+"""
            GROUP BY
              id_link_R,
              osm_id,
              country,
              member_role) as t1
            LEFT JOIN
              (SELECT
                Array[relations.id] AS id_link_R,
                Array['r' || relations.id] AS osm_id,
                (((ershkus_fn_geompoly(array_agg(ways.linestring))))) as geom
              FROM
                relations,
                relation_members,
                ways
              WHERE
                relations.id=relation_members.relation_id
                AND relation_members.member_id=ways.id
                AND relations.tags @> 'admin_level=>2'
                AND (relations.tags @> 'type=>boundary' OR relations.tags @> 'type=>multipolygon')
                --AND relations.tags->'addr:country' = 'RU'
                AND relation_members.member_role = 'inner'
              GROUP BY
                id_link_R,
                osm_id
                ) as t2
            ON (t1.id_link_R=t2.id_link_R)
        WHERE
          t1.geom is not null;
      """, whereTime)
      conn.commit()

    log.add ('insert region', level=loglevel+1, file=file_log) # # # # #
    cur = conn.cursor()
    cur.execute("""
      INSERT INTO
        ershkus_search_addr(
        addr_type,
        addr_type_id,
        id_link_R,
        osm_id,
        region,
        name,
        member_role,
        geom)
      SELECT
        'region' as addr_type,
        10 as addr_type_id,
        t1.id_link_R as id_link_R,
        t1.osm_id as osm_id,
        t1.region as region,
        t1.region as name,
        t1.member_role as member_role,
        t1.geom as geom

      FROM
          (SELECT
            Array[relations.id] AS id_link_R,
            Array['r' || relations.id] AS osm_id,
            coalesce(relations.tags->'name:ru',relations.tags->'name',relations.tags->'place_name') AS region,
            relation_members.member_role as member_role,
            (((ST_BuildArea(ST_Collect(ways.linestring))))) as geom
          FROM
            relations,
            relation_members,
            ways
          WHERE
            relations.id=relation_members.relation_id
            AND relation_members.member_id=ways.id
            AND relations.tags @> 'admin_level=>4'
            AND (relations.tags @> 'type=>boundary' OR relations.tags @> 'type=>multipolygon')
            --AND relations.tags->'addr:country' = 'RU'
            AND (relation_members.member_role = 'outer' OR relation_members.member_role = 'inner')
            """+wheretimeALL.replace('tstamp', 'relations.tstamp').replace('updated_at','relations.updated_at')+"""
          GROUP BY
            id_link_R,
            osm_id,
            region,
            member_role) as t1
      WHERE
        t1.geom is not null;
    """, whereTime)
    conn.commit()

  log.add ('insert district', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  cur.execute("""
    INSERT INTO
      ershkus_search_addr(
      addr_type,
      addr_type_id,
      id_link_R,
      osm_id,
      district,
      name,
      member_role,
      geom)
    SELECT
      'district' as addr_type,
      15 as addr_type_id,
      t1.id_link_R as id_link_R,
      t1.osm_id as osm_id,
      t1.district as district,
      t1.district as name,
      t1.member_role as member_role,
      t1.geom as geom
    FROM
        (SELECT
          Array[relations.id] AS id_link_R,
          Array['r' || relations.id] AS osm_id,
          coalesce(relations.tags->'name:ru',relations.tags->'name',relations.tags->'place_name') AS district,
          relation_members.member_role as member_role,
          (((ST_BuildArea(ST_Collect(ways.linestring))))) as geom
        FROM
          relations,
          relation_members,
          ways
        WHERE
          relations.id=relation_members.relation_id
          AND relation_members.member_id=ways.id
          AND relations.tags @> 'admin_level=>6'
          AND (relations.tags @> 'type=>boundary' OR relations.tags @> 'type=>multipolygon')
          AND (relation_members.member_role = 'outer' OR relation_members.member_role = 'inner')
          """+wheretimeALL.replace('tstamp', 'relations.tstamp').replace('updated_at','relations.updated_at')+"""
        GROUP BY
          id_link_R,
          osm_id,
          district,
          member_role) as t1
    WHERE
      t1.geom is not null;
  """, whereTime)
  conn.commit()

  log.add ('insert city', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  log.add ('relations', level=loglevel+2, file=file_log)
  cur.execute("""
    INSERT INTO
      ershkus_search_addr(
      addr_type,
      addr_type_id,
      id_link_R,
      id_link_N,
      osm_id,
      city,
      name,
      postcode,
      member_role,
      geom,
      c_geom)
    SELECT
      'city' as addr_type,
      20 as addr_type_id,
      Array[t1.id_link_R] as id_link_R,
      Array[t3.id_n] as id_link_N,
      t1.osm_id || t3.osm_id as osm_id,
      t1.city AS city,
      t1.city AS name,
      coalesce(t1.postcode, t3.postcode) as postcode,
      t1.member_role as member_role,
      t1.geom as geom,
      t3.c_geom
    FROM
      (SELECT
        relations.id AS id_link_R,
        Array['r' || relations.id] AS osm_id,
        coalesce(relations.tags->'full_name:ru',relations.tags->'full_name',relations.tags->'name:ru',relations.tags->'name',relations.tags->'place_name') AS city,
        relations.tags->'addr:postcode' as postcode,
        relation_members.member_role as member_role,
        (((ST_BuildArea(ST_Collect(ways.linestring))))) as geom
      FROM
        relations,
        relation_members,
        ways
      WHERE
        relations.id=relation_members.relation_id
        AND relation_members.member_id=ways.id
        AND (relations.tags @> 'place=>town' OR relations.tags @> 'place=>city')
        AND ways.linestring is not null
        AND (relation_members.member_role = 'outer' OR relation_members.member_role = 'inner')
        """+wheretimeALL.replace('tstamp', 'relations.tstamp').replace('updated_at','relations.updated_at')+"""
      GROUP BY
        id_link_R,
        osm_id,
        city,
        postcode,
        member_role) as t1
      LEFT JOIN
        (SELECT
          nodes.id as id_n,
          Array['n' || nodes.id] as osm_id,
          nodes.tags->'addr:postcode' as postcode,
          nodes.geom as c_geom
        FROM
          nodes
        WHERE
          (nodes.tags @> 'place=>town' OR nodes.tags @> 'place=>city')
          ) as t3
        ON ((t1.geom && t3.c_geom) AND ST_Covers(t1.geom, t3.c_geom));
  """, whereTime)
  log.add ('ways', level=loglevel+2, file=file_log)
  cur.execute("""
    INSERT INTO
      ershkus_search_addr(
      addr_type,
      addr_type_id,
      id_link_W,
      osm_id,
      city,
      name,
      postcode,
      geom,
      id_link_N,
      c_geom)
    SELECT
      addr_type,
      addr_type_id,
      id_link_W,
      t1.osm_id || t3.osm_id as osm_id,
      city,
      city AS name,
      coalesce(t1.postcode,t3.postcode) AS postcode,
      geom,
      id_link_N,
      c_geom
    FROM
      (SELECT
        'city' as addr_type,
        20 as addr_type_id,
        Array[ways.id] AS id_link_W,
        Array['w' || ways.id] AS osm_id,
        coalesce(ways.tags->'full_name:ru',ways.tags->'full_name',ways.tags->'name:ru',ways.tags->'name',ways.tags->'place_name') AS city,
        ways.tags->'addr:postcode' as postcode,
        (((ST_BuildArea(ST_Collect(ways.linestring))))) as geom
      FROM
        ways
      WHERE
        (ways.tags @> 'place=>town' OR ways.tags @> 'place=>city')
        AND ways.linestring is not null
        AND (coalesce(ways.tags->'full_name:ru',ways.tags->'full_name',ways.tags->'name',ways.tags->'place_name')) is not null
        """+wheretimeALL.replace('tstamp', 'ways.tstamp').replace('updated_at','ways.updated_at')+"""
      GROUP BY
        id_link_W,
        osm_id,
        city,
        postcode) as t1
      LEFT JOIN
        (SELECT
          Array[nodes.id] as id_link_N,
          Array['n' || nodes.id] as osm_id,
          coalesce(nodes.tags->'full_name:ru',nodes.tags->'full_name',nodes.tags->'name:ru',nodes.tags->'name', nodes.tags->'place_name') AS city_n,
          nodes.tags->'addr:postcode' as postcode,
          nodes.geom as c_geom
        FROM
          nodes
        WHERE
          (nodes.tags @> 'place=>town' OR nodes.tags @> 'place=>city')
          ) as t3
        ON ((t1.geom && t3.c_geom) AND ST_Covers(t1.geom, t3.c_geom) AND t1.city=t3.city_n);
  """, whereTime)
  log.add ('nodes', level=loglevel+2, file=file_log)
  cur.execute("""
    INSERT INTO
      ershkus_search_addr(
      addr_type,
      addr_type_id,
      id_link_N,
      osm_id,
      city,
      name,
      postcode,
      geom)
    SELECT
      'city' as addr_type,
      20 as addr_type_id,
      Array[nodes.id] AS id_link_N,
      Array['n' || nodes.id] AS osm_id,
      coalesce(nodes.tags->'full_name:ru',nodes.tags->'full_name',nodes.tags->'name:ru',nodes.tags->'name', nodes.tags->'place_name') AS city,
      coalesce(nodes.tags->'full_name:ru',nodes.tags->'full_name',nodes.tags->'name:ru',nodes.tags->'name', nodes.tags->'place_name') AS name,
      nodes.tags->'addr:postcode' as postcode,
      nodes.geom as geom
    FROM
      nodes
    WHERE
      (nodes.tags @> 'place=>town' OR nodes.tags @> 'place=>city')
      AND (coalesce(nodes.tags->'full_name:ru',nodes.tags->'full_name',nodes.tags->'name:ru',nodes.tags->'name',nodes.tags->'place_name')) is not null
      AND NOT(exists(SELECT * FROM ershkus_search_addr WHERE nodes.id=any(id_link_N) AND addr_type='city'))
      """+wheretimeN.replace('tstamp', 'nodes.tstamp').replace('updated_at','nodes.updated_at')+"""
      ;
  """, whereTime)
  conn.commit()

  log.add ('insert village', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  log.add ('relations', level=loglevel+2, file=file_log)
  cur.execute("""
    INSERT INTO
      ershkus_search_addr(
      addr_type,
      addr_type_id,
      id_link_R,
      id_link_N,
      osm_id,
      village,
      name,
      postcode,
      member_role,
      geom,
      c_geom)
    SELECT
      'village' as addr_type,
      25 as addr_type_id,
      Array[t1.id_link_R] AS id_link_R,
      Array[t3.id_n] AS id_link_N,
      t1.osm_id || t3.osm_id as osm_id,
      t1.village AS village,
      t1.village AS name,
      coalesce(t1.postcode, t3.postcode) as postcode,
      t1.member_role as member_role,
      t1.geom as geom,
      t3.c_geom
    FROM
      (SELECT
        relations.id AS id_link_R,
        Array['r' || relations.id] AS osm_id,
        coalesce(relations.tags->'full_name:ru',relations.tags->'full_name',relations.tags->'name:ru',relations.tags->'name',relations.tags->'place_name') AS village,
        relations.tags->'addr:postcode' as postcode,
        relation_members.member_role as member_role,
        (((ST_BuildArea(ST_Collect(ways.linestring))))) as geom
      FROM
        relations,
        relation_members,
        ways
      WHERE
        relations.id=relation_members.relation_id
        AND relation_members.member_id=ways.id
        AND (relations.tags @> 'place=>village' OR relations.tags @> 'place=>hamlet' OR relations.tags @> 'place=>allotments')
        AND ways.linestring is not null
        AND (relation_members.member_role = 'outer' OR relation_members.member_role = 'inner')
        """+wheretimeALL.replace('tstamp', 'relations.tstamp').replace('updated_at','relations.updated_at')+"""
      GROUP BY
        id_link_R,
        osm_id,
        village,
        postcode,
        member_role) as t1
      LEFT JOIN
        (SELECT
          nodes.id as id_n,
          Array['n' || nodes.id] as osm_id,
          coalesce(nodes.tags->'full_name:ru',nodes.tags->'full_name',nodes.tags->'name:ru',nodes.tags->'name',nodes.tags->'place_name') AS village_n,
          nodes.tags->'addr:postcode' as postcode,
          nodes.geom as c_geom
        FROM
          nodes
        WHERE
          (nodes.tags @> 'place=>village' OR nodes.tags @> 'place=>hamlet' OR nodes.tags @> 'place=>allotments')
          ) as t3
        ON ((t1.geom && t3.c_geom) AND ST_Covers(t1.geom, t3.c_geom) AND t1.village=t3.village_n);
  """, whereTime)
  log.add ('ways', level=loglevel+2, file=file_log)
  cur.execute("""
    INSERT INTO
      ershkus_search_addr(
      addr_type,
      addr_type_id,
      id_link_W,
      osm_id,
      village,
      name,
      postcode,
      geom,
      id_link_N,
      c_geom)
    SELECT
      addr_type,
      addr_type_id,
      id_link_W,
      t1.osm_id || t3.osm_id as osm_id,
      village,
      village AS name,
      coalesce(t1.postcode,t3.postcode) AS postcode,
      geom,
      id_link_N,
      c_geom
    FROM
      (SELECT
        'village' as addr_type,
        25 as addr_type_id,
        Array[ways.id] AS id_link_W,
        Array['w' || ways.id] AS osm_id,
        coalesce(ways.tags->'full_name:ru',ways.tags->'full_name',ways.tags->'name:ru',ways.tags->'name',ways.tags->'place_name') AS village,
        ways.tags->'addr:postcode' as postcode,
        (((ST_BuildArea(ST_Collect(ways.linestring))))) as geom
      FROM
        ways
      WHERE
        (ways.tags @> 'place=>village' OR ways.tags @>'place=>hamlet' OR ways.tags @>'place=>allotments')
        AND ways.linestring is not null
        AND (coalesce(ways.tags->'full_name:ru',ways.tags->'full_name',ways.tags->'name',ways.tags->'place_name')) is not null
        """+wheretimeALL.replace('tstamp', 'ways.tstamp').replace('updated_at','ways.updated_at')+"""
      GROUP BY
        id_link_W,
        osm_id,
        village,
        postcode) as t1
      LEFT JOIN
        (SELECT
          Array[nodes.id] as id_link_N,
          Array['n' || nodes.id] as osm_id,
          coalesce(nodes.tags->'full_name:ru',nodes.tags->'full_name',nodes.tags->'name:ru',nodes.tags->'name',nodes.tags->'place_name') AS village_n,
          nodes.tags->'addr:postcode' as postcode,
          nodes.geom as c_geom
        FROM
          nodes
        WHERE
          (nodes.tags @> 'place=>village' OR nodes.tags @> 'place=>hamlet' OR nodes.tags @> 'place=>allotments')
          ) as t3
        ON ((t1.geom && t3.c_geom) AND ST_Covers(t1.geom, t3.c_geom) AND t1.village=t3.village_n);
  """, whereTime)
  conn.commit()

  log.add ('nodes', level=loglevel+2, file=file_log)
  for n in range(limitNode['min'], limitNode['max']+1, conf.nodeStep):
    # log.add ('step = '+str(n)+' / '+str(int((float(n)-limitNode['min'])/(limitNode['max']-limitNode['min'])*100))+'%', level=loglevel+3, file=file_log)
    whereTime['min'] = n
    whereTime['max'] = n + conf.nodeStep
    cur.execute("""
      INSERT INTO
        ershkus_search_addr(
        addr_type,
        addr_type_id,
        id_link_N,
        osm_id,
        village,
        name,
        postcode,
        geom)
      SELECT
        'village' as addr_type,
        25 as addr_type_id,
        Array[nodes.id] AS id_link_N,
        Array['n' || nodes.id] AS osm_id,
        coalesce(nodes.tags->'full_name:ru', nodes.tags->'full_name', nodes.tags->'name:ru', nodes.tags->'name', nodes.tags->'place_name') AS village,
        coalesce(nodes.tags->'full_name:ru', nodes.tags->'full_name', nodes.tags->'name:ru', nodes.tags->'name', nodes.tags->'place_name') AS name,
        nodes.tags->'addr:postcode' as postcode,
        nodes.geom as geom
      FROM
        nodes
      WHERE
        (nodes.tags @> 'place=>village' OR nodes.tags @> 'place=>hamlet' OR nodes.tags @> 'place=>allotments')
        AND (coalesce(nodes.tags->'full_name:ru',nodes.tags->'full_name',nodes.tags->'name:ru',nodes.tags->'name',nodes.tags->'place_name')) is not null
        AND NOT(exists(SELECT * FROM ershkus_search_addr WHERE nodes.id=any(id_link_N) AND addr_type='village'))
        AND (nodes.id>=%(min)s AND nodes.id<%(max)s)
        """+wheretimeN.replace('tstamp', 'nodes.tstamp').replace('updated_at','nodes.updated_at')+"""
        ;
    """, whereTime)
    conn.commit()

  log.add ('insert street', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  cur.execute("""
    INSERT INTO
      ershkus_addr_street_upd(
      --addr_type,
      --addr_type_id,
      --id,
      --id_link_W,
      osm_id,
      street,
      street_name,
      postcode,
      geom)
    SELECT
      --'street' as addr_type,
      --30 as addr_type_id,
      --ways.id as id,
      --Array[ways.id] AS id_link_W,
      'w' || ways.id AS osm_id,
      coalesce(ways.tags->'name:ru',ways.tags->'name') AS street,
      CASE (ways.tags->'name:ru' = ways.tags->'name') WHEN false
        THEN ways.tags->'name'
        ELSE null
        END as street_name,
      ways.tags->'addr:postcode' as postcode,
      ways.linestring as geom
    FROM
      ways
    WHERE
      ways.tags ? 'highway'
      AND ways.linestring is not null
      --AND (ways.tags->'name') is not null;
      AND ways.tags ? 'name'
      """+wheretimeALL.replace('tstamp', 'ways.tstamp').replace('updated_at','ways.updated_at')+"""
      ;
  """, whereTime)
  conn.commit()

  cur = conn.cursor()
  cur.execute("""
    DELETE FROM ershkus_addr_street_upd
      WHERE
        ST_NumPoints(geom)<2
  """)
  conn.commit()

  log.add ('insert housenumber', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  log.add ('relations', level=loglevel+2, file=file_log)
  cur.execute("""
    INSERT INTO
      ershkus_search_addr(
      addr_type,
      addr_type_id,
      id_link_R,
      osm_id,
      street,
      housenumber,
      name,
      postcode,
      geom)
    SELECT
      'housenumber' as addr_type,
      35 as addr_type_id,
      Array[relations.id, addr_r.id] AS id_link_R,
      Array['r'||relations.id, 'r'||addr_r.id] AS osm_id,
      coalesce(relations.tags->'name:ru', relations.tags->'name', addr_r.tags->'addr:street') AS street,
      'дом ' || (replace(replace(addr_r.tags->'addr:housenumber',' к', ' корпус '),' с',' строение ')) ||
      CASE WHEN (addr_r.tags?'addr:letter') THEN (' литер ') || (addr_r.tags->'addr:letter') ELSE '' END AS housenumber,
      'дом ' || (replace(replace(addr_r.tags->'addr:housenumber',' к', ' корпус '),' с',' строение ')) ||
      CASE WHEN (addr_r.tags?'addr:letter') THEN (' литер ') || (addr_r.tags->'addr:letter') ELSE '' END AS name,
      addr_r.tags->'addr:postcode' as postcode,
      (((ershkus_fn_geompoly(array_agg(addr_w.linestring))))) as geom
    FROM
      relations,
      relation_members,
      relations as addr_r,
      relation_members as addr_rm,
      ways as addr_w
    WHERE
      relations.id = relation_members.relation_id
      AND addr_r.id = relation_members.member_id
      AND relation_members.member_type = 'R'
      AND addr_r.id = addr_rm.relation_id
      AND addr_rm.member_id = addr_w.id
      AND addr_rm.member_role = 'outer'
      AND (relations.tags @> 'type=>associatedStreet' OR relations.tags @> 'type=>street')
      AND (relation_members.member_role = 'housenumber' OR relation_members.member_role = 'house')
      """+wheretimeALL.replace('tstamp', 'relations.tstamp').replace('updated_at','relations.updated_at')+"""
    GROUP BY
      id_link_R,
      osm_id,
      street,
      housenumber,
      postcode;
  """, whereTime)
  log.add ('ways', level=loglevel+2, file=file_log)
  cur.execute("""
    INSERT INTO
      ershkus_search_addr(
      addr_type,
      addr_type_id,
      id_link_R,
      id_link_W,
      osm_id,
      street,
      housenumber,
      name,
      postcode,
      geom)
    SELECT
      'housenumber' as addr_type,
      35 as addr_type_id,
      Array[relations.id] AS id_link_R,
      Array[addr_w.id] AS id_link_W,
      Array['r'||relations.id, 'w'||addr_w.id] AS osm_id,
      coalesce(relations.tags->'name:ru', relations.tags->'name', addr_w.tags->'addr:street') AS street,
      'дом ' || (replace(replace(addr_w.tags->'addr:housenumber',' к', ' корпус '),' с',' строение ')) ||
      CASE WHEN (addr_w.tags?'addr:letter') THEN (' литер ') || (addr_w.tags->'addr:letter') ELSE '' END AS housenumber,
      'дом ' || (replace(replace(addr_w.tags->'addr:housenumber',' к', ' корпус '),' с',' строение ')) ||
      CASE WHEN (addr_w.tags?'addr:letter') THEN (' литер ') || (addr_w.tags->'addr:letter') ELSE '' END AS name,
      addr_w.tags->'addr:postcode' as postcode,
      (((ershkus_fn_geompoly(array_agg(addr_w.linestring))))) as geom
    FROM
      relations,
      relation_members,
      ways as addr_w
    WHERE
      relations.id = relation_members.relation_id
      AND addr_w.id = relation_members.member_id
      AND relation_members.member_type = 'W'
      AND (relations.tags @> 'type=>associatedStreet' OR relations.tags @> 'type=>street')
      AND (relation_members.member_role = 'housenumber' OR relation_members.member_role = 'house')
      """+wheretimeALL.replace('tstamp', 'relations.tstamp').replace('updated_at','relations.updated_at')+"""
    GROUP BY
      id_link_R,
      id_link_W,
      osm_id,
      street,
      housenumber,
      postcode;
  """, whereTime)
  log.add ('nodes', level=loglevel+2, file=file_log)
  cur.execute("""
    INSERT INTO
      ershkus_search_addr(
      addr_type,
      addr_type_id,
      id_link_R,
      id_link_N,
      osm_id,
      street,
      housenumber,
      name,
      postcode,
      geom)
    SELECT
      'housenumber' as addr_type,
      35 as addr_type_id,
      Array[relations.id] AS id_link_R,
      Array[addr_n.id] AS id_link_N,
      Array['r'||relations.id, 'n'||addr_n.id] AS osm_id,
      coalesce(relations.tags->'name:ru', relations.tags->'name', addr_n.tags->'addr:street') AS street,
      'дом ' || (replace(replace(addr_n.tags->'addr:housenumber',' к', ' корпус '),' с',' строение ')) ||
      CASE WHEN (addr_n.tags?'addr:letter') THEN (' литер ') || (addr_n.tags->'addr:letter') ELSE '' END AS housenumber,
      'дом ' || (replace(replace(addr_n.tags->'addr:housenumber',' к', ' корпус '),' с',' строение ')) ||
      CASE WHEN (addr_n.tags?'addr:letter') THEN (' литер ') || (addr_n.tags->'addr:letter') ELSE '' END AS name,
      addr_n.tags->'addr:postcode' as postcode,
      addr_n.geom as geom
    FROM
      relations,
      relation_members,
      nodes as addr_n
    WHERE
      relations.id = relation_members.relation_id
      AND addr_n.id = relation_members.member_id
      AND relation_members.member_type = 'N'
      AND (relations.tags @> 'type=>associatedStreet' OR relations.tags @> 'type=>street')
      AND (relation_members.member_role = 'housenumber' OR relation_members.member_role = 'house')
      """+wheretimeALL.replace('tstamp', 'relations.tstamp').replace('updated_at','relations.updated_at')+"""
    ;
  """, whereTime)
  conn.commit()

  log.add ('relations', level=loglevel+2, file=file_log)
  cur.execute("""
    INSERT INTO
      ershkus_search_addr(
      addr_type,
      addr_type_id,
      id_link_R,
      osm_id,
      street,
      housenumber,
      name,
      postcode,
      geom)
    SELECT
      'housenumber' as addr_type,
      35 as addr_type_id,
      Array[relations.id] AS id_link_R,
      Array['r' || relations.id] AS osm_id,
      coalesce(relations.tags->'addr:street') AS street,
      'дом ' || (replace(replace(relations.tags->'addr:housenumber',' к', ' корпус '),' с',' строение ')) ||
        CASE WHEN (relations.tags?'addr:letter') THEN (' литер ') || (relations.tags->'addr:letter') ELSE '' END AS housenumber,
      'дом ' || (replace(replace(relations.tags->'addr:housenumber',' к', ' корпус '),' с',' строение ')) ||
        CASE WHEN (relations.tags?'addr:letter') THEN (' литер ') || (relations.tags->'addr:letter') ELSE '' END AS name,
      relations.tags->'addr:postcode' as postcode,
      (((ershkus_fn_geompoly(array_agg(ways.linestring))))) as geom
    FROM
      ways,
      relation_members,
      relations
      LEFT JOIN relation_members as rm_addr
        ON rm_addr.member_id = relations.id
          AND rm_addr.member_type = 'R'
          AND (rm_addr.member_role = 'housenumber' OR rm_addr.member_role = 'house')
    WHERE
      --(relations.tags ? 'building' OR relations.tags ? 'entrance')
      ways.linestring is not null
      AND relations.tags ? 'addr:housenumber'
      AND relations.id = relation_members.relation_id
      AND relation_members.member_id = ways.id
      AND relation_members.member_role = 'outer'
      AND rm_addr.relation_id is null
      """+wheretimeALL.replace('tstamp', 'relations.tstamp').replace('updated_at','relations.updated_at')+"""
    GROUP BY
      id_link_R,
      osm_id,
      street,
      housenumber,
      postcode;
  """, whereTime)
  log.add ('ways', level=loglevel+2, file=file_log)
  for n in range(limitWay['min'], limitWay['max']+1, conf.wayStep):
    # log.add ('step = '+str(n)+' / '+str(int((float(n)-limitWay['min'])/(limitWay['max']-limitWay['min'])*100))+'%', level=loglevel+3, file=file_log)
    whereTime['min'] = n
    whereTime['max'] = n + conf.wayStep
    cur.execute("""
      INSERT INTO
        ershkus_search_addr(
        addr_type,
        addr_type_id,
        id_link_W,
        osm_id,
        street,
        housenumber,
        name,
        postcode,
        geom)
      SELECT
        'housenumber' as addr_type,
        35 as addr_type_id,
        Array[ways.id] AS id_link_W,
        Array['w' || ways.id] AS osm_id,
        coalesce(ways.tags->'addr:street') AS street,
        'дом ' || (replace(replace(ways.tags->'addr:housenumber',' к', ' корпус '),' с',' строение ')) ||
          CASE WHEN (ways.tags?'addr:letter') THEN (' литер ') || (ways.tags->'addr:letter') ELSE '' END AS housenumber,
        'дом ' || (replace(replace(ways.tags->'addr:housenumber',' к', ' корпус '),' с',' строение ')) ||
          CASE WHEN (ways.tags?'addr:letter') THEN (' литер ') || (ways.tags->'addr:letter') ELSE '' END AS name,
        ways.tags->'addr:postcode' as postcode,
        (((ershkus_fn_geompoly(array_agg(ways.linestring))))) as geom
      FROM
        ways
        LEFT JOIN relation_members as rm_addr
          ON rm_addr.member_id = ways.id
            AND rm_addr.member_type = 'W'
            AND (rm_addr.member_role = 'housenumber' OR rm_addr.member_role = 'house')
      WHERE
        --(ways.tags ? 'building' OR ways.tags ? 'entrance')
        ways.linestring is not null
        AND ways.tags ? 'addr:housenumber'
        AND rm_addr.relation_id is null
        AND (ways.id>=%(min)s AND ways.id<%(max)s)
        """+wheretimeALL.replace('tstamp', 'ways.tstamp').replace('updated_at','ways.updated_at')+"""
      GROUP BY
        id_link_W,
        osm_id,
        street,
        housenumber,
        postcode;
    """, whereTime)
    conn.commit()
  log.add ('nodes', level=loglevel+2, file=file_log)
  for n in range(limitNode['min'], limitNode['max']+1, conf.nodeStep):
    # log.add ('step = '+str(n)+' / '+str(int((float(n)-limitNode['min'])/(limitNode['max']-limitNode['min'])*100))+'%', level=loglevel+3, file=file_log)
    whereTime['min'] = n
    whereTime['max'] = n + conf.nodeStep
    cur.execute("""
      INSERT INTO
        ershkus_search_addr(
        addr_type,
        addr_type_id,
        id_link_N,
        osm_id,
        street,
        housenumber,
        name,
        postcode,
        geom)
      SELECT
        'housenumber' as addr_type,
        35 as addr_type_id,
        Array[nodes.id] AS id_link_N,
        Array['n' || nodes.id] AS osm_id,
        coalesce(nodes.tags->'addr:street') AS street,
        'дом ' || (replace(replace(nodes.tags->'addr:housenumber',' к', ' корпус '),' с',' строение ')) ||
          CASE WHEN (nodes.tags?'addr:letter') THEN (' литер ') || (nodes.tags->'addr:letter') ELSE '' END AS housenumber,
        'дом ' || (replace(replace(nodes.tags->'addr:housenumber',' к', ' корпус '),' с',' строение ')) ||
          CASE WHEN (nodes.tags?'addr:letter') THEN (' литер ') || (nodes.tags->'addr:letter') ELSE '' END AS name,
        nodes.tags->'addr:postcode' as postcode,
        nodes.geom as geom
      FROM
        nodes
        LEFT JOIN relation_members as rm_addr
          ON rm_addr.member_id = nodes.id
            AND rm_addr.member_type = 'N'
            AND (rm_addr.member_role = 'housenumber' OR rm_addr.member_role = 'house')
      WHERE
        --(nodes.tags ? 'building' OR nodes.tags ? 'entrance')
        nodes.geom is not null
        AND nodes.tags ? 'addr:housenumber'
        AND rm_addr.relation_id is null
        AND (nodes.id>=%(min)s AND nodes.id<%(max)s)
        """+wheretimeN.replace('tstamp', 'nodes.tstamp').replace('updated_at','nodes.updated_at')+"""
      ;
    """, whereTime)
    conn.commit()

  # log.add ('insert region', loglevel+1) # # # # #
  # cur = conn.cursor()
  # cur.execute("""
  # """)
  # conn.commit()


  log.add ('clear no valid geom', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  cur.execute("""
    DELETE FROM ershkus_search_addr
      WHERE
        ST_IsEmpty(geom)
  """)
  conn.commit()



  log.add ('end insertFromSimpleOSM', level=loglevel, file=file_log)


def updateGeomIn(conn, lastID=0, lastIDstreet=0, loglevel=0):
  log.add ('start updateGeomIn', level=loglevel, file=file_log)

  cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
  cur.execute("""SELECT min(id) as min, max(id) as max FROM ershkus_search_addr;""")
  limit = cur.fetchone()
  if lastID:
    limit['min']=lastID
  log.add ('limit: '+str(limit), level=loglevel+1, file=file_log)
  
  for n in range(limit['min'], limit['max']+1, conf.GeomInStep):
    if limit['min'] >= limit['max']:
      break

    log.add ('step = '+str(n)+' / '+str(int((float(n)-limit['min'])/(limit['max']-limit['min'])*100))+'%', level=loglevel+2, file=file_log)

    if conf.use_country:
      log.add ('region in country', level=loglevel+3, file=file_log) # # # # #
      cur = conn.cursor()
      cur.execute("""
        UPDATE ershkus_search_addr AS search1
          SET
            country=search2.country, country_id=search2.id
          FROM ershkus_search_addr AS search2
          WHERE ((search2.geom && search1.geom) AND ST_Covers(search2.geom, search1.geom))
            AND search2.country is not null
            AND search1.country is null
            AND search1.region is not null
            AND search2.addr_type_id = 5 --country
            AND search1.addr_type_id = 10 --region
            AND (ST_IsValid(search2.geom))
            AND (ST_IsValid(search1.geom))
            AND (search1.id>=%(min)s AND search1.id<%(max)s)
            AND (search1.member_role = 'outer' OR search1.member_role is null);
      """, {'min': n, 'max': n+conf.GeomInStep})
      conn.commit()

    log.add ('district in region', level=loglevel+3, file=file_log) # # # # #
    cur = conn.cursor()
    cur.execute("""
      UPDATE ershkus_search_addr AS search1
        SET
          country=search2.country, country_id=search2.country_id,
          region=search2.region, region_id=search2.id
        FROM ershkus_search_addr AS search2
        WHERE ((search2.geom && search1.geom) AND ST_Covers(search2.geom, search1.geom))
          AND search2.region is not null
          AND search1.region is null
          AND search1.district is not null
          AND search2.addr_type_id = 10 --region
          AND search1.addr_type_id = 15 --district
          AND (ST_IsValid(search2.geom))
          AND (ST_IsValid(search1.geom))
          AND (search1.id>=%(min)s AND search1.id<%(max)s)
          AND (search1.member_role = 'outer' OR search1.member_role is null);
    """, {'min': n, 'max': n+conf.GeomInStep})
    conn.commit()

    if conf.use_country:
      log.add ('district in country', level=loglevel+3, file=file_log) # # # # #
      cur = conn.cursor()
      cur.execute("""
        UPDATE ershkus_search_addr AS search1
          SET
            country=search2.country, country_id=search2.id
          FROM ershkus_search_addr AS search2
          WHERE ((search2.geom && search1.geom) AND ST_Covers(search2.geom, search1.geom))
            AND search2.country is not null
            AND search1.country is null
            AND search1.district is not null
            AND search2.addr_type_id = 5 --country
            AND search1.addr_type_id = 15 --district
            AND (ST_IsValid(search2.geom))
            AND (ST_IsValid(search1.geom))
            AND (search1.id>=%(min)s AND search1.id<%(max)s)
            AND (search1.member_role = 'outer' OR search1.member_role is null);
      """, {'min': n, 'max': n+conf.GeomInStep})
      conn.commit()

    log.add ('city in district', level=loglevel+3, file=file_log) # # # # #
    cur = conn.cursor()
    cur.execute("""
      UPDATE ershkus_search_addr AS search1
        SET
          country=search2.country, country_id=search2.country_id,
          region=search2.region, region_id=search2.region_id,
          district=search2.district, district_id=search2.id
        FROM ershkus_search_addr AS search2
        WHERE ((search2.geom && search1.geom) AND ST_Covers(search2.geom, search1.geom))
          AND search2.district is not null
          AND search1.district is null
          AND search1.city is not null
          AND search2.addr_type_id = 15 --district
          AND search1.addr_type_id = 20 --city
          AND (ST_IsValid(search2.geom))
          AND (ST_IsValid(search1.geom))
          AND (search1.id>=%(min)s AND search1.id<%(max)s)
          AND (search1.member_role = 'outer' OR search1.member_role is null);
    """, {'min': n, 'max': n+conf.GeomInStep})
    conn.commit()

    log.add ('city in region', level=loglevel+3, file=file_log) # # # # #
    cur = conn.cursor()
    cur.execute("""
      UPDATE ershkus_search_addr AS search1
        SET
          country=search2.country, country_id=search2.country_id,
          region=search2.region, region_id=search2.id
        FROM ershkus_search_addr AS search2
        WHERE ((search2.geom && search1.geom) AND ST_Covers(search2.geom, search1.geom))
          AND search2.region is not null
          AND search1.region is null
          AND search1.city is not null
          AND search2.addr_type_id = 10 --region
          AND search1.addr_type_id = 20 --city
          AND (ST_IsValid(search2.geom))
          AND (ST_IsValid(search1.geom))
          AND (search1.id>=%(min)s AND search1.id<%(max)s)
          AND (search1.member_role = 'outer' OR search1.member_role is null);
    """, {'min': n, 'max': n+conf.GeomInStep})
    conn.commit()

    if conf.use_country:
      log.add ('city in country', level=loglevel+3, file=file_log) # # # # #
      cur = conn.cursor()
      cur.execute("""
        UPDATE ershkus_search_addr AS search1
          SET
            country=search2.country, country_id=search2.id
          FROM ershkus_search_addr AS search2
          WHERE ((search2.geom && search1.geom) AND ST_Covers(search2.geom, search1.geom))
            AND search2.country is not null
            AND search1.country is null
            AND search1.city is not null
            AND search2.addr_type_id = 5 --country
            AND search1.addr_type_id = 20 --city
            AND (ST_IsValid(search2.geom))
            AND (ST_IsValid(search1.geom))
            AND (search1.id>=%(min)s AND search1.id<%(max)s)
            AND (search1.member_role = 'outer' OR search1.member_role is null);
      """, {'min': n, 'max': n+conf.GeomInStep})
      conn.commit()

    log.add ('village in city', level=loglevel+3, file=file_log) # # # # #
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
          AND search1.city is null
          AND search1.village is not null
          AND search2.addr_type_id = 20 --city
          AND search1.addr_type_id = 25 --village
          AND (ST_IsValid(search2.geom))
          AND (ST_IsValid(search1.geom))
          AND (search1.id>=%(min)s AND search1.id<%(max)s)
          AND (search1.member_role = 'outer' OR search1.member_role is null);
    """, {'min': n, 'max': n+conf.GeomInStep})
    conn.commit()

    log.add ('village in district ', level=loglevel+3, file=file_log) # # # # #
    cur = conn.cursor()
    cur.execute("""
      UPDATE ershkus_search_addr AS search1
        SET
          country=search2.country, country_id=search2.country_id,
          region=search2.region, region_id=search2.region_id,
          district=search2.district, district_id=search2.id
        FROM ershkus_search_addr AS search2
        WHERE ((search2.geom && search1.geom) AND ST_Covers(search2.geom, search1.geom))
          AND search2.district is not null
          AND search1.district is null
          AND search1.village is not null
          AND search2.addr_type_id = 15 --district
          AND search1.addr_type_id = 25 --village
          AND (ST_IsValid(search2.geom))
          AND (ST_IsValid(search1.geom))
          AND (search1.id>=%(min)s AND search1.id<%(max)s)
          AND (search1.member_role = 'outer' OR search1.member_role is null);
    """, {'min': n, 'max': n+conf.GeomInStep})
    conn.commit()

    log.add ('village in region', level=loglevel+3, file=file_log) # # # # #
    cur = conn.cursor()
    cur.execute("""
      UPDATE ershkus_search_addr AS search1
        SET
          country=search2.country, country_id=search2.country_id,
          region=search2.region, region_id=search2.id
        FROM ershkus_search_addr AS search2
        WHERE ((search2.geom && search1.geom) AND ST_Covers(search2.geom, search1.geom))
          AND search2.region is not null
          AND search1.region is null
          AND search1.village is not null
          AND search2.addr_type_id = 10 --region
          AND search1.addr_type_id = 25 --village
          AND (ST_IsValid(search2.geom))
          AND (ST_IsValid(search1.geom))
          AND (search1.id>=%(min)s AND search1.id<%(max)s)
          AND (search1.member_role = 'outer' OR search1.member_role is null);
    """, {'min': n, 'max': n+conf.GeomInStep})
    conn.commit()

    if conf.use_country:
      log.add ('village in country', level=loglevel+3, file=file_log) # # # # #
      cur = conn.cursor()
      cur.execute("""
        UPDATE ershkus_search_addr AS search1
          SET
            country=search2.country, country_id=search2.id
          FROM ershkus_search_addr AS search2
          WHERE ((search2.geom && search1.geom) AND ST_Covers(search2.geom, search1.geom))
            AND search2.country is not null
            AND search1.country is null
            AND search1.village is not null
            AND search2.addr_type_id = 5 --country
            AND search1.addr_type_id = 25 --village
            AND (ST_IsValid(search2.geom))
            AND (ST_IsValid(search1.geom))
            AND (search1.id>=%(min)s AND search1.id<%(max)s)
            AND (search1.member_role = 'outer' OR search1.member_role is null);
      """, {'min': n, 'max': n+conf.GeomInStep})
      conn.commit()

    log.add ('housenumber in village', level=loglevel+3, file=file_log) # # # # #
    cur = conn.cursor()
    cur.execute("""
      UPDATE ershkus_search_addr AS search1
        SET
          country=search2.country, country_id=search2.country_id,
          region=search2.region, region_id=search2.region_id,
          district=search2.district, district_id=search2.district_id,
          city=search2.city, city_id=search2.city_id,
          village=search2.village, village_id=search2.id
        FROM ershkus_search_addr AS search2
        WHERE ((search2.geom && search1.geom) AND ST_Covers(search2.geom, search1.geom))
          AND search2.village is not null
          AND search1.village is null
          AND (search1.street is not null or search1.housenumber is not null)
          AND search2.addr_type_id = 25 --village
          AND (ST_IsValid(search2.geom))
          AND (ST_IsValid(search1.geom))
          AND (search1.id>=%(min)s AND search1.id<%(max)s)
          AND (search1.member_role = 'outer' OR search1.member_role is null);
    """, {'min': n, 'max': n+conf.GeomInStep})
    conn.commit()

    log.add ('housenumber in city', level=loglevel+3, file=file_log) # # # # #
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
          AND search1.city is null
          AND (search1.street is not null or search1.housenumber is not null)
          AND search2.addr_type_id = 20 --city
          AND (ST_IsValid(search2.geom))
          AND (ST_IsValid(search1.geom))
          AND (search1.id>=%(min)s AND search1.id<%(max)s)
          AND (search1.member_role = 'outer' OR search1.member_role is null);
    """, {'min': n, 'max': n+conf.GeomInStep})
    conn.commit()


  log.add ('street in village', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  cur.execute("""
    UPDATE ershkus_addr_street_upd AS search1
      SET
        country=search2.country, country_id=search2.country_id,
        region=search2.region, region_id=search2.region_id,
        district=search2.district, district_id=search2.district_id,
        city=search2.city, city_id=search2.city_id,
        village=search2.village, village_id=search2.id
      FROM ershkus_search_addr AS search2
      WHERE ((search2.geom && search1.geom) AND ST_Covers(search2.geom, search1.geom))
        AND search2.village is not null
        AND search1.village is null
        AND search1.street is not null
        AND search2.addr_type_id = 25 --village
        AND (ST_IsValid(search2.geom))
        AND (ST_IsValid(search1.geom))
        AND search1.id >= %(lastIDstreet)s;
  """, {"lastIDstreet": lastIDstreet})
  conn.commit()

  log.add ('street in city', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  cur.execute("""
    UPDATE ershkus_addr_street_upd AS search1
      SET
        country=search2.country, country_id=search2.country_id,
        region=search2.region, region_id=search2.region_id,
        district=search2.district, district_id=search2.district_id,
        city=search2.city, city_id=search2.id
      FROM ershkus_search_addr AS search2
      WHERE ((search2.geom && search1.geom) AND ST_Covers(search2.geom, search1.geom))
        AND search2.city is not null
        AND search1.city is null
        AND search1.street is not null
        AND search2.addr_type_id = 20 --city
        AND (ST_IsValid(search2.geom))
        AND (ST_IsValid(search1.geom))
        AND search1.id >= %(lastIDstreet)s;
  """, {"lastIDstreet": lastIDstreet})
  conn.commit()


    # log.add ('insert region', loglevel+3) # # # # #
    # cur = conn.cursor()
    # cur.execute("""
    # """, {'min': n, 'max': n+conf.GeomInStep})
    # conn.commit()

  log.add ('end updateGeomIn', level=loglevel, file=file_log)


def splitStreetAndCentroid(conn, lastID=0, loglevel=0):
  log.add ('start splitStreetAndCentroid', level=loglevel, file=file_log)

  log.add ('delete old street', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  cur.execute("""
    DELETE FROM ershkus_search_addr
    WHERE
      --c_geom is null
      addr_type_id=30;
  """)
  conn.commit()

  log.add ('insert split', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  cur.execute("""
    INSERT INTO
      ershkus_search_addr(
        osm_id,
        country,
        country_id,
        region,
        region_id,
        district,
        district_id,
        city,
        city_id,
        village,
        village_id,
        street,
        name,
        postcode,
        addr_type,
        addr_type_id,
        geom,
        c_geom
      )
    SELECT
      *,
      ST_PointOnSurface(geom) as c_geom
    FROM
      (SELECT
        array_agg(osm_id) as osm_id,
        country as country,
        country_id as country_id,
        region as region,
        region_id as region_id,
        district as district,
        district_id as district_id,
        city as city,
        city_id as city_id,
        village as village,
        village_id as village_id,
        street as street,
        street as name,
        postcode as postcode,
        'street' as addr_type,
        30 as addr_type_id,
        ST_LineMerge(ST_Collect(geom)) as geom
      FROM
        ershkus_addr_street_upd
      WHERE
        geom is not null
        AND ST_IsValid(geom)
      GROUP BY
        country,
        country_id,
        region,
        region_id,
        district,
        district_id,
        city,
        city_id,
        village,
        village_id,
        street,
        name,
        postcode,
        addr_type,
        addr_type_id
        ) as t1;
  """)
  conn.commit()

  log.add ('update centroid', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  cur.execute("""
    UPDATE ershkus_search_addr
      SET c_geom=ST_Centroid(geom)
      WHERE
        c_geom is null
        AND id >= %(lastID)s;
  """, {"lastID": lastID})
  conn.commit()

  log.add ('update if extension in housenumber', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  cur.execute("""
    UPDATE ershkus_search_addr
      SET addr_type_id=40
      WHERE
        addr_type_id = 35
        AND (housenumber LIKE '%%орпу%%' OR housenumber LIKE '%%троени%%'  OR housenumber LIKE '%%ите%%')
        AND id >= %(lastID)s;
  """, {"lastID": lastID})
  conn.commit()

  log.add ('update street_id', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  cur.execute("""
    UPDATE ershkus_search_addr as search1
      SET street_id = search2.id
      FROM ershkus_search_addr as search2
      WHERE ((search1.region_id = search2.region_id) OR (search1.region_id is null AND search2.region_id is null))
        AND ((search1.district_id = search2.district_id) OR (search1.district_id is null AND search2.district_id is null))
        AND ((search1.city_id = search2.city_id) OR (search1.city_id is null AND search2.city_id is null))
        AND ((search1.village_id = search2.village_id) OR (search1.village_id is null AND search2.village_id is null))
        AND ((search1.country_id = search2.country_id) OR (search1.country_id is null AND search2.country_id is null))
        AND search1.street = search2.street
        AND search1.addr_type_id >= 35
        AND search2.addr_type_id = 30
        AND search1.street_id is null
        AND search1.id >= %(lastID)s;
  """, {"lastID": lastID})
  conn.commit()

  log.add ('add id on type', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  for addr_type in ['country', 'region', 'district', 'city', 'village', 'street']:
    cur.execute("""
      UPDATE ershkus_search_addr
        SET """+addr_type+"""_id=id
        WHERE
          addr_type='"""+addr_type+"""'
          AND id >= %(lastID)s;
    """, {"lastID": lastID})
  conn.commit()


  log.add ('end splitStreetAndCentroid', level=loglevel, file=file_log)


def updateFullName(conn, lastID=0, loglevel=0):
  log.add ('start updateFullName', level=loglevel, file=file_log)

  log.add ('full_name', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  cur.execute("""
    UPDATE ershkus_search_addr AS search1 SET full_name = search2.postcode
      || CASE WHEN """+str(conf.use_country)+""" THEN search2.country ELSE '' END
      || search2.region || search2.district || search2.city || search2.village || search2.street || search2.housenumber
    FROM
      (SELECT
        id,
        CASE WHEN((postcode is null) OR (postcode='')) THEN ''
          ELSE postcode || ', ' END as postcode,
        CASE WHEN((country is null) OR (country='')) THEN '' ELSE
          CASE WHEN addr_type='country' THEN country ELSE
            CASE WHEN country LIKE city THEN '' ELSE country || ', ' END END END as country,
        CASE WHEN((region is null) OR (region='')) THEN '' ELSE
          CASE WHEN addr_type='region' THEN region ELSE
            CASE WHEN region LIKE city THEN '' ELSE region || ', ' END END END as region,
        CASE WHEN((district is null) OR (district='')) THEN '' ELSE
          CASE WHEN addr_type='district' THEN district ELSE district || ', ' END END as district,
        CASE WHEN((city is null) OR (city='')) THEN '' ELSE
          CASE WHEN addr_type='city' THEN 'город ' || city ELSE 'город ' || city || ', ' END END as city,
        CASE WHEN((village is null) OR (village='')) THEN '' ELSE
          CASE WHEN addr_type='village' THEN village ELSE village || ', ' END END as village,
        CASE WHEN((street is null) OR (street='')) THEN '' ELSE
          CASE WHEN addr_type='street' THEN street ELSE  street || ', ' END END as street,
        CASE WHEN((housenumber is null) OR (housenumber='')) THEN '' ELSE housenumber END as housenumber,
        addr_type
      FROM ershkus_search_addr
      WHERE
        (region<>'' OR country<>'' OR city ='Санкт-Петербург' OR city ='Москва' )
        AND id >= %(lastID)s
      ) as search2
    WHERE
      search1.id=search2.id
      --AND (member_role is null OR member_role = 'outer')
    ;
  """, {"lastID": lastID})
  conn.commit()

  log.add ('index_name', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  cur.execute("""
    UPDATE ershkus_search_addr AS search1 SET index_name = replace(search2.postcode
        || replace(replace(replace(search2.housenumber,'строение ','строение_'),'корпус ','корпус_'),'литер ','литер_')
        || regexp_replace(search2.street,'([^4-9]?\d)([ -]|й|ой|ый|я|ая)+',E'\\\\1-','g') || search2.village || search2.city || search2.district || search2.region
        || CASE WHEN """+str(conf.use_country)+""" THEN search2.country ELSE '' END ,'-','ъ')
    FROM
      (SELECT
        id,
        CASE WHEN((postcode is null) OR (postcode='')) THEN '' ELSE
          postcode || ', ' END as postcode,
        CASE WHEN((housenumber is null) OR (housenumber='')) THEN '' ELSE
          housenumber || ', ' END as housenumber,
        CASE WHEN((street is null) OR (street='')) THEN '' ELSE
          street || ', ' END as street,
        CASE WHEN((village is null) OR (village='')) THEN '' ELSE
          village || ', ' END as village,
        CASE WHEN((city is null) OR (city='')) THEN '' ELSE
          'город ' || city || ', ' END as city,
        CASE WHEN((district is null) OR (district='')) THEN '' ELSE
          district || ', ' END as district,
        CASE WHEN((region is null) OR (region='')) THEN '' ELSE
          CASE WHEN region LIKE city THEN '' ELSE region || ', ' END END as region,
        CASE WHEN((country is null) OR (country='')) THEN '' ELSE
           country || ', ' END as country,
        addr_type
      FROM ershkus_search_addr
      WHERE
        (region<>'' OR country<>'' OR city ='Санкт-Петербург' OR city ='Москва')
        AND id >= %(lastID)s
      ) as search2
    WHERE
      search1.id=search2.id
      --AND (member_role is null OR member_role = 'outer')
    ;
  """, {"lastID": lastID})
  conn.commit()

  log.add ('end updateFullName', level=loglevel, file=file_log)


def insertAddrP(conn, loglevel=0):
  log.add ('start insertAddrP', level=loglevel, file=file_log)

  log.add ('clear', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  cur.execute("""
    TRUNCATE TABLE ershkus_search_addr_p;
  """)
  conn.commit()

  log.add ('insert', level=loglevel+1, file=file_log) # # # # #
  cur = conn.cursor()
  cur.execute("""
    INSERT INTO
      ershkus_search_addr_p(
        id, id_link_n, id_link_w, id_link_r, osm_id, full_name, name, postcode, country, country_id, region, region_id,
        district, district_id, city, city_id, village, village_id, street, housenumber,
        member_role, addr_type, index_name, addr_type_id, geom, c_geom
      )
    SELECT
        id, id_link_n, id_link_w, id_link_r, osm_id, full_name, name, postcode, country, country_id, region, region_id,
        district, district_id, city, city_id, village, village_id, street, housenumber,
        member_role, addr_type, index_name, addr_type_id, geom, c_geom
      FROM ershkus_search_addr
      WHERE addr_type_id<30
  """)
  conn.commit()

  log.add ('end insertAddrP', level=loglevel, file=file_log)


def updateDeleteAddr(conn, whereTime, loglevel=0):
  log.add ('start updateDeleteAddr', level=loglevel, file=file_log)

  # log.add ('delete and modify from ershkus_search_addr', level=loglevel+1, file=file_log)
  cur = conn.cursor()
  cur.execute("""
    DROP TABLE IF EXISTS tmp;
    DROP TABLE IF EXISTS addr;
    
    CREATE TEMP TABLE tmp AS (
        SELECT (type || osm_id) as osm_id
          FROM deleted_entries
          WHERE
            deleted_at>=%(ldel)s AND deleted_at<=%(ndel)s
      UNION ALL
        SELECT ('n' || id)
          FROM nodes
          WHERE
            (nodes.tstamp>=%(lupd)s AND nodes.tstamp<=%(nupd)s)
            AND tags @> ('')::hstore
      UNION ALL
        SELECT ('w' || id)
          FROM ways
          WHERE
            ((ways.tstamp>=%(lupd)s AND ways.tstamp<=%(nupd)s)
              OR (ways.updated_at>=%(lupd)s AND ways.updated_at<=%(nupd)s))
      UNION ALL
        SELECT ('r' || id)
          FROM relations
          WHERE
            ((relations.tstamp>=%(lupd)s AND relations.tstamp<=%(nupd)s)
              OR (relations.updated_at>=%(lupd)s AND relations.updated_at<=%(nupd)s))
    );
    
    CREATE TEMP TABLE addr AS (
      SELECT id
        FROM (SELECT id, unnest(osm_id) as osm_id FROM ershkus_search_addr) as t1
        WHERE osm_id IN (SELECT osm_id FROM tmp)
    );
    
    UPDATE ershkus_search_addr
      SET country_id=NULL, country=NULL, index_name=NULL, full_name=NULL, id=nextval('ershkus_search_addr_id_seq')
      WHERE country_id IN (SELECT id FROM addr);
    UPDATE ershkus_search_addr
      SET region_id=NULL, region=NULL, index_name=NULL, full_name=NULL, id=nextval('ershkus_search_addr_id_seq')
      WHERE region_id IN (SELECT id FROM addr);
    UPDATE ershkus_search_addr
      SET district_id=NULL, district=NULL, index_name=NULL, full_name=NULL, id=nextval('ershkus_search_addr_id_seq')
      WHERE district_id IN (SELECT id FROM addr);
    UPDATE ershkus_search_addr
      SET city_id=NULL, city=NULL, index_name=NULL, full_name=NULL, id=nextval('ershkus_search_addr_id_seq')
      WHERE city_id IN (SELECT id FROM addr);
    UPDATE ershkus_search_addr
      SET village_id=NULL, village=NULL, index_name=NULL, full_name=NULL, id=nextval('ershkus_search_addr_id_seq')
      WHERE village_id IN (SELECT id FROM addr);
    UPDATE ershkus_search_addr
      SET street_id=NULL, street=NULL, index_name=NULL, full_name=NULL, id=nextval('ershkus_search_addr_id_seq')
      WHERE street_id IN (SELECT id FROM addr);

    UPDATE ershkus_addr_street_upd
      SET country_id = NULL, country = NULL, id = nextval('ershkus_addr_street_upd_id_seq')
      WHERE country_id IN (SELECT id FROM addr);
    UPDATE ershkus_addr_street_upd
      SET region_id = NULL, region = NULL, id = nextval('ershkus_addr_street_upd_id_seq')
      WHERE region_id IN (SELECT id FROM addr);
    UPDATE ershkus_addr_street_upd
      SET district_id = NULL, district = NULL, id = nextval('ershkus_addr_street_upd_id_seq')
      WHERE district_id IN (SELECT id FROM addr);
    UPDATE ershkus_addr_street_upd
      SET city_id = NULL, city = NULL, id = nextval('ershkus_addr_street_upd_id_seq')
      WHERE city_id IN (SELECT id FROM addr);
    UPDATE ershkus_addr_street_upd
      SET village_id = NULL, village = NULL, id = nextval('ershkus_addr_street_upd_id_seq')
      WHERE village_id IN (SELECT id FROM addr);
    
    DELETE 
      FROM ershkus_search_addr
      WHERE
        id IN (SELECT id FROM addr);

    DELETE 
      FROM ershkus_addr_street_upd
      WHERE
        id IN (
          SELECT id
            FROM ershkus_addr_street_upd
            WHERE osm_id IN (SELECT osm_id FROM tmp)
        );

    DROP TABLE IF EXISTS addr;
    DROP TABLE IF EXISTS tmp;
  """, whereTime)
  
  conn.commit()

  log.add ('end updateDeleteAddr', level=loglevel, file=file_log)


def insertAddr(conn, loglevel=0):
  log.add ('start insertAddr', level=loglevel, file=file_log)

  # time последнего объекта и удаленные
  log.add ('get tstamp', level=loglevel, file=file_log)
  cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
  cur.execute("""
    SELECT MAX(tstamp) as nupd, (SELECT MAX(deleted_at) FROM deleted_entries) as ndel
    FROM
    (
      SELECT MAX(tstamp) as tstamp
        FROM relations
      UNION
      SELECT MAX(updated_at)
        FROM relations
      UNION
      SELECT MAX(tstamp)
        FROM ways
      UNION
      SELECT MAX(updated_at)
        FROM ways
      UNION
      SELECT MAX(tstamp)
        FROM nodes
    ) as t1
  """)
  whereTime = cur.fetchone()
  if not whereTime['ndel']:
    whereTime['ndel'] = datetime.datetime.now()

  # clear table
  cur = conn.cursor()
  cur.execute("""
    TRUNCATE TABLE ershkus_search_addr;
    TRUNCATE TABLE ershkus_addr_street_upd;
  """)
  conn.commit()

  insertFromSimpleOSM(conn = conn, loglevel = loglevel+1)

  updateGeomIn(conn = conn, loglevel = loglevel+1)

  splitStreetAndCentroid(conn = conn, loglevel = loglevel+1)

  updateFullName(conn = conn, loglevel = loglevel+1)

  insertAddrP(conn = conn, loglevel = loglevel+1)

  # сохраним текущую позицию
  utils.saveDate(whereTime=whereTime, file=fupd_time, key_config='dateaddr')

  log.add ('end insertAddr', level=loglevel, file=file_log)


def updateAddr(conn, loglevel=0):
  log.add ('start updateAddr', level=loglevel, file=file_log)

  cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

  if not os.path.exists(conf.workactual + fupd_time):
    log.add ('no exists file `'+conf.workactual + fupd_time+'`', level=loglevel+1, file=file_log)
    return

  # time последнего объекта и удаленные
  log.add ('last time edit obj and delete', level=loglevel, file=file_log)
  cur.execute("""
    SELECT MAX(deleted_at) as ndel
      FROM deleted_entries
  """)
  whereTime = cur.fetchone()
  if not whereTime['ndel']:
    whereTime['ndel'] = datetime.datetime.now()

  arrObjTime = [['relations','tstamp'],['relations','updated_at'],['ways','tstamp'],['ways','updated_at'],['nodes','tstamp']]
  editTime = 0
  for nObj in arrObjTime:
    cur.execute("""
      SELECT MAX("""+nObj[1]+""") as tstamp
        FROM """+nObj[0]+"""
    """)
    nTime = cur.fetchone()['tstamp']
    if editTime:
      if nTime:
        if editTime < nTime:
          editTime = nTime
    else:
      editTime = nTime
  whereTime['nupd'] = editTime

  # добавим прошлую позицию
  f = open(conf.workactual + fupd_time,'r')
  fjson = json.loads(f.read(), object_hook=utils.jsonloads)
  f.close()
  whereTime['lupd'] = fjson['nupd']
  whereTime['ldel'] = fjson['ndel']

  cur.execute("""
    SELECT max(id) as maxid
    FROM ershkus_search_addr
  """)
  lastID = cur.fetchone()['maxid']+1
  cur.execute("""
    SELECT max(id) as maxid
    FROM ershkus_addr_street_upd
  """)
  lastIDstreet = cur.fetchone()['maxid']+1

  updateDeleteAddr(conn, whereTime, loglevel+1)

  insertFromSimpleOSM(conn = conn, whereTime = whereTime, loglevel = loglevel+1)

  updateGeomIn(conn, lastID, lastIDstreet, loglevel+1)

  splitStreetAndCentroid(conn, lastID, loglevel+1)

  updateFullName(conn, lastID, loglevel+1)

  insertAddrP(conn, loglevel+1)


  # сохраним текущую позицию
  utils.saveDate(whereTime=whereTime, file=fupd_time, key_config='dateaddr')

  log.add ('end updateAddr', level=loglevel, file=file_log)

def test(conn, loglevel=0):

  # updateGeomIn(conn = conn, loglevel = loglevel+1)

  # splitStreetAndCentroid(conn = conn, loglevel = loglevel+1)

  # updateFullName(conn = conn, loglevel = loglevel+1)

  insertAddrP(conn = conn, loglevel = loglevel+1)



def main():
  try:
    parser = argparse.ArgumentParser(add_help=True, version='0.6')
    parser.add_argument('action', metavar='action', type=str, choices=['insert', 'update', 'insertAddrSave', 'test'], help='action operations `insert` or `update` or `insertAddrSave`')
    args = parser.parse_args()

    log.add ('start main', file=file_log)
    conn = psycopg2.connect(host=conf.addrfull_host, database=conf.addrfull_database, user=conf.addrfull_user, password=conf.addrfull_password)
    if args.action == 'insert':
      insertAddr(conn, 1)
    elif args.action == 'update':
      updateAddr(conn, 1)
    elif args.action == 'insertAddrSave':
      insertAddrSave(conn, 1)
    elif args.action == 'test':
      test(conn, 1)


    # updateOwner(conn)




    log.add ('end main', file=file_log)
  except :
    log.add ('! error: '+str(traceback.format_exc()), file=file_log)


if __name__ == '__main__':
  main()