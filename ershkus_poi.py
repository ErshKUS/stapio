#!/usr/bin/env python
# -*- coding: utf8 -*-

import json, psycopg2, os, datetime, traceback
import psycopg2.extras
import argparse
import codecs
import stapio_config as conf
import stapio_utils as utils
import logger as log

lang=['ru']
fupd_time = 'upd_time_poi.json'
file_log = 'poi.log'


def catalog(cur, isupdate=False, whereTime=None, loglevel=0):
  log.add ('start load catalog', level=loglevel, file=file_log)
  dictionary_json=[]

  with codecs.open(conf.file_catalog_json, 'rt', encoding='utf-8') as f:
    catalog_json = json.load(f)
  for fd in os.listdir(conf.path_dictionary_json):
    with codecs.open(conf.path_dictionary_json+fd, 'rt', encoding='utf-8') as f:
      fjson = json.load(f)
      if (fjson['language'] in lang):
        dictionary_json.append(fjson)

  # добавляем исключающие теги tags_ex
  for icat in catalog_json:
    tags = [k+'='+v for k,v in icat['tags'].iteritems()]
    tags.sort()
    icat['_tags_line'] = ','.join(tags)
  for icat in catalog_json:
    if len(icat['tags'])>1:
      for icat_mini in catalog_json:
        if icat_mini['_tags_line']:
          pFind = icat['_tags_line'].find(icat_mini['_tags_line'])
          if pFind >= 0 and icat['name'] != icat_mini['name']:
            icat_mini['tags_ex']={}
            tags = icat['_tags_line'].replace(icat_mini['_tags_line'],'').replace(',,',',').strip(',').split(',')
            for tag in tags:
              tag = tag.split('=')
              if len(tag) == 2:
                icat_mini['tags_ex'][tag[0]]=tag[1]


  wheretimeN = ""
  wheretimeALL = ""
  if isupdate:
    wheretimeN = "AND ((tstamp>=%(lupd)s AND tstamp<=%(nupd)s)"
    wheretimeALL = wheretimeN + "OR (updated_at>=%(lupd)s AND updated_at<=%(nupd)s))"
    wheretimeN = wheretimeN + ")"

  langname = ""
  langin = {"name":'', "class":'', "tags":''}
  for ilang in dictionary_json:
    langin['name'] = langin['name'] + " \"name_%(lang)s\", " % {"lang":ilang['language']}
    langin['class'] = langin['class'] + " \"class_%(lang)s\", " % {"lang":ilang['language']}
    langin['tags'] = langin['tags'] + " \"tags_%(lang)s\", " % {"lang":ilang['language']}
    langname = langname + " coalesce(tags->'name:%(lang)s',tags->'name') as \"name_%(lang)s\", " % {"lang":ilang['language']}
  langinsert = langin['name'] + langin['class'] + langin['tags']

  if not isupdate:
    log.add ('clear table ershkus_poi', level=loglevel+1, file=file_log)
    cur.execute("DELETE FROM ershkus_poi")

  for icatalog in catalog_json:
    wheretags = ""
    wheretagsR = ""
    for k,v in icatalog['tags'].iteritems():
      notPrefix = 'NOT' if v[0] == '!' else ''
      if v == '*' or v == '!*':
        wheretags = wheretags+("AND %(notPrefix)s tags ? '%(k)s' " % {"k":k, "notPrefix":notPrefix})
        wheretagsR = wheretagsR+("AND %(notPrefix)s relations.tags ? '%(k)s' " % {"k":k, "notPrefix":notPrefix})
      else:
        wheretags = wheretags+("AND %(notPrefix)s tags @> '%(k)s=>%(v)s' " % {"k":k, "v":v, "notPrefix":notPrefix})
        wheretagsR = wheretagsR+("AND %(notPrefix)s relations.tags @> '%(k)s=>%(v)s' " % {"k":k, "v":v, "notPrefix":notPrefix})
    if 'tags_ex' in icatalog:
      for k,v in icatalog['tags_ex'].iteritems():
        wheretags = wheretags+("AND NOT (tags @> '%(k)s=>%(v)s') " % {"k":k, "v":v})
        wheretagsR = wheretagsR+("AND NOT (relations.tags @> '%(k)s=>%(v)s') " % {"k":k, "v":v})
    if wheretags <> '':
      wheretags = wheretags[4:]
      wheretagsR = wheretagsR[4:]
      lmoretags = ""
      langtype = ""
      log.add (icatalog['name'], level=loglevel+1, file=file_log)
      for ilang in dictionary_json:
        langtype = langtype + " '%(ltype)s' as class_%(lang)s, " % {"ltype":ilang['catalog'][icatalog['name']]['name'], "lang":ilang['language']}
        case = ""
        for mt in icatalog['moretags']:
          case1 = " || (hstore('%(name)s',(" % {"name":ilang['moretags'][mt]['name']}
          case2 = ""
          if (icatalog['moretags'][mt]['type'] == 'translate'):
            case1 = case1 + "CASE "
            for k,v in ilang['class'][icatalog['moretags'][mt]['class']].iteritems():
              case2 = case2 + "WHEN tags->'%(mt)s'='%(k)s' THEN '%(v)s' " % {"mt":icatalog['moretags'][mt]['tag'], "k":k, "v":v}
            case2 = case2 + "WHEN NOT(defined(tags,'%(mt)s')) THEN '%(nu)s' ELSE tags->'%(mt)s' " % {"mt":mt, "nu":ilang['class']['null']}
            case2 = case2 + "END"
          elif (icatalog['moretags'][mt]['type'] == 'number' or icatalog['moretags'][mt]['type'] == 'period'):
            case2 = "tags->'%(tag)s'" % icatalog['moretags'][mt]

          case = case + case1 + case2 + "))) "
        if (case==''):
          lmoretags = lmoretags + ("NULL as tags_%(lang)s, " % {"lang":ilang['language']})
        else:
          lmoretags = lmoretags + case[4:] + (" as tags_%(lang)s, " % {"lang":ilang['language']})
      moretags=""
      moretags1=""
      for mt in icatalog['moretags']:
        moretags1 = moretags1 + (" || (CASE WHEN defined(tags,'%(mt)s') THEN (hstore('%(mt)s',(tags->'%(mt)s'))) ELSE '' END)" % {"mt":mt})
      if (moretags1==''):
        moretags = " NULL as tags, "
      else:
        moretags = moretags1[4:] + " as tags, "

      # log.add ('insert nodes', level=loglevel+2, file=file_log)
      execute = """
        INSERT INTO
          ershkus_poi(
            "osm_id",
            "class",
            "tags",
            "opening_hours",
            "operator",
            "brand",
            "phone",
            "fax",
            "website",
            "email",
            "wikipedia",
            "description",
             %(langinsert)s
            "addr_street",
            "addr_house",
            "c_geom")
        SELECT
          'n' || id as "osm_id",
          '%(class)s' as "class",
          %(moretags)s
          tags->'opening_hours' as "opening_hours",
          tags->'operator' as "operator",
          tags->'brand' as "brand",
          coalesce(tags->'contact:phone', tags->'phone') as "phone",
          coalesce(tags->'contact:fax', tags->'fax') as "fax",
          coalesce(tags->'contact:website', tags->'website') as "website",
          coalesce(tags->'contact:email', tags->'email') as "email",
          tags->'wikipedia' as "wikipedia",
          tags->'description' as "description",
          %(langname)s
          %(langtype)s
          %(lmoretags)s
          tags->'addr:street' as addr_street,
          tags->'addr:housenumber' as addr_house,
          geom as "c_geom"
        FROM nodes
        WHERE
          %(wheretags)s
          %(wheretimeN)s
      ;""" % {"langinsert":langinsert, "class":icatalog['name'], "moretags":moretags, "langname":langname, "langtype":langtype, "lmoretags":lmoretags, "wheretags":wheretags, "wheretimeN":wheretimeN}
      # if (icatalog['name'] == 'park'):
        # print execute
      # print execute
      # cur = conn.cursor()
      cur.execute(execute, whereTime)
      # conn.commit()

      # log.add ('insert ways', level=loglevel+2, file=file_log)
      execute = """
        INSERT INTO
          ershkus_poi(
            "osm_id",
            "class",
            "tags",
            "opening_hours",
            "operator",
            "brand",
            "phone",
            "fax",
            "website",
            "email",
            "wikipedia",
            "description",
             %(langinsert)s
            "addr_street",
            "addr_house",
            "c_geom")
        SELECT
          'w' || id as "osm_id",
          '%(class)s' as "class",
          %(moretags)s
          tags->'opening_hours' as "opening_hours",
          tags->'operator' as "operator",
          tags->'brand' as "brand",
          coalesce(tags->'contact:phone', tags->'phone') as "phone",
          coalesce(tags->'contact:fax', tags->'fax') as "fax",
          coalesce(tags->'contact:website', tags->'website') as "website",
          coalesce(tags->'contact:email', tags->'email') as "email",
          tags->'wikipedia' as "wikipedia",
          tags->'description' as "description",
          %(langname)s
          %(langtype)s
          %(lmoretags)s
          tags->'addr:street' as addr_street,
          tags->'addr:housenumber' as addr_house,
          ST_Centroid(linestring) as "c_geom"
        FROM ways
        WHERE
          %(wheretags)s
          %(wheretimeALL)s
          AND ST_NumPoints(linestring)>1
      ;""" % {"langinsert":langinsert, "class":icatalog['name'], "moretags":moretags, "langname":langname, "langtype":langtype, "lmoretags":lmoretags, "wheretags":wheretags, "wheretimeALL":wheretimeALL}
      # if (icatalog['name'] == 'cafe'):
        # print execute
        # raise Exception('debug')
      # cur = conn.cursor()
      # if (icatalog['name'] == 'park'):
        # print execute

      cur.execute(execute, whereTime)
      # conn.commit()

      # log.add ('insert relation', level=loglevel+2, file=file_log)
      execute = """
        INSERT INTO
          ershkus_poi(
            "osm_id",
            "class",
            "tags",
            "opening_hours",
            "operator",
            "brand",
            "phone",
            "fax",
            "website",
            "email",
            "wikipedia",
            "description",
             %(langinsert)s
            "addr_street",
            "addr_house",
            "c_geom")
        SELECT
          'r' || t1.id as "osm_id",
          '%(class)s' as "class",
          %(moretags)s
          tags->'opening_hours' as "opening_hours",
          tags->'operator' as "operator",
          tags->'brand' as "brand",
          coalesce(tags->'contact:phone', tags->'phone') as "phone",
          coalesce(tags->'contact:fax', tags->'fax') as "fax",
          coalesce(tags->'contact:website', tags->'website') as "website",
          coalesce(tags->'contact:email', tags->'email') as "email",
          tags->'wikipedia' as "wikipedia",
          tags->'description' as "description",
          %(langname)s
          %(langtype)s
          %(lmoretags)s
          tags->'addr:street' as addr_street,
          tags->'addr:housenumber' as addr_house,
          c_geom as "c_geom"
        FROM
          (SELECT
            relations.id,
            (ST_Centroid(ershkus_fn_BuildArea(ST_Collect(linestring)))) as c_geom
          FROM
            relations,
            relation_members,
            ways
          WHERE
            %(wheretags)s
            %(wheretimeALL)s
            AND relations.id=relation_members.relation_id
            AND relation_members.member_id=ways.id
            AND ST_NumPoints(ways.linestring)>1
            AND (relation_members.member_role in ('', 'outer')
              OR relation_members.member_role is null)
          GROUP BY
            relations.id) as t1,relations
        WHERE
          relations.id=t1.id
      ;""" % {"langinsert":langinsert, "class":icatalog['name'], "moretags":moretags, "langname":langname, "langtype":langtype, "lmoretags":lmoretags, "wheretags":wheretagsR,
        "wheretimeALL":wheretimeALL.replace('tstamp', 'relations.tstamp').replace('updated_at','relations.updated_at')}
      # cur = conn.cursor()
      # if (icatalog['name'] == 'park'):
        # print execute
      cur.execute(execute, whereTime)
      # conn.commit()

  log.add ('end load catalog', level=loglevel, file=file_log)


def addr(cur, lastID=0, loglevel=0):
  log.add ('start set addr', level=loglevel, file=file_log)

  # cur2 = conn2.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
  cur.execute("""SELECT min(id), max(id) FROM ershkus_poi""")
  limit = cur.fetchone()
  step = 7000

  if lastID:
    limit['min'] = lastID

  if limit['min'] >= limit['max']:
    return

  for n in range(limit['min'], limit['max'], step):
    log.add ('step = '+str(n)+' / '+str(int((float(n)-limit['min'])/(limit['max']-limit['min'])*100))+'%', level=loglevel+1, file=file_log)
    log.add ('set region to all', level=loglevel+2, file=file_log)
    cur.execute("""
      UPDATE ershkus_poi
      SET
        addr_region_id=ershkus_search_addr.id
      FROM
        ershkus_search_addr_p as ershkus_search_addr
      WHERE
        ershkus_poi.id>=%(min)s AND ershkus_poi.id<%(max)s
        AND ((ershkus_search_addr.geom && ershkus_poi.c_geom) AND ST_Covers(ershkus_search_addr.geom, ershkus_poi.c_geom))
        AND ershkus_search_addr.addr_type_id=10
        AND addr_full_name is null
      ;
    """, {'min': n, 'max': n+step})


    log.add ('set city to all', level=loglevel+2, file=file_log)
    cur.execute("""
      UPDATE ershkus_poi
      SET
        addr_district_id=ershkus_search_addr.district_id,
        addr_city_id=ershkus_search_addr.id
      FROM
        ershkus_search_addr_p as ershkus_search_addr
      WHERE
        ershkus_poi.id>=%(min)s AND ershkus_poi.id<%(max)s
        AND ((ershkus_search_addr.geom && ershkus_poi.c_geom) AND ST_Covers(ershkus_search_addr.geom, ershkus_poi.c_geom))
        AND ershkus_search_addr.addr_type_id=20
        AND ershkus_poi.addr_region_id=ershkus_search_addr.region_id
        AND addr_full_name is null
      ;
      -- Запрос успешно завершён: 4664 строки изменено, 278050 мс время выполнения.
      -- Запрос успешно завершён: 78334 строки изменено, 256588 мс время выполнения.
    """, {'min': n, 'max': n+step})


    log.add ('set house', level=loglevel+2, file=file_log)
    cur.execute("""
      UPDATE ershkus_poi
      SET
        addr_district_id=ershkus_search_addr.district_id,
        addr_city_id=ershkus_search_addr.city_id,
        addr_in_id=ershkus_search_addr.id,
        addr_in_type=ershkus_search_addr.addr_type_id,
        addr_full_name=ershkus_search_addr.full_name,
        addr_house=ershkus_search_addr.housenumber,
        addr_street=ershkus_search_addr.street,
        addr_village=ershkus_search_addr.village,
        addr_city=ershkus_search_addr.city,
        addr_district=ershkus_search_addr.district,
        addr_region=ershkus_search_addr.region,
        addr_country=ershkus_search_addr.country
      FROM
        ershkus_search_addr
      WHERE
        ershkus_poi.id>=%(min)s AND ershkus_poi.id<%(max)s
        AND ((ershkus_search_addr.geom && ershkus_poi.c_geom) AND ST_Covers(ershkus_search_addr.geom, ershkus_poi.c_geom))
        AND (ershkus_search_addr.addr_type_id=35 OR ershkus_search_addr.addr_type_id=40)
        AND ershkus_poi.addr_region_id=ershkus_search_addr.region_id
        AND ershkus_poi.addr_city_id=ershkus_search_addr.city_id
        AND ershkus_poi.addr_house is null
        AND addr_full_name is null
      ;
    """, {'min': n, 'max': n+step})
  

  log.add ('set village', level=loglevel+1, file=file_log)
  cur.execute("""
    UPDATE ershkus_poi
    SET
      addr_district_id=ershkus_search_addr.district_id,
      addr_city_id=ershkus_search_addr.city_id,
      addr_in_id=ershkus_search_addr.id,
      addr_in_type=ershkus_search_addr.addr_type_id,
      addr_full_name=ershkus_search_addr.full_name,
      addr_village=ershkus_search_addr.village,
      addr_city=ershkus_search_addr.city,
      addr_district=ershkus_search_addr.district,
      addr_region=ershkus_search_addr.region,
      addr_country=ershkus_search_addr.country
    FROM
      ershkus_search_addr_p as ershkus_search_addr
    WHERE
      ((ershkus_search_addr.geom && ershkus_poi.c_geom) AND ST_Covers(ershkus_search_addr.geom, ershkus_poi.c_geom))
      AND ershkus_search_addr.addr_type_id=25
      AND ershkus_poi.addr_region_id=ershkus_search_addr.region_id
      AND ershkus_poi.addr_full_name is null
      AND ershkus_poi.id >= %(lastID)s;
  """, {"lastID": lastID})

  log.add ('set city', level=loglevel+1, file=file_log)
  cur.execute("""
    UPDATE ershkus_poi
    SET
      addr_district_id=ershkus_search_addr.district_id,
      addr_city_id=ershkus_search_addr.city_id,
      addr_in_id=ershkus_search_addr.id,
      addr_in_type=ershkus_search_addr.addr_type_id,
      addr_full_name=ershkus_search_addr.full_name,
      addr_city=ershkus_search_addr.city,
      addr_district=ershkus_search_addr.district,
      addr_region=ershkus_search_addr.region,
      addr_country=ershkus_search_addr.country
    FROM
      ershkus_search_addr_p as ershkus_search_addr
    WHERE
      ((ershkus_search_addr.geom && ershkus_poi.c_geom) AND ST_Covers(ershkus_search_addr.geom, ershkus_poi.c_geom))
      AND ershkus_search_addr.addr_type_id=20
      AND ershkus_poi.addr_region_id=ershkus_search_addr.region_id
      AND ershkus_poi.addr_full_name is null
      AND ershkus_poi.id >= %(lastID)s;
  """, {"lastID": lastID})

  log.add ('set region', level=loglevel+1, file=file_log)
  cur.execute("""
    UPDATE ershkus_poi
    SET
      addr_in_id=ershkus_search_addr.id,
      addr_in_type=ershkus_search_addr.addr_type_id,
      addr_full_name=ershkus_search_addr.full_name,
      addr_region=ershkus_search_addr.region,
      addr_country=ershkus_search_addr.country
    FROM
      ershkus_search_addr_p as ershkus_search_addr
    WHERE
      ershkus_poi.addr_region_id=ershkus_search_addr.id
      AND ershkus_search_addr.addr_type_id=10
      AND ershkus_poi.addr_full_name is null
      AND ershkus_poi.id >= %(lastID)s;
  """, {"lastID": lastID})

  # log.add ('delete addr_region is null', level=loglevel+1, file=file_log)
  # cur.execute("""
    # DELETE
      # FROM
        # ershkus_poi
      # WHERE
        # addr_region is null
        # AND ershkus_poi.id >= %(lastID)s;
  # """, {"lastID": lastID})

  # log.add ('set addr_full_name', level=loglevel+1, file=file_log)
  # cur.execute("""
    # UPDATE ershkus_poi
    # SET
      # addr_full_name =
        # rtrim(
          # CASE WHEN((addr_country is null) OR (addr_country='')) THEN '' ELSE
            # addr_country || ', ' END ||
          # CASE WHEN((addr_region is null) OR (addr_region='')) THEN '' ELSE
            # CASE WHEN addr_region LIKE addr_city THEN '' ELSE addr_region || ', ' END END ||
          # CASE WHEN((addr_district is null) OR (addr_district='')) THEN '' ELSE
            # CASE WHEN addr_district LIKE '%%' || addr_city || '%%' THEN '' ELSE addr_district || ', ' END END ||
          # CASE WHEN((addr_city is null) OR (addr_city='')) THEN '' ELSE
            # 'город ' || addr_city || ', ' END ||
          # CASE WHEN((addr_village is null) OR (addr_village='')) THEN '' ELSE
            # addr_village || ', ' END ||
          # CASE WHEN((addr_street is null) OR (addr_street='')) THEN '' ELSE
            # addr_street || ', ' END ||
          # CASE WHEN((addr_house is null) OR (addr_house='')) THEN '' ELSE addr_house || ', ' END
        # , ', ')
    # WHERE
      # ershkus_poi.id >= %(lastID)s;
  # """, {"lastID": lastID})

  log.add ('set index_name', level=loglevel+1, file=file_log)
  cur.execute("""
    UPDATE ershkus_poi
    SET
      index_name =((CASE (lower(name_ru) LIKE ( '%%' || lower(class_ru) || '%%' ))
          WHEN true THEN name_ru
          ELSE class_ru || ' , ' || (CASE name_ru is null WHEN true THEN '' ELSE name_ru END)
          END) || ' , ' || ershkus_search_addr.index_name)
      --index_name = (ershkus_poi.class_ru || ' , ' || ershkus_poi.name_ru || ' , ' || ershkus_poi.addr_full_name)
    FROM
      ershkus_search_addr
    WHERE
      ershkus_poi.addr_in_id = ershkus_search_addr.id
      AND addr_region is not null
      AND ershkus_poi.id >= %(lastID)s;
  """, {"lastID": lastID})

  log.add ('end set addr', level=loglevel, file=file_log)


def insertPOI(conn, loglevel=0):
  log.add ('start insert poi', level=loglevel, file=file_log)

  cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

  # time последнего объекта и удаленные
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

  catalog(cur, loglevel=loglevel+1)
  addr(cur, loglevel=loglevel+1)
  conn.commit()

  # сохраним текущую позицию
  utils.saveDate(whereTime=whereTime, file=fupd_time, key_config='datepoi')
  # f = open(conf.workactual + fupd_time,'w')
  # f.write(json.dumps(whereTime, default=utils.jsondumps))
  # f.close()

  log.add ('end insert poi', level=loglevel, file=file_log)


def updatePOI(conn, loglevel=0):
  log.add ('start update poi', level=loglevel, file=file_log)

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
    FROM ershkus_poi
  """)
  lastID = cur.fetchone()['maxid']+1


  # удаление удаленных ПОИ
  cur.execute("""
    DELETE
      FROM ershkus_poi
      WHERE
        osm_id in
          (
            SELECT
              type || osm_id
            FROM
              deleted_entries
            WHERE
              deleted_at>=%(ldel)s AND deleted_at<=%(ndel)s
          )
  """, whereTime)

  # удаление изменёных ПОИ
  cur.execute("""
    DELETE
      FROM ershkus_poi
      WHERE
        osm_id in
          (
            SELECT
              'n' || id
            FROM
              nodes
            WHERE
              (tstamp>=%(lupd)s AND tstamp<=%(nupd)s)
          )
  """, whereTime)
  cur.execute("""
    DELETE
      FROM ershkus_poi
      WHERE
        osm_id in
          (
            SELECT
              'w' || id
            FROM
              ways
            WHERE
              (tstamp>=%(lupd)s AND tstamp<=%(nupd)s)
              OR (updated_at>=%(lupd)s AND updated_at<=%(nupd)s)
          )
  """, whereTime)
  cur.execute("""
    DELETE
      FROM ershkus_poi
      WHERE
        osm_id in
          (
            SELECT
              'r' || id
            FROM
              relations
            WHERE
              (tstamp>=%(lupd)s AND tstamp<=%(nupd)s)
              OR (updated_at>=%(lupd)s AND updated_at<=%(nupd)s)
          )
  """, whereTime)

  catalog(cur, True, whereTime, loglevel=loglevel+1)
  addr(cur, lastID, loglevel=loglevel+1)
  conn.commit()

  # сохраним текущую позицию
  utils.saveDate(whereTime=whereTime, file=fupd_time, key_config='datepoi')
  # log.add ('save time', level=loglevel+1, file=file_log)
  # f = open(conf.workactual + fupd_time,'w')
  # f.write(json.dumps(whereTime, default=utils.jsondumps))
  # f.close()


  log.add ('end update poi', level=loglevel, file=file_log)

# lang only "lang[0]"
def createTree(loglevel=0):
  log.add ('start createTree', level=loglevel, file=file_log)
  def fnChildren(catalog_json, dictionary_json, parent=''):
    data = []
    for icat in catalog_json:
      if len(icat['parent']) == 0:
        icat['parent'] = ['']
      for ipar in icat['parent']:
        if ipar == parent:
          rec={}
          rec['data'] = dictionary_json['catalog'][icat['name']]['name']
          rec['attr'] = {'nclass':icat['name']}
          children = fnChildren(
                          catalog_json=catalog_json,
                          dictionary_json=dictionary_json,
                          parent=icat['name'])
          if (children):
            rec['children'] = children
          data.append(rec)
    data.sort(key=lambda x: x['data'])
    return data
  
  with codecs.open(conf.file_catalog_json, 'rt', encoding='utf-8') as f:
    catalog_json = json.load(f)
  for fd in os.listdir(conf.path_dictionary_json):
    with codecs.open(conf.path_dictionary_json+fd, 'rt', encoding='utf-8') as f:
      fjson = json.load(f)
      if (fjson['language'] == lang[0]):
        dictionary_json = fjson
        break

  datajson = fnChildren(catalog_json=catalog_json,
                        dictionary_json=dictionary_json)

  datajson = [{'data':u'выбрать все', 'attr':{'class':'root', 'nclass':'root'}, 'state':'open', 'children':datajson}]
  
  write_file = open(conf.file_tree_json, "w")
  write_file.write(json.dumps(datajson))
  log.add ('end createTree', level=loglevel, file=file_log)

def createListPermalink(loglevel=0):
  log.add ('start createListPermalink', level=loglevel, file=file_log)
  with codecs.open(conf.file_catalog_json, 'rt', encoding='utf-8') as f:
    catalog_json = json.load(f)
  with codecs.open(conf.file_listPerm_json, 'rt', encoding='utf-8') as f:
    listPerm_json = json.load(f)
  
  for icat in catalog_json:
    if listPerm_json.count(icat['name']) == 0:
      listPerm_json.append(icat['name'])
  
  write_file = open(conf.file_listPerm_json, "w")
  write_file.write(json.dumps(listPerm_json))
  
  log.add ('end createListPermalink', level=loglevel, file=file_log)
  
def findImgMarker(loglevel=0):
  log.add ('start findImgMarker', level=loglevel, file=file_log)
  markers = []
  for fd in os.listdir(conf.path_markers):
    markers.append(fd[:-4])
  
  write_file = open(conf.file_marker_json, "w")
  write_file.write(json.dumps(markers))
  log.add ('end findImgMarker', level=loglevel, file=file_log)
  
  
def main():
  try:
    parser = argparse.ArgumentParser(add_help=True, version='0.1')
    parser.add_argument('action', metavar='action', type=str, choices=['insert', 'update', 'createTree'], help='action operations `insert` or `update` or `createTree`')
    args = parser.parse_args()

    log.add ('start main', file=file_log)
    conn = psycopg2.connect(host=conf.addrfull_host, database=conf.addrfull_database, user=conf.addrfull_user, password=conf.addrfull_password)



    if args.action == 'insert':
      insertPOI(conn, loglevel = 1)
    elif args.action == 'update':
      updatePOI(conn,  loglevel = 1)
    elif args.action == 'createTree':
      createTree(loglevel = 1)
      createListPermalink(loglevel = 1)
      findImgMarker(loglevel = 1)




    log.add ('end main', file=file_log)
  except :
    log.add ('! error: '+str(traceback.format_exc()), file=file_log)


if __name__ == '__main__':
  main()