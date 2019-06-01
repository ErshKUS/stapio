#!/usr/bin/env python3
# -*- coding: utf8 -*-

# created by ErshKUS
# stapio utils

import datetime, json, psycopg2
import stapio_config as conf


def jsondumps(obj):
  if isinstance(obj, datetime.datetime):
    return obj.strftime(conf.format_datetime)

def jsonloads(d):  # source=http://stackoverflow.com/questions/455580/json-datetime-between-python-and-javascript
  if isinstance(d, list):
    pairs = enumerate(d)
  elif isinstance(d, dict):
    pairs = d.items()
  result = []
  for k,v in pairs:
    if isinstance(v, basestring):
      try:
        v = datetime.datetime.strptime(v, conf.format_datetime)
      except ValueError:
        pass
    elif isinstance(v, (dict, list)):
      v = jsonloads(v)
    result.append((k, v))
  if isinstance(d, list):
    return [x[1] for x in result]
  elif isinstance(d, dict):
    return dict(result)

def saveDate(whereTime, file, key_config):
  f = open(conf.workactual + file,'w')
  f.write(json.dumps(whereTime, default=jsondumps))
  f.close()

  if conf.saveDateForSite:
    whereTime['_key_config'] = key_config
    connsite = psycopg2.connect(host=conf.sitefull_host, database=conf.sitefull_database, user=conf.sitefull_user, password=conf.sitefull_password)
    connsite.set_client_encoding('UTF8')
    cursite = connsite.cursor()
    cursite.execute("""
      UPDATE "config" SET "value"=%(nupd)s WHERE "key"=%(_key_config)s;
      INSERT INTO "config" ("key", "value")
        SELECT %(_key_config)s, %(nupd)s
        WHERE NOT EXISTS (SELECT 1 FROM "config" WHERE "key"=%(_key_config)s);
    """, whereTime)
    connsite.commit()
