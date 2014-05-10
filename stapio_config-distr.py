#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# STAPIO CONFIGURATION
#

# ! все пути до каталога должны оканчиватся на '/'

# global
format_datetime = '%Y-%m-%d %H:%M:%S'

stapio_dir = '/stapio/'

file_log = 'stapio_run.log'
workdir = '/stapio/' # путь до каталога stapio (путь в конце с /)
workactual = stapio_dir + 'actual/' # путь до каталога дат актуализации данных (путь в конце с /)
logdir = stapio_dir + 'log/' # путь до каталога логов stapio (путь в конце с /)
tempdir = '/tmp/stapio/' # путь до каталога временных файлов, в основном osmosis-а (путь в конце с /)
authFileOsmosis = stapio_dir + 'auth_osmosis' # файл аутентификации postgresql
runAfter = '' # команда выпоняемая после работы, например очистка temp файлов после работы osmosis


# sphinx
sphinx_reindex = True # делать реиндекс сфинкса по окончании работы
cmdindexrotate = 'indexer --all --rotate' # строка реиндекса


# osmCatalog
osm_catalog = '/osmCatalog/'
file_catalog_json = osm_catalog + 'catalog.json' # catalog.json
path_dictionary_json = osm_catalog + 'dictionary/' # путь до dictionary (путь в конце с /)
file_tree_json = stapio_dir + 'poi/poidatatree.json' # poidatatree.json (обычно stapio/poi/poidatatree.json)
file_marker_json = stapio_dir + 'poi/poimarker.json' # poimarker.json (обычно stapio/poi/poimarker.json)
path_markers = '/osm.ru/www/img/poi_marker/' # путь до иконок маркеров poi_marker (для генерации poimarker.json)
file_listPerm_json = stapio_dir + 'poi/poidatalistperm.json' # poidatalistperm.json (обычно stapio/poi/poidatalistperm.json)


# addr config
use_country = True # *old*
use_addr_save = True # использовать сохраненные данные по странам и регионам (область, край, ...)


# load file
urlpbf = ['http://data.gis-lab.info/osm_dump/dump/latest/RU-KGD.osm.pbf']
#urlpbf = ['http://data.gis-lab.info/osm_dump/dump/latest/RU.osm.pbf', 'http://data.gis-lab.info/osm_dump/dump/latest/UA.osm.pbf', 'http://data.gis-lab.info/osm_dump/dump/latest/BY.osm.pbf']

urlpbfmeta = 'http://data.gis-lab.info/osm_dump/dump/latest/RU-KGD.osm.pbf.meta'
# urlpbfmeta = 'http://data.gis-lab.info/osm_dump/dump/latest/RU.osm.pbf.meta'

# urlmaskosc = ['http://data.gis-lab.info/osm_dump/diff/RU/RU-%(daystart)s-%(dayend)s.osc.gz','http://data.gis-lab.info/osm_dump/diff/UA/UA-%(daystart)s-%(dayend)s.osc.gz','http://data.gis-lab.info/osm_dump/diff/BY/BY-%(daystart)s-%(dayend)s.osc.gz']


# for logger
write_in_file = True
write_in_singlefile = True
singlefile_log = 'singlestapio.log'
email_send = False
email_from = '' # stapio@server.ru
email_to = '' # ershkus@server.ru
email_send_finish = False
smtp_server = '' # server.ru
email_pass = '' #
email_head_error = 'stapio error'
email_head_finish = 'stapio finish'


# utils
saveDateForSite = True


# steps
nodeStep = 2500000
wayStep = 1000000
GeomInStep = 5000


#
# DATABASE CONFIGURATION
#

db_host = "localhost"
db_name = "stapio_testing"
db_user = "stapio_user"
db_password = "stapio_password"

addr_table = 'addr'
addr_p_table = 'addr_p'
poi_table = 'poi'
addr_upd_table = 'addr_street_upd'
deleted_entries_table = 'deleted_entries'

# права на insert, update, delete, select
addrfull_host = db_host
addrfull_database = 'osm_simple'
addrfull_user = db_user
addrfull_password = db_password

# for saveDateForSite
# права на insert, update, select (можно только к одной таблице 'config')
sitefull_host = db_host
sitefull_database = 'osmru_web'
sitefull_user = db_user
sitefull_password = db_password
