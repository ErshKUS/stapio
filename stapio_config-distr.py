#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# STAPIO CONFIGURATION
#

# ! все пути до каталога должны оканчиватся на '/'

# global
format_datetime = '%Y-%m-%d %H:%M:%S'

file_log = 'stapio_run.log'
workdir = '/stapio/' # путь до каталога stapio (путь в конце с /)
workactual = '/stapio/actual/' # путь до каталога дат актуализации данных (путь в конце с /)
logdir = '/stapio/log/' # путь до каталога логов stapio (путь в конце с /)
tempdir = '/tmp/ershkus_osmosis/' # путь до каталога временных файлов, в основном osmosis-а (путь в конце с /)
authFileOsmosis = '/stapio/.authOsmosis' # файл аутентификации postgresql
runAfter = '' # команда выпоняемая после работы, например очистка temp файлов после работы osmosis

#osmosisExport = 'JAVACMD_OPTIONS="-Djava.io.tmpdir=' + workdir + 'tmp"' # export для java у osmosis. "Djava.io.tmpdir" указывает где складывать temp
osmosisExport = '' # или оставить пустой


# sphinx
sphinx_reindex = True # делать реиндекс сфинкса по окончании работы
cmdindexrotate = 'indexer --all --rotate' # строка реиндекса


# osmCatalog
file_catalog_json = '/osmCatalog/catalog.json' # catalog.json
path_dictionary_json = '/osmCatalog/dictionary/' # путь до dictionary (путь в конце с /)
file_tree_json = '/stapio/poi/poidatatree.json' # poidatatree.json (обычно stapio/poi/poidatatree.json)
file_marker_json = '/stapio/poi/poimarker.json' # poimarker.json (обычно stapio/poi/poimarker.json)
path_markers = '/osm.ru/www/img/poi_marker/' # путь до иконок маркеров poi_marker (для генерации poimarker.json)
file_listPerm_json = '/stapio/poi/poidatalistperm.json' # poidatalistperm.json (обычно stapio/poi/poidatalistperm.json)


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

# права на insert, update, delete, select
addrfull_host = 'localhost'
addrfull_database = 'osm_simple'
addrfull_user = 'ershkus'
addrfull_password = ''

# for saveDateForSite
# права на insert, update, select (можно только к одной таблице 'config')
sitefull_host = 'localhost'
sitefull_database = 'osmru_web'
sitefull_user = 'ershkus'
sitefull_password = ''
