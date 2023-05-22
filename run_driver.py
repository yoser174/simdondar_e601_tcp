###############################
# run_driver.py
#
# Desc: Running driver menggunakan format minimal tanpa threading
#
# Auth: Yose
# Date: 4 Mei 2017
#

import os
from cobas6k import cobas6k
import logging.config
import yaml
import configparser


PORT_COM = 'COM0'
SERVER = '127.0.0.1'
DB_OFFLINE = True
VERSION = '0.0.2'

message = """
With running this program you aggreed to share data
for quality improvemnet and bug hunting.
Please read README.txt for licenses
"""

config = configparser.ConfigParser()
config.read('run_driver.ini')
SERVER = config.get('General','server')
TCP_HOST = config.get('General','tcp_host')
TCP_PORT = config.get('General','tcp_port')
DB_OFFLINE = config.get('General','db_offline')

if DB_OFFLINE=='True':
    DB_OFFLINE = True
else:
    DB_OFFLINE = False

DEV = False



def main():
    logging.info('VERSION [%s]' % VERSION)
    logging.info('TCP_HOST:%s' % TCP_HOST)
    logging.info('TCP_PORT:%s' % TCP_PORT)
    logging.info('SERVER SIMDONDAR:%s' % SERVER)
    if DEV:
        logging.info('trying to run driver [DEV MODE]')
        con = cobas6k(tcp_host = TCP_HOST, tcp_port = TCP_PORT, server = SERVER, db_offline=DB_OFFLINE)
        logging.info('Run driver..')
        con.open()
    else:
        try:
            logging.info('trying to run driver...')
            con = cobas6k(tcp_host = TCP_HOST, tcp_port = TCP_PORT, server = SERVER, db_offline=DB_OFFLINE)
            logging.info('Run driver..')
            con.open()
        except Exception as e:
            logging.error('Failed [%s]' % str(e))

if __name__ == "__main__":
    with open('run_driver.yaml', 'rt') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    logging.info('Starting program.')
    main()
