import sys
import pymongo
from pymongo import MongoClient
from config.envparams import Params
from pymongo.errors import AutoReconnect
import time

PARAM_FILE_PATH = "./params.json"
prms = Params(param_file_path=PARAM_FILE_PATH)


def connect2db(host, port, maxservdelay, replicaSet=prms.DB_REPLICA_SET_NAME, logging=None):
    _client = None
    is_master = False
    retry_count = 0
    if replicaSet:
        while not is_master and retry_count < 10:
            try:
                _client = MongoClient(host=prms.DB_HOST_IP, port=prms.DB_HOST_PORT)
                time.sleep(1)
                _client.server_info()
                logging.info(f"Connected to Mongodb server.")
                if _client.is_primary:
                    logging.info(f"Connected Mongodb server is primary.")
                    is_master = True
                else:
                    logging.info(f"Connected Mongodb server is not primary. Waiting for {pow(2, retry_count)} secs.")
                    time.sleep(pow(2, retry_count))
                    retry_count += 1
            except (pymongo.errors.ServerSelectionTimeoutError, pymongo.errors.AutoReconnect) as err:
                logging.error(f"{err}. Mongodb Connection Error! I'll try again and again until I reach the success. Waiting for {pow(2, retry_count)} secs.")
                time.sleep(pow(2, retry_count))
                retry_count += 1
            except Exception as err:
                logging.error(f"{err}. Abnormal error. Watch out!")
                time.sleep(1)
                retry_count += 1

        if not is_master:
            logging.error(f"Max retry count is reached.. Exiting..")
            sys.exit(3)

    else:
        server_info_success = False
        while server_info_success is False and retry_count < 10:
            try:
                _client = MongoClient(host=host, port=port, serverSelectionTimeoutMS=maxservdelay, connectTimeoutMS=999999)
                _client.server_info()  # will raise pymongo.errors.ServerSelectionTimeoutError
                server_info_success = True
            except pymongo.errors.AutoReconnect as err:
                logging.error(f"{err}. Mongodb Connection Error! I'll try again and again until I reach the success. Waiting for {pow(2, retry_count)} secs.")
                time.sleep(pow(2, retry_count))
                retry_count += 1
            except Exception as err:
                logging.error("Timeout! %s", err)
                time.sleep(1)

    if _client is None:  # guarantee quitting..
        logging.fatal("Cannot create mongoclient to db! quitting...")
        sys.exit(1)
    return _client


def getDb(client, DB_NAME, logging=None):

    _db = client[DB_NAME]
    return _db


def createIndexes(client, prms, logging=None):

    db = getDb(client, prms.DB, logging=logging)
    _result = db[prms.COLLECTION].create_index('country', unique=True)
    logging.debug("Unique index param set as <id> for collection:{collection} : {status}".format(
        collection=prms.COLLECTION,
        status=_result))
    _result = db[prms.RESULTS].create_index('id', unique=True)
    logging.debug("Unique index param set as <id> for collection:{collection} : {status}".format(
        collection=prms.RESULTS,
        status=_result))

