import pandas as pd
import pymongo
from config.envparams import Params
from config.statuslogger import LOGGER
from db.mongo import getDb, connect2db, createIndexes
from pymongo import errors
PARAM_FILE_PATH = "./params.json"
prms = Params(param_file_path=PARAM_FILE_PATH)
logger = LOGGER(status=prms.DEBUG, param_handler=prms)
data_from_excel = pd.read_excel('case.xlsx', engine='openpyxl')
mongoclient = connect2db(prms.DB_HOST_IP, prms.DB_HOST_PORT, prms.MAXSEVSELDELAY, logging=logger.log)
createIndexes(mongoclient, prms, logging=logger.log)
db = getDb(mongoclient, prms.DB, logging=logger.log)
data_coll = db[prms.COLLECTION]
result_coll = db[prms.RESULTS]


def excel_to_db(infos):
    """Read excel file and insert to DB, if it is possible."""
    for index, info in enumerate(infos.values):
        country_dict = dict()
        if pd.isna(info[0]):
            break
        elif not pd.isna(info[0]) and not info[0] == 'COUNTRY':
            country_dict['country'] = info[0]
            inner_index = 1
            while not pd.isna(info[inner_index]):
                country_dict['shipment_firm_' + str(inner_index)] = info[inner_index].replace(' $', '')
                inner_index += 1
            try:
                data_coll.insert_one(country_dict)
                logger.log.info('Country info is inserted')
                continue
            except pymongo.errors.DuplicateKeyError:
                data_coll.update_one({"country": country_dict['country']},
                                     {"$set": {
                                         'shipment_firm_' + str(inner_index-1): country_dict['shipment_firm_' + str(inner_index-1)]
                                     }})
                pass
    return db_keys_to_firm_name(infos)


def db_keys_to_firm_name(info):
    """Matching shipment firm names to dict keys"""
    inner_index = 1
    firm_name_keys = dict()
    while not pd.isna(info.values[0][inner_index]):
        firm_name_keys['shipment_firm_' + str(inner_index)] = str(info.values[0][inner_index])
        inner_index += 1
    return firm_name_keys


def read_from_db(f_keys):
    """Read data from DB."""
    country_data = data_coll.find()
    countries = dict()
    shipment_dict = dict()
    for country_obj in country_data:
        country_dict = dict()
        for key in country_obj.keys():
            if not key == '_id' and not key == 'country':
                country_dict[key] = country_obj[key]
                country_dict[key] = int(country_obj[key])
                shipment_dict[key] = prms.QUOTA
        countries[country_obj['country']] = country_dict
    shipping_from_country(countries, shipment_dict, f_keys)
    return True


def shipping_from_country(country_info, shipment_info, f_keys):
    """ Shipping process for every country to every target country"""
    countries = list(country_info.keys())
    targets = list(country_info.keys())
    q_info = shipment_info
    for country in countries:
        for target in targets:
            f_keys = f_keys
            try:
                q_info, f_keys = insert_results(country, target, country_info[country], q_info, f_keys)
            except ValueError:
                continue


def insert_results(start_point, target_point, c_info, q_info, f_keys):
    """Results are calculate and written to results collection in DB."""
    q_info, f_keys = check_quotas(q_info, f_keys)
    selection_dict = dict()
    for info in q_info:
        selection_dict[info] = c_info[info]
    min_val = min(selection_dict.values())
    selected_firm_key = list(selection_dict.keys())[list(selection_dict.values()).index(min_val)]
    selected_firm = f_keys[selected_firm_key]
    if start_point == target_point:
        return q_info, f_keys
    elif q_info[selected_firm_key] > 0:
        result_dict = dict()
        result_dict['id'] = str(start_point)+str(target_point)
        result_dict['from_country'] = start_point
        result_dict['to_country'] = target_point
        result_dict['shipment_company'] = selected_firm
        result_dict['cargo_amount'] = c_info[selected_firm_key]
        try:
            result_coll.insert_one(result_dict)
            logger.log.info("From " + start_point + " to " + target_point + " with " + selected_firm + " and amount is " +
                            str(c_info[selected_firm_key]))
            q_info[selected_firm_key] -= 1
        except pymongo.errors.DuplicateKeyError:
            pass
    return q_info, f_keys


def check_quotas(q_info, f_keys):
    """ Check quotas if it is higher than zero or not."""
    delete_keys = list()
    for quota in q_info:
        if q_info[quota] == 0:
            delete_keys.append(quota)
    for key in delete_keys:
        q_info.pop(key)
        f_keys.pop(key)
    return q_info, f_keys


if __name__ == '__main__':
    firm_keys = excel_to_db(data_from_excel)
    _ = read_from_db(firm_keys)
