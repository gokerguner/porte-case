import json


class Params(object):
    """
    Define class wide objects to __dict__

    see here https://www.toptal.com/python/python-class-attributes-an-overly-thorough-guide

    """

    DEBUG = None
    DEBUG_LOG_FILE = None
    DEBUG_LOG_LEVEL = None
    DIR_DATA = None
    DB_HOST_IP = None
    DB_HOST_PORT = None
    DB_NAME = None
    DB_REPLICA_SET_NAME = None
    DB = None
    MAXSEVSELDELAY = None

    def __init__(self, param_file_path="./params.json"):
        # in object level this class wide objects can be changed
        self._jparams = None
        self.load_params(param_file_path)

    def set_debug_status(self, status):
        self.DEBUG = status

    def set_param(self, param_name, new_val):
        if param_name in self._jparams:
            setattr(self, param_name, new_val)
            return True
        else:
            return False

    def load_params(self, param_file_path):
        """
        parameter initialization (param.json and defaults)

        """
        self._jparams = self.load_json_obj(param_file_path)
        if "DB" in self._jparams:
            self.DB = self._jparams["DB"]
        else:
            self.DB = "case"

        if "MAXSEVSELDELAY" in self._jparams:
            self.MAXSEVSELDELAY = self._jparams["MAXSEVSELDELAY"]
        else:
            self.MAXSEVSELDELAY = 1000

        if "DB_REPLICA_SET_NAME" in self._jparams:
            self.DB_REPLICA_SET_NAME = self._jparams["DB_REPLICA_SET_NAME"]
        else:
            self.DB_REPLICA_SET_NAME = None

        if "DB_HOST_IP" in self._jparams:
            self.DB_HOST_IP = self._jparams["DB_HOST_IP"]
        else:
            self.DB_HOST_IP = "127.0.0.1"

        if "DB_HOST_PORT" in self._jparams:
            self.DB_HOST_PORT = self._jparams["DB_HOST_PORT"]
        else:
            self.DB_HOST_PORT = 27017

        if "QUOTA" in self._jparams:
            self.QUOTA = self._jparams["QUOTA"]
        else:
            self.QUOTA = 10

        if "COLLECTION" in self._jparams:
            self.COLLECTION = self._jparams["COLLECTION"]
        else:
            self.COLLECTION = "countries"

        if "RESULTS" in self._jparams:
            self.RESULTS = self._jparams["RESULTS"]
        else:
            self.RESULTS = "results"

    @staticmethod
    def load_json_obj(param_file_path):
        '''Load json object from PARAM_FILE_PATH file'''
        json_data = open(param_file_path)
        _jparams = json.load(json_data)
        json_data.close()
        return _jparams