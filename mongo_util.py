import os
import sys
import yaml
import pymongo
from sets import Set

class MongoDBUtil(object):

    def __init__(self):

        self.file_dir = os.path.dirname(os.path.realpath(__file__))
        self.config_dir = os.path.join(self.file_dir,os.path.join( 'configs' ))
        self.conf_file_path = os.path.abspath(self.config_dir) + os.sep + 'settings.yaml'
        self.config_file = open(self.conf_file_path,'r')
        self.config_data = yaml.load(self.config_file)
        self.client = pymongo.MongoClient(self.config_data['mongodb']['host'],self.config_data['mongodb']['port'])
        self.db = self.client[self.config_data['mongodb']['eod_db']] 


    def _create_indices(self):
        #TODO revise
        # Symbols should be unique and only occur once
        self.db['symbols'].create_index("symbol",unique=True) #No longer required

    def has_historical_data(self,symbols):
        self.symbols_collection = self.db['symbols']
        hist_sym = Set()

        for symbol in symbols:
            sym_data = self.symbols_collection.find({'symbol':symbol})
            if (sym_data.count() <= 1):
                hist_sym.add(symbol)  

        return hist_sym


    def load_symbols(self,symbols):
        self.symbols_collection = self.db['symbols']
        self.symbols_collection.insert_many(symbols)


if __name__ == "__main__":
    db = MongoDBUtil()
    db._create_indices()
