import os
import sys
import yaml
import pymongo

try:
    set
except NameError:
    from sets import Set as set

import datetime

class MongoDBUtil(object):

    def __init__(self):

        self.file_dir = os.path.dirname(os.path.realpath(__file__))
        self.config_dir = os.path.join(self.file_dir,os.path.join( 'configs' ))
        self.conf_file_path = os.path.abspath(self.config_dir) + os.sep + 'settings.yaml'
        self.config_file = open(self.conf_file_path,'r')
        self.config_data = yaml.load(self.config_file)
        self.client = pymongo.MongoClient(self.config_data['mongodb']['host'],self.config_data['mongodb']['port'])
        self.db = self.client[self.config_data['mongodb']['eod_db']]


        # Create indices on first run and update conf file
        if not self.config_data['mongodb']['indices_created']:
            self._create_indices()
            self.config_data['mongodb']['indices_created'] = True
            with open(self.conf_file_path, "w") as conf_fh:
                    yaml.dump(self.config_data, conf_fh)


    def _create_indices(self):
        # Symbols should be unique and only occur once
        self.db['eod_data'].create_index("symbol") 
        self.db['eod_data'].create_index([("symbol",pymongo.DESCENDING),("date", pymongo.DESCENDING)],unique=True)
        self.db['eod_data'].create_index([("symbol", pymongo.DESCENDING),("price_data.High", pymongo.DESCENDING)])
        self.db['eod_data'].create_index([("symbol", pymongo.DESCENDING),("price_data.AdjClose", pymongo.DESCENDING)])
        self.db['eod_data'].create_index([("symbol", pymongo.DESCENDING),("price_data.Low", pymongo.DESCENDING)])
        print('Created indices')

    def has_historical_data(self,symbols):
        self.symbols_collection = self.db['symbols']
        hist_sym = set()

        for symbol in symbols:
            sym_data = self.symbols_collection.find({'symbol':symbol})
            if (sym_data.count() <= 1):
                hist_sym.add(symbol) 

        return hist_sym


    def _format_date(self, date):
        """
        Converts date in format $year-$month-$day to datetime.datetime
        """
        (year, month, day) = date.split('-')
        return datetime.datetime(int(year), int(month), int(day), 0, 0)


    def _structure_data(self, data):
        """
        Creates dictonary structure to be used by datastore, to store data 
        """

        eod_data_set = []

        for date in data['data'].keys():
             eod_data = dict()
 
             #Change value type
             data['data'][date]['High'] = float(data['data'][date]['High'])
             data['data'][date]['AdjClose'] =  float(data['data'][date]['Adj Close'])
             del data['data'][date]['Adj Close']
             data['data'][date]['Volume'] =  float(data['data'][date]['Volume'])
             data['data'][date]['Low'] =  float(data['data'][date]['Low'])
             data['data'][date]['Close'] =  float(data['data'][date]['Close'])
             data['data'][date]['Open'] =  float(data['data'][date]['Open'])

             eod_data['price_data'] = data['data'][date]
             eod_data['date'] = self._format_date(date)
             eod_data['symbol'] = data['symbol']
             eod_data_set.append(eod_data) 
             
        return eod_data_set

    def insert_to_db(self, data):
        """
        Inserts data into mongoDB 
        """
        eod_data_set = self._structure_data(data)

        self.collection =  self.db['eod_data']
        for eod_data in eod_data_set:
            self.collection.update({'symbol':eod_data['symbol'],'date':eod_data['date']},
                                    eod_data,
                                    True )
            



if __name__ == "__main__":
    db = MongoDBUtil()
