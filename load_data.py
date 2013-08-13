import pymongo
import datetime
import yaml
import os 

class LoadData(object):
    """
    Base class for loading data to data store
    """ 
    def __init__(self):
        pass

    @staticmethod
    def get_start_date():
        return '1817-01-01'

    def get_date(self, date):
        """
        Converts date in format $year-$month-$day to datetime.datetime
        """
        (year, month, day) = date.split('-')
        return datetime.datetime(int(year), int(month), int(day), 0, 0)

    def insert_to_dstore(self, data):
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
             eod_data['date'] = self.get_date(date)
             eod_data['symbol'] = data['symbol']
             eod_data_set.append(eod_data) 
             
        return eod_data_set

    def get_date_string(self, date):
        """
        Converts datetime object to date format required to obtain data
        """
        return "%d-%02d-%d" %(date.year,date.month,date.day)

    def set_last_load_date_dstore(self, symbol, status, initial=True):
        """
        Creates dictionary used to save the status of each symbol obtained
        """
        last_load = {
                     'symbol':symbol,
                     'load_status':{
                                    'initial':initial,
                                    'last_run_date':self.get_current_date(),
                                    'last_run_status': status
                                   }
                    }
        return last_load
    
    @staticmethod
    def get_current_date():
        """
        Gets the current system time.
        """
        now = datetime.datetime.now()
        return "%d-%02d-%d" %(now.year, now.month, now.day) 
    
    def get_end_date(self):
        return self.get_current_date()
    
    def get_date_range():
        raise NotImplementedError()
    
class LoadMongoDB(LoadData):

    def __init__(self):
        #Settings from config file
        self.module_path = os.path.dirname(os.path.realpath(__file__))
        self.config_dir = os.path.join(self.module_path,'configs')
        self.file_path = os.path.abspath(self.config_dir) + os.sep + 'mongo_settings.yaml'
        self.config_file = open(self.file_path,'r')
        self.config_data = yaml.load(self.config_file)
        
        self.client = pymongo.MongoClient(self.config_data['hostname'],self.config_data['port'])
        self.db = self.client[self.config_data['historical_database']] 

    def insert_to_db(self, data):
        """
        Inserts data into mongoDB 
        """
        eod_data_set = self.insert_to_dstore(data)
        self.collection = self.db[self.config_data['stock_collection']]
        for eod_data in eod_data_set:
            self.collection.update({'symbol':eod_data['symbol'],'date':eod_data['date']},
                                    eod_data,
                                    True )
        
    
    def get_last_load_date(self, symbol):
        data_load = self.db[self.config_data['load_status_collection']]
        load_stats = data_load.find_one({'symbol':symbol})
       
        if load_stats is None or load_stats['load_status']['initial'] is True:
            return self.get_start_date()
        elif load_stats['load_status']['initial'] is False and load_stats['load_status']['last_run_status'] == 'failed':
            return self.get_start_date() 
        elif load_stats['load_status']['initial'] is False and load_stats['load_status']['last_run_status'] == 'success':
            return load_stats['load_status']['last_run_date']

    def set_last_load_date(self, symbol, status, initial):
        load_data = self.set_last_load_date_dstore(symbol,status,initial)
        self.collection = self.db[self.config_data['load_status_collection']]
        if(status == 'success'):
            self.collection.update({'symbol':load_data['symbol']},load_data,False)
        else:
            self.collection.update({'symbol':load_data['symbol']},load_data,True)
            


    def get_date_range(self, symbol):
        """
        Gets the start and end range to run job
        """
        start_range = self.get_last_load_date(symbol)
        date_range = dict()
       
        date_range['start_date'] = start_range
        date_range['end_date'] =   self.get_end_date() 
        return date_range

    def done(self):
        self.client.close()
