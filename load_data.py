import pymongo
import pprint
import datetime

class LoadData(object):
    "Used as abstract class for loading data to data store"
    
    start_date = '1817-01-01'
    
    def __init__(self):
        pass


    def get_date(self,date):
        (year,month,day) = date.split('-')
        return datetime.date(int(year),int(month),int(day))


    def insert_to_dstore(self,data):
        eod_data_set = []

        for date in data['data'].keys():
             eod_data = dict()
             eod_data['price_data'] = data['data'][date]
             eod_data['date'] = self.get_date(date)
             eod_data['symbol'] = data['symbol']
             eod_data_set.append(eod_data) 
             
        return eod_data_set

    def get_last_load_date(self,symbol):
        raise NotImplementedError()

    def set_last_load_date_dstore(self,symbol,raw_date,status):
        last_load = {
                     'symbol':symbol,
                     'load_status':{
                                    'initial':True,
                                    'last_run_date':self.get_date(raw_date),
                                    'last_run_status': status
                                   }
                    }
        return last_load
    
    @staticmethod
    def get_current_date():
        now = datetime.datetime.now()
        return "%d-%d-%d" %(now.year, now.month, now.day) 
    
    #Checks the datababse for initial load status 
    #Set static variables for start_date and end_date
    #Uses those static variables if initial load is not present
    #If initial load occurred, get last_updated_date and use current_date as end_date
    def get_date_range():
        raise NotImplementedError()
    
class LoadMongoDB(LoadData):

    def __init__(self):
        #TODO read all variables  from  config file
        self.client = pymongo.MongoClient('localhost',27017)
        self.db = self.client['stocks'] 

    #Redefine collection used here
    def insert_to_db(self,data):
        self.collection = self.db['eod_data']
        eod_data_set = self.insert_to_dstore(data)
        pprint.pprint(eod_data_set)
        #TODO insert to mongodb here.Do upsert based on symbol and date 
        
        #raise NotImplementedError()
    
    def get_last_load_date(self,symbol):
        self.data_load = self.db['data-load']
        load_stats = self.data_load.find_one({'symbol':symbol})
        
        if load_stats is None or load_stats.load_status.initial is False:
            print "initial load has not occured\n"
            return self.start_date
        elif load_status.initial is True and load_status.last_run_status == 'failed':
            print 'initial load occurrred but failed '
        elif load_status.initial is True and load_status.last_run_status == 'success':
            print 'initial load occurrred and was successful '
            self.get_current_date()

        #TODO check if initial load has occured. It it has not return start_date
        # If it  occurred and was successful return the current date, else  if it failed return last date

    def set_last_load_date(self,symbol,raw_date,status):
        load_data = set_last_load_date_dstore(symbol,raw_date,status)
        pprint.pprint(load_data)
        #TODO insert into mongoDB here. Lookup and find 
    
    def get_date_range(self,symbol):
        raise NotImplementedError()
