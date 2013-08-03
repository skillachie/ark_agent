class LoadData(object):

    def insert_to_db(self):
        raise NotImplementedError()

    def get_last_load_date(self,symbol):
        raise NotImplementedError()

    def set_last_load_date(self,symbol,date):
        raise NotImplementedError()
    
    #Make private class method 
    def get_current_date(self):
        raise NotImplementedError()
    
    #Checks the datababse for initial load status 
    #Set static variables for start_date and end_date
    #Uses those static variables if initial load is not present
    #If initial load occurred, get last_updated_date and use current_date as end_date
    def get_date_range():
        raise NotImplementedError()
    
class LoadMongoDB(LoadData):

    def insert_to_db(self,data):
        raise NotImplementedError()

    def get_last_load_date(self,symbol):
        raise NotImplementedError()

    def set_last_load_date(self,data):
        raise NotImplementedError()

    def get_date_range():
        raise NotImplementedError()
