from __future__ import absolute_import
from ark_agent.celery import celery
import ark_agent.load_data
from celery.result import ResultSet
import sys
sys.path.append('/Users/skillachie') 
import ystockquote
import pprint
import finsymbols #TODO create module to be used in PyPi
from time import sleep
from sets import Set
import httplib
import urllib2
import logging

logger = logging.getLogger('eod_data_pull')
logger.setLevel(logging.DEBUG)

# file to store logs to
fh = logging.FileHandler('eod_data_pull.log')
fh.setLevel(logging.DEBUG)

# set format of logger
frmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(frmt)

# add the Handler to the logger
logger.addHandler(fh)


#http://charting.nasdaq.com/ext/charts.dll?2-1-14-0-0-5999-03NA000000CSCO-&SF:4%7C5%7C8%7C27-SH:8=20%7C27=15-XTBL-
#Symbol above is CSCO
#TODO add logging for to check for symbols that are not tracked by Yahoo
#TODO task that actually goes out gets data then save it back to mongoDB

def get_symbol_set(symbols):
    symbol_set = Set()
    for symbol in symbols:
        symbol_set.add(symbol['symbol'])
    return symbol_set

#@celery.task(rate_limit='15/m')
@celery.task()
def get_eod_data(symbol, start_date,end_date):
   
    #TODO only print if log level is set to INFO
    print "Symbol is %s \n" %(symbol)
    logger.info("Obtaining %s from %s to %s" %(symbol,start_date,end_date))
    
    try:
        price_data = ystockquote.get_historical_prices(symbol,start_date,end_date) 
        return {'symbol':symbol,'data': price_data,'status':'success'}
    except urllib2.HTTPError,exc: 
        logger.debug({'status':'failed','symbol':symbol,'msg':str(exc),'exception':'HTTPError'})
        return {'status':'failed','symbol':symbol,'msg':str(exc)}
    except urllib2.URLError,exc:
        logger.debug({'status':'failed','symbol':symbol,'msg':str(exc),'exception':'URLError'})
        return {'status':'failed','symbol':symbol,'msg':str(exc)}
    except Exception, exc:
        logger.debug({'status':'failed','symbol':symbol,'msg':str(exc),'exception':'Last'})
        return {'status':'failed','symbol':symbol,'msg':str(exc)}

    #TODO: Do check for IO error and sleep process then retry task
    #TOOO: check for timeout error and then submit task for retry 

@celery.task(ignore_result=True)
def generate_eod_tasks():

    symbol_sets = Set()
    symbol_sets.add('GOOG')
    symbol_sets.add('MPG')
    symbol_sets.add('LAND')
    #Gets all symbols
    sp500 = finsymbols.get_sp500_symbols()
    #amex = finsymbols.get_amex_symbols()
    #nyse = finsymbols.get_nyse_symbols()
    #nasdaq = finsymbols.get_nasdaq_symbols()
    
    #Adds all symbols to set which removes duplicates
    #symbol_sets.update(get_symbol_set(sp500))
    #symbol_sets.update(get_symbol_set(amex))
    #symbol_sets.update(get_symbol_set(nyse))
    #symbol_sets.update(get_symbol_set(nasdaq))

    #TODO: dynamically get new start and end date here
    #TODO: Always 

    #TODO:Check Database to see if intial load was done before. If not use the starting days of the stock markte.
    #TODO: If initial load is complete, get data from current date

    #start_date = "1817-01-01"
    start_date = "2013-01-01"
    end_date = "2013-01-03"
    
    result_ids = []
    for symbol in symbol_sets:
        #TODO get start_date here 
    #    print "The symbol is ...%s " %(symbol)
        #result_id = get_eod_data.subtask((symbol,start_date,end_date))
        result_id = get_eod_data.apply_async((symbol,start_date,end_date))
        result_ids.append(result_id)

    result_data = ResultSet(result_ids)
    print "There are %s results completed \n" %(result_data.failed())
    my_results = result_data.iterate()
    for data in my_results:
        print "The data returned is %s \n" %(data)
        #TODO update database entry here
    
    #TODO: Save last completed time to collection

    #TODO: consider removing items from set
    #TODO:  When set is empty set the date as the last scanned
    #TODO: Or have each symbol updates its scan time after successful completion
    #TODO: return a list of result_ids. Then check those ID;s for success
