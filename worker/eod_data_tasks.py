import os
import sys
import finsymbols
from sets import Set
import datetime

file_path = os.path.dirname(os.path.realpath(__file__))
libs_path = os.path.join(file_path,os.path.join('..' + os.sep  ))
sys.path.append(libs_path)
from scheduler import app

#NOTE Only run on Business days or days when market is open

## TEMP
import yaml
config_dir = os.path.join(file_path,os.path.join( '..' + os.sep +  'configs' ))
conf_file_path = os.path.abspath(config_dir) + os.sep + 'settings.yaml'
config_file = open(conf_file_path,'r')
config_data = yaml.load(config_file)
from mongo_util import MongoDBUtil


from celery import Task
from celery import group
import sys
import ystockquote
#import httplib # Python2
#import http.client as httplib
import urllib2 # Fix for py2 and py3
from celery.utils.log import get_task_logger
import logging 

logger = get_task_logger(__name__)

# create a file handler
handler = logging.FileHandler('tasks.log')
handler.setLevel(logging.INFO)
# add the handlers to the logger
logger.addHandler(handler)


class MongoLoadTask(Task):
  abstract = True
 

  def after_return(self, status, result, task_id, args, kwargs, einfo):

    if(result['status'] == 'success'):
        db = MongoDBUtil()
        logger.info('Inserting data into database {0}'.format(result['symbol'],))
        db.insert_to_db(result)

    if(result['status'] == 'failed'):
       logger.error('Failed to obtain data for {0} retrying after 30 minutes'.format(result['symbol'],))
       raise self.retry(countdown=1800)



@app.task(base=MongoLoadTask)
def get_eod_data(symbol,start_date,end_date):
    '''
    Task used to obtain end of day data
    '''

    try:
        price_data = ystockquote.get_historical_prices(symbol,start_date,end_date)
        logger.info('Obtained data for {0} from {1} to {2}'.format(symbol,start_date,end_date))
        return {'symbol':symbol,'data': price_data,'status':'success'}
    except urllib2.HTTPError as exc:
        logger.error({'status':'failed','symbol':symbol,'msg':str(exc),'exception':'HTTPError'})
        return {'status':'failed','symbol':symbol,'msg':str(exc)}
    except urllib2.URLError as exc:
        logger.error({'status':'failed','symbol':symbol,'msg':str(exc),'exception':'URLError'})
        return {'status':'failed','symbol':symbol,'msg':str(exc)}
    except Exception as exc:
        logger.error({'status':'failed','symbol':symbol,'msg':str(exc),'exception':'Last'})
        return {'status':'failed','symbol':symbol,'msg':str(exc)}



def _get_symbol_set(symbols):
  symbol_set = Set()
  for symbol in symbols:
    symbol_set.add(symbol['symbol'])
  return symbol_set



@app.task(gnore_result=True)
def generate_eod_tasks():
    '''
    Task responsible for generating work items used to obtain end of day
    data for stocks using get_eod_data() task
    '''
    db = MongoDBUtil()
    symbol_sets = Set()

    #Gets all symbols
    sp500 = finsymbols.get_sp500_symbols()
    amex = finsymbols.get_amex_symbols()
    nyse = finsymbols.get_nyse_symbols()
    nasdaq = finsymbols.get_nasdaq_symbols()

    #Adds all symbols to set which removes duplicates
    symbol_sets.update(_get_symbol_set(sp500))
    symbol_sets.update(_get_symbol_set(amex))
    symbol_sets.update(_get_symbol_set(nyse))
    symbol_sets.update(_get_symbol_set(nasdaq))

    now = datetime.datetime.now()
    end_date = '-'.join([str(now.year),str(now.month),str(now.day)])

    his_symbols = db.has_historical_data(symbol_sets)
    if(len(his_symbols) >= 1):
        start_date = '1980-01-01'
        hist_job = group(get_eod_data.s(symbol,start_date,end_date) for symbol in symbol_sets)
        hist_job.apply_async()

    # Obtain data for current date
    job = group(get_eod_data.s(symbol,end_date,end_date) for symbol in symbol_sets)
    job.apply_async()
