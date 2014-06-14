from __future__ import absolute_import
from ark_agent.celery import celery
from ark_agent.load_data import LoadMongoDB
from ark_agent.ark_logger import *
from celery import Task
from celery import group
import sys
import ystockquote
import finsymbols 
from time import sleep
from sets import Set
import httplib
import urllib2
    
logger = QuantLogger.get_logger(__name__)
load_mongo = LoadMongoDB()

class MongoLoadTask(Task):
  abstract = True

  def after_return(elf, status, result, task_id, args, kwargs, einfo):
      
    load_mongo.set_last_load_date(result['symbol'],result['status']) 
    
    if(result['status'] == 'success'):
      load_mongo.insert_to_db(result)


def get_symbol_set(symbols):
  symbol_set = Set()
  for symbol in symbols:
    symbol_set.add(symbol['symbol'])
  return symbol_set


@celery.task(base=MongoLoadTask)
def get_eod_data(symbol):
    '''
    Task used to obtain end of day data
    '''
    date_range = load_mongo.get_date_range(symbol)
    start_date = date_range['start_date']
    end_date = date_range['end_date']
    
    logger.info("Obtaining %s from %s to %s" %(symbol,start_date,end_date))
    
    try:
        price_data = ystockquote.get_historical_prices(symbol,start_date,end_date) 
        return {'symbol':symbol,'data': price_data,'status':'success'}
    except urllib2.HTTPError,exc: 
        logger.error({'status':'failed','symbol':symbol,'msg':str(exc),'exception':'HTTPError'})
        return {'status':'failed','symbol':symbol,'msg':str(exc)}
    except urllib2.URLError,exc:
        logger.error({'status':'failed','symbol':symbol,'msg':str(exc),'exception':'URLError'})
        return {'status':'failed','symbol':symbol,'msg':str(exc)}
    except Exception, exc:
        logger.error({'status':'failed','symbol':symbol,'msg':str(exc),'exception':'Last'})
        return {'status':'failed','symbol':symbol,'msg':str(exc)}


@celery.task(gnore_result=True)
def generate_eod_tasks():
    '''
    Task responsible for generating work items used to obtain end of day 
    data for stocks using get_eod_data() task
    '''

    symbol_sets = Set()
    
    #Gets all symbols
    sp500 = finsymbols.get_sp500_symbols()
    #amex = finsymbols.get_amex_symbols()
    #nyse = finsymbols.get_nyse_symbols()
    #nasdaq = finsymbols.get_nasdaq_symbols()
    
    #Adds all symbols to set which removes duplicates
    symbol_sets.update(get_symbol_set(sp500))
    #symbol_sets.update(get_symbol_set(amex))
    #symbol_sets.update(get_symbol_set(nyse))
    #symbol_sets.update(get_symbol_set(nasdaq))
    
    
    job = group(get_eod_data.s(symbol) for symbol in symbol_sets)
    job.apply_async() 
  
    load_mongo.done()
