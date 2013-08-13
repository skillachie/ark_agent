from __future__ import absolute_import
from celery.schedules import crontab
from celery import Celery


#TODO move hostname and port to configfile
#Specify mongodb host and datababse to connect to
BROKER_URL = 'mongodb://localhost:27017/jobs'

'''
class Config:
    CELERYBEAT_SCHEDULE = {
            'every-minute': {
             'task': 'stock_eod_data.generate_eod_tasks',
             'schedule': crontab(minute='*/1'),
             'args': (),
                          },
    }

    
    CELERY_ENABLE_UTC = True
    CELERY_TIMEZONE = 'Europe/London'

celery.config_from_object(Config)






CELERYBEAT_SCHEDULE = {
    'every-minute': {
    'task': 'tasks.add',
    'schedule': crontab(minute='*/1'),
    'args': (1,2),
                    },
}
'''
celery = Celery('ark_agent.celery',
		broker=BROKER_URL,
		backend='mongodb://localhost:27017/jobs',
		include=['ark_agent.stock_eod_data'])  #list of modules to import when Celery starts



#Loads settings for Backend to store results of jobs
celery.conf.update(CELERYBEAT_SCHEDULE = {
                        'every-day': {
                        'task': 'ark_agent.stock_eod_data.generate_eod_tasks',
                        'schedule': crontab(minute='*/1'),
                                        },
                    },)

if __name__ == '__main__':
	celery.start()
