from __future__ import absolute_import
from celery.schedules import crontab
from celery import Celery
import os
import yaml


file_path = os.path.dirname(os.path.realpath(__file__))
config_dir = os.path.join(file_path,os.path.join( '..' + os.sep +  'configs' ))
conf_file_path = os.path.abspath(config_dir) + os.sep + 'settings.yaml'
config_file = open(conf_file_path,'r')
config_data = yaml.load(config_file)

BROKER_URL = "mongodb://%s:%d/jobs" %(config_data['mongodb']['host'],config_data['mongodb']['port'])

#Loads settings for Backend to store results of jobs
app = Celery('ark_agent.scheduler',
		broker=BROKER_URL,
		backend=BROKER_URL,
		include=['eod_data_tasks'])  #list of modules to import when Celery starts


#Schedule Config
app.conf.update(CELERYBEAT_SCHEDULE = {
                        'every-day-at-seven': {
                        'task': 'eod_data_tasks.generate_eod_tasks',
                        #'schedule': crontab(minute=00, hour=19),
                        'schedule': 120.0,
                                        },
                    },
                    CELERY_TIMEZONE = 'US/Eastern',
                    CELERY_ACCEPT_CONTENT = ['pickle', 'json']
                    )

if __name__ == '__main__':
	celery.start()
