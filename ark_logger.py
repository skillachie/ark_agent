import logging

class QuantLogger(object):

    @staticmethod 
    def get_logger(log_name): 
        logger = logging.getLogger(log_name)
        logger.setLevel(logging.INFO)
        
        log_file = '/tmp/'+ log_name + '.log'

        fh =  logging.handlers.RotatingFileHandler(log_file, maxBytes=9242880, backupCount=7)
        fh.setLevel(logging.INFO)

        # set format of logger
        frmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(frmt)

        # add the Handler to the logger
        logger.addHandler(fh)

        return logger
