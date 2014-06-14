mongo stocks --eval "db.eod_data.ensureIndex({'symbol':1,'date':1})"
mongo stocks --eval "db.eod_data.ensureIndex({'symbol':1})"
mongo stocks --eval "db.load_status.ensureIndex({'symbol':1})"
