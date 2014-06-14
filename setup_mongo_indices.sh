mongo stocks --eval "db.eod_data.ensureIndex({'symbol':1,'date':1})"
mongo stocks --eval "db.eod_data.ensureIndex({'symbol':1})"
mongo stocks --eval "db.load_status.ensureIndex({'symbol':1})"

#Addional Indices depending on how you query your data
#mongo stocks --eval "db.eod_data.ensureIndex({'symbol':1,'date':1,'price_data.High':1})"
#mongo stocks --eval "db.eod_data.ensureIndex({'symbol':1,'date':1,'price_data.Low':1})"
#mongo stocks --eval "db.eod_data.ensureIndex({'symbol':1,'date':1,'price_data.Close':1})"
#mongo stocks --eval "db.eod_data.ensureIndex({'symbol':1,'date':1,'price_data.Open':1})"
