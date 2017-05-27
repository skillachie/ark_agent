import os
import sys
from finsymbols import symbols


from mongo_util import MongoDBUtil
db = MongoDBUtil()

def initial_symbols_load(initial=True):

    sp500 = symbols.get_sp500_symbols()

    if db.has_symbols():
        print("Symbols already loaded")
    else:
        print("Loading initial symbols")
        db.load_symbols(sp500)

def add_single_symbols(symbol):
    #TODO if you need to collect EOD for symbol not in sp500
    pass

if __name__ == "__main__":
    initial_symbols_load()
