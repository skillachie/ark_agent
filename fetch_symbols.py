import os
import sys
from finsymbols import symbols
from mongo_util import MongoDBUtil
db = MongoDBUtil()

def initial_symbols_load(initial=True):

    sp500 = symbols.get_sp500_symbols()

    if db.has_symbols_loaded():
        print("Symbols already loaded")
    else:
        print("Loading initial symbols")
        db.load_symbols(sp500)


def add_symbols(symbols):

    symbol_list = []
    
    for symbol in symbols:
        sym_dict = dict()
        sym_dict['symbol'] = symbol

        symbol_list.append(sym_dict)

    db.load_symbols(symbol_list)

if __name__ == "__main__":
    initial_symbols_load()
