
from Trax.Algo.Calculations.Core.DataProvider import Data

from Projects.CCBOTTLERSUS_SAND.REDSCORE.Const import Const

class Checks(object):
    def __init__(self,data_provider):
        self.store_info = data_provider[Data.STORE_INFO]


    def check_store(self, template):

        store_type = self.store_info['store_type'].iloc[0]

        if template[Const.STORE_TYPE].find(',') > 0:
            store_types_list = [str(item.strip()) for item in template[Const.STORE_TYPE].split(',')]
            for store in store_types_list:
                return True if store_type == store else False
        else:
            return True if store_type == template[Const.STORE_TYPE] else False