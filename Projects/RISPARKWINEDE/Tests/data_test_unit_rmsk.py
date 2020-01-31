import datetime
import os
import pandas as pd
from numpy import nan
import numpy as np


class DataTestUnitRmsk(object):

    kpi_static_data = pd.DataFrame.from_records(
        [{'client_name': u'OOS-SKU', 'pk': 1, 'type': u'OOS-SKU'},
         {'client_name': u'OOS', 'pk': 2, 'type': u'OOS'},
         {'client_name': u'Distribution', 'pk': 3, 'type': u'Distribution'},
         {'client_name': u'Could Stock - SKU', 'pk': 4, 'type': u'Could Stock - SKU'},
         {'client_name': u'Must Stock - SKU', 'pk': 5, 'type': u'Must Stock - SKU'},
         {'client_name': u'Could Stock', 'pk': 10, 'type': u'Could Stock'},
         {'client_name': u'Must Stock', 'pk': 11, 'type': u'Must Stock'},
         {'client_name': u'DirectOrder', 'pk': 12, 'type': u'DirectOrder'},
         {'client_name': u'DirectOrder - SKU', 'pk': 13, 'type': u'DirectOrder - SKU'},
         {'client_name': u'Wine Availability', 'pk': 641, 'type': u'Wine Availability'},
         {'client_name': u'Wine Availability - SKU', 'pk': 642, 'type': u'Wine Availability - SKU'}]
    )

    test_case = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'test_case.xlsx')

    store_info = pd.DataFrame.from_records([
        [{u'additional_attribute_1': u'Metro',
          u'region_fk': 3,
          u'region_name': u'Deutschland',
          u'retailer_fk': 4,
          u'retailer_name': u'Real',
          u'store_fk': 10787,
          u'store_name': u'REAL SB-WARENHAUS  8274, FLENSBURG',
          u'store_number_1': u'0004548598',
          u'store_type': u'SBW > 5.000 qm',
          u'test_store': None}]
    ])
