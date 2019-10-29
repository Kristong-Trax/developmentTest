import datetime
import os
import pandas as pd
from numpy import nan
import numpy as np


class DataTestUnitNestleil(object):

    kpi_static_data = pd.DataFrame.from_records(
        [{'pk': 2000, 'type': u'Distribution'}, {'pk': 2001, 'type': u'Distribution - SKU'},
         {'pk': 2002, 'type': u'OOS'}, {'pk': 2003, 'type': u'OOS - SKU'}, {'pk': 2004, 'type': u'Distribution Snacks'},
         {'pk': 2005, 'type': u'Distribution Sabra'}, {'pk': 2006, 'type': u'Distribution Snacks - SKU'},
         {'pk': 2007, 'type': u'Distribution Sabra - SKU'}, {'pk': 2008, 'type': u'OOS Snacks'},
         {'pk': 2009, 'type': u'OOS Sabra'}, {'pk': 2010, 'type': u'OOS Snacks - SKU'},
         {'pk': 2011, 'type': u'OOS Sabra - SKU'}]
    )

    kpi_results_values_table = pd.DataFrame.from_records(
        [{'pk': 1, 'value': 'OOS', 'kpi_result_type_fk': 1}, {'pk': 2, 'value': 'DISTRIBUTED', 'kpi_result_type_fk': 1},
         {'pk': 3, 'value': 'EXTRA', 'kpi_result_type_fk': 1}]
    )

    test_case_1 = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'test_case_1.xlsx')

    assortment_res = pd.DataFrame.from_records(
        [{"additional_attributes": "{}",
          "assortment_fk": 40,
          "assortment_group_fk": 36,
          "assortment_super_group_fk": "nan",
          "facings": 0.0,
          "group_target_date": "nan",
          "in_store": 0,
          "kpi_fk_lvl1": "nan",
          "kpi_fk_lvl2": 2004,
          "kpi_fk_lvl3": 2006,
          "product_fk": 152,
          "super_group_target": "None",
          "target": "nan"},
         {"additional_attributes": "{}",
          "assortment_fk": 40,
          "assortment_group_fk": 36,
          "assortment_super_group_fk": "nan",
          "facings": 7.0,
          "group_target_date": "nan",
          "in_store": 1,
          "kpi_fk_lvl1": "nan",
          "kpi_fk_lvl2": 2004,
          "kpi_fk_lvl3": 2006,
          "product_fk": 153,
          "super_group_target": "None",
          "target": "nan"},
         {"additional_attributes": "{}",
          "assortment_fk": 40,
          "assortment_group_fk": 36,
          "assortment_super_group_fk": "nan",
          "facings": 0.0,
          "group_target_date": "nan",
          "in_store": 1,
          "kpi_fk_lvl1": "nan",
          "kpi_fk_lvl2": 2004,
          "kpi_fk_lvl3": 2006,
          "product_fk": 157,
          "super_group_target": "None",
          "target": "nan"},

         {"additional_attributes": "{}",
          "assortment_fk": 41,
          "assortment_group_fk": 37,
          "assortment_super_group_fk": "nan",
          "facings": 0.0,
          "group_target_date": "nan",
          "in_store": 0,
          "kpi_fk_lvl1": "nan",
          "kpi_fk_lvl2": 2005,
          "kpi_fk_lvl3": 2007,
          "product_fk": 253,
          "super_group_target": "None",
          "target": "nan"},
         {"additional_attributes": "{}",
          "assortment_fk": 41,
          "assortment_group_fk": 37,
          "assortment_super_group_fk": "nan",
          "facings": 3.0,
          "group_target_date": "nan",
          "in_store": 1,
          "kpi_fk_lvl1": "nan",
          "kpi_fk_lvl2": 2005,
          "kpi_fk_lvl3": 2007,
          "product_fk": 255,
          "super_group_target": "None",
          "target": "nan"}]
    )