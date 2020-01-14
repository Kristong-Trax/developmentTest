# coding=utf-8
import datetime
import os
import pandas as pd
from numpy import nan
import numpy as np

OBLIGATORY = u'חובה'
OPTIONAL = u'אופציונאלי'
OBLIGATORY_SKU = u'חובה - SKU'
OPTIONAL_SKU = u'אופציונאלי - SKU'


class DataTestUnitTnuva(object):

    kpi_static_data = pd.DataFrame.from_records(
        [{'pk': 1997, 'type': u'DAIRY_MANUFACTURER_OUT_OF_STORE_SOS'},
         {'pk': 1998, 'type': u'DAIRY_OWN_MANUFACTURER_OUT_OF_CATEGORY_SOS'},
         {'pk': 1999, 'type': u'DAIRY_ALL_MANUFACTURERS_OUT_OF_ALL_CATEGORIES_SOS'},
         {'pk': 2000, 'type': u'TIRAT_TSVI_MANUFACTURER_OUT_OF_STORE_SOS'},
         {'pk': 2001, 'type': u'TIRAT_TSVI_OWN_MANUFACTURER_OUT_OF_CATEGORY_SOS'},
         {'pk': 2002,
          'type': u'TIRAT_TSVI_ALL_MANUFACTURERS_OUT_OF_ALL_CATEGORIES_SOS'},
         {'pk': 2003, 'type': u'DISTRIBUTION_STORE_LEVEL_DAIRY'},
         {'pk': 2004, 'type': u'DISTRIBUTION_CATEGORY_LEVEL_DAIRY'},
         {'pk': 2005, 'type': u'DISTRIBUTION_SKU_LEVEL_DAIRY'},
         {'pk': 2006, 'type': u'DISTRIBUTION_STORE_LEVEL_TIRAT_TSVI'},
         {'pk': 2007, 'type': u'DISTRIBUTION_CATEGORY_LEVEL_TIRAT_TSVI'},
         {'pk': 2008, 'type': u'DISTRIBUTION_SKU_LEVEL_TIRAT_TSVI'},
         {'pk': 2009, 'type': u'OOS_STORE_LEVEL_DAIRY'},
         {'pk': 2010, 'type': u'OOS_CATEGORY_LEVEL_DAIRY'},
         {'pk': 2011, 'type': u'OOS_SKU_LEVEL_DAIRY'},
         {'pk': 2012, 'type': u'OOS_STORE_LEVEL_TIRAT_TSVI'},
         {'pk': 2013, 'type': u'OOS_CATEGORY_LEVEL_TIRAT_TSVI'},
         {'pk': 2014, 'type': u'OOS_SKU_LEVEL_TIRAT_TSVI'},
         {'pk': 2015, 'type': u'OOS_SKU_IN_STORE_LEVEL'},
         {'pk': 2016, 'type': u'OOS_STORE_LEVEL'},

         {'pk': 3000, 'type': OBLIGATORY}, {'pk': 3001, 'type': OBLIGATORY_SKU}, {'pk': 3002, 'type': OPTIONAL},
         {'pk': 3003, 'type': OPTIONAL_SKU},
         {'pk': 2017, 'type': u'OOS_STORE_LEVEL_DAIRY_WITH_PREVIOUS_RESULTS'},
         {'pk': 2018, 'type': u'OOS_STORE_LEVEL_TIRAT_TSVI_WITH_PREVIOUS_RESULTS'},
         {'pk': 2019, 'type': u'OOS_SKU_LEVEL_DAIRY_WITH_PREVIOUS_RESULTS'},
         {'pk': 2020, 'type': u'OOS_SKU_LEVEL_TIRAT_TSVI_WITH_PREVIOUS_RESULTS'},
        ]
    )

    store_data = pd.DataFrame.from_records(
        [{'pk': 1, 'store_type': 'A1', 'additional_attribute_7': 'A', 'store_number_1': 12}])

    store_info_dict_other_type = {'pk': 3, 'store_type': 'Other', 'store_number_1': '100978891',
                                  'additional_attribute_1': 'Other', 'additional_attribute_2': 'Other',
                                  'additional_attribute_3': 'Lulu'}

    session_info_new = pd.DataFrame.from_records([
        {'pk': 101, 'visit_date': '2019-12-18', 'store_fk': 1, 's_sales_rep_fk': 42, 'exclude_status_fk': None,
         'status': 'New'}
    ])

    session_info_completed = pd.DataFrame.from_records([
        {'pk': 101, 'visit_date': '2019-12-18', 'store_fk': 1, 's_sales_rep_fk': 42, 'exclude_status_fk': None,
         'status': 'Completed', 'session_uid': '236c1577-0ecb-4bf9-88b9-c9e87ab17c58'}
    ])

    session_info_2 = pd.DataFrame.from_records([
        {'pk': 102, 'visit_date': '2019-06-27', 'store_fk': 2, 's_sales_rep_fk': 84, 'exclude_status_fk': None,
         'status': 'Completed', 'session_uid': '236c1577-0ecb-4bf9-88b9-c9e87ab17c58'}
    ])

    kpi_results_values_table = pd.DataFrame.from_records(
        [{'pk': 1, 'value': u'OOS'}, {'pk': 2, 'value': u'DISTRIBUTED'}, {'pk': 3, 'value': u'EXTRA'}]
    )

    #change product pks
    assortment_store = pd.DataFrame.from_records([
        {'additional_attributes': '{}', 'assortment_fk': 108, 'assortment_group_fk': 16,
         'assortment_super_group_fk': nan, 'group_target_date': nan, 'in_store': 0,
         'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3000, 'kpi_fk_lvl3': 3001, 'product_fk': 1,
         'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 108, 'assortment_group_fk': 16,
         'assortment_super_group_fk': nan, 'group_target_date': nan, 'in_store': 0,
         'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3000, 'kpi_fk_lvl3': 3001, 'product_fk': 2,
         'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 108, 'assortment_group_fk': 16,
         'assortment_super_group_fk': nan, 'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan,
         'kpi_fk_lvl2': 3000, 'kpi_fk_lvl3': 3001, 'product_fk': 3, 'super_group_target': None,
         'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 108, 'assortment_group_fk': 16,
         'assortment_super_group_fk': nan, 'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan,
         'kpi_fk_lvl2': 3000, 'kpi_fk_lvl3': 3001, 'product_fk': 4, 'super_group_target': None,
         'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 168, 'assortment_group_fk': 76,
         'assortment_super_group_fk': nan, 'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan,
         'kpi_fk_lvl2': 3002, 'kpi_fk_lvl3': 3003, 'product_fk': 5, 'super_group_target': None,
         'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 168, 'assortment_group_fk': 76,
         'assortment_super_group_fk': nan, 'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan,
         'kpi_fk_lvl2': 3002, 'kpi_fk_lvl3': 3003, 'product_fk': 6, 'super_group_target': None,
         'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 153, 'assortment_group_fk': 61,
         'assortment_super_group_fk': nan, 'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan,
         'kpi_fk_lvl2': 3000, 'kpi_fk_lvl3': 3001, 'product_fk': 7, 'super_group_target': None,
         'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 153, 'assortment_group_fk': 61,
         'assortment_super_group_fk': nan, 'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan,
         'kpi_fk_lvl2': 3000, 'kpi_fk_lvl3': 3001, 'product_fk': 8, 'super_group_target': None,
         'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 153, 'assortment_group_fk': 61,
         'assortment_super_group_fk': nan, 'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan,
         'kpi_fk_lvl2': 3000, 'kpi_fk_lvl3': 3001, 'product_fk': 9, 'super_group_target': None,
         'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 153, 'assortment_group_fk': 61,
         'assortment_super_group_fk': nan, 'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan,
         'kpi_fk_lvl2': 3000, 'kpi_fk_lvl3': 3001, 'product_fk': 10, 'super_group_target': None,
         'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 153, 'assortment_group_fk': 61,
         'assortment_super_group_fk': nan, 'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan,
         'kpi_fk_lvl2': 3000, 'kpi_fk_lvl3': 3001, 'product_fk': 11, 'super_group_target': None,
         'target': nan}])

    test_case_1 = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'test_case.xlsx')

    own_manuf_property = pd.DataFrame.from_records([
        {u'param_group': u'Customer Specifications', u'param_name': u'manufacturer_id', u'param_value': u'810'}])

    oos_exclude_res_empty = pd.DataFrame()
    previous_results_empty = pd.DataFrame()
    previous_results_no_session = None
    oos_exclude_res_1 = pd.DataFrame.from_records([
        {'store_fk': 1, 'date': '2019-12-18', 'session_uid': '236c1577-0ecb-4bf9-88b9-c9e87ab17c58',
         'product_fk': 6, 'oos_message_fk': 40, 'type': 3, 'description': 'OOS-Correct Tag'}
    ])
    oos_exclude_res_2 = pd.DataFrame.from_records([
        {'store_fk': 1, 'date': '2019-12-18', 'session_uid': '236c1577-0ecb-4bf9-88b9-c9e87ab17c58',
         'product_fk': 11, 'oos_message_fk': 40, 'type': 3, 'description': 'OOS-Correct Tag'}
    ])

    oos_exclude_res_3 = pd.DataFrame.from_records([
        {'store_fk': 1, 'date': '2019-12-18', 'session_uid': '236c1577-0ecb-4bf9-88b9-c9e87ab17c58',
         'product_fk': 6, 'oos_message_fk': 40, 'type': 3, 'description': 'OOS-Correct Tag'},
        {'store_fk': 1, 'date': '2019-12-18', 'session_uid': '236c1577-0ecb-4bf9-88b9-c9e87ab17c58',
         'product_fk': 11, 'oos_message_fk': 40, 'type': 3, 'description': 'OOS-Correct Tag'}
    ])