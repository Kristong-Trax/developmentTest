import datetime
import pandas as pd
from numpy import nan
from Projects.CCBZA_SAND.Utils.KPIToolBox import KPI_TAB, KPI_TYPE, PLANOGRAM_TAB, PRICE_TAB, SURVEY_TAB, AVAILABILITY_TAB, SOS_TAB, COUNT_TAB, \
    SET_NAME, KPI_NAME, KPI_TYPE, SPLIT_SCORE, DEPENDENCY, ATOMIC_KPI_NAME, EXPECTED_RESULT, SURVEY_QUESTION_CODE, SCORE, STORE_TYPE, \
    ATTRIBUTE_1, ATTRIBUTE_2

class DataScores(object):
    SCORES_1 = [(None, 0.15), (100, 0.15), (3, 0.15)]
    SCORES_2 = [(False, None), (100, None), (False, None)]
    SCORES_3 = [(100, 0.05),  (None, 0.05)]
    SCORES_4_NONE_NO_WEIGHTS = [(None, None), (None, None), (None, None)]
    SCORES_5_NONE_WEIGHTS = [(None, 0.15), (None, 0.15), (None, 0.15)] # score_1_1 in my example
    SCORES_6 = [(100, None), (0, None), (100, None)]

class DataTestUnitCCBZA_SAND(object):

    static_data = pd.DataFrame.from_records(
        [{'atomic_kpi_fk': 465.0,
          'atomic_kpi_name': u'SHEBA FRESH CHOICE FISH SELECTION IN GRAVY 50G X6,', 'kpi_fk': 96,
          'kpi_name': u'Sheba', 'kpi_set_fk': 7, 'kpi_set_name': u'ASSORTMENT SCORE'},
         {'atomic_kpi_fk': 466.0, 'atomic_kpi_name': u'SHEBA POUCH FINE FLAKES POULTRY IN JELLY 85GX12,',
          'kpi_fk': 96, 'kpi_name': u'Sheba', 'kpi_set_fk': 7, 'kpi_set_name': u'ASSORTMENT SCORE'},
         {'atomic_kpi_fk': 467.0,
          'atomic_kpi_name': u'WHISKAS 1+ Cat Pouches Poultry Selection in Jelly 12x100g pk', 'kpi_fk': 97,
          'kpi_name': u'Whi Pouch', 'kpi_set_fk': 7, 'kpi_set_name': u'ASSORTMENT SCORE'},
         {'atomic_kpi_fk': 468.0, 'atomic_kpi_name': u'WHISKAS 1+ Cat Pouches Fish Selection in Jelly 12x100g pk',
          'kpi_fk': 97, 'kpi_name': u'Whi Pouch', 'kpi_set_fk': 7, 'kpi_set_name': u'ASSORTMENT SCORE'},
         {'atomic_kpi_fk': 469.0, 'atomic_kpi_name': u'Sheba', 'kpi_fk': 99, 'kpi_name': u'Sheba', 'kpi_set_fk': 8,
          'kpi_set_name': u'Share of Shelf SCORE'},
         {'atomic_kpi_fk': 470.0, 'atomic_kpi_name': u'Whi Pouch', 'kpi_fk': 100, 'kpi_name': u'Whi Pouch',
          'kpi_set_fk': 8, 'kpi_set_name': u'Share of Shelf SCORE'},
         {'atomic_kpi_fk': 475.0, 'atomic_kpi_name': u'SHEBA FRESH CHOICE FISH SELECTION IN GRAVY 50G X6,',
          'kpi_fk': 105, 'kpi_name': u'Sheba', 'kpi_set_fk': 10,
          'kpi_set_name': u'Position on Shelf - Position score'},
         {'atomic_kpi_fk': 476.0, 'atomic_kpi_name': u'SHEBA POUCH FINE FLAKES POULTRY IN JELLY 85GX12,',
          'kpi_fk': 105, 'kpi_name': u'Sheba', 'kpi_set_fk': 10,
          'kpi_set_name': u'Position on Shelf - Position score'},
         {'atomic_kpi_fk': 477.0,
          'atomic_kpi_name': u'WHISKAS 1+ Cat Pouches Poultry Selection in Jelly 12x100g pk', 'kpi_fk': 106,
          'kpi_name': u'Whi Pouch', 'kpi_set_fk': 10, 'kpi_set_name': u'Position on Shelf - Position score'},
         {'atomic_kpi_fk': 478.0,
          'atomic_kpi_name': u'WHISKAS 1+ Cat Pouches Fish Selection in Jelly 12x100g pk', 'kpi_fk': 106,
          'kpi_name': u'Whi Pouch', 'kpi_set_fk': 10, 'kpi_set_name': u'Position on Shelf - Position score'},
         {'atomic_kpi_fk': 479.0, 'atomic_kpi_name': u'Dreamies', 'kpi_fk': 108, 'kpi_name': u'Dreamies',
          'kpi_set_fk': 11, 'kpi_set_name': u'Clip strips Score'},
         {'atomic_kpi_fk': 480.0, 'atomic_kpi_name': u'Ped - 1st SKU', 'kpi_fk': 109, 'kpi_name': u'Ped - 1st SKU',
          'kpi_set_fk': 11, 'kpi_set_name': u'Clip strips Score'},
         {'atomic_kpi_fk': 481.0, 'atomic_kpi_name': u'Sheba', 'kpi_fk': 111, 'kpi_name': u'Sheba',
          'kpi_set_fk': 12, 'kpi_set_name': u'Macro space KPI'},
         {'atomic_kpi_fk': 482.0, 'atomic_kpi_name': u'Whi Pouch', 'kpi_fk': 112, 'kpi_name': u'Whi Pouch',
          'kpi_set_fk': 12, 'kpi_set_name': u'Macro space KPI'},
         {'atomic_kpi_fk': nan, 'atomic_kpi_name': None, 'kpi_fk': 95, 'kpi_name': u'ASSORTMENT SCORE',
          'kpi_set_fk': 6, 'kpi_set_name': u'PERFECT STORE'},
         {'atomic_kpi_fk': nan, 'atomic_kpi_name': None, 'kpi_fk': 98, 'kpi_name': u'Share of Shelf SCORE',
          'kpi_set_fk': 6, 'kpi_set_name': u'PERFECT STORE'},
         {'atomic_kpi_fk': nan, 'atomic_kpi_name': None, 'kpi_fk': 101,
          'kpi_name': u'Position on Shelf  - Facing Score', 'kpi_set_fk': 6, 'kpi_set_name': u'PERFECT STORE'},
         {'atomic_kpi_fk': nan, 'atomic_kpi_name': None, 'kpi_fk': 104,
          'kpi_name': u'Position on Shelf - Position score', 'kpi_set_fk': 6, 'kpi_set_name': u'PERFECT STORE'},
         {'atomic_kpi_fk': nan, 'atomic_kpi_name': None, 'kpi_fk': 107, 'kpi_name': u'Clip strips Score',
          'kpi_set_fk': 6, 'kpi_set_name': u'PERFECT STORE'},
         {'atomic_kpi_fk': nan, 'atomic_kpi_name': None, 'kpi_fk': 110, 'kpi_name': u'Macro space KPI',
          'kpi_set_fk': 6, 'kpi_set_name': u'PERFECT STORE'}]
    )

    new_kpi_static_data = pd.DataFrame.from_records([])

    store_data = pd.DataFrame.from_records([{
        'store_type': 'L&T',
        'additional_attribute_1': 'Spaza Affordable',
        'additional_attribute_2': 'Gold'
    }])

    session_info_1 = pd.DataFrame.from_records([
        {'pk': 100, 'visit_date': '2018-06-01', 'store_fk': 1, 's_sales_rep_fk': 111, 'exclude_status_fk': None, 'status': 'Completed'}
    ])

    required_template_tabs = [KPI_TAB, PLANOGRAM_TAB, PRICE_TAB, SURVEY_TAB, AVAILABILITY_TAB, SOS_TAB, COUNT_TAB]
    columns_kpi_tab = [SET_NAME, KPI_NAME, KPI_TYPE, SPLIT_SCORE, DEPENDENCY]
    columns_survey_tab = [KPI_NAME, ATOMIC_KPI_NAME, EXPECTED_RESULT, SURVEY_QUESTION_CODE, STORE_TYPE, ATTRIBUTE_1, ATTRIBUTE_2]
    columns_kpi_results = [SET_NAME, KPI_NAME, ATOMIC_KPI_NAME, SCORE]
    kpi_set_names_from_template = ['COOLERS & MERCHANDISING', 'KEY PACK: Availability, Pricing, Activation',
                                   'AVAILABILITY', 'PRICE COMPLIANCE', 'COMBOS & ACTIVATION', 'BONUS POINTS']
    kpi_types_split_by_comma = 'Price,Survey,Availability,SOS,Count'
    kpi_types_split_irregularly = 'Price,Survey, Availability ,SOS , Count'
    kpi_types_one_value = 'Price'

