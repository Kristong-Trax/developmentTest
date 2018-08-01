import datetime
import pandas as pd
from numpy import nan
from Projects.CCBZA_SAND.Utils.KPIToolBox import KPI_TAB, KPI_TYPE, PLANOGRAM_TAB, PRICE_TAB, SURVEY_TAB, AVAILABILITY_TAB, SOS_TAB, COUNT_TAB, \
    SET_NAME, KPI_NAME, KPI_TYPE, SPLIT_SCORE, DEPENDENCY, ATOMIC_KPI_NAME, EXPECTED_RESULT, SURVEY_QUESTION_CODE, SCORE, STORE_TYPE, \
    ATTRIBUTE_1, ATTRIBUTE_2, TEMPLATE_NAME, TYPE1, TYPE2, TYPE3, VALUE1, VALUE2, VALUE3, TARGET, SCORE, AVAILABILITY_TYPE, \
    CONDITION_1_NUMERATOR, CONDITION_1_NUMERATOR_TYPE, CONDITION_1_DENOMINATOR, CONDITION_1_DENOMINATOR_TYPE, CONDITION_1_TARGET, \
    CONDITION_2_NUMERATOR, CONDITION_2_NUMERATOR_TYPE, CONDITION_2_DENOMINATOR, CONDITION_2_DENOMINATOR_TYPE, \
    CONDITION_2_TARGET, KO_PRODUCTS, TEMPLATE_DISPLAY_NAME, BY_SCENE, KO_ONLY

class DataScores(object):
    SCORES_1 = [(None, 0.15), (100, 0.15), (3, 0.15)]
    SCORES_2 = [(False, None), (100, None), (False, None)]
    SCORES_3 = [(100, 0.05),  (None, 0.05)]
    SCORES_4_NONE_NO_WEIGHTS = [(None, None), (None, None), (None, None)]
    SCORES_5_NONE_WEIGHTS = [(None, 0.15), (None, 0.15), (None, 0.15)] # score_1_1 in my example
    SCORES_6 = [(100, None), (0, None), (100, None)]

class StoreTypes(object):
    LT_Spaza_Affordable = 'L&T Spaza Affordable'
    LT_Gen_D_Affordable = 'L&T Gen D Affordable'
    LT_Spaza_Mainstream = 'L&T Spaza Mainstream'
    LT_Gen_D_Mainstream = 'L&T Gen D Mainstream'
    LT_Spaza_Premium = 'L&T Spaza Premium'
    LT_Gen_D_Premium = 'L&T Gen D Premium'
    store_list = [LT_Spaza_Affordable, LT_Gen_D_Affordable, LT_Spaza_Mainstream, LT_Gen_D_Mainstream, LT_Spaza_Premium, LT_Gen_D_Premium]

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

    required_template_tabs = [KPI_TAB, PRICE_TAB, SURVEY_TAB, AVAILABILITY_TAB, SOS_TAB, COUNT_TAB, PLANOGRAM_TAB]
    columns_kpi_tab = [SET_NAME, KPI_NAME, KPI_TYPE, SPLIT_SCORE, DEPENDENCY]
    columns_survey_tab = [KPI_NAME, ATOMIC_KPI_NAME, EXPECTED_RESULT, SURVEY_QUESTION_CODE, STORE_TYPE, ATTRIBUTE_1, ATTRIBUTE_2]
    columns_price_tab = [KPI_NAME, ATOMIC_KPI_NAME, TEMPLATE_NAME, TYPE1, TYPE2, TYPE3, VALUE1, VALUE2, VALUE3, TARGET,
                         SCORE, STORE_TYPE, ATTRIBUTE_1, ATTRIBUTE_2, BY_SCENE, KO_ONLY]
    columns_avaialability_tab = [KPI_NAME, ATOMIC_KPI_NAME, AVAILABILITY_TYPE, TEMPLATE_NAME, TYPE1, TYPE2, TYPE3,
                                 VALUE1, VALUE2, VALUE3, TARGET, SCORE, STORE_TYPE, ATTRIBUTE_1, ATTRIBUTE_2, BY_SCENE, KO_ONLY]
    columns_sos_tab = [KPI_NAME, ATOMIC_KPI_NAME, TEMPLATE_NAME, CONDITION_1_NUMERATOR, CONDITION_1_NUMERATOR_TYPE,
                       CONDITION_1_TARGET, CONDITION_1_DENOMINATOR, CONDITION_1_DENOMINATOR_TYPE, CONDITION_2_NUMERATOR,
                       CONDITION_2_NUMERATOR_TYPE, CONDITION_2_DENOMINATOR, CONDITION_2_DENOMINATOR_TYPE,
                       CONDITION_2_TARGET, SCORE, STORE_TYPE, ATTRIBUTE_1, ATTRIBUTE_2, BY_SCENE, KO_ONLY]
    columns_count_tab = [KPI_NAME, ATOMIC_KPI_NAME, TEMPLATE_NAME, TARGET, SCORE, STORE_TYPE, ATTRIBUTE_1, ATTRIBUTE_2,
                         BY_SCENE, KO_ONLY]
    columns_planogram_tab = [KPI_NAME, ATOMIC_KPI_NAME, TEMPLATE_DISPLAY_NAME, TARGET, SCORE, STORE_TYPE, ATTRIBUTE_1, ATTRIBUTE_2]

    columns_kpi_results = [SET_NAME, KPI_NAME, ATOMIC_KPI_NAME, SCORE]
    kpi_set_names_from_template = ['COOLERS & MERCHANDISING', 'KEY PACK: Availability, Pricing, Activation',
                                   'AVAILABILITY', 'PRICE COMPLIANCE', 'COMBOS & ACTIVATION', 'BONUS POINTS', 'TEST SET']
    kpi_types_split_by_comma = 'Price,Survey,Availability,SOS,Count'
    kpi_types_split_irregularly = 'Price,Survey, Availability ,SOS , Count'
    kpi_types_one_value = 'Price'
    kpi_types_empty_string = ''
    kpi_types_name_with_space = 'Availability KPI ,SOS, Count'
    string_represented_by_number = 200

    index_kpi_tab = [SET_NAME, KPI_NAME, KPI_TYPE, SPLIT_SCORE, DEPENDENCY]+StoreTypes.store_list
    index_count_tab = [KPI_NAME, TEMPLATE_NAME, ATOMIC_KPI_NAME, TARGET, SCORE, STORE_TYPE, ATTRIBUTE_1,
                       ATTRIBUTE_2, BY_SCENE, KO_ONLY]
    index_survey_tab = [KPI_NAME, ATOMIC_KPI_NAME, EXPECTED_RESULT, SURVEY_QUESTION_CODE, SCORE, STORE_TYPE,
                        ATTRIBUTE_1, ATTRIBUTE_2]
    index_price_tab = [KPI_NAME, ATOMIC_KPI_NAME, TEMPLATE_NAME, TYPE1, VALUE1, TYPE2, VALUE2, TYPE3, VALUE3, TARGET,
                       SCORE, STORE_TYPE, ATTRIBUTE_1, ATTRIBUTE_2]
    index_sos_tab = [KPI_NAME, ATOMIC_KPI_NAME, TEMPLATE_NAME, CONDITION_1_NUMERATOR, CONDITION_1_NUMERATOR_TYPE,
                     CONDITION_1_DENOMINATOR, CONDITION_1_DENOMINATOR_TYPE, CONDITION_1_TARGET, CONDITION_2_NUMERATOR,
                     CONDITION_2_NUMERATOR_TYPE, CONDITION_2_DENOMINATOR, CONDITION_2_DENOMINATOR_TYPE,
                     CONDITION_2_TARGET, SCORE, STORE_TYPE, ATTRIBUTE_1, ATTRIBUTE_2]

    test_kpi_1_series = pd.Series(['TEST SET', 'TEST KPI 1', 'Survey', 'Y', '', '', '', '', '', '', ''],
                                  index=index_kpi_tab)
    avail_and_pricing_all_bonus_kpi_series = pd.Series(['BONUS POINTS', 'Availability and Pricing of ALL Key Packs ',
                                                        '', 'N', 'KEY PACK: Availability, Pricing, Activation', '', '',
                                                        '', '', '', ''], index=index_kpi_tab)
    coolers_kpi_series = pd.Series(['COOLERS & MERCHANDISING', 'Coolers', 'Price, Survey, Availability, SOS, Count',
                                    'Y', '', '', '', '', '', '', ''], index=index_kpi_tab)
    count_atomic_series = pd.Series(['Coolers', 'CCBSA Cooler, DOC Cooler', 'Min 4 x Cooler Doors', 4, 10,
                                     'L&T', 'Spaza Affordable', 'Gold', 'N', 'Y'], index=index_count_tab)
    count_atomic_template_field_empty = pd.Series(['Coolers', '', 'Min 4 x Cooler Doors', 4, 10,
                                     'L&T', 'Spaza Affordable', 'Gold', 'N', 'N'], index=index_count_tab)
    survey_atomic_series = pd.Series(['TEST KPI 1', 'Atomic KPI 1', 'Yes', 1, 5,
                                     'L&T', '', ''], index=index_survey_tab)
    price_atomic_series = pd.Series(['SSD IC', 'Price compliance of 200ml Can', '', 'size', '200.0', 'brand_name',
                                     'FANTA ORANGE, COCA COLA, SPRITE, STONEY', 'form_factor', 'can', '5.0', '', 'L&T',
                                     'Spaza Affordable', ''], index=index_price_tab)
    price_atomic_series_diffr_sizes = pd.Series(['SSD IC', 'Price compliance of 200ml Can', '', 'size', '200.0, 150.0', 'brand_name',
                                     'FANTA ORANGE, COCA COLA, SPRITE, STONEY', 'form_factor', 'can', '5.0', '', 'L&T',
                                     'Spaza Affordable', ''], index=index_price_tab)
    price_atomic_series_one_type_value = pd.Series(['SSD IC', 'Price compliance of 200ml Can', '', '', '', 'brand_name',
                                     'FANTA ORANGE, COCA COLA, SPRITE, STONEY', '', '', '5.0', '', 'L&T',
                                     'Spaza Affordable', ''], index=index_price_tab)
    price_atomic_series_missing_between = pd.Series(['SSD IC', 'Price compliance of 200ml Can', '', 'product_ean_code', '3434443,232323', '',
                                     '', 'form_factor', 'can', '5.0', '', 'L&T', 'Spaza Affordable', ''], index=index_price_tab)
    price_atomic_series_no_type_values = pd.Series(['SSD IC', 'Price compliance of 200ml Can', '', '', '', '',
                                     '', '', '', '5.0', '', 'L&T', 'Spaza Affordable', ''], index=index_price_tab)
    sos_atomic_series = pd.Series(['Coolers', 'CCBSA Quad Cola>= 50% of CCBSA SSDs - cold of which Diets/Zeros/Lights>=30%',
                                   'CCBSA Cooler', 'Quad Cola', 'Attribute 2', 'SSD', 'Category', '50', 'Diets',
                                   'Attribute 3', 'SSD', 'Category', '30', '5', 'L&T', 'Spaza Affordable', ''],
                                  index=index_sos_tab)
    sos_atomic_series_one_condition = pd.Series(
        ['Coolers', 'Diets > 65% out of all CCBSA SSD (CCBSA Cooler)',
         'CCBSA Cooler', '5449000234612,5449000027559,5449000234636', 'product_ean_code', 'SSD', 'category', '65', '',
         '', '', '', '', '5', 'L&T', 'Spaza Affordable', ''],
        index=index_sos_tab)

    kpi_results_values_table = pd.DataFrame.from_records([{'pk': 1, 'value': 'Passed', 'kpi_result_type_fk': 1},
                                                          {'pk': 2, 'value': 'Failed', 'kpi_result_type_fk': 1},
                                                          {'pk': 3, 'value': 'V', 'kpi_result_type_fk': 2},
                                                          {'pk': 4, 'value': 'X', 'kpi_result_type_fk': 2}])
    kpi_scores_values_table = pd.DataFrame.from_records([{'pk': 1, 'value': 'Passed', 'kpi_result_type_fk': 1},
                                                          {'pk': 2, 'value': 'Failed', 'kpi_result_type_fk': 1},
                                                          {'pk': 3, 'value': 'V', 'kpi_result_type_fk': 2},
                                                          {'pk': 4, 'value': 'X', 'kpi_result_type_fk': 2}])

class SCIFDataTestCCBZA_SAND(object):

    scif_for_filtering = pd.DataFrame.from_records([
        {'pk': 1, 'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler', 'item_id': 1,
         'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Coca-Cola 500ml', 'product_type': 'SKU',
         'product_ean_code': '7290001594150', 'brand_name': 'Coca-Cola', 'facings': 2},
        {'pk': 2, 'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler', 'item_id': 2,
         'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Coca-Cola 300ml', 'product_type': 'SKU',
         'product_ean_code': '7290001594151', 'brand_name': 'Coca-Cola', 'facings': 1},
        {'pk': 3, 'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler', 'item_id': 3,
         'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Fanta Orange 500ml', 'product_type': 'SKU',
         'product_ean_code': '7290001594152', 'brand_name': 'Fanta', 'facings': 3},
        {'pk': 4, 'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler', 'item_id': 4,
         'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Fanta Exotic 1.5L',
         'product_type': 'SKU', 'product_ean_code': '7290001594153', 'brand_name': 'Fanta', 'facings': 1},
        {'pk': 5, 'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler', 'item_id': 5,
         'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Coca-Cola Zero 500ml bottle',
         'product_type': 'SKU', 'product_ean_code': '7290001594154', 'brand_name': 'Coca-Cola Zero', 'facings': 4},
        {'pk': 6,'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler', 'item_id': 6,
         'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Brand Strip Coca-Cola',
         'product_type': 'POS', 'product_ean_code': '1000000000001', 'brand_name': 'Coca-Cola', 'facings': 1},
        {'pk': 7, 'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler', 'item_id': 7,
         'manufacturer_name': 'Jafora Tavori', 'store_id': 1000, 'product_name': 'Schweppes Flavourd sparkling Other',
         'product_type': 'Other', 'product_ean_code': '7290001594160', 'brand_name': 'Schweppes', 'facings': 2},
        {'pk': 8, 'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler', 'item_id': 8,
         'manufacturer_name': 'Adir R. Trade', 'store_id': 1000, 'product_name': 'Jumex Juice Other',
         'product_type': 'Other', 'product_ean_code': '7290001594161', 'brand_name': 'Jumex', 'facings': 1},

        {'pk': 9, 'session_id': 160, 'scene_fk': 96, 'scene_id': 96, 'template_name': 'DOC Cooler', 'item_id': 1,
         'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Coca-Cola 500ml', 'product_type': 'SKU',
         'product_ean_code': '7290001594150', 'brand_name': 'Coca-Cola', 'facings': 1},
        {'pk': 10, 'session_id': 160, 'scene_fk': 96, 'scene_id': 96, 'template_name': 'DOC Cooler', 'item_id': 9,
         'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Coca-Cola Zero 330ml', 'product_type': 'SKU',
         'product_ean_code': '7290001594155', 'brand_name': 'Coca-Cola Zero', 'facings': 2},
        {'pk': 11, 'session_id': 160, 'scene_fk': 96, 'scene_id': 96, 'template_name': 'DOC Cooler', 'item_id': 10,
         'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Sprite Zero 500ml bottle',
         'product_type': 'SKU', 'product_ean_code': '7290001594156', 'brand_name': 'Sprite', 'facings': 3},
        {'pk': 12, 'session_id': 160, 'scene_fk': 96, 'scene_id': 96, 'template_name': 'DOC Cooler', 'item_id': 7,
         'manufacturer_name': 'Jafora Tavori', 'store_id': 1000, 'product_name': 'Schweppes Flavourd sparkling Other',
         'product_type': 'Other', 'product_ean_code': '7290001594160', 'brand_name': 'Schweppes', 'facings': 1},
        {'pk': 13, 'session_id': 160, 'scene_fk': 96, 'scene_id': 96, 'template_name': 'DOC Cooler', 'item_id': 13,
         'manufacturer_name': 'Other', 'store_id': 1000, 'product_name': 'Empty',
         'product_type': 'Empty', 'product_ean_code': None, 'brand_name': 'General', 'facings': 2},

        {'pk': 14, 'session_id': 160, 'scene_fk': 97, 'scene_id': 97, 'template_name': 'Main Shelf', 'item_id': 11,
         'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Neviot Water Other',
         'product_type': 'Other', 'product_ean_code': '7290001594157', 'brand_name': 'Neviot', 'facings': 1},
        {'pk': 15, 'session_id': 160, 'scene_fk': 97, 'scene_id': 97, 'template_name': 'Main Shelf', 'item_id': 12,
         'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Fuze Tea Ice Tea Other',
         'product_type': 'Other', 'product_ean_code': '7290001594158', 'brand_name': 'Fuze Tea', 'facings': 2},
        {'pk': 16, 'session_id': 160, 'scene_fk': 97, 'scene_id': 97, 'template_name': 'Main Shelf', 'item_id': 13,
         'manufacturer_name': 'Other', 'store_id': 1000, 'product_name': 'Empty',
         'product_type': 'Empty', 'product_ean_code': None, 'brand_name': 'General', 'facings': 1},
        {'pk': 17, 'session_id': 160, 'scene_fk': 97, 'scene_id': 97, 'template_name': 'Main Shelf', 'item_id': 3,
         'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Fanta Orange 500ml',
         'product_type': 'SKU', 'product_ean_code': '7290001594152', 'brand_name': 'Fanta', 'facings': 2}
    ])

    scif_no_manufacturer = pd.DataFrame.from_records([
        {'pk': 7, 'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler', 'item_id': 7,
         'manufacturer_name': 'Jafora Tavori', 'store_id': 1000, 'product_name': 'Schweppes Flavourd sparkling Other',
         'product_type': 'Other', 'product_ean_code': '7290001594160', 'brand_name': 'Schweppes', 'facings': 2},
        {'pk': 8, 'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler', 'item_id': 8,
         'manufacturer_name': 'Adir R. Trade', 'store_id': 1000, 'product_name': 'Jumex Juice Other',
         'product_type': 'Other', 'product_ean_code': '7290001594161', 'brand_name': 'Jumex', 'facings': 1},
        {'pk': 16, 'session_id': 160, 'scene_fk': 97, 'scene_id': 97, 'template_name': 'Main Shelf', 'item_id': 13,
         'manufacturer_name': 'Other', 'store_id': 1000, 'product_name': 'Empty',
         'product_type': 'Empty', 'product_ean_code': None, 'brand_name': 'General', 'facings': 1},
        {'pk': 12, 'session_id': 160, 'scene_fk': 96, 'scene_id': 96, 'template_name': 'DOC Cooler', 'item_id': 7,
         'manufacturer_name': 'Jafora Tavori', 'store_id': 1000, 'product_name': 'Schweppes Flavourd sparkling Other',
         'product_type': 'Other', 'product_ean_code': '7290001594160', 'brand_name': 'Schweppes', 'facings': 1},
        {'pk': 13, 'session_id': 160, 'scene_fk': 96, 'scene_id': 96, 'template_name': 'DOC Cooler', 'item_id': 13,
         'manufacturer_name': 'Other', 'store_id': 1000, 'product_name': 'Empty',
         'product_type': 'Empty', 'product_ean_code': None, 'brand_name': 'General', 'facings': 2}
    ])

    # scif = pd.DataFrame.from_records([
    #     {'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler',
    #
    #     }
    # ])

class MatchProdSceneDataTestCCBZA_SAND(object):
    matches_scif_for_filtering = pd.DataFrame.from_records([
        {'pk': 1, 'scene_fk': 95, 'product_fk': 1, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 2, 'scene_fk': 95, 'product_fk': 1, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 2, 'stacking_layer': 1},
        {'pk': 3, 'scene_fk': 95, 'product_fk': 2, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 4, 'scene_fk': 95, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 5, 'scene_fk': 95, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 2, 'stacking_layer': 1},
        {'pk': 6, 'scene_fk': 95, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 3, 'stacking_layer': 1},
        {'pk': 7, 'scene_fk': 95, 'product_fk': 4, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 8, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 9, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
         'facing_sequence_number': 2, 'stacking_layer': 1},
        {'pk': 10, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
         'facing_sequence_number': 3, 'stacking_layer': 1},
        {'pk': 11, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 3,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 12, 'scene_fk': 95, 'product_fk': 6, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 13, 'scene_fk': 95, 'product_fk': 7, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 14, 'scene_fk': 95, 'product_fk': 7, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
         'facing_sequence_number': 2, 'stacking_layer': 1},
        {'pk': 15, 'scene_fk': 95, 'product_fk': 8, 'front_facing': 'Y', 'bay_number': 4, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1},

        {'pk': 16, 'scene_fk': 96, 'product_fk': 1, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 17, 'scene_fk': 96, 'product_fk': 9, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 18, 'scene_fk': 96, 'product_fk': 9, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 2, 'stacking_layer': 1},
        {'pk': 19, 'scene_fk': 96, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 3,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 20, 'scene_fk': 96, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 3,
         'facing_sequence_number': 2, 'stacking_layer': 1},
        {'pk': 21, 'scene_fk': 96, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 22, 'scene_fk': 96, 'product_fk': 7, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
         'facing_sequence_number': 2, 'stacking_layer': 1},
        {'pk': 23, 'scene_fk': 96, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 24, 'scene_fk': 96, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
         'facing_sequence_number': 2, 'stacking_layer': 1},

        {'pk': 25, 'scene_fk': 97, 'product_fk': 11, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 26, 'scene_fk': 97, 'product_fk': 12, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 2, 'stacking_layer': 1},
        {'pk': 27, 'scene_fk': 97, 'product_fk': 12, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 3, 'stacking_layer': 1},
        {'pk': 28, 'scene_fk': 97, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 29, 'scene_fk': 97, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 2, 'stacking_layer': 1},
        {'pk': 30, 'scene_fk': 97, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 3, 'stacking_layer': 1},
    ])

    matches_scif_for_filtering_less_bays = pd.DataFrame.from_records([
        {'pk': 1, 'scene_fk': 95, 'product_fk': 1, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 2, 'scene_fk': 95, 'product_fk': 1, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 2, 'stacking_layer': 1},
        {'pk': 3, 'scene_fk': 95, 'product_fk': 2, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 4, 'scene_fk': 95, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 5, 'scene_fk': 95, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 2, 'stacking_layer': 1},
        {'pk': 6, 'scene_fk': 95, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 3, 'stacking_layer': 1},
        {'pk': 7, 'scene_fk': 95, 'product_fk': 4, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 8, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 9, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
         'facing_sequence_number': 2, 'stacking_layer': 1},
        {'pk': 10, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
         'facing_sequence_number': 3, 'stacking_layer': 1},
        {'pk': 11, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 3,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 12, 'scene_fk': 95, 'product_fk': 6, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 13, 'scene_fk': 95, 'product_fk': 7, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 14, 'scene_fk': 95, 'product_fk': 7, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
         'facing_sequence_number': 2, 'stacking_layer': 1},
        {'pk': 15, 'scene_fk': 95, 'product_fk': 8, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
         'facing_sequence_number': 3, 'stacking_layer': 1},

        {'pk': 16, 'scene_fk': 96, 'product_fk': 1, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 17, 'scene_fk': 96, 'product_fk': 9, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 18, 'scene_fk': 96, 'product_fk': 9, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 2, 'stacking_layer': 1},
        {'pk': 19, 'scene_fk': 96, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 3,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 20, 'scene_fk': 96, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 3,
         'facing_sequence_number': 2, 'stacking_layer': 1},
        {'pk': 21, 'scene_fk': 96, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 22, 'scene_fk': 96, 'product_fk': 7, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
         'facing_sequence_number': 2, 'stacking_layer': 1},
        {'pk': 23, 'scene_fk': 96, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 24, 'scene_fk': 96, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
         'facing_sequence_number': 2, 'stacking_layer': 1},

        {'pk': 25, 'scene_fk': 97, 'product_fk': 11, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 26, 'scene_fk': 97, 'product_fk': 12, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 2, 'stacking_layer': 1},
        {'pk': 27, 'scene_fk': 97, 'product_fk': 12, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 3, 'stacking_layer': 1},
        {'pk': 28, 'scene_fk': 97, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 1, 'stacking_layer': 1},
        {'pk': 29, 'scene_fk': 97, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 2, 'stacking_layer': 1},
        {'pk': 30, 'scene_fk': 97, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 3, 'stacking_layer': 1},
    ])

    matches_price_presence = pd.DataFrame.from_records([
        {'pk': 1, 'scene_fk': 95, 'product_fk': 1, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1, 'price': None},
        {'pk': 2, 'scene_fk': 95, 'product_fk': 1, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 2, 'stacking_layer': 1, 'price': None},
        {'pk': 3, 'scene_fk': 95, 'product_fk': 2, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 5},
        {'pk': 4, 'scene_fk': 95, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 7},
        {'pk': 5, 'scene_fk': 95, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 2, 'stacking_layer': 1, 'price': 5.50},
        {'pk': 6, 'scene_fk': 95, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 3, 'stacking_layer': 1, 'price': 6},
        {'pk': 7, 'scene_fk': 95, 'product_fk': 4, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 7},
        {'pk': 8, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
         'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 6},
        {'pk': 9, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
         'facing_sequence_number': 2, 'stacking_layer': 1, 'price': 5.34},
        {'pk': 10, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
         'facing_sequence_number': 3, 'stacking_layer': 1, 'price': 5.3},
        {'pk': 11, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 3,
         'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 6},
        {'pk': 12, 'scene_fk': 95, 'product_fk': 6, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 5.1},
        {'pk': 13, 'scene_fk': 95, 'product_fk': 7, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 5.5},
        {'pk': 14, 'scene_fk': 95, 'product_fk': 7, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
         'facing_sequence_number': 2, 'stacking_layer': 1, 'price': 6.2},
        {'pk': 15, 'scene_fk': 95, 'product_fk': 8, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
         'facing_sequence_number': 3, 'stacking_layer': 1, 'price': 7},

        {'pk': 16, 'scene_fk': 96, 'product_fk': 1, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 5},
        {'pk': 17, 'scene_fk': 96, 'product_fk': 9, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 5},
        {'pk': 18, 'scene_fk': 96, 'product_fk': 9, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 2, 'stacking_layer': 1, 'price': None},
        {'pk': 19, 'scene_fk': 96, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 3,
         'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 7},
        {'pk': 20, 'scene_fk': 96, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 3,
         'facing_sequence_number': 2, 'stacking_layer': 1, 'price': 10},
        {'pk': 21, 'scene_fk': 96, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 8.45},
        {'pk': 22, 'scene_fk': 96, 'product_fk': 7, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
         'facing_sequence_number': 2, 'stacking_layer': 1, 'price': 6},
        {'pk': 23, 'scene_fk': 96, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
         'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 8.15},
        {'pk': 24, 'scene_fk': 96, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
         'facing_sequence_number': 2, 'stacking_layer': 1, 'price': 7},

        {'pk': 25, 'scene_fk': 97, 'product_fk': 11, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 5},
        {'pk': 26, 'scene_fk': 97, 'product_fk': 12, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 2, 'stacking_layer': 1, 'price': None},
        {'pk': 27, 'scene_fk': 97, 'product_fk': 12, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 3, 'stacking_layer': 1, 'price': 6.99},
        {'pk': 28, 'scene_fk': 97, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 5},
        {'pk': 29, 'scene_fk': 97, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 2, 'stacking_layer': 1, 'price': 5.95},
        {'pk': 30, 'scene_fk': 97, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 3, 'stacking_layer': 1, 'price': 5.35},

        {'pk': 16, 'scene_fk': 98, 'product_fk': 1, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 5},
        {'pk': 17, 'scene_fk': 98, 'product_fk': 9, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 5},
        {'pk': 18, 'scene_fk': 98, 'product_fk': 9, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
         'facing_sequence_number': 2, 'stacking_layer': 1, 'price': None},
        {'pk': 19, 'scene_fk': 98, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 3,
         'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 7},
        {'pk': 20, 'scene_fk': 98, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 3,
         'facing_sequence_number': 2, 'stacking_layer': 1, 'price': 10},
        {'pk': 21, 'scene_fk': 98, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
         'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 8.45},
        {'pk': 22, 'scene_fk': 98, 'product_fk': 7, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
         'facing_sequence_number': 2, 'stacking_layer': 1, 'price': 6},
        {'pk': 23, 'scene_fk': 98, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
         'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 8.15},
        {'pk': 24, 'scene_fk': 98, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
         'facing_sequence_number': 2, 'stacking_layer': 1, 'price': 8.05},
    ])