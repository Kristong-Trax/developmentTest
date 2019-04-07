import datetime
import os
import pandas as pd
from numpy import nan
import numpy as np

class DataScores(object):
    SCORES_1 = [(None, 0.15), (100, 0.15), (3, 0.15)]
    SCORES_2 = [(False, None), (100, None), (False, None)]
    SCORES_3 = [(100, 0.05),  (None, 0.05)]
    SCORES_4_NONE_NO_WEIGHTS = [(None, None), (None, None), (None, None)]
    SCORES_5_NONE_WEIGHTS = [(None, 0.15), (None, 0.15), (None, 0.15)] # score_1_1 in my example
    SCORES_6 = [(100, None), (0, None), (100, None)]

# class StoreTypes(object):
#     LT_Spaza_Affordable = 'L&T SPAZA AFFORDABLE'
#     LT_Gen_D_Affordable = 'L&T GENERAL DEALER AFFORDABLE'
#     LT_Spaza_Mainstream = 'L&T SPAZA MAINSTREAM'
#     LT_Gen_D_Mainstream = 'L&T GENERAL DEALER MAINSTREAM'
#     LT_Spaza_Premium = 'L&T SPAZA PREMIUM'
#     LT_Gen_D_Premium = 'L&T GENERAL DEALER PREMIUM'
#     store_list = [LT_Spaza_Affordable, LT_Gen_D_Affordable, LT_Spaza_Mainstream, LT_Gen_D_Mainstream, LT_Spaza_Premium, LT_Gen_D_Premium]

class DataTestUnitPEPSICOUK(object):

    kpi_static_data = pd.DataFrame.from_records(
        [{'pk': 287, 'type': 'Hero SKU Space to Sales Index'}, {'pk': 288, 'type': 'Hero SKU SOS vs Target'},
         {'pk': 289, 'type': 'Hero SKU Availability'}, {'pk': 290, 'type': 'Hero SKU Availability - SKU'},
         {'pk': 291, 'type': 'Hero SKU OOS'}, {'pk': 292, 'type': 'Hero SKU OOS - SKU'},
         {'pk': 293, 'type': 'Brand Space to Sales Index'}, {'pk': 294, 'type': 'Sub Brand Space to Sales Index'},
         {'pk': 295, 'type': 'PepsiCo Segment Space to Sales Index'}, {'pk': 296, 'type': 'PepsiCo Sub Segment Space to Sales Index'},
         {'pk': 297, 'type': 'PepsiCo Sub Segment SOS vs Target'}, {'pk': 298, 'type': 'PepsiCo Segment SOS vs Target'},
         {'pk': 299, 'type': 'Sub Brand Space SOS vs Target'}, {'pk': 300, 'type': 'Brand Space SOS vs Target'},
         {'pk': 301, 'type': 'Sensations Greater Linear Space vs Kettle'}, {'pk': 302, 'type': 'Doritos Greater Linear space vs Pringles'},
         {'pk': 303, 'type': 'Linear SOS Index'}, {'pk': 304, 'type': 'Placement by shelf numbers_Top'},
         {'pk': 305, 'type': 'Placement by shelf numbers_Eye'}, {'pk': 306, 'type': 'Placement by shelf numbers_Middle'},
         {'pk': 307, 'type': 'Placement by shelf numbers_Bottom'}, {'pk': 308, 'type': 'Placement by shelf numbers'},
         {'pk': 309, 'type': 'Hero Placement'}, {'pk': 310, 'type': 'Hero SKU Placement by shelf numbers'},
         {'pk': 311, 'type': 'Hero SKU Placement by shelf numbers_Top'}, {'pk': 312, 'type': 'Hero SKU Placement by shelf numbers_Eye'},
         {'pk': 313, 'type': 'Hero SKU Placement by shelf numbers_Middle'}, {'pk': 314, 'type': 'Hero SKU Placement by shelf numbers_Bottom'},
         {'pk': 315, 'type': 'Hero SKU Stacking'}, {'pk': 316, 'type': 'Brand Full Bay'}, {'pk': 317, 'type': 'Hero SKU Price'},
         {'pk': 318, 'type': 'Hero SKU Promo Price'}, {'pk': 319, 'type': 'Product Blocking'}, {'pk': 320, 'type': 'Product Blocking Adjacency'},
         {'pk': 321, 'type': 'Number of Facings'}, {'pk': 322, 'type': 'Total Linear Space'}, {'pk': 323, 'type': 'Number of bays'},
         {'pk': 324, 'type': 'Number of shelves'}, {'pk': 325, 'type': 'Shelf Placement Vertical_Left'},
         {'pk': 326, 'type': 'Shelf Placement Vertical_Center'}, {'pk': 327, 'type': 'Shelf Placement Vertical_Right'},
         {'pk': 327, 'type': 'Brand Full Bay_90'}]
    )

    custom_entity = pd.DataFrame.from_records(
        [{'entity_type_fk': 1003, 'name': 'Healthier Multipack', 'parent_id': nan, 'pk': 11},
         {'entity_type_fk': 1004, 'name': 'Fun times together Tortilla', 'parent_id': nan, 'pk': 12},
         {'entity_type_fk': 1004, 'name': 'Fun times together Tubes', 'parent_id': nan, 'pk': 13},
         {'entity_type_fk': 1003, 'name': 'Premium Sharing', 'parent_id': nan, 'pk': 14},
         {'entity_type_fk': 1002, 'name': 'TRANSFORM-A-SNACK', 'parent_id': nan, 'pk': 15},
         {'entity_type_fk': 1005, 'name': 'Pringles_FTT_Tubes', 'parent_id': nan, 'pk': 165},
         {'entity_type_fk': 1005, 'name': 'Hula Hoops_LMP_Snacks', 'parent_id': nan, 'pk': 166},
         {'entity_type_fk': 1005, 'name': 'DORITOS GROUP', 'parent_id': nan, 'pk': 167},
         {'entity_type_fk': 1005, 'name': 'Walkers Crisps_Small MP PC', 'parent_id': nan, 'pk': 168}]
    )

    store_data = pd.DataFrame.from_records([{
        'store_type': 'CORE', 'additional_attribute_1': 'OT', 'additional_attribute_2': 'SAINSBURY',
        'additional_attribute_3': ''}])

    external_targets = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data',
                                    'external_targets.xlsx')

    exclusion_template_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data',
                                           'Inclusion_Exclusion_Template.xlsx')
    # exclusion_template_path ='{}/Data/Template_L&T_test_updated.xlsx'.format(os.path.dirname(os.path.realpath(__file__)))

    test_case_1 = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'test_case_1.xlsx')

    on_display_products = pd.DataFrame.from_records([{'probe_match_fk': 1, 'smart_attribute': 'stock'},
                                                     {'probe_match_fk': 2, 'smart_attribute': 'additional display'},
                                                     {'probe_match_fk': 9, 'smart_attribute': 'additional display'},
                                                     {'probe_match_fk': 10, 'smart_attribute': 'additional display'}])

    session_info_1 = pd.DataFrame.from_records([
        {'pk': 100, 'visit_date': '2019-03-29', 'store_fk': 1, 's_sales_rep_fk': 111, 'exclude_status_fk': None, 'status': 'Completed'}
    ])

    kpi_results_values_table = pd.DataFrame.from_records(
        [{'pk': 4, 'value': 'YES', 'kpi_result_type_fk': 2}, {'pk': 5, 'value': 'NO', 'kpi_result_type_fk': 2},
         {'pk': 6, 'value': 'HORIZONTAL', 'kpi_result_type_fk': 3}, {'pk': 7, 'value': 'VERTICAL', 'kpi_result_type_fk': 3}]
    )
    kpi_scores_values_table = pd.DataFrame.from_records(
        [{'pk': 4, 'value': 'YES', 'kpi_score_type_fk': 2}, {'pk': 5, 'value': 'NO', 'kpi_score_type_fk': 2},
         {'pk': 6, 'value': 'HORIZONTAL', 'kpi_score_type_fk': 3}, {'pk': 7, 'value': 'VERTICAL', 'kpi_score_type_fk': 3}]
    )

    data_json_1 = pd.DataFrame.from_records(
        [{'pk': 2, 'json_field': u'{"store_type": "CORE", "additional_attribute_1": "ALL"}'},
         {'pk': 3, 'json_field': u'{"store_type": "ALL", "additional_attribute_1": "OT"}'},
         {'pk': 10, 'json_field': u'{"store_type": "CORE", "additional_attribute_2": "SAINSBURY"}'}
         ])

    data_json_empty = pd.DataFrame(columns=['pk', 'json_field'])
    data_json_empty_with_pks = pd.DataFrame([
        {'pk': 2, 'json_field': u''}, {'pk': 3, 'json_field': u''}
    ])

    all_templates = pd.DataFrame.from_records(
        [{'template_fk': 1, 'template_name': 'Template 1', 'location_type': 'Primary Shelf'},
         {'template_fk': 2, 'template_name': 'Template 2', 'location_type': 'Secondary Shelf'}]
    )

    empty_exclusion_template = pd.DataFrame(columns=['KPI', 'Action', 'Type', 'Value'])
    exclusion_template_missing_action = pd.DataFrame([
        {'KPI': 'All', 'Action': 'Include', 'Type': 'location_type', 'Value': 'Primary Shelf'},
        {'KPI': 'ALL', 'Action': np.nan, 'Type': 'product_name', 'Value': 'General Empty'},
        {'KPI': 'All', 'Action': 'Exclude', 'Type': 'category', 'Value': 'Cat 1, Cat 2'}
    ])

    test_case_1_ass_result = pd.DataFrame([{'product_fk': 1,  'in_store': 1}, {'product_fk': 2,  'in_store': 1},
                                           {'product_fk': 5,  'in_store': 0}])

    external_targets_columns = ['kpi_operation_type_fk', 'operation_type', 'kpi_level_2_fk', 'store_type',
            'additional_attribute_1', 'additional_attribute_2', 'additional_attribute_3', 'numerator_type', 'numerator_value',
            'denominator_type', 'denominator_value', 'additional_filter_type_1',
            'additional_filter_value_1', 'Target', 'KPI Parent',
            'Shelves From Bottom To Include (data)', 'No of Shelves in Fixture (per bay) (key)', 'type']

    scene_info = pd.DataFrame([{'scene_fk': 1,  'template_fk': 1}, {'scene_fk': 2,  'template_fk': 1},
                               {'scene_fk': 3, 'template_fk': 2}])

    scene_kpi_results_test_case_1 = pd.DataFrame(
        [{'scene_fk': 2, 'kpi_level_2_fk': 304, 'numerator_id': 1, 'numerator_result': 5, 'denominator_result': 5},
         {'scene_fk': 2, 'kpi_level_2_fk': 305, 'numerator_id': 2, 'numerator_result': 2, 'denominator_result': 6},
         {'scene_fk': 2, 'kpi_level_2_fk': 306, 'numerator_id': 2, 'numerator_result': 2, 'denominator_result': 6},
         {'scene_fk': 2, 'kpi_level_2_fk': 307, 'numerator_id': 2, 'numerator_result': 2, 'denominator_result': 6},
         {'scene_fk': 2, 'kpi_level_2_fk': 307, 'numerator_id': 3, 'numerator_result': 1, 'denominator_result': 1},

         {'scene_fk': 1, 'kpi_level_2_fk': 304, 'numerator_id': 1, 'numerator_result': 2, 'denominator_result': 7},
         {'scene_fk': 1, 'kpi_level_2_fk': 305, 'numerator_id': 1, 'numerator_result': 3, 'denominator_result': 7},
         {'scene_fk': 1, 'kpi_level_2_fk': 307, 'numerator_id': 1, 'numerator_result': 2, 'denominator_result': 7},

         {'scene_fk': 1, 'kpi_level_2_fk': 305, 'numerator_id': 2, 'numerator_result': 6, 'denominator_result': 6},

         {'scene_fk': 1, 'kpi_level_2_fk': 306, 'numerator_id': 3, 'numerator_result': 3, 'denominator_result': 8},
         {'scene_fk': 1, 'kpi_level_2_fk': 307, 'numerator_id': 3, 'numerator_result': 5, 'denominator_result': 8},

         {'scene_fk': 1, 'kpi_level_2_fk': 306, 'numerator_id': 4, 'numerator_result': 3, 'denominator_result': 6},
         {'scene_fk': 1, 'kpi_level_2_fk': 304, 'numerator_id': 4, 'numerator_result': 3, 'denominator_result': 6},
         ])
    # required_template_tabs = [KPI_TAB, PRICE_TAB, SURVEY_TAB, AVAILABILITY_TAB, SOS_TAB, COUNT_TAB, PLANOGRAM_TAB]
    # columns_kpi_tab = [SET_NAME, KPI_NAME, KPI_TYPE, SPLIT_SCORE, DEPENDENCY, BONUS]
    # columns_survey_tab = [KPI_NAME, ATOMIC_KPI_NAME, EXPECTED_RESULT, SURVEY_QUESTION_CODE, STORE_TYPE, ATTRIBUTE_1, ATTRIBUTE_2]
    # columns_price_tab = [KPI_NAME, ATOMIC_KPI_NAME, TEMPLATE_NAME, TYPE1, TYPE2, TYPE3, VALUE1, VALUE2, VALUE3, TARGET,
    #                      SCORE, STORE_TYPE, ATTRIBUTE_1, ATTRIBUTE_2, BY_SCENE, KO_ONLY]
    # columns_avaialability_tab = [KPI_NAME, ATOMIC_KPI_NAME, AVAILABILITY_TYPE, TEMPLATE_NAME, TYPE1, TYPE2, TYPE3,
    #                              VALUE1, VALUE2, VALUE3, TARGET, SCORE, STORE_TYPE, ATTRIBUTE_1, ATTRIBUTE_2, BY_SCENE, KO_ONLY]
    # columns_sos_tab = [KPI_NAME, ATOMIC_KPI_NAME, TEMPLATE_NAME, CONDITION_1_NUMERATOR, CONDITION_1_NUMERATOR_TYPE,
    #                    CONDITION_1_TARGET, CONDITION_1_DENOMINATOR, CONDITION_1_DENOMINATOR_TYPE, CONDITION_2_NUMERATOR,
    #                    CONDITION_2_NUMERATOR_TYPE, CONDITION_2_DENOMINATOR, CONDITION_2_DENOMINATOR_TYPE,
    #                    CONDITION_2_TARGET, SCORE, STORE_TYPE, ATTRIBUTE_1, ATTRIBUTE_2, BY_SCENE, KO_ONLY]
    # columns_count_tab = [KPI_NAME, ATOMIC_KPI_NAME, TEMPLATE_NAME, TARGET, SCORE, STORE_TYPE, ATTRIBUTE_1, ATTRIBUTE_2,
    #                      BY_SCENE, KO_ONLY]
    # columns_planogram_tab = [KPI_NAME, ATOMIC_KPI_NAME, TEMPLATE_DISPLAY_NAME, TARGET, SCORE, STORE_TYPE, ATTRIBUTE_1, ATTRIBUTE_2]
    #
    # columns_kpi_results = [SET_NAME, KPI_NAME, ATOMIC_KPI_NAME, SCORE, MAX_SCORE]
    # kpi_set_names_from_template = ['COOLERS & MERCHANDISING', 'KEY PACK: Availability, Pricing, Activation',
    #                                'AVAILABILITY', 'PRICE COMPLIANCE', 'COMBOS & ACTIVATION', 'BONUS POINTS', 'TEST SET']
    # kpi_types_split_by_comma = 'Price,Survey,Availability,SOS,Count'
    # kpi_types_split_irregularly = 'Price,Survey, Availability ,SOS , Count'
    # kpi_types_one_value = 'Price'
    # kpi_types_empty_string = ''
    # kpi_types_name_with_space = 'Availability KPI ,SOS, Count'
    # string_represented_by_number = 200
    #
    # index_kpi_tab = [SET_NAME, KPI_NAME, KPI_TYPE, SPLIT_SCORE, DEPENDENCY, BONUS]+StoreTypes.store_list
    # index_count_tab = [KPI_NAME, TEMPLATE_NAME, ATOMIC_KPI_NAME, TARGET, SCORE, STORE_TYPE, ATTRIBUTE_1,
    #                    ATTRIBUTE_2, BY_SCENE, KO_ONLY]
    # index_survey_tab = [KPI_NAME, ATOMIC_KPI_NAME, EXPECTED_RESULT, SURVEY_QUESTION_CODE, SCORE, STORE_TYPE,
    #                     ATTRIBUTE_1, ATTRIBUTE_2]
    # index_price_tab = [KPI_NAME, ATOMIC_KPI_NAME, TEMPLATE_NAME, TYPE1, VALUE1, TYPE2, VALUE2, TYPE3, VALUE3, TARGET,
    #                    SCORE, STORE_TYPE, ATTRIBUTE_1, ATTRIBUTE_2]
    # index_sos_tab = [KPI_NAME, ATOMIC_KPI_NAME, TEMPLATE_NAME, CONDITION_1_NUMERATOR, CONDITION_1_NUMERATOR_TYPE,
    #                  CONDITION_1_DENOMINATOR, CONDITION_1_DENOMINATOR_TYPE, CONDITION_1_TARGET, CONDITION_2_NUMERATOR,
    #                  CONDITION_2_NUMERATOR_TYPE, CONDITION_2_DENOMINATOR, CONDITION_2_DENOMINATOR_TYPE,
    #                  CONDITION_2_TARGET, SCORE, STORE_TYPE, ATTRIBUTE_1, ATTRIBUTE_2]
    #
    # test_kpi_1_series = pd.Series(['TEST SET', 'TEST KPI 1', 'Survey', 'Y', '', '', '', '', '', '', '', ''],
    #                               index=index_kpi_tab)
    # avail_and_pricing_all_bonus_kpi_series = pd.Series(['BONUS POINTS', 'Availability and Pricing of ALL Key Packs ',
    #                                                     '', 'N', 'KEY PACK: Availability, Pricing, Activation', 'Y', 20,'',
    #                                                     '', '', '', ''], index=index_kpi_tab)
    # coolers_kpi_series = pd.Series(['COOLERS & MERCHANDISING', 'Coolers', 'Price, Survey, Availability, SOS, Count',
    #                                 'Y', '', 'N', '', '', '', '', '', ''], index=index_kpi_tab)
    # count_atomic_series = pd.Series(['Coolers', 'CCBSA Cooler, DOC Cooler', 'Min 4 x Cooler Doors', 4, 10,
    #                                  'L&T', 'Spaza Affordable', 'Gold', 'N', 'Y'], index=index_count_tab)
    # count_atomic_template_field_empty = pd.Series(['Coolers', '', 'Min 4 x Cooler Doors', 4, 10,
    #                                  'L&T', 'Spaza Affordable', 'Gold', 'N', 'N'], index=index_count_tab)
    # survey_atomic_series = pd.Series(['TEST KPI 1', 'Atomic KPI 1', 'Yes', 1, 5,
    #                                  'L&T', '', ''], index=index_survey_tab)
    # price_atomic_series = pd.Series(['SSD IC', 'Price compliance of 200ml Can', '', 'size', '200.0', 'brand_name',
    #                                  'FANTA ORANGE, COCA COLA, SPRITE, STONEY', 'form_factor', 'can', '5.0', '', 'L&T',
    #                                  'Spaza Affordable', ''], index=index_price_tab)
    # price_atomic_series_diffr_sizes = pd.Series(['SSD IC', 'Price compliance of 200ml Can', '', 'size', '200.0, 150.0', 'brand_name',
    #                                  'FANTA ORANGE, COCA COLA, SPRITE, STONEY', 'form_factor', 'can', '5.0', '', 'L&T',
    #                                  'Spaza Affordable', ''], index=index_price_tab)
    # price_atomic_series_one_type_value = pd.Series(['SSD IC', 'Price compliance of 200ml Can', '', '', '', 'brand_name',
    #                                  'FANTA ORANGE, COCA COLA, SPRITE, STONEY', '', '', '5.0', '', 'L&T',
    #                                  'Spaza Affordable', ''], index=index_price_tab)
    # price_atomic_series_missing_between = pd.Series(['SSD IC', 'Price compliance of 200ml Can', '', 'product_ean_code', '3434443,232323', '',
    #                                  '', 'form_factor', 'can', '5.0', '', 'L&T', 'Spaza Affordable', ''], index=index_price_tab)
    # price_atomic_series_no_type_values = pd.Series(['SSD IC', 'Price compliance of 200ml Can', '', '', '', '',
    #                                  '', '', '', '5.0', '', 'L&T', 'Spaza Affordable', ''], index=index_price_tab)
    # sos_atomic_series = pd.Series(['Coolers', 'CCBSA Quad Cola>= 50% of CCBSA SSDs - cold of which Diets/Zeros/Lights>=30%',
    #                                'CCBSA Cooler', 'Quad Cola', 'Attribute 2', 'SSD', 'Category', '50', 'Diets',
    #                                'Attribute 3', 'SSD', 'Category', '30', '5', 'L&T', 'Spaza Affordable', ''],
    #                               index=index_sos_tab)
    # sos_atomic_series_one_condition = pd.Series(
    #     ['Coolers', 'Diets > 65% out of all CCBSA SSD (CCBSA Cooler)',
    #      'CCBSA Cooler', '5449000234612,5449000027559,5449000234636', 'product_ean_code', 'SSD', 'category', '65', '',
    #      '', '', '', '', '5', 'L&T', 'Spaza Affordable', ''],
    #     index=index_sos_tab)

    scene_kpi_results_placement = pd.DataFrame.from_records(
        [{'context_id': None, 'denominator_id': None,  'denominator_result': 0,
          'kpi_level_2_fk': 300021, 'numerator_id': 11, 'numerator_result': 0, 'pk': 19, 'result': 0.0,
          'scene_fk': 4, 'score': 5.0, 'target': None, 'weight': None},
         {'context_id': None, 'denominator_id': None, 'denominator_result': 0, 'kpi_level_2_fk': 300029,
          'numerator_id': 11, 'numerator_result': 0, 'pk': 20, 'result': 0.0, 'scene_fk': 4, 'score': 5.0,
          'target': None, 'weight': None},
         {'context_id': None, 'denominator_id': None, 'denominator_result': 0, 'kpi_level_2_fk': 300030,
          'numerator_id': 11, 'numerator_result': 0, 'pk': 21, 'result': 0.0, 'scene_fk': 4, 'score': 5.0,
          'target': None, 'weight': None},
         {'context_id': None, 'denominator_id': None, 'denominator_result': 0, 'kpi_level_2_fk': 300021,
          'numerator_id': 11, 'numerator_result': 0, 'pk': 22, 'result': 100.0, 'scene_fk': 5, 'score': 4.0,
          'target': None, 'weight': None},
         {'context_id': None, 'denominator_id': None, 'denominator_result': 0, 'kpi_level_2_fk': 300029,
          'numerator_id': 11, 'numerator_result': 0, 'pk': 23, 'result': 0.0, 'scene_fk': 5, 'score': 5.0,
          'target': None, 'weight': None},
         {'context_id': None, 'denominator_id': None, 'denominator_result': 0, 'kpi_level_2_fk': 300030,
          'numerator_id': 11, 'numerator_result': 0, 'pk': 24, 'result': 100.0, 'scene_fk': 5, 'score': 4.0,
          'target': None, 'weight': None}]) # design result for placement

# class SCIFDataTestCCBZA_SAND(object):
#
#     scif_1 = pd.DataFrame.from_records([
#         {'pk': 1, 'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler', 'item_id': 1,
#          'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Coca-Cola 500ml', 'product_type': 'SKU',
#          'product_ean_code': '7290001594150', 'brand_name': 'Coca-Cola', 'facings': 2},
#         {'pk': 2, 'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler', 'item_id': 2,
#          'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Coca-Cola 300ml', 'product_type': 'SKU',
#          'product_ean_code': '7290001594151', 'brand_name': 'Coca-Cola', 'facings': 1},
#         {'pk': 3, 'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler', 'item_id': 3,
#          'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Fanta Orange 500ml', 'product_type': 'SKU',
#          'product_ean_code': '7290001594152', 'brand_name': 'Fanta', 'facings': 3},
#         {'pk': 4, 'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler', 'item_id': 4,
#          'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Fanta Exotic 1.5L',
#          'product_type': 'SKU', 'product_ean_code': '7290001594153', 'brand_name': 'Fanta', 'facings': 1},
#         {'pk': 5, 'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler', 'item_id': 5,
#          'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Coca-Cola Zero 500ml bottle',
#          'product_type': 'SKU', 'product_ean_code': '7290001594154', 'brand_name': 'Coca-Cola Zero', 'facings': 4},
#         {'pk': 6,'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler', 'item_id': 6,
#          'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Brand Strip Coca-Cola',
#          'product_type': 'POS', 'product_ean_code': '1000000000001', 'brand_name': 'Coca-Cola', 'facings': 1},
#         {'pk': 7, 'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler', 'item_id': 7,
#          'manufacturer_name': 'Jafora Tavori', 'store_id': 1000, 'product_name': 'Schweppes Flavourd sparkling Other',
#          'product_type': 'Other', 'product_ean_code': '7290001594160', 'brand_name': 'Schweppes', 'facings': 2},
#         {'pk': 8, 'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler', 'item_id': 8,
#          'manufacturer_name': 'Adir R. Trade', 'store_id': 1000, 'product_name': 'Jumex Juice Other',
#          'product_type': 'Other', 'product_ean_code': '7290001594161', 'brand_name': 'Jumex', 'facings': 1},
#
#         {'pk': 9, 'session_id': 160, 'scene_fk': 96, 'scene_id': 96, 'template_name': 'DOC Cooler', 'item_id': 1,
#          'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Coca-Cola 500ml', 'product_type': 'SKU',
#          'product_ean_code': '7290001594150', 'brand_name': 'Coca-Cola', 'facings': 1},
#         {'pk': 10, 'session_id': 160, 'scene_fk': 96, 'scene_id': 96, 'template_name': 'DOC Cooler', 'item_id': 9,
#          'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Coca-Cola Zero 330ml', 'product_type': 'SKU',
#          'product_ean_code': '7290001594155', 'brand_name': 'Coca-Cola Zero', 'facings': 2},
#         {'pk': 11, 'session_id': 160, 'scene_fk': 96, 'scene_id': 96, 'template_name': 'DOC Cooler', 'item_id': 10,
#          'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Sprite Zero 500ml bottle',
#          'product_type': 'SKU', 'product_ean_code': '7290001594156', 'brand_name': 'Sprite', 'facings': 3},
#         {'pk': 12, 'session_id': 160, 'scene_fk': 96, 'scene_id': 96, 'template_name': 'DOC Cooler', 'item_id': 7,
#          'manufacturer_name': 'Jafora Tavori', 'store_id': 1000, 'product_name': 'Schweppes Flavourd sparkling Other',
#          'product_type': 'Other', 'product_ean_code': '7290001594160', 'brand_name': 'Schweppes', 'facings': 1},
#         {'pk': 13, 'session_id': 160, 'scene_fk': 96, 'scene_id': 96, 'template_name': 'DOC Cooler', 'item_id': 13,
#          'manufacturer_name': 'Other', 'store_id': 1000, 'product_name': 'Empty',
#          'product_type': 'Empty', 'product_ean_code': None, 'brand_name': 'General', 'facings': 2},
#
#         {'pk': 14, 'session_id': 160, 'scene_fk': 97, 'scene_id': 97, 'template_name': 'Main Shelf', 'item_id': 11,
#          'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Neviot Water Other',
#          'product_type': 'Other', 'product_ean_code': '7290001594157', 'brand_name': 'Neviot', 'facings': 1},
#         {'pk': 15, 'session_id': 160, 'scene_fk': 97, 'scene_id': 97, 'template_name': 'Main Shelf', 'item_id': 12,
#          'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Fuze Tea Ice Tea Other',
#          'product_type': 'Other', 'product_ean_code': '7290001594158', 'brand_name': 'Fuze Tea', 'facings': 2},
#         {'pk': 16, 'session_id': 160, 'scene_fk': 97, 'scene_id': 97, 'template_name': 'Main Shelf', 'item_id': 13,
#          'manufacturer_name': 'Other', 'store_id': 1000, 'product_name': 'Empty',
#          'product_type': 'Empty', 'product_ean_code': None, 'brand_name': 'General', 'facings': 1},
#         {'pk': 17, 'session_id': 160, 'scene_fk': 97, 'scene_id': 97, 'template_name': 'Main Shelf', 'item_id': 3,
#          'manufacturer_name': KO_PRODUCTS, 'store_id': 1000, 'product_name': 'Fanta Orange 500ml',
#          'product_type': 'SKU', 'product_ean_code': '7290001594152', 'brand_name': 'Fanta', 'facings': 2}
#     ])
#
#     scif_no_manufacturer = pd.DataFrame.from_records([
#         {'pk': 7, 'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler', 'item_id': 7,
#          'manufacturer_name': 'Jafora Tavori', 'store_id': 1000, 'product_name': 'Schweppes Flavourd sparkling Other',
#          'product_type': 'Other', 'product_ean_code': '7290001594160', 'brand_name': 'Schweppes', 'facings': 2},
#         {'pk': 8, 'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler', 'item_id': 8,
#          'manufacturer_name': 'Adir R. Trade', 'store_id': 1000, 'product_name': 'Jumex Juice Other',
#          'product_type': 'Other', 'product_ean_code': '7290001594161', 'brand_name': 'Jumex', 'facings': 1},
#         {'pk': 16, 'session_id': 160, 'scene_fk': 97, 'scene_id': 97, 'template_name': 'Main Shelf', 'item_id': 13,
#          'manufacturer_name': 'Other', 'store_id': 1000, 'product_name': 'Empty',
#          'product_type': 'Empty', 'product_ean_code': None, 'brand_name': 'General', 'facings': 1},
#         {'pk': 12, 'session_id': 160, 'scene_fk': 96, 'scene_id': 96, 'template_name': 'DOC Cooler', 'item_id': 7,
#          'manufacturer_name': 'Jafora Tavori', 'store_id': 1000, 'product_name': 'Schweppes Flavourd sparkling Other',
#          'product_type': 'Other', 'product_ean_code': '7290001594160', 'brand_name': 'Schweppes', 'facings': 1},
#         {'pk': 13, 'session_id': 160, 'scene_fk': 96, 'scene_id': 96, 'template_name': 'DOC Cooler', 'item_id': 13,
#          'manufacturer_name': 'Other', 'store_id': 1000, 'product_name': 'Empty',
#          'product_type': 'Empty', 'product_ean_code': None, 'brand_name': 'General', 'facings': 2}
#     ])
#
#     # scif = pd.DataFrame.from_records([
#     #     {'session_id': 160, 'scene_fk': 95, 'scene_id': 95, 'template_name': 'CCBSA Cooler',
#     #
#     #     }
#     # ])
#
# class MatchProdSceneDataTestPEPSICOUK(object):
#     matches_scif_1 = pd.DataFrame.from_records([
#         {'pk': 1, 'probe_match_fk': 1, 'scene_fk': 95, 'product_fk': 1, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 2, 'scene_fk': 95, 'product_fk': 1, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 2, 'stacking_layer': 1},
#         {'pk': 3, 'scene_fk': 95, 'product_fk': 2, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 4, 'scene_fk': 95, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 5, 'scene_fk': 95, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 2, 'stacking_layer': 1},
#         {'pk': 6, 'scene_fk': 95, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 3, 'stacking_layer': 1},
#         {'pk': 7, 'scene_fk': 95, 'product_fk': 4, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 8, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 9, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
#          'facing_sequence_number': 2, 'stacking_layer': 1},
#         {'pk': 10, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
#          'facing_sequence_number': 3, 'stacking_layer': 1},
#         {'pk': 11, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 3,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 12, 'scene_fk': 95, 'product_fk': 6, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 13, 'scene_fk': 95, 'product_fk': 7, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 14, 'scene_fk': 95, 'product_fk': 7, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
#          'facing_sequence_number': 2, 'stacking_layer': 1},
#         {'pk': 15, 'scene_fk': 95, 'product_fk': 8, 'front_facing': 'Y', 'bay_number': 4, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#
#         {'pk': 16, 'scene_fk': 96, 'product_fk': 1, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 17, 'scene_fk': 96, 'product_fk': 9, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 18, 'scene_fk': 96, 'product_fk': 9, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 2, 'stacking_layer': 1},
#         {'pk': 19, 'scene_fk': 96, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 3,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 20, 'scene_fk': 96, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 3,
#          'facing_sequence_number': 2, 'stacking_layer': 1},
#         {'pk': 21, 'scene_fk': 96, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 22, 'scene_fk': 96, 'product_fk': 7, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
#          'facing_sequence_number': 2, 'stacking_layer': 1},
#         {'pk': 23, 'scene_fk': 96, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 24, 'scene_fk': 96, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
#          'facing_sequence_number': 2, 'stacking_layer': 1},
#
#         {'pk': 25, 'scene_fk': 97, 'product_fk': 11, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 26, 'scene_fk': 97, 'product_fk': 12, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 2, 'stacking_layer': 1},
#         {'pk': 27, 'scene_fk': 97, 'product_fk': 12, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 3, 'stacking_layer': 1},
#         {'pk': 28, 'scene_fk': 97, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 29, 'scene_fk': 97, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 2, 'stacking_layer': 1},
#         {'pk': 30, 'scene_fk': 97, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 3, 'stacking_layer': 1},
#     ])
#
#     matches_scif_for_filtering_less_bays = pd.DataFrame.from_records([
#         {'pk': 1, 'scene_fk': 95, 'product_fk': 1, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 2, 'scene_fk': 95, 'product_fk': 1, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 2, 'stacking_layer': 1},
#         {'pk': 3, 'scene_fk': 95, 'product_fk': 2, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 4, 'scene_fk': 95, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 5, 'scene_fk': 95, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 2, 'stacking_layer': 1},
#         {'pk': 6, 'scene_fk': 95, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 3, 'stacking_layer': 1},
#         {'pk': 7, 'scene_fk': 95, 'product_fk': 4, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 8, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 9, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
#          'facing_sequence_number': 2, 'stacking_layer': 1},
#         {'pk': 10, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
#          'facing_sequence_number': 3, 'stacking_layer': 1},
#         {'pk': 11, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 3,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 12, 'scene_fk': 95, 'product_fk': 6, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 13, 'scene_fk': 95, 'product_fk': 7, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 14, 'scene_fk': 95, 'product_fk': 7, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
#          'facing_sequence_number': 2, 'stacking_layer': 1},
#         {'pk': 15, 'scene_fk': 95, 'product_fk': 8, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
#          'facing_sequence_number': 3, 'stacking_layer': 1},
#
#         {'pk': 16, 'scene_fk': 96, 'product_fk': 1, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 17, 'scene_fk': 96, 'product_fk': 9, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 18, 'scene_fk': 96, 'product_fk': 9, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 2, 'stacking_layer': 1},
#         {'pk': 19, 'scene_fk': 96, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 3,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 20, 'scene_fk': 96, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 3,
#          'facing_sequence_number': 2, 'stacking_layer': 1},
#         {'pk': 21, 'scene_fk': 96, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 22, 'scene_fk': 96, 'product_fk': 7, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
#          'facing_sequence_number': 2, 'stacking_layer': 1},
#         {'pk': 23, 'scene_fk': 96, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 24, 'scene_fk': 96, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
#          'facing_sequence_number': 2, 'stacking_layer': 1},
#
#         {'pk': 25, 'scene_fk': 97, 'product_fk': 11, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 26, 'scene_fk': 97, 'product_fk': 12, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 2, 'stacking_layer': 1},
#         {'pk': 27, 'scene_fk': 97, 'product_fk': 12, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 3, 'stacking_layer': 1},
#         {'pk': 28, 'scene_fk': 97, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 1, 'stacking_layer': 1},
#         {'pk': 29, 'scene_fk': 97, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 2, 'stacking_layer': 1},
#         {'pk': 30, 'scene_fk': 97, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 3, 'stacking_layer': 1},
#     ])
#
#     matches_price_presence = pd.DataFrame.from_records([
#         {'pk': 1, 'scene_fk': 95, 'product_fk': 1, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1, 'price': None},
#         {'pk': 2, 'scene_fk': 95, 'product_fk': 1, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 2, 'stacking_layer': 1, 'price': None},
#         {'pk': 3, 'scene_fk': 95, 'product_fk': 2, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 5},
#         {'pk': 4, 'scene_fk': 95, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 7},
#         {'pk': 5, 'scene_fk': 95, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 2, 'stacking_layer': 1, 'price': 5.50},
#         {'pk': 6, 'scene_fk': 95, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 3, 'stacking_layer': 1, 'price': 6},
#         {'pk': 7, 'scene_fk': 95, 'product_fk': 4, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 7},
#         {'pk': 8, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
#          'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 6},
#         {'pk': 9, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
#          'facing_sequence_number': 2, 'stacking_layer': 1, 'price': 5.34},
#         {'pk': 10, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
#          'facing_sequence_number': 3, 'stacking_layer': 1, 'price': 5.3},
#         {'pk': 11, 'scene_fk': 95, 'product_fk': 5, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 3,
#          'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 6},
#         {'pk': 12, 'scene_fk': 95, 'product_fk': 6, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 5.1},
#         {'pk': 13, 'scene_fk': 95, 'product_fk': 7, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 5.5},
#         {'pk': 14, 'scene_fk': 95, 'product_fk': 7, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
#          'facing_sequence_number': 2, 'stacking_layer': 1, 'price': 6.2},
#         {'pk': 15, 'scene_fk': 95, 'product_fk': 8, 'front_facing': 'Y', 'bay_number': 3, 'shelf_number': 1,
#          'facing_sequence_number': 3, 'stacking_layer': 1, 'price': 7},
#
#         {'pk': 16, 'scene_fk': 96, 'product_fk': 1, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 5},
#         {'pk': 17, 'scene_fk': 96, 'product_fk': 9, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 5},
#         {'pk': 18, 'scene_fk': 96, 'product_fk': 9, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 2, 'stacking_layer': 1, 'price': None},
#         {'pk': 19, 'scene_fk': 96, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 3,
#          'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 7},
#         {'pk': 20, 'scene_fk': 96, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 3,
#          'facing_sequence_number': 2, 'stacking_layer': 1, 'price': 10},
#         {'pk': 21, 'scene_fk': 96, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 8.45},
#         {'pk': 22, 'scene_fk': 96, 'product_fk': 7, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
#          'facing_sequence_number': 2, 'stacking_layer': 1, 'price': 6},
#         {'pk': 23, 'scene_fk': 96, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
#          'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 8.15},
#         {'pk': 24, 'scene_fk': 96, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
#          'facing_sequence_number': 2, 'stacking_layer': 1, 'price': 7},
#
#         {'pk': 25, 'scene_fk': 97, 'product_fk': 11, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 5},
#         {'pk': 26, 'scene_fk': 97, 'product_fk': 12, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 2, 'stacking_layer': 1, 'price': None},
#         {'pk': 27, 'scene_fk': 97, 'product_fk': 12, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 3, 'stacking_layer': 1, 'price': 6.99},
#         {'pk': 28, 'scene_fk': 97, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 5},
#         {'pk': 29, 'scene_fk': 97, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 2, 'stacking_layer': 1, 'price': 5.95},
#         {'pk': 30, 'scene_fk': 97, 'product_fk': 3, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 3, 'stacking_layer': 1, 'price': 5.35},
#
#         {'pk': 16, 'scene_fk': 98, 'product_fk': 1, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 5},
#         {'pk': 17, 'scene_fk': 98, 'product_fk': 9, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 5},
#         {'pk': 18, 'scene_fk': 98, 'product_fk': 9, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 2,
#          'facing_sequence_number': 2, 'stacking_layer': 1, 'price': None},
#         {'pk': 19, 'scene_fk': 98, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 3,
#          'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 7},
#         {'pk': 20, 'scene_fk': 98, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 1, 'shelf_number': 3,
#          'facing_sequence_number': 2, 'stacking_layer': 1, 'price': 10},
#         {'pk': 21, 'scene_fk': 98, 'product_fk': 10, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
#          'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 8.45},
#         {'pk': 22, 'scene_fk': 98, 'product_fk': 7, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 1,
#          'facing_sequence_number': 2, 'stacking_layer': 1, 'price': 6},
#         {'pk': 23, 'scene_fk': 98, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
#          'facing_sequence_number': 1, 'stacking_layer': 1, 'price': 8.15},
#         {'pk': 24, 'scene_fk': 98, 'product_fk': 13, 'front_facing': 'Y', 'bay_number': 2, 'shelf_number': 2,
#          'facing_sequence_number': 2, 'stacking_layer': 1, 'price': 8.05},
#     ])