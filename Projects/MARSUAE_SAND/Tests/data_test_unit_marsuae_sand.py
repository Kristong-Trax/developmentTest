import datetime
import os
import pandas as pd
from numpy import nan
import numpy as np


class DataTestNode(object):
    atomics_df_1 = pd.DataFrame([{'kpi_type': 'one', 'kpi_parent': None, 'kpi_child': ['two', 'four']},
                               {'kpi_type': 'two', 'kpi_parent': 'one', 'kpi_child': 'five'},
                               {'kpi_type': 'three', 'kpi_parent': 'five', 'kpi_child': None},
                               {'kpi_type': 'four', 'kpi_parent': 'one', 'kpi_child': None},
                               {'kpi_type': 'five', 'kpi_parent': 'two', 'kpi_child': 'three'},
                               {'kpi_type': 'six', 'kpi_parent': 'two', 'kpi_child': None}])

    atomics_df_2 = pd.DataFrame([{'kpi_type': 'one', 'kpi_parent': None, 'kpi_child': ['two', 'four']},
                                 {'kpi_type': 'seven', 'kpi_parent': None, 'kpi_child': None},
                                 {'kpi_type': 'two', 'kpi_parent': 'one', 'kpi_child': 'five'},
                                 {'kpi_type': 'three', 'kpi_parent': 'two', 'kpi_child': None},
                                 {'kpi_type': 'four', 'kpi_parent': 'one', 'kpi_child': None},
                                 {'kpi_type': 'five', 'kpi_parent': None, 'kpi_child': None},
                                 {'kpi_type': 'six', 'kpi_parent': 'two', 'kpi_child': None}])

    atomics_df_3 = pd.DataFrame([{'kpi_type': 'one', 'kpi_parent': None, 'kpi_child': None},
                                 {'kpi_type': 'two', 'kpi_parent': None, 'kpi_child': None},
                                 {'kpi_type': 'three', 'kpi_parent': None, 'kpi_child': None},
                                 {'kpi_type': 'four', 'kpi_parent': None, 'kpi_child': None}])


class DataTestUnitMarsuae(object):

    kpi_static_data = pd.DataFrame.from_records(
        [{'pk': 3000, 'type': u'Total UAE Score'}, {'pk': 3001, 'type': u'Chocolate & Ice Cream'},
         {'pk': 3002, 'type': u'Gum & Fruity'}, {'pk': 3003, 'type': u'Pet Food'},
         {'pk': 3004, 'type': u'Price'}, {'pk': 3005, 'type': u'Checkout Penetration - Chocolate'},
         {'pk': 3006, 'type': u'Checkout Penetration - Gum'}, {'pk': 3007, 'type': u'Freezer Penetration - Ice Cream'},
         {'pk': 3008, 'type': u'Gum / Fruity Checkout Compliance'}, {'pk': 3009, 'type': u'NBL - Chocolate Checkout'},
         {'pk': 3010, 'type': u'NBL - Chocolate Display'}, {'pk': 3011, 'type': u'NBL - Chocolate Main'},
         {'pk': 3012, 'type': u'NBL - Gum Checkout'}, {'pk': 3013, 'type': u'NBL - Gum Main'},
         {'pk': 3014, 'type': u'NBL - Ice Cream Main'}, {'pk': 3015, 'type': u'NBL - Petfood Main'},
         {'pk': 3016, 'type': u'Red Block Compliance - Main'}, {'pk': 3017, 'type': u'NBL - Chocolate Checkout - SKU'},
         {'pk': 3018, 'type': u'NBL - Chocolate Display - SKU'}, {'pk': 3019, 'type': u'NBL - Chocolate Main - SKU'},
         {'pk': 3020, 'type': u'NBL - Gum Checkout - SKU'}, {'pk': 3021, 'type': u'NBL - Gum Main - SKU'},
         {'pk': 3022, 'type': u'NBL - Ice Cream Main - SKU'}, {'pk': 3023, 'type': u'NBL - Petfood Main - SKU'},
         {'pk': 3024, 'type': u'Red Block Compliance - Main - SKU'},
         {'pk': 3025, 'type': u'POI Compliance - Chocolate / Ice Cream'},
         {'pk': 3026, 'type': u'POI Compliance - Gum'},
         {'pk': 3027, 'type': u'POI Compliance - Gum / Fruity Checkout'},
         {'pk': 3028, 'type': u'SOD - Chocolate Display'}, {'pk': 3029, 'type': u'SOS - Chocolate Checkout'},
         {'pk': 3030, 'type': u'SOS - Chocolate Main'}, {'pk': 3031, 'type': u'SOS - Gum Checkout'},
         {'pk': 3032, 'type': u'SOS - Gum Main'}, {'pk': 3033, 'type': u'SOS - Gum/Fruity Checkout'},
         {'pk': 3034, 'type': u'SOS - Petfood Main'},
         {'pk': 3035, 'type': u'Checkout Penetration - Chocolate_2'},
         {'pk': 3036, 'type': u'Checkout Penetration - Gum_2'},
         {'pk': 3037, 'type': u'Freezer Penetration - Ice Cream_2'},
         {'pk': 3038, 'type': u'SOD - Chocolate Display_2'}, {'pk': 3039, 'type': u'SOS - Chocolate Checkout_2'},
         {'pk': 3040, 'type': u'SOS - Chocolate Main_2'}, {'pk': 3041, 'type': u'SOS - Gum Checkout_2'},
         {'pk': 3042, 'type': u'SOS - Gum Main_2'}, {'pk': 3043, 'type': u'SOS - Gum/Fruity Checkout_2'},
         {'pk': 3044, 'type': u'SOS - Petfood Main_2'}]
    )

    store_data_sss_a = pd.DataFrame.from_records(
        [{'pk': 1, 'store_type': 'SSS A', 'store_number_1': '10388932', 'additional_attribute_1': 'All Others',
          'additional_attribute_2': 'General Trade', 'additional_attribute_3': 'SSS'}])

    store_data_hypers = pd.DataFrame.from_records(
        [{'pk': 2, 'store_type': 'Hypers', 'store_number_1': '10097162', 'additional_attribute_1': 'Hypers',
          'additional_attribute_2': 'MT', 'additional_attribute_3': 'Lulu'}])

    store_data_supers_a = pd.DataFrame.from_records(
        [{'pk': 3, 'store_type': 'Supers A', 'store_number_1': '10388934', 'additional_attribute_1': 'All Others',
          'additional_attribute_2': 'General Trade', 'additional_attribute_3': 'Supers'}])

    store_info_dict_other_type = {'pk': 3, 'store_type': 'Other', 'store_number_1': '100978891',
                                  'additional_attribute_1': 'Other', 'additional_attribute_2': 'Other',
                                  'additional_attribute_3': 'Lulu'}

    input_df_for_targets = pd.DataFrame.from_records(
        [{'score_logic': 'Binary', 'Target': '0.5', 'kpi_type': 'kpi_a', 'kpi_fk':1},
         {'score_logic': 'Relative Score', 'Target': None, 'kpi_type': 'kpi_b', 'kpi_fk':2},
         {'score_logic': 'Binary', 'Target': np.nan, 'kpi_type': 'kpi_c', 'kpi_fk':3},
         {'score_logic': 'Tiered', 'Target': '', 'kpi_type': 'kpi_d', 'kpi_fk':4},
         {'score_logic': 'Relative Score', 'Target': '', 'kpi_type': 'kpi_e', 'kpi_fk':5}]
    )

    # exclusion_template_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data',
    #                                        'Inclusion_Exclusion_Template.xlsx')
    # exclusion_template_path ='{}/Data/Template_L&T_test_updated.xlsx'.format(os.path.dirname(os.path.realpath(__file__)))

    # test_case_2 = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'test_case_2_empty_scene.xlsx')

    session_info_1 = pd.DataFrame.from_records([
        {'pk': 101, 'visit_date': '2019-06-18', 'store_fk': 1, 's_sales_rep_fk': 42, 'exclude_status_fk': None,
         'status': 'Completed'}
    ])

    session_info_2 = pd.DataFrame.from_records([
        {'pk': 102, 'visit_date': '2019-06-27', 'store_fk': 2, 's_sales_rep_fk': 84, 'exclude_status_fk': None,
         'status': 'Completed'}
    ])

    kpi_results_values_table = pd.DataFrame.from_records(
        [{'pk': 1, 'value': 'OOS', 'kpi_result_type_fk': 1}, {'pk': 2, 'value': 'DISTRIBUTED', 'kpi_result_type_fk': 1},
         {'pk': 3, 'value': 'EXTRA', 'kpi_result_type_fk': 1}]
    )

    # kpi_scores_values_table = pd.DataFrame.from_records(
    #     [{'pk': 4, 'value': 'YES', 'kpi_score_type_fk': 2}, {'pk': 5, 'value': 'NO', 'kpi_score_type_fk': 2},
    #      {'pk': 6, 'value': 'HORIZONTAL', 'kpi_score_type_fk': 3}, {'pk': 7, 'value': 'VERTICAL', 'kpi_score_type_fk': 3}]
    # )

    data_json_1 = pd.DataFrame.from_records(
        [{'pk': 2, 'json_field': u'{"store_att_name_1": "store_type", "store_att_value_1": "SSS A"}'},
         {'pk': 3, 'json_field': u'{"store_att_name_1": "store_type", "store_att_value_1": "Hypers"}'},
         {'pk': 10, 'json_field': u'{"store_att_name_1": "store_type", "store_att_value_1": '
                                  u'["SSS A", "SSS B", "Impulse A", "Impulse B", "Convenience A", '
                                  u'"Convenience B", "Convenience C"]}'}
         ])

    data_json_empty = pd.DataFrame(columns=['pk', 'json_field'])
    data_json_empty_with_pks = pd.DataFrame([
        {'pk': 2, 'json_field': u''}, {'pk': 3, 'json_field': u''}
    ])

    all_products_scene = pd.DataFrame([
        {'product_fk': 1, 'product_name': 'Product 1'}, {'product_fk': 2, 'product_name': 'Product 2'},
        {'product_fk': 3, 'product_name': 'Product 3'}, {'product_fk': 4, 'product_name': 'Product 4'}
    ])

    scene_1_no_prices = pd.DataFrame([
        {'probe_match_fk': 1, 'scene_fk': 1, 'product_fk': 1, 'price': None, 'promotion_price': None},
        {'probe_match_fk': 2, 'scene_fk': 1, 'product_fk': 1, 'price': None, 'promotion_price': None},
        {'probe_match_fk': 3, 'scene_fk': 1, 'product_fk': 2, 'price': None, 'promotion_price': None},
        {'probe_match_fk': 4, 'scene_fk': 1, 'product_fk': 3, 'price': None, 'promotion_price': None},
    ])

    scene_1_scif = pd.DataFrame([{'product_fk': 1, 'product_type': 'SKU'}, {'product_fk': 2,  'product_type': 'SKU'},
                                 {'product_fk': 3, 'product_type': 'SKU'}])

    scene_2 = pd.DataFrame([
        {'probe_match_fk': 1, 'scene_fk': 1, 'product_fk': 1, 'price': 4, 'promotion_price': None},
        {'probe_match_fk': 2, 'scene_fk': 1, 'product_fk': 1, 'price': None, 'promotion_price': None},
        {'probe_match_fk': 3, 'scene_fk': 1, 'product_fk': 1, 'price': 4.5, 'promotion_price': None},
        {'probe_match_fk': 4, 'scene_fk': 1, 'product_fk': 3, 'price': None, 'promotion_price': None},
        {'probe_match_fk': 5, 'scene_fk': 1, 'product_fk': 3, 'price': 2, 'promotion_price': None},
        {'probe_match_fk': 6, 'scene_fk': 1, 'product_fk': 2, 'price': None, 'promotion_price': None},
    ])

    scene_2_scif = pd.DataFrame([
        {'product_fk': 1,  'product_type': 'SKU'}, {'product_fk': 2,  'product_type': 'SKU'},
        {'product_fk': 3,  'product_type': 'SKU'}
    ])

    scene_3 = pd.DataFrame([
        {'probe_match_fk': 1, 'scene_fk': 1, 'product_fk': 1, 'price': 4, 'promotion_price': None},
        {'probe_match_fk': 2, 'scene_fk': 1, 'product_fk': 1, 'price': 3, 'promotion_price': 5},
        {'probe_match_fk': 3, 'scene_fk': 1, 'product_fk': 1, 'price': None, 'promotion_price': 4.8},
        {'probe_match_fk': 4, 'scene_fk': 1, 'product_fk': 3, 'price': 2, 'promotion_price': 1},
        {'probe_match_fk': 5, 'scene_fk': 1, 'product_fk': 3, 'price': 2, 'promotion_price': 1.5},
    ])

    scene_3_scif = pd.DataFrame([
        {'product_fk': 1,  'product_type': 'SKU'}, {'product_fk': 2,  'product_type': 'SKU'},
        {'product_fk': 3,  'product_type': 'SKU'}
    ])

    scene_4_with_non_mars = pd.DataFrame([
        {'probe_match_fk': 1, 'scene_fk': 1, 'product_fk': 1, 'price': 5, 'promotion_price': None},
        {'probe_match_fk': 2, 'scene_fk': 1, 'product_fk': 1, 'price': None, 'promotion_price': None},
        {'probe_match_fk': 3, 'scene_fk': 1, 'product_fk': 4, 'price': 6, 'promotion_price': None},
        {'probe_match_fk': 4, 'scene_fk': 1, 'product_fk': 3, 'price': None, 'promotion_price': None},
    ])
    scene_4_scif = pd.DataFrame([
        {'product_fk': 1,  'product_type': 'SKU'}, {'product_fk': 4,  'product_type': 'Other'},
        {'product_fk': 3,  'product_type': 'SKU'}
    ])
    scene_5 = pd.DataFrame([
        {'probe_match_fk': 1, 'scene_fk': 1, 'product_fk': 1, 'price': 4, 'promotion_price': None},
        {'probe_match_fk': 2, 'scene_fk': 1, 'product_fk': 2, 'price': None, 'promotion_price': 5},
    ])
    scene_5_scif = pd.DataFrame([
        {'product_fk': 1, 'product_type': 'SKU'}, {'product_fk': 2, 'product_type': 'SKU'},
    ])

    #change product pks
    assortment_store_sss_a = pd.DataFrame.from_records([
        # NBL - Chocolate Checkout
        {'additional_attributes': '{}', 'assortment_fk': 128, 'assortment_group_fk': 94, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3009, 'kpi_fk_lvl3': 3017,
         'product_fk': 2, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 128, 'assortment_group_fk': 94, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3009, 'kpi_fk_lvl3': 3017,
         'product_fk': 3, 'super_group_target': None, 'target': nan},
        # NBL - Chocolate Main
        {'additional_attributes': '{}', 'assortment_fk': 147, 'assortment_group_fk': 113, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3011, 'kpi_fk_lvl3': 3019,
         'product_fk': 1, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 147, 'assortment_group_fk': 113, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3011, 'kpi_fk_lvl3': 3019,
         'product_fk': 2, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 147, 'assortment_group_fk': 113, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3011, 'kpi_fk_lvl3': 3019,
         'product_fk': 12, 'super_group_target': None, 'target': nan},
        # Red Block Compliance - Main
        {'additional_attributes': '{}', 'assortment_fk': 152, 'assortment_group_fk': 118, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3016, 'kpi_fk_lvl3': 3024,
         'product_fk': 1, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 152, 'assortment_group_fk': 118, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3016, 'kpi_fk_lvl3': 3024,
         'product_fk': 2, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 152, 'assortment_group_fk': 118,
         'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3016, 'kpi_fk_lvl3': 3024,
         'product_fk': 3, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 152, 'assortment_group_fk': 118,
         'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3016, 'kpi_fk_lvl3': 3024,
         'product_fk': 8, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 152, 'assortment_group_fk': 118,
         'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3016, 'kpi_fk_lvl3': 3024,
         'product_fk': 5, 'super_group_target': None, 'target': nan},
        # NBL - Gum Checkout
        {'additional_attributes': '{}', 'assortment_fk': 135, 'assortment_group_fk': 101, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3012, 'kpi_fk_lvl3': 3020,
         'product_fk': 7, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 135, 'assortment_group_fk': 101,
         'assortment_super_group_fk': nan, 'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan,
         'kpi_fk_lvl2': 3012, 'kpi_fk_lvl3': 3020,
         'product_fk': 8, 'super_group_target': None, 'target': nan},
    ])

    all_templates = pd.DataFrame.from_records(
        [{'template_fk': 1, 'template_name': 'Main shelf chocolate', 'template_group': 'Chocolate'},
         {'template_fk': 2, 'template_name': 'Checkout Chocolate', 'template_group': 'Chocolate'},
         {'template_fk': 3, 'template_name': 'Secondary Display 1.2x1.2', 'template_group': 'Chocolate'},
         {'template_fk': 4, 'template_name': 'POI Chocolate', 'template_group': 'Chocolate'},
         {'template_fk': 5, 'template_name': 'Main Shelf Gum and Confectionary',
          'template_group': 'Gum and Confectionary'},
         {'template_fk': 6, 'template_name': 'Ice Cream MBL', 'template_group': 'Ice Cream'},
         {'template_fk': 7, 'template_name': 'Checkout Gum & Confectionary',
          'template_group': 'Gum and Confectionary'},
         {'template_fk': 8, 'template_name': 'POI Gum & Confectionary', 'template_group': 'Gum and Confectionary'},
         {'template_fk': 9, 'template_name': 'Main Shelf Pet food', 'template_group': 'Pet food'}]
    )

    external_targets = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data',
                                    'external_targets.xlsx')

    test_case_1 = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'test_case_1.xlsx')
    kpi_results_df_for_kpi_combination_test_1 = pd.DataFrame.from_records(
        [{'kpi_fk': 3033, 'kpi_type': 'SOS - Gum/Fruity Checkout', 'result': None, 'score': 1, 'weight': 0,
          'score_by_weight': 0, 'parent_name': 'Gum & Fruity'},
         {'kpi_fk': 3027, 'kpi_type': 'POI Compliance - Gum / Fruity Checkout', 'result': None, 'score': 100,
          'weight': 0, 'score_by_weight': 0, 'parent_name': 'Gum & Fruity'},
         {'kpi_fk': 3012, 'kpi_type': 'NBL - Gum Checkout', 'result': None, 'score': 1, 'weight': 0,
          'score_by_weight': 0, 'parent_name': 'Gum & Fruity'}]
    )[['kpi_fk', 'kpi_type', 'result', 'score', 'weight', 'score_by_weight', 'parent_name']]

    kpi_results_df_for_kpi_combination_test_2 = pd.DataFrame.from_records(
        [{'kpi_fk': 3033, 'kpi_type': 'SOS - Gum/Fruity Checkout', 'result': None, 'score': 0, 'weight': 0,
          'score_by_weight': 0, 'parent_name': 'Gum & Fruity'},
         {'kpi_fk': 3027, 'kpi_type': 'POI Compliance - Gum / Fruity Checkout', 'result': None, 'score': 100,
          'weight': 0, 'score_by_weight': 0, 'parent_name': 'Gum & Fruity'},
         {'kpi_fk': 3012, 'kpi_type': 'NBL - Gum Checkout', 'result': None, 'score': 1, 'weight': 0,
          'score_by_weight': 0, 'parent_name': 'Gum & Fruity'}]
    )[['kpi_fk', 'kpi_type', 'result', 'score', 'weight', 'score_by_weight', 'parent_name']]

    kpi_results_df_for_kpi_combination_test_3 = pd.DataFrame.from_records(
        [{'kpi_fk': 3033, 'kpi_type': 'SOS - Gum/Fruity Checkout', 'result': None, 'score': 0, 'weight': 0,
          'score_by_weight': 0, 'parent_name': 'Gum & Fruity'},
         {'kpi_fk': 3027, 'kpi_type': 'POI Compliance - Gum / Fruity Checkout', 'result': None, 'score': 0,
          'weight': 0, 'score_by_weight': 0, 'parent_name': 'Gum & Fruity'},
         {'kpi_fk': 3012, 'kpi_type': 'NBL - Gum Checkout', 'result': None, 'score': 1, 'weight': 0,
          'score_by_weight': 0, 'parent_name': 'Gum & Fruity'}]
    )[['kpi_fk', 'kpi_type', 'result', 'score', 'weight', 'score_by_weight', 'parent_name']]

    kpi_results_df_for_cat_level_all_cat = pd.DataFrame(
        [{'kpi_fk': 3033, 'score_by_weight': 10, 'parent_name': 'Gum & Fruity'},
         {'kpi_fk': 3011, 'score_by_weight': 5, 'parent_name': 'Chocolate & Ice Cream'},
         {'kpi_fk': 3030, 'score_by_weight': 20, 'parent_name': 'Chocolate & Ice Cream'},
         {'kpi_fk': 3005, 'score_by_weight': 20, 'parent_name': 'Chocolate & Ice Cream'},
         {'kpi_fk': 3015, 'score_by_weight': 40, 'parent_name': 'Pet Food'},
         {'kpi_fk': 3034, 'score_by_weight': 60, 'parent_name': 'Pet Food'}]
    )

    kpi_results_df_for_cat_level_2_cat = pd.DataFrame(
        [{'kpi_fk': 3030, 'score_by_weight': 20, 'parent_name': 'Chocolate & Ice Cream'},
         {'kpi_fk': 3005, 'score_by_weight': 10, 'parent_name': 'Chocolate & Ice Cream'},
         {'kpi_fk': 3015, 'score_by_weight': 40, 'parent_name': 'Pet Food'},
         {'kpi_fk': 3034, 'score_by_weight': 60, 'parent_name': 'Pet Food'}]
    )

    external_targets_columns = ['pk', 'kpi_operation_type_fk', 'kpi_level_2_fk', 'key_json', 'data_json', 'start_date',
                                'end_date', 'received_time', 'operation_type', 'kpi_type', 'Template Group',
                                'store_att_name_1', 'store_att_value_1', 'KPI Family', 'KPI Level 2 Name',
                                'Target', 'Template Name', 'Weight', 'exclude_param_type_1',
                                'exclude_param_value_1', 'kpi_child', 'kpi_parent', 'param_type_1/numerator_type',
                                'param_type_2/denom_type', 'param_value_1/numerator_value',
                                'param_value_2/denom_value', 'score_cond_score_1', 'score_cond_score_2',
                                'score_cond_score_3', 'score_cond_score_4', 'score_cond_target_1',
                                'score_cond_target_2', 'score_cond_target_3', 'score_cond_target_4', 'score_logic',
                                'type']

    block_results_empty = pd.DataFrame(columns=['cluster', 'scene_fk', 'orientation',
                                                'facing_percentage', 'is_block'])
    block_results_failed = pd.DataFrame.from_records([{'cluster': 1, 'scene_fk': 9, 'orientation': '',
                                                       'facing_percentage': 1, 'is_block': False},
                                                      {'cluster': 2, 'scene_fk': 9, 'orientation': '',
                                                       'facing_percentage': 1, 'is_block': False}])

    cluster_7_1 = {1: {'match_fk': set([604]), 'product_fk': set([1])},
                   2: {'match_fk': set([605]), 'product_fk': set([1])},
                   3: {'match_fk': set([606]), 'product_fk': set([2])}}

    block_results_sc_7 = pd.DataFrame.from_records([{'cluster': pd.Series({'nodes': cluster_7_1}),
                                                     'scene_fk': 7, 'orientation': '',
                                                     'facing_percentage': 1, 'is_block': True}
                                                   ])

    cluster_8_1 = {1: {'match_fk': set([703]), 'product_fk': set([5])},
                   2: {'match_fk': set([704]), 'product_fk': set([5])},
                   3: {'match_fk': set([705]), 'product_fk': set([5])},
                   4: {'match_fk': set([706]), 'product_fk': set([5])},
                   5: {'match_fk': set([707]), 'product_fk': set([5])},
                   6: {'match_fk': set([708]), 'product_fk': set([5])},
                   }

    block_results_sc_8 = pd.DataFrame.from_records([{'cluster': pd.Series({'nodes': cluster_8_1}),
                                                     'scene_fk': 8, 'orientation': '',
                                                     'facing_percentage': 1, 'is_block': True}
                                                    ])

    cluster_10_1 = {1: {'match_fk': set([902]), 'product_fk': set([1])},
                    2: {'match_fk': set([907]), 'product_fk': set([2])},
                    3: {'match_fk': set([908]), 'product_fk': set([3])}
                    }

    cluster_10_2 = {1: {'match_fk': set([905]), 'product_fk': set([8])},
                    2: {'match_fk': set([906]), 'product_fk': set([8])},
                    3: {'match_fk': set([910]), 'product_fk': set([8])},
                    4: {'match_fk': set([911]), 'product_fk': set([8])},
                    }

    block_results_sc_10 = pd.DataFrame.from_records([
        {'cluster': pd.Series({'nodes': cluster_10_1}), 'scene_fk': 10, 'orientation': '', 'facing_percentage': 3/7.0,
         'is_block': True},
        {'cluster': pd.Series({'nodes': cluster_10_2}), 'scene_fk': 10, 'orientation': '', 'facing_percentage': 4/7.0,
         'is_block': True}
    ])

    scenes_for_display_1 = pd.DataFrame.from_records([
        {'scene_fk': 1, 'template_fk': 1, 'template_group': 'Chocolate'}, {'scene_fk': 2, 'template_fk': 2,
                                                                           'template_group': 'Chocolate'},
        {'scene_fk': 3, 'template_fk': 2, 'template_group': 'Chocolate'},
        {'scene_fk': 4, 'template_fk': 4, 'template_group': 'Chocolate'}
    ])

    scenes_for_display_2 = pd.DataFrame.from_records([
        {'scene_fk': 1, 'template_fk': 1, 'template_group': 'Chocolate'},
        {'scene_fk': 2, 'template_fk': 2, 'template_group': 'Chocolate'},
        {'scene_fk': 3, 'template_fk': 2, 'template_group': 'Chocolate'}
    ])

    scenes_for_display_3 = pd.DataFrame.from_records([
        {'scene_fk': 1, 'template_fk': 1, 'template_group': 'Chocolate'},
        {'scene_fk': 2, 'template_fk': 2, 'template_group': 'Chocolate'},
        {'scene_fk': 3, 'template_fk': 2, 'template_group': 'Chocolate'},
        {'scene_fk': 4, 'template_fk': 4, 'template_group': 'Chocolate'},
        {'scene_fk': 5, 'template_fk': 4, 'template_group': 'Chocolate'}
    ])

    scenes_for_display_including_no_tags = pd.DataFrame.from_records([
        {'scene_fk': 1, 'template_fk': 1, 'template_group': 'Chocolate'},
        {'scene_fk': 2, 'template_fk': 2, 'template_group': 'Chocolate'},
        {'scene_fk': 3, 'template_fk': 2, 'template_group': 'Chocolate'},
        {'scene_fk': 13, 'template_fk': 4, 'template_group': 'Chocolate'}
    ])

    scenes_for_checkout_count_including_no_tags = pd.DataFrame.from_records([
        {'scene_fk': 2, 'template_fk': 2, 'template_group': 'Chocolate'},
        {'scene_fk': 3, 'template_fk': 2, 'template_group': 'Chocolate'},
        {'scene_fk': 14, 'template_fk': 2, 'template_group': 'Chocolate'}
    ])

    scenes_for_checkout_1 = pd.DataFrame.from_records([
        {'scene_fk': 1, 'template_fk': 1, 'template_group': 'Chocolate'},
        {'scene_fk': 2, 'template_fk': 2, 'template_group': 'Chocolate'},
        {'scene_fk': 12, 'template_fk': 2, 'template_group': 'Chocolate'}
    ])

    scenes_for_checkout_2 = pd.DataFrame.from_records([
        {'scene_fk': 1, 'template_fk': 1, 'template_group': 'Chocolate'},
        {'scene_fk': 2, 'template_fk': 2, 'template_group': 'Chocolate'},
        {'scene_fk': 3, 'template_fk': 2, 'template_group': 'Chocolate'},
        {'scene_fk': 12, 'template_fk': 2, 'template_group': 'Chocolate'}
    ])