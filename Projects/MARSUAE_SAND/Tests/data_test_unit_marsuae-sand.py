import datetime
import os
import pandas as pd
from numpy import nan
import numpy as np

# class DataScores(object):
#     SCORES_1 = [(None, 0.15), (100, 0.15), (3, 0.15)]
#     SCORES_2 = [(False, None), (100, None), (False, None)]
#     SCORES_3 = [(100, 0.05),  (None, 0.05)]
#     SCORES_4_NONE_NO_WEIGHTS = [(None, None), (None, None), (None, None)]
#     SCORES_5_NONE_WEIGHTS = [(None, 0.15), (None, 0.15), (None, 0.15)] # score_1_1 in my example
#     SCORES_6 = [(100, None), (0, None), (100, None)]


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
         {'pk': 3034, 'type': u'SOS - Petfood Main'}]
    )

    # custom_entity = pd.DataFrame.from_records(
    #     [{'entity_type_fk': 1003, 'name': 'Healthier Multipack', 'parent_id': nan, 'pk': 11},
    #      {'entity_type_fk': 1004, 'name': 'Fun times together Tortilla', 'parent_id': nan, 'pk': 12},
    #      {'entity_type_fk': 1004, 'name': 'Fun times together Tubes', 'parent_id': nan, 'pk': 13},
    #      {'entity_type_fk': 1003, 'name': 'Premium Sharing', 'parent_id': nan, 'pk': 14},
    #      {'entity_type_fk': 1002, 'name': 'TRANSFORM-A-SNACK', 'parent_id': nan, 'pk': 15},
    #      {'entity_type_fk': 1005, 'name': 'Pringles_FTT_Tubes', 'parent_id': nan, 'pk': 165},
    #      {'entity_type_fk': 1005, 'name': 'Hula Hoops_LMP_Snacks', 'parent_id': nan, 'pk': 166},
    #      {'entity_type_fk': 1005, 'name': 'DORITOS GROUP', 'parent_id': nan, 'pk': 167},
    #      {'entity_type_fk': 1005, 'name': 'Walkers Crisps_Small MP PC', 'parent_id': nan, 'pk': 168},
    #      {'entity_type_fk': 1002, 'name': 'EAT REAL HUMMUS LENTIL & QUINOA CHIPS', 'pk': 10}]
    # )

    store_data_sss_a = pd.DataFrame.from_records(
        [{'pk': 1, 'store_type': 'SSS A', 'store_number_1': '10388932', 'additional_attribute_1': 'All Others',
          'additional_attribute_2': 'General Trade', 'additional_attribute_3': 'SSS'}])

    store_data_hypers = pd.DataFrame.from_records(
        [{'pk': 2, 'store_type': 'Hypers', 'store_number_1': '10097162', 'additional_attribute_1': 'Hypers',
          'additional_attribute_2': 'MT', 'additional_attribute_3': 'Lulu'}])

    # exclusion_template_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data',
    #                                        'Inclusion_Exclusion_Template.xlsx')
    # exclusion_template_path ='{}/Data/Template_L&T_test_updated.xlsx'.format(os.path.dirname(os.path.realpath(__file__)))

    # test_case_2 = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'test_case_2_empty_scene.xlsx')

    # on_display_products = pd.DataFrame.from_records([{'probe_match_fk': 1, 'smart_attribute': 'stock'},
    #                                                  {'probe_match_fk': 2, 'smart_attribute': 'additional display'},
    #                                                  {'probe_match_fk': 9, 'smart_attribute': 'additional display'},
    #                                                  {'probe_match_fk': 10, 'smart_attribute': 'additional display'}])

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

    assortment_store_sss_a = pd.DataFrame.from_records([
        {'additional_attributes': '{}', 'assortment_fk': 128, 'assortment_group_fk': 94, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3009, 'kpi_fk_lvl3': 3017,
         'product_fk': 214, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 128, 'assortment_group_fk': 94, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3009, 'kpi_fk_lvl3': 3017,
         'product_fk': 293, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 128, 'assortment_group_fk': 94, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3009, 'kpi_fk_lvl3': 3017,
         'product_fk': 296, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 128, 'assortment_group_fk': 94, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3009, 'kpi_fk_lvl3': 3017,
         'product_fk': 363, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 128, 'assortment_group_fk': 94, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3009, 'kpi_fk_lvl3': 3017,
         'product_fk': 699, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 147, 'assortment_group_fk': 113, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3011, 'kpi_fk_lvl3': 3019,
         'product_fk': 214, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 147, 'assortment_group_fk': 113, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3011, 'kpi_fk_lvl3': 3019,
         'product_fk': 293, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 147, 'assortment_group_fk': 113, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3011, 'kpi_fk_lvl3': 3019,
         'product_fk': 296, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 147, 'assortment_group_fk': 113, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3011, 'kpi_fk_lvl3': 3019,
         'product_fk': 363, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 147, 'assortment_group_fk': 113, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3011, 'kpi_fk_lvl3': 3019,
         'product_fk': 699, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 152, 'assortment_group_fk': 118, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3016, 'kpi_fk_lvl3': 3024,
         'product_fk': 214, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 152, 'assortment_group_fk': 118, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3016, 'kpi_fk_lvl3': 3024,
         'product_fk': 293, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 152, 'assortment_group_fk': 118, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3016, 'kpi_fk_lvl3': 3024,
         'product_fk': 363, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 152, 'assortment_group_fk': 118, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3016, 'kpi_fk_lvl3': 3024,
         'product_fk': 699, 'super_group_target': None, 'target': nan},
        {'additional_attributes': '{}', 'assortment_fk': 135, 'assortment_group_fk': 101, 'assortment_super_group_fk': nan,
         'group_target_date': nan, 'in_store': 0, 'kpi_fk_lvl1': nan, 'kpi_fk_lvl2': 3010, 'kpi_fk_lvl3': 3018,
         'product_fk': 363, 'super_group_target': None, 'target': nan}])


#-------start here

    all_templates = pd.DataFrame.from_records(
        [{'template_fk': 1, 'template_name': 'Template 1', 'location_type': 'Primary Shelf'},
         {'template_fk': 2, 'template_name': 'Template 2', 'location_type': 'Secondary Shelf'}]
    )

    external_targets = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data',
                                    'external_targets.xlsx')

    test_case_1 = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'test_case_1.xlsx')



    # empty_exclusion_template = pd.DataFrame(columns=['KPI', 'Action', 'Type', 'Value'])
    # exclusion_template_missing_action = pd.DataFrame([
    #     {'KPI': 'All', 'Action': 'Include', 'Type': 'location_type', 'Value': 'Primary Shelf'},
    #     {'KPI': 'ALL', 'Action': np.nan, 'Type': 'product_name', 'Value': 'General Empty'},
    #     {'KPI': 'All', 'Action': 'Exclude', 'Type': 'category', 'Value': 'Cat 1, Cat 2'}
    # ])

    test_case_1_ass_result = pd.DataFrame([{'product_fk': 1,  'in_store': 1}, {'product_fk': 2,  'in_store': 1},
                                           {'product_fk': 5,  'in_store': 0}])
    test_case_1_ass_base = pd.DataFrame([{'product_fk': 1, 'in_store': 0}, {'product_fk': 2, 'in_store': 0},
                                           {'product_fk': 5, 'in_store': 0}])

    test_case_1_ass_base_extended = pd.DataFrame(
        [{'product_fk': 1, 'in_store': 0, 'kpi_fk_lvl3': 290, 'kpi_fk_lvl2': 289, 'target': nan,
          'assortment_group_fk': 1, 'assortment_fk': 2, 'assortment_super_group_fk': nan, 'kpi_fk_lvl1': nan,
          'group_target_date': nan, 'super_group_target': nan, 'additional_attributes': nan},
         {'product_fk': 2, 'in_store': 0, 'kpi_fk_lvl3': 290, 'kpi_fk_lvl2': 289, 'target': nan,
          'assortment_group_fk': 1, 'assortment_fk': 2, 'assortment_super_group_fk': nan, 'kpi_fk_lvl1': nan,
          'group_target_date': nan, 'super_group_target': nan, 'additional_attributes': nan
          },
         {'product_fk': 5, 'in_store': 0, 'kpi_fk_lvl3': 290, 'kpi_fk_lvl2': 289, 'ta   rget': nan,
          'assortment_group_fk': 1, 'assortment_fk': 2, 'assortment_super_group_fk': nan, 'kpi_fk_lvl1': nan,
          'group_target_date': nan, 'super_group_target': nan, 'additional_attributes': nan}]
    )

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

    block_results = pd.DataFrame.from_records([{'cluster': 1, 'scene_fk': 1, 'orientation': '',
                                                'facing_percentage': 0.08, 'is_block': False},
                                               {'cluster': 2, 'scene_fk': 1, 'orientation': 'VERTICAL',
                                                'facing_percentage': 0.92, 'is_block': True}
                                               ])

    block_results_2 = pd.DataFrame.from_records([{'cluster': 1, 'scene_fk': 1, 'orientation': '',
                                                'facing_percentage': 0.05, 'is_block': False},
                                                {'cluster': 2, 'scene_fk': 1, 'orientation': 'HORIZONTAL',
                                                'facing_percentage': 0.95, 'is_block': True}
                                                ])

    block_results_empty = pd.DataFrame(columns=['cluster','scene_fk', 'orientation',
                                                'facing_percentage', 'is_block'])
    block_results_failed = pd.DataFrame.from_records([{'cluster': 1, 'scene_fk': 1, 'orientation': '',
                                                         'facing_percentage': 0.4, 'is_block': False},
                                                         {'cluster': 2, 'scene_fk': 1, 'orientation': '',
                                                          'facing_percentage': 0.6, 'is_block': False}])
    blocks_all_pass = pd.DataFrame.from_records([{'Group Name': 'Pringles_FTT_Tubes', 'Score': True},
                                                 {'Group Name': 'Hula Hoops_LMP_Snacks', 'Score': True}])
    blocks_none_passes = pd.DataFrame.from_records([{'Group Name': 'Pringles_FTT_Tubes', 'Score': False},
                                                    {'Group Name': 'Hula Hoops_LMP_Snacks', 'Score': False}])
    blocks_one_passes = pd.DataFrame.from_records([{'Group Name': 'Pringles_FTT_Tubes', 'Score': True},
                                                   {'Group Name': 'Hula Hoops_LMP_Snacks', 'Score': False}])

    adjacency_results_true = pd.DataFrame.from_records([{'anchor_block': 1, 'tested_block': 1, 'anchor_facing_percentage': 5,
                                                   'tested_facing_percentage': 4, 'scene_fk': 1, 'is_adj': True}])
    adjacency_results_false = pd.DataFrame.from_records(
        [{'anchor_block': 1, 'tested_block': 1, 'anchor_facing_percentage': 5,
          'tested_facing_percentage': 4, 'scene_fk': 1, 'is_adj': False}])

    blocks_combinations_3_pass_all = pd.DataFrame.from_records([{'Group Name': 'Group 1', 'Score': True},
                                                     {'Group Name': 'Group 2', 'Score': True},
                                                     {'Group Name': 'Group 3', 'Score': True}])

    blocks_combinations_2_pass_of_3 = pd.DataFrame.from_records([{'Group Name': 'Group 1', 'Score': True},
                                                            {'Group Name': 'Group 2', 'Score': True},
                                                            {'Group Name': 'Group 3', 'Score': False}])

    blocks_combinations_1_pass_of_3 = pd.DataFrame.from_records([{'Group Name': 'Group 1', 'Score': True},
                                                                 {'Group Name': 'Group 2', 'Score': False},
                                                                 {'Group Name': 'Group 3', 'Score': False}])

    blocks_combinations_4_pass_of_4 = pd.DataFrame.from_records([{'Group Name': 'Group 1', 'Score': True},
                                                                 {'Group Name': 'Group 2', 'Score': True},
                                                                 {'Group Name': 'Group 3', 'Score': True},
                                                                 {'Group Name': 'Group 4', 'Score': True},
                                                                 ])
