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


class DataTestUnitPEPSICOUK(object):

    kpi_static_data = pd.DataFrame.from_records(
        [{'pk': 287, 'type': 'Hero SKU SOS'}, {'pk': 288, 'type': 'Hero SKU SOS vs Target'},
         {'pk': 289, 'type': 'Hero SKU Availability'}, {'pk': 290, 'type': 'Hero SKU Availability - SKU'},
         {'pk': 291, 'type': 'Hero SKU OOS'}, {'pk': 292, 'type': 'Hero SKU OOS - SKU'},
         {'pk': 293, 'type': 'Brand SOS'}, {'pk': 294, 'type': 'Sub Brand SOS'},
         {'pk': 295, 'type': 'PepsiCo Segment SOS'}, {'pk': 296, 'type': 'PepsiCo Sub Segment Space to Sales Index'},
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
         {'pk': 318, 'type': 'Hero SKU Promo Price'}, {'pk': 320, 'type': 'Product Blocking Adjacency'},
         {'pk': 321, 'type': 'Number of Facings'}, {'pk': 322, 'type': 'Total Linear Space'}, {'pk': 323, 'type': 'Number of bays'},
         {'pk': 324, 'type': 'Number of shelves'}, {'pk': 325, 'type': 'Shelf Placement Vertical_Left'},
         {'pk': 326, 'type': 'Shelf Placement Vertical_Center'}, {'pk': 327, 'type': 'Shelf Placement Vertical_Right'},
         {'pk': 327, 'type': 'Brand Full Bay_90'},

         {'pk': 402, 'type': 'Facings per Product'}, {'pk': 403, 'type': 'Linear Space Per Product'},
         {'pk': 400, 'type': 'Price Scene'}, {'pk': 401, 'type': 'Promo Price Scene'},
         {'pk': 319, 'type': 'Product Blocking'}, {'pk': 405, 'type': 'Category Full Bay'},
         {'pk': 330, 'type': 'Hero SKU Availability by Hero Type'},
         {'pk': 329, 'type': 'Share of Assortment by Hero Type'},
         {'pk': 404, 'type': 'Hero SKU SOS of Category by Hero Type'}]
    )

    custom_entity = pd.DataFrame.from_records(
        [{'entity_type': u'PDH_Sub_segment','entity_type_fk': 1003, 'name': 'Healthier Multipack', 'parent_id': nan, 'pk': 11},
         {'entity_type': u'PDH_Format', 'entity_type_fk': 1004, 'name': 'Fun times together Tortilla', 'parent_id': nan, 'pk': 12},
         {'entity_type': u'PDH_Format', 'entity_type_fk': 1004, 'name': 'Fun times together Tubes', 'parent_id': nan, 'pk': 13},
         {'entity_type': u'PDH_Sub_segment', 'entity_type_fk': 1003, 'name': 'Premium Sharing', 'parent_id': nan, 'pk': 14},
         {'entity_type': u'sub_brand', 'entity_type_fk': 1002, 'name': 'TRANSFORM-A-SNACK', 'parent_id': nan, 'pk': 15},
         {'entity_type': u'sub_brand', 'entity_type_fk': 1002, 'name': 'GOLDEN WONDER AWESOME OINKS', 'parent_id': nan, 'pk': 1000},
         {'entity_type': u'group_name', 'entity_type_fk': 1005, 'name': 'Pringles_FTT_Tubes', 'parent_id': nan, 'pk': 165},
         {'entity_type': u'group_name', 'entity_type_fk': 1005, 'name': 'Hula Hoops_LMP_Snacks', 'parent_id': nan, 'pk': 166},
         {'entity_type': u'group_name', 'entity_type_fk': 1005, 'name': 'DORITOS GROUP', 'parent_id': nan, 'pk': 167},
         {'entity_type': u'group_name', 'entity_type_fk': 1005, 'name': 'Walkers Crisps_Small MP PC', 'parent_id': nan, 'pk': 168},
         {'entity_type': u'sub_brand', 'entity_type_fk': 1002, 'name': 'EAT REAL HUMMUS LENTIL & QUINOA CHIPS', 'pk': 10},
         {'entity_type': u'hero_type', 'entity_type_fk': 1007, 'name': u'Competitor', 'parent_id': None, 'pk': 559},
         {'entity_type': u'hero_type', 'entity_type_fk': 1007, 'name': u'PepsiCo', 'parent_id': None, 'pk': 560},
         {'entity_type': u'hero_type', 'entity_type_fk': 1007, 'name': u'Private Label', 'parent_id': None, 'pk': 561},
         {'entity_type': u'shelf_number', 'entity_type_fk': 1009,  'name': u'1',  'parent_id': None,  'pk': 562},
         {'entity_type': u'shelf_number',  'entity_type_fk': 1009,  'name': u'2',  'parent_id': None,  'pk': 563},
         {'entity_type': u'shelf_number',  'entity_type_fk': 1009,  'name': u'3',  'parent_id': None,  'pk': 564},
         {'entity_type': u'shelf_number',  'entity_type_fk': 1009,  'name': u'4',  'parent_id': None,  'pk': 565},
         {'entity_type': u'shelf_number',  'entity_type_fk': 1009,  'name': u'5',  'parent_id': None,  'pk': 566},
         {'entity_type': u'shelf_number',  'entity_type_fk': 1009,  'name': u'6',  'parent_id': None,  'pk': 567},
         {'entity_type': u'shelf_number',  'entity_type_fk': 1009,  'name': u'7',  'parent_id': None,  'pk': 568},
         {'entity_type': u'shelf_number',  'entity_type_fk': 1009,  'name': u'8',  'parent_id': None,  'pk': 569},
         {'entity_type': u'shelf_number',  'entity_type_fk': 1009,  'name': u'9',  'parent_id': None,  'pk': 570},
         {'entity_type': u'shelf_number',  'entity_type_fk': 1009,  'name': u'10',  'parent_id': None,  'pk': 571},
         {'entity_type': u'shelf_number',  'entity_type_fk': 1009,  'name': u'11',  'parent_id': None,  'pk': 572},
         {'entity_type': u'shelf_number',  'entity_type_fk': 1009,  'name': u'12',  'parent_id': None,  'pk': 573},
         {'entity_type': u'shelf_number',  'entity_type_fk': 1009,  'name': u'13',  'parent_id': None,  'pk': 574},
         {'entity_type': u'shelf_number',  'entity_type_fk': 1009,  'name': u'14',  'parent_id': None,  'pk': 575},
         {'entity_type': u'shelf_number',  'entity_type_fk': 1009,  'name': u'15',  'parent_id': None,  'pk': 576},
         {'entity_type': u'bay_number',  'entity_type_fk': 1010,  'name': u'1',  'parent_id': None,  'pk': 577},
         {'entity_type': u'bay_number',  'entity_type_fk': 1010,  'name': u'2',  'parent_id': None,  'pk': 578},
         {'entity_type': u'bay_number',  'entity_type_fk': 1010,  'name': u'3',  'parent_id': None,  'pk': 579},
         {'entity_type': u'bay_number',  'entity_type_fk': 1010,  'name': u'4',  'parent_id': None,  'pk': 580},
         {'entity_type': u'bay_number',  'entity_type_fk': 1010,  'name': u'5',  'parent_id': None,  'pk': 581},
         {'entity_type': u'bay_number',  'entity_type_fk': 1010,  'name': u'6',  'parent_id': None,  'pk': 582},
         {'entity_type': u'bay_number',  'entity_type_fk': 1010,  'name': u'7',  'parent_id': None,  'pk': 583},
         {'entity_type': u'bay_number',  'entity_type_fk': 1010,  'name': u'8',  'parent_id': None,  'pk': 584},
         {'entity_type': u'bay_number',  'entity_type_fk': 1010,  'name': u'9',  'parent_id': None,  'pk': 585},
         {'entity_type': u'bay_number',  'entity_type_fk': 1010,  'name': u'10',  'parent_id': None,  'pk': 586},
         {'entity_type': u'bay_number',  'entity_type_fk': 1010,  'name': u'11',  'parent_id': None,  'pk': 587},
         {'entity_type': u'bay_number',  'entity_type_fk': 1010,  'name': u'12',  'parent_id': None,  'pk': 588},
         {'entity_type': u'bay_number',  'entity_type_fk': 1010,  'name': u'13',  'parent_id': None,  'pk': 589},
         {'entity_type': u'bay_number',  'entity_type_fk': 1010,  'name': u'14',  'parent_id': None,  'pk': 590},
         {'entity_type': u'bay_number',  'entity_type_fk': 1010,  'name': u'15',  'parent_id': None,  'pk': 591}]
    )

    store_data = pd.DataFrame.from_records([{
        'store_type': 'CORE', 'additional_attribute_1': 'OT', 'additional_attribute_2': 'SAINSBURY',
        'additional_attribute_3': ''}])

    external_targets = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data',
                                    'external_targets.xlsx')

    exclusion_template_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data',
                                           'Inclusion_Exclusion_Template_Rollout.xlsx')
    # exclusion_template_path ='{}/Data/Template_L&T_test_updated.xlsx'.format(os.path.dirname(os.path.realpath(__file__)))

    test_case_1 = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'test_case_1.xlsx')
    test_case_2 = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'test_case_2_empty_scene.xlsx')

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
        [{'template_fk': 1, 'template_name': 'Template 1', 'location_type': 'Primary Shelf', 'location_type_fk': 1},
         {'template_fk': 2, 'template_name': 'Template 2', 'location_type': 'Secondary Shelf', 'location_type_fk': 2}]
    )

    empty_exclusion_template = pd.DataFrame(columns=['KPI', 'Action', 'Type', 'Value'])
    exclusion_template_missing_action = pd.DataFrame([
        {'KPI': 'All', 'Action': 'Include', 'Type': 'location_type', 'Value': 'Primary Shelf'},
        {'KPI': 'ALL', 'Action': np.nan, 'Type': 'product_name', 'Value': 'General Empty'},
        {'KPI': 'All', 'Action': 'Exclude', 'Type': 'category', 'Value': 'Cat 1, Cat 2'}
    ])

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
         {'product_fk': 5, 'in_store': 0, 'kpi_fk_lvl3': 290, 'kpi_fk_lvl2': 289, 'target': nan,
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

    adjacency_results_true = pd.DataFrame.from_records([{'anchor_block': 1, 'tested_block': 1, 'anchor_facing_percentage': 0.92,
                                                   'tested_facing_percentage': 0.95, 'scene_fk': 1, 'is_adj': True}])
    adjacency_results_false = pd.DataFrame.from_records(
        [{'anchor_block': 1, 'tested_block': 1, 'anchor_facing_percentage': 0.5,
          'tested_facing_percentage': 0.4, 'scene_fk': 1, 'is_adj': False}])

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
