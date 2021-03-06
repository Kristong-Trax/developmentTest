# -*- coding: utf-8 -*-
import os
import json
import numpy as np
import pandas as pd
import datetime as dt
import copy

from Trax.Cloud.Services.Storage.Factory import StorageFactory
from Trax.Algo.Calculations.Core.Constants import Fields as Fd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Utils.TargetFinder.KpiTargetFinder import KpiTargetFinder

from Projects.CCRU.Utils.Fetcher import CCRUCCHKPIFetcher
from Projects.CCRU.Utils.Consts import CCRUConsts
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import ScifConsts, StoreInfoConsts
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider


__author__ = 'sergey'


SOURCE = 'SOURCE'
SET = 'SET'
FILE = 'FILE'
SHEET = 'SHEET'
POS = 'POS'
GAPS = 'GAPS'
TARGET = 'TARGET'
MARKETING = 'MARKETING'
SPIRITS = 'SPIRITS'
CONTRACT = 'CONTRACT'
EQUIPMENT = 'EQUIPMENT'
INTEGRATION = 'INTEGRATION'
TOPSKU = 'TOPSKU'
KPI_CONVERSION = 'KPI_CONVERSION'
BENCHMARK = 'BENCHMARK'

SKIP_OLD_KPIS_FROM_WRITING = [TARGET, MARKETING]
SKIP_NEW_KPIS_FROM_WRITING = [TARGET, MARKETING]
NEW_KPIS_TO_WRITE_TO_DB = [POS, INTEGRATION, GAPS,
                           SPIRITS, TOPSKU, EQUIPMENT, CONTRACT, BENCHMARK]

BINARY = 'BINARY'
PROPORTIONAL = 'PROPORTIONAL'
CONDITIONAL_PROPORTIONAL = 'CONDITIONAL PROPORTIONAL'
AVERAGE = 'AVERAGE'
KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
CUSTOM_GAPS_TABLE = 'pservice.custom_gaps'

EQUIPMENT_TARGETS_BUCKET = 'traxuscalc'
EQUIPMENT_TARGETS_CLOUD_BASE_PATH = 'CCRU/KPIData/Contract/'

ALLOWED_POS_SETS = tuple(CCRUConsts.ALLOWED_POS_SETS)


PROMO_KPI_LEVEL_2_TYPE = 'PROMO_COMPLIANCE_DISPLAY'
PROMO_KPI_OPERATION_TYPE = 'Custom'

PROMO_DISPLAY_ENTITY = 'promo_display'
PROMO_PRODUCT_GROUP_ENTITY = 'promo_product_group'
PROMO_LOCATION_ENTITY = 'promo_location'

PROMO_SCENE_TYPES_DISPLAY = \
    [
        'Company Display Promo Energy',
        'Company Display Promo Ice Tea',
        'Company Display Promo Juice',
        'Company Display Promo SSD',
        'Company Display Promo Water'
    ]
PROMO_SCENE_TYPES_MAIN_SHELF =\
    [
        'Company Display Promo Price'
    ]

PROMO_PRODUCT_GROUP_FILTER = \
    {
        'QUERY': 'select pk as product_fk from static_new.product where 1 ',
        'CATEGORY': 'and product.category_fk {} in ({}) ',
        'BRAND': 'and product.brand_fk {} in ({}) ',
        'SUB_BRAND': """and REPLACE(JSON_EXTRACT(product.labels, '$.sub_brand'), '"', '') {} in ({}) """,
        'SIZE': 'and CAST(product.size AS CHAR(10)) {} in ({}) ',
        'FORM_FACTOR' : 'and product.form_factor {} in ({}) ',
        'EAN_CODE': 'and product.pk {} in ({}) '
    }

PROMO_COMPLIANCE_STORE = 'PROMO_COMPLIANCE_STORE'
PROMO_COMPLIANCE_DISPLAY = 'PROMO_COMPLIANCE_DISPLAY'
PROMO_COMPLIANCE_LOCATION = 'PROMO_COMPLIANCE_LOCATION'
PROMO_COMPLIANCE_DISPLAY_PRESENCE = 'PROMO_COMPLIANCE_DISPLAY_PRESENCE'
PROMO_COMPLIANCE_DISTRIBUTION_TARGET = 'PROMO_COMPLIANCE_DISTRIBUTION_TARGET'
PROMO_COMPLIANCE_FACINGS_TARGET = 'PROMO_COMPLIANCE_FACINGS_TARGET'
PROMO_COMPLIANCE_PRICE_AVAILABILITY = 'PROMO_COMPLIANCE_PRICE_AVAILABILITY'
PROMO_COMPLIANCE_PRICE_AVAILABILITY_TOTAL = 'PROMO_COMPLIANCE_PRICE_AVAILABILITY_TOTAL'
PROMO_COMPLIANCE_PRICE_TARGET = 'PROMO_COMPLIANCE_PRICE_TARGET'

PROMO_LOC_DISPLAY = 'Promo Display'
PROMO_LOC_MAIN_SHELF = 'Main Shelf'

CATEGORIES_LIST = ['SSD', 'Water', 'ice tea', 'Juices', 'Energy']
SOS_CAT_FOR_MR = 'SOS_MANUFACTURER_OUT_OF_CAT_CUSTOM_MR'
AVAILABILITY_CAT_FOR_MR = 'AVAILABILITY_MANUFACTURER_OUT_OF_CAT_CUSTOM_MR'
OSA_CAT_FOR_MR = 'OSA_MANUFACTURER_OUT_OF_CAT_CUSTOM_MR'
CAT_KPI_TYPE = 'Category KPI Type'
CAT_KPI_VALUE = 'Category KPI Value'
POS_CAT_KPI_DICT = {'Availability': AVAILABILITY_CAT_FOR_MR, 'SOS': SOS_CAT_FOR_MR}


class CCRUKPIToolBox:

    MIN_CALC_DATE = '2019-10-26'

    ALLOWED_POS_SETS = tuple(CCRUConsts.ALLOWED_POS_SETS)

    STANDARD_VISIT = 'Standard visit'
    PROMO_VISIT = 'Promo visit'
    SOVI_SOCVI_VISIT = 'SOVI/SOCVI'
    SEGMENTATION_VISIT = 'Segmentation'
    GROWTH_FORMULA = 'Growth formula'

    VISIT_TYPE = {1: STANDARD_VISIT,
                  2: PROMO_VISIT,
                  3: SOVI_SOCVI_VISIT,
                  4: SEGMENTATION_VISIT,
                  5: GROWTH_FORMULA}

    def __init__(self, data_provider, output, kpi_set_name=None, kpi_set_type=None, update_kpi_set=False):
        self.data_provider = data_provider
        self.output = output
        self.project_name = self.data_provider.project_name
        self.rds_conn = self.rds_connection()

        self.common = Common(self.data_provider)
        self.k_engine = BaseCalculationsGroup(self.data_provider, self.output)
        self.session_info = SessionInfo(self.data_provider)

        self.session_uid = self.data_provider.session_uid
        self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]
        self.visit_type = self.VISIT_TYPE.get(self.data_provider[Data.SESSION_INFO]['visit_type_fk'].iloc[0],
                                              self.data_provider[Data.SESSION_INFO]['visit_type_fk'].iloc[0])
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.own_manufacturer_id = int(
            self.data_provider[Data.OWN_MANUFACTURER][self.data_provider[Data.OWN_MANUFACTURER]['param_name'] == 'manufacturer_id']['param_value'].tolist()[0])
        self.sales_rep_fk = self.data_provider[Data.SESSION_INFO]['s_sales_rep_fk'].iloc[0]
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]

        self.products = self.data_provider[Data.ALL_PRODUCTS]

        self.templates = self.data_provider[Data.ALL_TEMPLATES]
        self.templates['template_name'] = self.templates['template_name'].apply(
            lambda x: x.encode('utf-8'))

        self.scenes_info = self.data_provider[Data.SCENES_INFO]

        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif['template_name'] = self.scif['template_name'].apply(lambda x: x.encode('utf-8'))

        self.matches = self.data_provider[Data.MATCHES].merge(self.products, on='product_fk')

        self.pos_kpi_set_name = self.get_pos_kpi_set_name(update_kpi_set=update_kpi_set)
        self.kpi_set_name = kpi_set_name if kpi_set_name else self.pos_kpi_set_name
        self.kpi_set_type = kpi_set_type if kpi_set_type else POS
        self.kpi_name_to_id = {kpi_set_type: {}}
        self.kpi_children = {kpi_set_type: {}}
        self.kpi_scores_and_results = {kpi_set_type: {}}

        self.kpi_fetcher = CCRUCCHKPIFetcher(self.project_name)
        self.external_session_id = self.kpi_fetcher.get_external_session_id(self.session_uid)
        self.store_number = self.kpi_fetcher.get_store_number(self.store_id)
        self.test_store = self.kpi_fetcher.get_test_store(self.store_id)
        self.attr15 = self.kpi_fetcher.get_attr15_store(self.store_id)
        self.kpi_result_values = self.kpi_fetcher.get_kpi_result_values()
        self.kpi_entity_types = self.kpi_fetcher.get_kpi_entity_types()
        self.kpi_entities = self.get_kpi_entities('top_sku_type')
        self.store_areas = self.kpi_fetcher.get_store_area_df(self.session_uid)
        self.session_user = self.kpi_fetcher.get_session_user(self.session_uid)

        try:
            self.planned_visit_flag = int(self.kpi_fetcher.get_planned_visit_flag(self.session_uid))
        except:
            self.planned_visit_flag = None

        self.passed_scenes_per_kpi = {}
        self.kpi_facts_hidden = []
        self.kpi_results_queries = []
        self.gaps_dict = {}
        self.gaps_queries = []
        self.gap_groups_limit = {'Availability': 3,
                                 'Cooler/Cold Availability': 2,
                                 'Shelf/Displays/Activation': 4}
        self.top_sku_queries = []
        self.equipment_execution_score = None
        self.top_sku_score = None

        self.promo_displays = None
        self.promo_locations = None
        self.promo_products = None
        self.mr_targets = {}

        # For Cooler Audit
        if self.visit_type in [self.STANDARD_VISIT]:
            self.scenes_in_session = self.get_all_scenes_in_session()
            self.cooler_scenes = self.scenes_in_session[self.scenes_in_session[ScifConsts.LOCATION_TYPE]
                                                        == CCRUConsts.COOLER][ScifConsts.SCENE_FK].values.tolist()
            self.cooler_assortment = self.retrieve_and_unpack_cooler_assortment_list()
            if not self.cooler_assortment.empty:
                self.cooler_info = self.get_all_coolers_in_assortment()
                self.coolers_by_bar_code = False  # switch to true when bar code reader solution is introduced
                if not self.coolers_by_bar_code:
                    self.cooler_survey_responses = self.retrieve_scene_survey_responses()
                else:
                    self.coolers_in_session = self.get_coolers_by_bar_code()
                self.scif_session = self.scif
                self.matches_session = self.matches
                self.scenes_session_initial = self.scenes_info

    # Change once bar code solution is introduced
    def get_coolers_by_bar_code(self):
        return pd.DataFrame(columns=[ScifConsts.SCENE_FK, 'cooler_code'])

    def retrieve_and_unpack_cooler_assortment_list(self):
        cooler_targets = self.kpi_fetcher.get_kpi_external_targets(self.visit_date, self.store_id)
        key_json_df = self.unpack_kpi_targets_from_db(cooler_targets, 'key_json')
        cooler_targets = cooler_targets.merge(key_json_df, on='pk', how='left')
        cooler_targets = cooler_targets.drop_duplicates(subset=[CCRUConsts.COOLER_ID], keep='last')
        return cooler_targets

    def unpack_kpi_targets_from_db(self, input_df, field_name):
        if input_df.empty:
            return pd.DataFrame(columns=['pk'])
        input_df['json_dict_with_pk'] = input_df.apply(self.add_pk_to_json, args=(field_name,), axis=1)
        json_dict_list = input_df['json_dict_with_pk'].values.tolist()
        output_df = pd.DataFrame(json_dict_list)
        return output_df

    @staticmethod
    def add_pk_to_json(row, field_name):
        json_value = row[field_name]
        json_to_dict = json.loads(json_value)
        json_to_dict.update({'pk': row['pk']})
        return json_to_dict

    def get_all_coolers_in_assortment(self):
        cooler_list = self.cooler_assortment[CCRUConsts.COOLER_ID].values.tolist()
        store_coolers = self.kpi_fetcher.get_all_coolers_from_assortment_list(cooler_list)
        self.cooler_assortment = self.cooler_assortment.merge(store_coolers, left_on=CCRUConsts.COOLER_ID,
                                                              right_on=CCRUConsts.COOLER_ID_DB, how='left')
        return store_coolers

    def retrieve_scene_survey_responses(self):
        survey_responses = self.kpi_fetcher.get_scene_survey_response(self.cooler_scenes)
        survey_responses[CCRUConsts.SURVEY_ANSWER] = survey_responses[CCRUConsts.SURVEY_ANSWER].astype(
            str)
        return survey_responses

    def get_all_scenes_in_session(self):
        scenes_in_ses = self.data_provider[Data.SCENES_INFO][[ScifConsts.SCENE_FK, ScifConsts.TEMPLATE_FK]]
        scenes_in_ses = scenes_in_ses.merge(self.data_provider[Data.ALL_TEMPLATES],
                                            on=ScifConsts.TEMPLATE_FK, how='left')
        return scenes_in_ses

    @staticmethod
    def children_to_int_list(children):
        children = map(int, str(children).strip().replace(
            " ", "").replace(",", "\n").replace("\n\n", "\n").replace("\n\n", "\n").split("\n"))
        return children

    @staticmethod
    def children_to_str_list(children):
        children = str(children).strip().replace(
            " ", "").replace(",", "\n").replace("\n\n", "\n").replace("\n\n", "\n").split("\n")
        return children

    def set_kpi_set(self, kpi_set_name, kpi_set_type, empty_kpi_scores_and_results=True):
        self.kpi_set_name = kpi_set_name
        self.kpi_set_type = kpi_set_type
        self.kpi_fetcher.kpi_set_name = kpi_set_name
        self.kpi_fetcher.kpi_static_data = self.kpi_fetcher.get_static_kpi_data(kpi_set_name)
        if empty_kpi_scores_and_results:
            self.kpi_name_to_id[kpi_set_type] = {}
            self.kpi_children[kpi_set_type] = {}
            self.kpi_scores_and_results[kpi_set_type] = {}

    def update_kpi_scores_and_results(self, param, kpi_scores_and_results):
        if param.get('KPI ID') is not None:
            kpi_id = str(param.get('KPI ID'))
            parent = str(param.get('Parent')).split('.')[0] if param.get('Parent') else '0'
            self.kpi_name_to_id[self.kpi_set_type][param.get('KPI name Eng')] = kpi_id
            self.kpi_children[self.kpi_set_type][parent] = list(set(self.kpi_children[self.kpi_set_type][parent] + [kpi_id]))\
                if self.kpi_children[self.kpi_set_type].get(parent) else [kpi_id]
            if not self.kpi_scores_and_results[self.kpi_set_type].get(kpi_id):
                self.kpi_scores_and_results[self.kpi_set_type][kpi_id] = \
                    {'kpi_id': kpi_id,
                     'kpi_name': param.get('KPI name Eng').strip().replace("'", "").replace('"', '').replace(',', '.').replace('  ', ' ').upper(),
                     'eng_name': param.get('KPI name Eng'),
                     'rus_name': param.get('KPI name Rus'),
                     'score_func': param.get('score_func'),
                     'target': None,
                     'weight': param.get('KPI Weight'),
                     'result': None,
                     'format': None,
                     'score': None,
                     'weighted_score': None,
                     'scene_id': None,
                     'scene_uid': None,
                     'level': int(param.get('level')) if param.get('level') else 1,
                     'parent': parent,
                     'additional_level': None,
                     'sort_order': param.get('Sorting')}
            self.kpi_scores_and_results[self.kpi_set_type][kpi_id].update(kpi_scores_and_results)

    def rds_connection(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self._rds_conn.db)
        except:
            self._rds_conn.disconnect_rds()
            self._rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        return self._rds_conn

    def get_relevant_scenes(self, params):
        all_scenes = self.scenes_info['scene_fk'].unique().tolist()
        filtered_scenes = []
        scenes_data = {}
        location_data = {}
        sub_location_data = {}
        store_area_data = {}
        relevant_scenes = []

        for scene in all_scenes:
            scene_type = list(self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values)
            if scene_type:
                scene_type = scene_type[0]
                if scene_type not in scenes_data.keys():
                    scenes_data[scene_type] = []
                scenes_data[scene_type].append(scene)
                filtered_scenes.append(scene)
            else:
                # Log.warning('Scene {} is not defined in reporting.scene_item_facts table'.format(scene))
                continue

            location = list(self.scif.loc[self.scif['scene_id'] == scene]['location_type'].values)
            if location:
                location = location[0]
                if location not in location_data.keys():
                    location_data[location] = []
                location_data[location].append(scene)

            sub_location = list(
                self.scif.loc[self.scif['template_name'] == scene_type]['additional_attribute_2'].values)
            if sub_location:
                sub_location = unicode(sub_location[0])
                if sub_location not in sub_location_data.keys():
                    sub_location_data[sub_location] = []
            sub_location_data[sub_location].append(scene)

            store_area = list(
                self.store_areas.loc[self.store_areas['scene_fk'] == scene]['store_area_name'].values)
            if store_area:
                store_area = store_area[0]
                if store_area not in store_area_data.keys():
                    store_area_data[store_area] = []
            if len(store_area) != 0:
                store_area_data[store_area].append(scene)

        include_list = []
        include_list_candidate = []

        if not params.get('Scenes to include') and not params.get('Locations to include') and \
                not params.get('Sub locations to include') and not params.get('Zone to include'):
            include_list.extend(filtered_scenes)
        else:
            if params.get('Scenes to include'):
                scenes_to_include = \
                    [unicode(x).strip().encode('utf-8')
                     for x in unicode(params.get('Scenes to include')).split('; ')]
                for scene in scenes_to_include:
                    if scene in scenes_data.keys():
                        include_list_candidate.extend(scenes_data[scene])
                if not include_list_candidate:
                    return relevant_scenes
                include_list = list(set(include_list_candidate))

            if params.get('Locations to include'):
                include_list_candidate = []
                locations_to_include = \
                    [unicode(x).strip()
                     for x in unicode(params.get('Locations to include')).split(', ')]
                for location in locations_to_include:
                    if location in location_data.keys():
                        include_list_candidate.extend(location_data[location])
                if not include_list_candidate:
                    return relevant_scenes
                if include_list:
                    include_list = list(set(include_list) & set(include_list_candidate))
                else:
                    include_list = list(set(include_list_candidate))

            if params.get('Sub locations to include'):
                include_list_candidate = []
                if type(params.get('Sub locations to include')) == float:
                    sub_locations_to_include = \
                        [unicode(x).strip() for x in unicode(
                            int(params.get('Sub locations to include'))).split(', ')]
                else:
                    sub_locations_to_include = \
                        [unicode(x).strip()
                         for x in unicode(params.get('Sub locations to include')).split(', ')]
                for sub_location in sub_locations_to_include:
                    if sub_location in sub_location_data.keys():
                        include_list_candidate.extend(sub_location_data[sub_location])
                if not include_list_candidate:
                    return relevant_scenes
                if include_list:
                    include_list = list(set(include_list) & set(include_list_candidate))
                else:
                    include_list = list(set(include_list_candidate))

            if params.get('Zone to include'):
                include_list_candidate = []
                store_areas_to_include = \
                    [unicode(x).strip() for x in unicode(params.get('Zone to include')).split(', ')]
                for store_area in store_areas_to_include:
                    if store_area in store_area_data.keys():
                        include_list_candidate.extend(store_area_data[store_area])
                if not include_list_candidate:
                    return relevant_scenes
                if include_list:
                    include_list = list(set(include_list) & set(include_list_candidate))
                else:
                    include_list = list(set(include_list_candidate))

        exclude_list = []
        if params.get('Scenes to exclude'):
            scenes_to_exclude = \
                [unicode(x).strip().encode('utf-8')
                 for x in unicode(params.get('Scenes to exclude')).split('; ')]
            for scene in scenes_to_exclude:
                if scene in scenes_data.keys():
                    exclude_list.extend(scenes_data[scene])

        if params.get('Locations to exclude'):
            locations_to_exclude = \
                [unicode(x).strip()
                 for x in unicode(params.get('Locations to exclude')).split(', ')]
            for location in locations_to_exclude:
                if location in location_data.keys():
                    exclude_list.extend(location_data[location])

        if params.get('Sub locations to exclude'):
            sub_locations_to_exclude = \
                [unicode(x).strip()
                 for x in unicode(params.get('Sub locations to exclude')).split(', ')]
            for sub_location in sub_locations_to_exclude:
                if sub_location in sub_location_data.keys():
                    exclude_list.extend(sub_location_data[sub_location])
        exclude_list = list(set(exclude_list))

        for scene in include_list:
            if scene not in exclude_list:
                relevant_scenes.append(scene)
        return relevant_scenes

    @kpi_runtime()
    def check_availability(self, params, level=2, cooler_dict=None):
        set_total_res = 0
        availability_types = ['SKUs', 'BRAND', 'MAN', 'CAT',
                              'MAN in CAT', 'SUB_BRAND', 'SUB_CATEGORY']
        formula_types = ['number of SKUs', 'number of facings']
        atomic_result_total = 0
        for p in params.values()[0]:
            if p.get('level') != level:
                continue
            if p.get('Type') not in availability_types or p.get('Formula').strip() not in formula_types:
                continue
            is_atomic = False
            kpi_total_res = 0
            score = 0
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))

            if p.get('Children') is not None:
                is_atomic = True
                children = self.children_to_int_list(p.get("Children"))
                atomic_scores = []
                atomic_result_total = 0
                for child in params.values()[0]:
                    if child.get('KPI ID') in children:

                        if child.get('Children') is not None:  # atomic of atomic
                            atomic_score = 0
                            atomic_children = self.children_to_int_list(child.get("Children"))
                            for atomic_child in params.values()[0]:
                                if atomic_child.get('KPI ID') in atomic_children:
                                    atomic_child_res = self.calculate_availability(atomic_child)
                                    atomic_child_score = self.calculate_score(
                                        atomic_child_res, atomic_child)
                                    atomic_score += atomic_child.get('additional_weight',
                                                                     1.0 / len(atomic_children)) * atomic_child_score

                        else:
                            atomic_res = self.calculate_availability(child)
                            atomic_score = self.calculate_score(atomic_res, child)

                        # saving to DB
                        if not cooler_dict:
                            attributes_for_level3 = self.create_attributes_for_level3_df(
                                child, atomic_score, kpi_fk)
                            self.write_to_kpi_results_old(attributes_for_level3, 'level3')

                            atomic_result = attributes_for_level3['result']
                            if atomic_result.size > 0:
                                atomic_result_total += atomic_result.values[0]

                            if p.get('Logical Operator') in ('OR', 'AND', 'MAX'):
                                atomic_scores.append(atomic_score)
                            elif p.get('Logical Operator') == 'SUM':
                                kpi_total_res += child.get('additional_weight',
                                                           1 / float(len(children))) * atomic_score

                if p.get('Logical Operator') == 'OR':
                    if len([sc for sc in atomic_scores if sc > 0]) > 0:
                        score = 100
                    else:
                        score = 0
                elif p.get('Logical Operator') == 'AND':
                    if 0 not in atomic_scores:
                        score = 100
                    else:
                        score = 0
                elif p.get('Logical Operator') == 'SUM':
                    score = round(kpi_total_res, 3) / 100.0
                    if score < p.get('score_min', 0):
                        score = 0
                    elif score > p.get('score_max', 1):
                        score = p.get('score_max', 1)
                    score *= 100
                elif p.get('Logical Operator') == 'MAX':
                    if atomic_scores:
                        score = max(atomic_scores)
                        if not ((score > p.get('score_min', 0) * 100) and (score <= p.get('score_max', 1) * 100)):
                            score = 0
                    else:
                        score = 0
            else:
                kpi_total_res = self.calculate_availability(p)
                score = self.calculate_score(kpi_total_res, p)

            score = round(float(score), 3)

            if not cooler_dict:
                if not is_atomic:  # saving also to level3 in case this KPI has only one level
                    attributes_for_table3 = self.create_attributes_for_level3_df(
                        p, score, kpi_fk, level=2, additional_level=3)
                    self.write_to_kpi_results_old(attributes_for_table3, 'level3')

                # Saving to old tables
                attributes_for_table2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_kpi_results_old(attributes_for_table2, 'level2')

                if p.get("KPI ID") in params.values()[2]["SESSION LEVEL"]:
                    self.write_to_kpi_facts_hidden(p.get("KPI ID"), None, atomic_result_total, score)

            set_total_res += round(score * p.get('KPI Weight'), 3)

            if cooler_dict is not None:
                kpi_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.COOLER_SCORE_LVL_2)
                kpi_ref_fk = self.common.get_kpi_fk_by_kpi_type(p['KPI name Eng'].upper())
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=cooler_dict[CCRUConsts.COOLER_FK],
                                               denominator_id=kpi_ref_fk, context_id=cooler_dict[ScifConsts.SCENE_FK],
                                               result=score, score=score * p.get('KPI Weight'),
                                               weight=p.get("KPI Weight"), identifier_parent=cooler_dict,
                                               should_enter=True)

        return set_total_res

    def calculate_availability(self, params, scenes=None, all_params=None):
        values_list = \
            [unicode(x).strip() for x in unicode(params.get('Values')).strip().split(', ')]
        if not scenes:
            if params.get('depends on'):
                depends_on_kpi_name = params.get('depends on')
                for c in all_params.values()[0]:
                    if c.get('KPI name Eng') == depends_on_kpi_name:
                        if c.get('Formula').strip() == 'number of doors with more than Target facings':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(
                                c, 'get scenes')
                        elif c.get('Formula').strip() == 'number of coolers with facings target and fullness target':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(
                                c, 'get scenes')
                            scenes = self.calculate_number_of_doors_of_filled_coolers(c, scenes,
                                                                                      func='get scenes',
                                                                                      proportion_param=0.9)
                        else:
                            scenes = []
                        break

                if not scenes:
                    return 0
            else:
                scenes = self.get_relevant_scenes(params)

        if params.get("Form Factor"):
            form_factors = \
                [unicode(x).strip() for x in unicode(params.get("Form Factor")).split(", ")]
        else:
            form_factors = []
        if params.get("Size"):
            sizes = [float(x) for x in str(params.get('Size')).split(", ")]
            sizes = [int(x) if int(x) == x else x for x in sizes]
        else:
            sizes = []
        if params.get("Products to exclude"):
            products_to_exclude = \
                [unicode(float(x)) for x in unicode(params.get("Products to exclude")).split(", ")]
        else:
            products_to_exclude = []
        if params.get("Form factors to exclude"):
            form_factors_to_exclude = \
                [unicode(x).strip()
                 for x in unicode(params.get("Form factors to exclude")).split(", ")]
        else:
            form_factors_to_exclude = []
        if params.get("Product Category"):
            product_categories = \
                [unicode(x).strip() for x in unicode(params.get("Product Category")).split(", ")]
        else:
            product_categories = []
        if params.get("Sub category"):
            product_sub_categories = \
                [unicode(x).strip() for x in unicode(params.get("Sub category")).split(", ")]
        else:
            product_sub_categories = []
        if params.get("Brand"):
            product_brands = \
                [unicode(x).strip() for x in unicode(params.get("Brand")).split(", ")]
        else:
            product_brands = []
        if params.get("Sub brand"):
            product_sub_brands = \
                [unicode(x).strip() for x in unicode(params.get("Sub brand")).split(", ")]
        else:
            product_sub_brands = []
        if params.get("Manufacturer"):
            product_manufacturers = \
                [unicode(x).strip() for x in unicode(params.get("Manufacturer")).split(", ")]
        else:
            product_manufacturers = []
        if not product_manufacturers:
            product_manufacturers = ['TCCC']

        result = self.get_object_facings(scenes=scenes,
                                         objects=values_list,
                                         object_type=params.get('Type'),
                                         formula=params.get('Formula').strip(),
                                         shelves=params.get("shelf_number", None),
                                         size=sizes,
                                         form_factor=form_factors,
                                         products_to_exclude=products_to_exclude,
                                         form_factors_to_exclude=form_factors_to_exclude,
                                         product_categories=product_categories,
                                         product_sub_categories=product_sub_categories,
                                         product_brands=product_brands,
                                         product_sub_brands=product_sub_brands,
                                         product_manufacturers=product_manufacturers)

        if params.get('Formula').strip() == 'each SKU hits facings target':
            if params.get('Target'):
                number_of_fails = sum([x < int(params.get('Target')) for x in result])
                result = 100 - round(number_of_fails / float(len(result)) * 100, 3) if len(result) else 0
            else:
                result = 0

        return result

    def calculate_number_facings_near_food(self, params, all_params):
        total_res = 0
        if params.get('depends on'):
            depends_on_kpi_name = params.get('depends on')
            for c in all_params.values()[0]:
                if c.get('KPI name Eng') == depends_on_kpi_name:
                    scenes = self.get_relevant_scenes(params)
                    for scene in scenes:
                        num_facings = self.calculate_availability(c, [scene])
                        if num_facings >= c.get('Target'):
                            num_facings_food = self.calculate_availability(params, [scene])
                            if num_facings_food >= params.get('Target'):
                                total_res += num_facings_food
                    return total_res
        total_res = self.calculate_availability(params)
        return total_res

    def calculate_number_of_scenes_with_target(self, params, scenes=None):
        if not scenes:
            if params.get('depends on'):
                depends_on_kpi_name = params.get('depends on')
                for c in params.values()[0]:
                    if c.get('KPI name Eng') == depends_on_kpi_name:
                        if c.get('Formula').strip() == 'number of doors with more than Target facings':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(
                                c, 'get scenes')
                        elif c.get('Formula').strip() == 'number of coolers with facings target and fullness target':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(
                                c, 'get scenes')
                            scenes = self.calculate_number_of_doors_of_filled_coolers(c, scenes,
                                                                                      func='get scenes',
                                                                                      proportion_param=0.9)
                        else:
                            scenes = []
                        break
                if not scenes:
                    return 0
            else:
                scenes = self.get_relevant_scenes(params)
        kpi_total_res = 0
        for scene in scenes:
            res = self.calculate_availability(params, scenes=[scene])
            if res >= params.get('target_min'):
                kpi_total_res += 1
        return kpi_total_res

    @kpi_runtime()
    def check_number_of_scenes(self, params, level=2, cooler_dict=None):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('level') != level:
                continue
            if p.get('Formula').strip() != 'number of scenes':
                continue
            kpi_total_res = 0
            if p.get('depends on'):
                depends_scenes = []
                depends_on_kpi_name = p.get('depends on')
                for c in params.values()[0]:
                    if c.get('KPI name Eng') == depends_on_kpi_name:
                        if c.get('Formula').strip() == 'number of doors with more than Target facings':
                            depends_scenes = self.calculate_number_of_doors_more_than_target_facings(
                                c, 'get scenes')
                        elif c.get('Formula').strip() == 'number of doors of filled Coolers':
                            depends_scenes = self.check_number_of_doors_of_filled_coolers(
                                c, 'get scenes')
                        elif c.get('Formula').strip() == 'number of coolers with facings target and fullness target':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(
                                c, 'get scenes')
                            depends_scenes = self.calculate_number_of_doors_of_filled_coolers(c, scenes,
                                                                                              func='get scenes',
                                                                                              proportion_param=0.9)
                        break
                if not depends_scenes:
                    scenes = []
                else:
                    relevant_scenes = self.get_relevant_scenes(p)
                    scenes = list(set(relevant_scenes) & set(depends_scenes))
            else:
                scenes = self.get_relevant_scenes(p)

            if p.get('Type') == 'NUM_SCENES':
                kpi_total_res = len(scenes)

            elif p.get('Type') == 'SCENES':
                values_list = [unicode(x).strip() for x in p.get('Values').split(', ')]
                for scene in scenes:
                    try:
                        scene_type = self.scif.loc[self.scif['scene_id']
                                                   == scene]['template_name'].values[0]
                        if scene_type in values_list:
                            res = 1
                        else:
                            res = 0
                        kpi_total_res += res
                    except IndexError as e:
                        continue
            elif p.get('Type') == 'LOCATION_TYPE':
                values_list = [unicode(x).strip() for x in p.get('Values').split(', ')]
                for scene in scenes:
                    try:
                        location_type = self.scif.loc[self.scif['scene_id']
                                                      == scene]['location_type'].values[0]
                        if location_type in values_list:
                            res = 1
                        else:
                            res = 0
                        kpi_total_res += res
                    except IndexError as e:
                        continue
            elif p.get('Type') == 'SUB_LOCATION_TYPE':
                values_list = [p.get('Values')]
                for scene in scenes:
                    try:
                        scene_type = self.scif.loc[self.scif['scene_id']
                                                   == scene]['template_name'].values[0]
                        sub_location_type = int(self.templates.loc[self.templates['template_name'] == scene_type][
                            'additional_attribute_2'].values[0])
                        if sub_location_type in values_list:
                            res = 1
                        else:
                            res = 0
                        kpi_total_res += res
                    except IndexError as e:
                        continue
            # checking for number of scenes with a complex condition (only certain products/brands/etc)
            else:
                p_copy = p.copy()
                p_copy["Formula"] = "number of facings"
                for scene in scenes:
                    if self.calculate_availability(p_copy, scenes=[scene]) > 0:
                        res = 1
                    else:
                        res = 0
                    kpi_total_res += res

            score = self.calculate_score(kpi_total_res, p)

            score = round(float(score), 3)

            if p.get('KPI Weight') is None:
                set_total_res += score
            else:
                set_total_res += round(score * p.get('KPI Weight'), 3)
            if not cooler_dict:
                kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))

                if not p.get('Children'):
                    atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(p.get('KPI name Eng'), kpi_fk)
                    attributes_for_level3 = self.create_attributes_for_level3_df(
                        p, score, kpi_fk, atomic_kpi_fk, level=2, additional_level=3)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')

                if p.get('level') == 2:
                    attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level2, 'level2')

                    if p.get("KPI ID") in params.values()[2]["SESSION LEVEL"]:
                        self.write_to_kpi_facts_hidden(p.get("KPI ID"), None, None, score)

            if cooler_dict is not None:
                kpi_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.COOLER_SCORE_LVL_2)
                kpi_ref_fk = self.common.get_kpi_fk_by_kpi_type(p['KPI name Eng'].upper())
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=cooler_dict[CCRUConsts.COOLER_FK],
                                               denominator_id=kpi_ref_fk, context_id=cooler_dict[ScifConsts.SCENE_FK],
                                               result=score, score=score * p.get('KPI Weight'),
                                               weight=p.get("KPI Weight"), identifier_parent=cooler_dict,
                                               should_enter=True)

        return set_total_res

    @kpi_runtime()
    def check_number_of_doors(self, params, level=2, cooler_dict=None):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('level') != level:
                continue
            if p.get('Type') not in ('DOORS', 'SUB_LOCATION_TYPE', 'LOCATION_TYPE') or p.get('Formula').strip() != 'number of doors':
                continue
            kpi_total_res = self.calculate_number_of_doors(p)
            score = self.calculate_score(kpi_total_res, p)

            score = round(float(score), 3)

            set_total_res += round(score * p.get('KPI Weight'), 3)

            # saving to DB
            if not cooler_dict:
                kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                if p.get('level') == 2:
                    attributes_for_level3 = self.create_attributes_for_level3_df(
                        p, score, kpi_fk, level=2, additional_level=3)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')
                    attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level2, 'level2')
                else:
                    attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')

            if cooler_dict is not None:
                kpi_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.COOLER_SCORE_LVL_2)
                kpi_ref_fk = self.common.get_kpi_fk_by_kpi_type(p['KPI name Eng'].upper())
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=cooler_dict[CCRUConsts.COOLER_FK],
                                               denominator_id=kpi_ref_fk, context_id=cooler_dict[ScifConsts.SCENE_FK],
                                               result=score, score=score * p.get('KPI Weight'),
                                               weight=p.get("KPI Weight"), identifier_parent=cooler_dict,
                                               should_enter=True)
        return set_total_res

    def calculate_number_of_doors(self, params):
        total_res = 0
        relevant_scenes = self.get_relevant_scenes(params)
        for scene in relevant_scenes:
            res = 0
            scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
            num_of_doors = \
                self.templates[self.templates['template_name'] ==
                               scene_type]['additional_attribute_1'].values[0]
            if num_of_doors is not None:
                res = float(num_of_doors)
            total_res += res
        return total_res

    def calculate_number_of_doors_more_than_target_facings(self, params, func=None):
        total_res = 0
        relevant_scenes = self.get_relevant_scenes(params)
        scenes_passed = []
        for scene in relevant_scenes:
            scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
            if any(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1']):
                num_of_doors = \
                    float(self.templates[self.templates['template_name'] ==
                                         scene_type]['additional_attribute_1'].values[0])
                facings = self.calculate_availability(params, scenes=[scene])
                if num_of_doors is not None:
                    res = facings / float(num_of_doors)
                    if res >= int(params.get('target_min')):
                        total_res += num_of_doors
                        scenes_passed.append(scene)
        if scenes_passed:
            if params.get('depends on') == 'filled coolers target':
                total_res = 0
                scenes_passed_filled = self.check_number_of_doors_of_filled_coolers(params,
                                                                                    func='get scenes',
                                                                                    proportion=0.9)
                total_scenes_passed = list(set(scenes_passed_filled) & set(scenes_passed))
                for scene in total_scenes_passed:
                    scene_type = self.scif.loc[self.scif['scene_id']
                                               == scene]['template_name'].values[0]
                    if any(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1']):
                        num_of_doors = \
                            float(self.templates[self.templates['template_name'] == scene_type][
                                'additional_attribute_1'].values[0])
                        total_res += num_of_doors
            else:
                total_scenes_passed = list(set(scenes_passed))
        else:
            total_scenes_passed = []
        if func == 'get scenes':
            return total_scenes_passed
        return total_res

    @kpi_runtime()
    def check_survey_answer(self, params):
        """
        This function is used to calculate survey answer according to given target

        """
        set_total_res = 0
        d = {'Yes': u'Да', 'No': u'Нет'}
        for p in params.values()[0]:
            score = 0  # default score
            if p.get('Type') != 'SURVEY' or p.get('Formula').strip() != 'answer for survey':
                continue
            survey_data = self.survey_response.loc[self.survey_response['question_text'] == p.get(
                'Values')]
            if not survey_data['selected_option_text'].empty:
                result = survey_data['selected_option_text'].values[0]
                targets = [d.get(x) if x in d.keys() else x
                           for x in unicode(p.get('Target')).split(", ")]
                if result in targets:
                    score = 100
                else:
                    score = 0
            elif not survey_data['number_value'].empty:
                result = survey_data['number_value'].values[0]
                if result == p.get('Target'):
                    score = 100
                else:
                    score = 0
            else:
                Log.warning('No survey data for this session')
            score = round(score, 3)
            set_total_res += round(score * p.get('KPI Weight'), 3)
            if p.get('level') == 3:
                kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level3, 'level3')
            elif p.get('level') == 2:
                kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                attributes_for_level3 = self.create_attributes_for_level3_df(
                    p, score, kpi_fk, level=2, additional_level=3)
                self.write_to_kpi_results_old(attributes_for_level3, 'level3')
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level2, 'level2')
            else:
                Log.warning('No level indicated for this KPI')

        return set_total_res

    @kpi_runtime()
    def check_facings_sos(self, params, level=2, cooler_dict=None):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('level') != level:
                continue
            if p.get('Type') in ('MAN in CAT', 'MAN', 'BRAND', 'BRAND_IN_CAT', 'SUB_BRAND_IN_CAT') and \
                    p.get('Formula').strip() in ['sos', 'SOS', 'sos with empty']:
                ratio = self.calculate_facings_sos(p)
                if ratio is None:
                    ratio = 0
            else:
                continue
            if p.get('depends on'):
                scenes_info = pd.merge(self.scenes_info, self.templates, on='template_fk')
                values_list = [unicode(x).strip() for x in p.get('depends on').split(', ')]
                number_relevant_scenes = scenes_info['template_name'].isin(values_list).sum()
                if number_relevant_scenes < 1:
                    ratio = 0
                    score = self.calculate_score(ratio, p)
                else:
                    score = self.calculate_score(ratio, p)
            else:
                score = self.calculate_score(ratio, p)
            # saving to DB
            if np.isnan(score):
                score = 0

            score = round(float(score), 3)

            if not cooler_dict:
                kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                if p.get('level') == 2:
                    attributes_for_level3 = self.create_attributes_for_level3_df(
                        p, score, kpi_fk, level=2, additional_level=3)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')
                    attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level2, 'level2')
                else:
                    attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')

                atomic_result = attributes_for_level3['result']
                if p.get("KPI ID") in params.values()[2]["SESSION LEVEL"]:
                    self.write_to_kpi_facts_hidden(p.get("KPI ID"), None, atomic_result, score)
                self.write_to_db_category_kpis_for_mr(p, result=ratio*100, score=ratio*100)

            set_total_res += round(score * p.get('KPI Weight'), 3)

            if cooler_dict is not None:
                kpi_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.COOLER_SCORE_LVL_2)
                kpi_ref_fk = self.common.get_kpi_fk_by_kpi_type(p['KPI name Eng'].upper())
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=cooler_dict[CCRUConsts.COOLER_FK],
                                               denominator_id=kpi_ref_fk, context_id=cooler_dict[ScifConsts.SCENE_FK],
                                               result=score, score=score * p.get('KPI Weight'),
                                               weight=p.get("KPI Weight"), identifier_parent=cooler_dict,
                                               should_enter=True)
        return set_total_res

    def calculate_facings_sos(self, params, scenes=None, all_params=None):
        if not scenes:
            if params.get('depends on') and all_params:
                depends_on_kpi_name = params.get('depends on')
                for c in all_params.values()[0]:
                    if c.get('KPI name Eng') == depends_on_kpi_name:
                        if c.get('KPI name Eng') == depends_on_kpi_name:
                            if c.get('Formula').strip() == 'number of doors with more than Target facings':
                                scenes = self.calculate_number_of_doors_more_than_target_facings(
                                    c, 'get scenes')
                            elif c.get('Formula').strip() == 'number of coolers with facings target and fullness target':
                                scenes = self.calculate_number_of_doors_more_than_target_facings(
                                    c, 'get scenes')
                                scenes = self.calculate_number_of_doors_of_filled_coolers(c, scenes,
                                                                                          func='get scenes',
                                                                                          proportion_param=0.9)
                            else:
                                scenes = []
                            break

                if not scenes:
                    return 0
            else:
                scenes = self.get_relevant_scenes(params)

        # relevant_scenes = scenes
        relevant_scenes = list(set(scenes).intersection(self.get_relevant_scenes(params)))
        if not relevant_scenes:
            return None

        values_list = [unicode(x).strip()
                       for x in unicode(params.get('Values')).split(', ')]

        categories = [unicode(x).strip()
                      for x in unicode(params.get('Product Category')).strip().split(', ')]

        if params.get('Manufacturer'):
            manufacturers = [unicode(x).strip()
                             for x in unicode(params.get('Manufacturer')).strip().split(', ')]
        else:
            manufacturers = self.kpi_fetcher.TCCC

        if params.get('Formula').strip() == 'sos with empty':
            if params.get('Type') == 'MAN':
                pop_filter = (self.scif['scene_id'].isin(relevant_scenes))
                subset_filter = (self.scif[Fd.M_NAME].isin(manufacturers))
            elif params.get('Type') == 'MAN in CAT':
                pop_filter = ((self.scif[Fd.CAT].isin(values_list)) &
                              (self.scif['scene_id'].isin(relevant_scenes)))
                subset_filter = (self.scif[Fd.M_NAME].isin(manufacturers))
            else:
                return 0
        else:
            if params.get('Type') == 'MAN':
                pop_filter = ((self.scif['scene_id'].isin(relevant_scenes)) &
                              (~self.scif['product_type'].isin(['Empty'])))
                subset_filter = ((self.scif[Fd.M_NAME].isin(manufacturers)) &
                                 (~self.scif['product_type'].isin(['Empty'])))
            elif params.get('Type') == 'MAN in CAT':
                pop_filter = ((self.scif[Fd.CAT].isin(values_list)) &
                              (self.scif['scene_id'].isin(relevant_scenes)) &
                              (~self.scif['product_type'].isin(['Empty'])))
                subset_filter = ((self.scif[Fd.M_NAME].isin(manufacturers)) &
                                 (~self.scif['product_type'].isin(['Empty'])))
            elif params.get('Type') == 'SUB_BRAND_IN_CAT':
                pop_filter = ((self.scif[Fd.CAT].isin(categories)) &
                              (self.scif['scene_id'].isin(relevant_scenes)) &
                              (~self.scif['product_type'].isin(['Empty'])))
                subset_filter = ((self.scif['sub_brand_name'].isin(values_list)) &
                                 (~self.scif['product_type'].isin(['Empty'])))
            elif params.get('Type') == 'BRAND_IN_CAT':
                pop_filter = ((self.scif[Fd.CAT].isin(categories)) &
                              (self.scif['scene_id'].isin(relevant_scenes)) &
                              (~self.scif['product_type'].isin(['Empty'])))
                subset_filter = ((self.scif['brand_name'].isin(values_list)) &
                                 (~self.scif['product_type'].isin(['Empty'])))
            elif params.get('Type') == 'BRAND':
                pop_filter = ((self.scif['scene_id'].isin(relevant_scenes)) &
                              (~self.scif['product_type'].isin(['Empty'])))
                subset_filter = ((self.scif['brand_name'].isin(values_list)) &
                                 (~self.scif['product_type'].isin(['Empty'])))
            else:
                return 0

        try:
            ratio = self.k_engine.calculate_sos_by_facings(pop_filter, subset_filter)
        except Exception as e:
            ratio = 0
        if ratio is None:
            ratio = 0
        return round(ratio, 5)

    def calculate_score(self, kpi_total_res, params):
        """
        This function calculates score according to predefined score functions

        """
        self.update_kpi_scores_and_results(params, {'result': kpi_total_res})
        if not params.get('Target'):
            return kpi_total_res

        if params.get('Target') == 'range of targets':
            if not (params.get('target_min', 0) < kpi_total_res <= params.get('target_max', 100)):
                score = 0
                if kpi_total_res < params.get('target_min', 0):
                    self.update_kpi_scores_and_results(
                        params, {'target': params.get('target_min')})
                else:
                    self.update_kpi_scores_and_results(
                        params, {'target': params.get('target_max')})
            else:
                self.update_kpi_scores_and_results(params, {'target': params.get('target_min')})
                numerator = kpi_total_res - params.get('target_min', 0)
                denominator = params.get('target_max', 1) - params.get('target_min', 0)
                score = (numerator / float(denominator)) * 100
            return score

        elif params.get('Target') == 'targets by guide':
            target = self.kpi_fetcher.get_category_target_by_region(
                params.get('Values'), self.store_id)
        else:
            target = params.get('Target')
        self.update_kpi_scores_and_results(params, {'target': target})
        target = float(target)
        if not target:
            score = 0
        else:
            if params.get('score_func') == PROPORTIONAL:
                score = (kpi_total_res / target) * 100
                if score > 100:
                    score = 100
            elif params.get('score_func') == CONDITIONAL_PROPORTIONAL:
                score = kpi_total_res / target
                if score > params.get('score_max', 1):
                    score = params.get('score_max', 1)
                elif score < params.get('score_min', 0):
                    score = 0
                score *= 100
            elif params.get('score_func') == 'Customer_CCRU_1':
                if kpi_total_res < target:
                    score = 0
                else:
                    score = ((kpi_total_res - target) + 1) * 100
            elif params.get('score_func') == BINARY:
                if kpi_total_res >= target:
                    score = 100
                else:
                    score = 0
            else:
                if kpi_total_res >= target:
                    score = 100
                else:
                    score = 0
        return score

    @kpi_runtime()
    def check_share_of_cch(self, params, level=2, cooler_dict=None):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('level') != level:
                continue
            if p.get('Formula').strip() not in ['number of pure Coolers',
                                                'Share of CCH doors which have 98% TCCC facings',
                                                'Share of CCH doors which have 98% TCCC facings and no FC packs']:
                continue
            scenes = None
            if p.get('depends on'):
                depends_on_kpi_name = p.get('depends on')
                for c in params.values()[0]:
                    if c.get('KPI name Eng') == depends_on_kpi_name:
                        if c.get('Formula').strip() == 'number of doors of filled Coolers':
                            scenes = self.check_number_of_doors_of_filled_coolers(c, 'get scenes')
                        elif c.get('Formula').strip() == 'number of doors with more than Target facings':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(
                                c, 'get scenes')
                        elif c.get('Formula').strip() == 'number of coolers with facings target and fullness target':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(
                                c, 'get scenes')
                            scenes = self.calculate_number_of_doors_of_filled_coolers(c, scenes,
                                                                                      func='get scenes',
                                                                                      proportion_param=0.9)
                        break
                if not scenes:
                    if p.get('level') == 2:
                        scenes = []
                    else:
                        return 0
            else:
                scenes = self.get_relevant_scenes(p)
            if p.get('Formula').strip() == 'number of pure Coolers':
                result = self.calculate_share_of_cch(p, scenes, sos=False)
            elif p.get('Formula').strip() == 'Share of CCH doors which have 98% TCCC facings':
                result = self.calculate_share_of_cch(p, scenes)
            elif p.get('Formula').strip() == 'Share of CCH doors which have 98% TCCC facings and no FC packs':
                result = self.calculate_share_of_cch(p, scenes, no_fc_packs=True)
            else:
                result = self.calculate_share_of_cch(p, scenes)
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(p.get('KPI name Eng'), kpi_fk)
            score = self.calculate_score(result, p)

            score = round(float(score), 3)

            if p.get('KPI Weight') is None:
                set_total_res += score
            else:
                set_total_res += round(score * p.get('KPI Weight'), 3)
            # saving to DB
            if not cooler_dict:
                if level == 2:
                    attributes_for_level3 = self.create_attributes_for_level3_df(
                        p, result, kpi_fk, atomic_kpi_fk, level=2, additional_level=3)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')
                    attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level2, 'level2')
                else:
                    attributes_for_level3 = self.create_attributes_for_level3_df(
                        p, result, kpi_fk, atomic_kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')

                if p.get("KPI ID") in params.values()[2]["SESSION LEVEL"]:
                    self.write_to_kpi_facts_hidden(p.get("KPI ID"), None, None, score)

            if cooler_dict is not None:
                kpi_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.COOLER_SCORE_LVL_2)
                kpi_ref_fk = self.common.get_kpi_fk_by_kpi_type(p['KPI name Eng'].upper())
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=cooler_dict[CCRUConsts.COOLER_FK],
                                               denominator_id=kpi_ref_fk, context_id=cooler_dict[ScifConsts.SCENE_FK],
                                               result=score, score=score * p.get('KPI Weight'),
                                               weight=p.get("KPI Weight"), identifier_parent=cooler_dict,
                                               should_enter=True)

        return set_total_res

    def check_number_of_doors_of_filled_coolers(self, params, func=None, proportion=None):
        if func == 'get scenes':
            if not proportion:
                proportion = 0.8
            relevant_scenes = self.get_relevant_scenes(params)
            scenes = self.calculate_number_of_doors_of_filled_coolers(params, relevant_scenes,
                                                                      func='get scenes',
                                                                      proportion_param=proportion)
            return scenes
        scenes = self.get_relevant_scenes(params)
        result = self.calculate_number_of_doors_of_filled_coolers(params, scenes)
        return result

    def check_number_of_scenes_given_facings(self, params):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula').strip() != 'number of scenes with have at least target amount of facings':
                continue
            scenes = self.get_relevant_scenes(p)
            score = self.calculate_number_of_scenes_given_facings(p, scenes)
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(p.get('KPI name Eng'), kpi_fk)
            score = round(score, 3)
            if p.get('KPI Weight') is None:
                set_total_res += round(score)
            else:
                set_total_res += round(score * p.get('KPI Weight'), 3)
            # saving to DB
            if p.get('level') == 2:
                attributes_for_level3 = self.create_attributes_for_level3_df(
                    p, score, kpi_fk, atomic_kpi_fk, level=2, additional_level=3)
                self.write_to_kpi_results_old(attributes_for_level3, 'level3')
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level2, 'level2')
            else:
                attributes_for_level3 = self.create_attributes_for_level3_df(
                    p, score, kpi_fk, atomic_kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level3, 'level3')

        return set_total_res

    def calculate_share_of_cch(self, p, scenes, sos=True, no_fc_packs=False, all_params=None):
        sum_of_passed_doors = 0
        sum_of_passed_scenes = 0
        sum_of_all_doors = 0

        if not scenes:
            if p.get('depends on') and all_params:
                depends_on_kpi_name = p.get('depends on')
                for c in all_params.values()[0]:
                    if c.get('KPI name Eng') == depends_on_kpi_name:
                        if c.get('KPI name Eng') == depends_on_kpi_name:
                            if c.get('Formula').strip() == 'number of doors with more than Target facings':
                                scenes = self.calculate_number_of_doors_more_than_target_facings(
                                    c, 'get scenes')
                            elif c.get('Formula').strip() == 'number of coolers with facings target and fullness target':
                                scenes = self.calculate_number_of_doors_more_than_target_facings(
                                    c, 'get scenes')
                                scenes = self.calculate_number_of_doors_of_filled_coolers(c, scenes,
                                                                                          func='get scenes',
                                                                                          proportion_param=0.9)
                            else:
                                scenes = []
                            break

                if not scenes:
                    return 0
            else:
                scenes = self.get_relevant_scenes(p)

        for scene in scenes:
            products_of_tccc = self.scif[(self.scif['scene_id'] == scene) &
                                         (self.scif['manufacturer_name'] == 'TCCC') &
                                         (self.scif['location_type'] == p.get('Locations to include')) &
                                         (self.scif['product_type'] != 'Empty')]['facings'].sum()
            all_products = self.scif[(self.scif['scene_id'] == scene) &
                                     (self.scif['location_type'] == p.get('Locations to include')) &
                                     (self.scif['product_type'] != 'Empty')]['facings'].sum()
            if no_fc_packs:  # If there is a product with size > 1L, the scene fails
                fc_packs = self.scif[(self.scif['scene_id'] == scene) &
                                     (self.scif['size_unit'] == 'l') & (self.scif['size'] > 1) &
                                     (self.scif['location_type'] == p.get('Locations to include')) &
                                     (self.scif['product_type'] != 'Empty')]['facings'].sum()
                fc_packs_passed = False if fc_packs > 0 else True
            else:
                fc_packs_passed = True
            if products_of_tccc == 0:
                proportion = 0
            else:
                proportion = products_of_tccc / float(all_products)
            scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
            if any(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1']):
                num_of_doors = \
                    float(self.templates[self.templates['template_name'] ==
                                         scene_type]['additional_attribute_1'].values[0])
                sum_of_all_doors += num_of_doors
            else:
                num_of_doors = 1
                sum_of_all_doors += num_of_doors
            if proportion > 0.98 and fc_packs_passed:
                sum_of_passed_doors += num_of_doors
                sum_of_passed_scenes += 1
        if not sos:
            return sum_of_passed_scenes
        if sum_of_all_doors:
            ratio = (sum_of_passed_doors / float(sum_of_all_doors)) * 100
        else:
            ratio = 0
        self.update_kpi_scores_and_results(p, {'result': sum_of_passed_doors})
        return ratio

    def calculate_number_of_doors_of_filled_coolers(self, p, scenes, func=None, proportion_param=0.8):
        sum_of_passed_doors = 0
        scenes_passed = []
        for scene in scenes:
            products_of_tccc = self.scif[(self.scif['scene_id'] == scene) &
                                         (self.scif['manufacturer_name'] == 'TCCC') &
                                         (self.scif['location_type'] == p.get('Locations to include')) &
                                         (self.scif['product_type'] != 'Empty')]['facings'].sum()
            all_products = self.scif[(self.scif['scene_id'] == scene) &
                                     (self.scif['location_type'] == p.get('Locations to include'))]['facings'].sum()
            if all_products:
                proportion = products_of_tccc / float(all_products)
            else:
                proportion = 0
            scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
            if any(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1']):
                num_of_doors = \
                    float(self.templates[self.templates['template_name'] ==
                                         scene_type]['additional_attribute_1'].values[0])
            else:
                num_of_doors = 1
            if proportion >= proportion_param:
                sum_of_passed_doors += num_of_doors
                scenes_passed.append(scene)
        if func == 'get scenes':
            return scenes_passed
        return sum_of_passed_doors

    def calculate_number_of_scenes_given_facings(self, p, scenes):
        sum_of_passed_scenes = 0
        for scene in scenes:
            facings = self.calculate_availability(p, scenes=[scene])
            if facings >= p.get('Target'):
                sum_of_passed_scenes += 1
        return sum_of_passed_scenes

    @kpi_runtime()
    def check_number_of_skus_per_door_range(self, params, level=2, cooler_dict=None):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('level') != level:
                continue
            if p.get('Formula').strip() not in ('number of SKU per Door RANGE', 'number of SKU per Door RANGE TOTAL'):
                continue
            scenes = None
            if p.get('depends on'):
                depends_on_kpi_name = p.get('depends on')
                for c in params.values()[0]:
                    if c.get('KPI name Eng') == depends_on_kpi_name:
                        if c.get('Formula').strip() == 'number of doors of filled Coolers':
                            scenes = self.check_number_of_doors_of_filled_coolers(c, 'get scenes')
                        elif c.get('Formula').strip() == 'number of coolers with facings target and fullness target':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(
                                c, 'get scenes')
                            scenes = self.calculate_number_of_doors_of_filled_coolers(c, scenes,
                                                                                      func='get scenes',
                                                                                      proportion_param=0.9)
                        else:
                            scenes = self.calculate_number_of_doors_more_than_target_facings(
                                c, 'get scenes')
                        break
                if not scenes:
                    if p.get('level') == 2:
                        scenes = []
                    else:
                        return 0
            else:
                scenes = self.get_relevant_scenes(p)
            if p.get('Formula').strip() == 'number of SKU per Door RANGE':
                score = self.calculate_number_of_skus_per_door_range(p, scenes)
            elif p.get('Formula').strip() == 'number of SKU per Door RANGE TOTAL':
                score = self.calculate_number_of_skus_per_door_range_total(p, scenes)
            else:
                score = 0

            score = round(float(score), 3)

            if p.get('level') == 3:
                return score

            if p.get('level') == 2:
                if p.get('KPI Weight') is None:
                    set_total_res += score
                else:
                    set_total_res += round(score * p.get('KPI Weight'), 3)

            # saving to DB
            if not cooler_dict:
                if p.get('level') == 2:
                    kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                    atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(p.get('KPI name Eng'), kpi_fk)

                    attributes_for_level3 = self.create_attributes_for_level3_df(
                        p, score, kpi_fk, atomic_kpi_fk, level=2, additional_level=3)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')
                    attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level2, 'level2')

                    if p.get("KPI ID") in params.values()[2]["SESSION LEVEL"]:
                        self.write_to_kpi_facts_hidden(p.get("KPI ID"), None, None, score)

            if cooler_dict is not None:
                kpi_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.COOLER_SCORE_LVL_2)
                kpi_ref_fk = self.common.get_kpi_fk_by_kpi_type(p['KPI name Eng'].upper())
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=cooler_dict[CCRUConsts.COOLER_FK],
                                               denominator_id=kpi_ref_fk, context_id=cooler_dict[ScifConsts.SCENE_FK],
                                               result=score, score=score * p.get('KPI Weight'),
                                               weight=p.get("KPI Weight"), identifier_parent=cooler_dict,
                                               should_enter=True)

        return set_total_res

    def calculate_number_of_skus_per_door_range(self, p, scenes):
        sum_of_passed_doors = 0
        sum_of_all_doors = 0
        for scene in scenes:
            eans_count = len(self.scif[(self.scif['scene_id'] == scene) &
                                       (self.scif['manufacturer_name'] == 'TCCC') &
                                       (self.scif['location_type'] == p.get('Locations to include')) &
                                       (self.scif['facings'] > 0) & (self.scif['product_type'] != 'Empty')][
                'product_ean_code'].unique())
            scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
            if any(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1']):
                num_of_doors = \
                    float(self.templates[self.templates['template_name'] ==
                                         scene_type]['additional_attribute_1'].values[0])
            else:
                num_of_doors = 1
            sum_of_all_doors += num_of_doors
            if p.get('target_min') <= eans_count / num_of_doors <= p.get('target_max'):
                sum_of_passed_doors += num_of_doors
        self.update_kpi_scores_and_results(p, {'result': sum_of_passed_doors})
        if sum_of_all_doors:
            return (sum_of_passed_doors / sum_of_all_doors) * 100
        return 0

    def calculate_number_of_skus_per_door_range_total(self, p, scenes):
        doors_count = 0
        eans_count = 0
        for scene in scenes:
            eans_count += len(self.scif[(self.scif['scene_id'] == scene) &
                                        (self.scif['manufacturer_name'] == 'TCCC') &
                                        (self.scif['location_type'] == p.get('Locations to include')) &
                                        (self.scif['facings'] > 0) & (self.scif['product_type'] != 'Empty')][
                'product_ean_code'].unique())
            scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
            if any(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1']):
                doors_count += float(self.templates[self.templates['template_name']
                                                    == scene_type]['additional_attribute_1'].values[0])
            else:
                doors_count += 1
        if doors_count:
            result = round(eans_count / float(doors_count), 3)
        else:
            result = 0
        if p.get('target_min') <= result <= p.get('target_max'):
            score = 100
        else:
            score = 0
        self.update_kpi_scores_and_results(p, {'result': result})

        return score

    @kpi_runtime()
    def check_number_of_skus_in_single_scene_type(self, params):
        """
        This function calculates number of SKUs per single scene type

        """
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula').strip() != 'number of SKUs in one scene type' or p.get('level') == 3:
                continue
            score = self.calculate_number_of_skus_in_single_scene_type(params, p)
            score = round(score, 3)
            set_total_res += round(score * p.get('KPI Weight'), 3)

        return set_total_res

    def calculate_number_of_skus_in_single_scene_type(self, params, p, kpi_fk=None):
        if kpi_fk is None:
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
        results_dict = {}
        relevant_scenes = self.get_relevant_scenes(params)
        for scene in relevant_scenes:
            scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
            location = list(self.scif.loc[self.scif['scene_id'] == scene]['location_type'].values)
            if location:
                location = location[0]
            sub_location = list(
                self.scif.loc[self.scif['template_name'] == scene_type]['additional_attribute_2'].values)
            if sub_location:
                sub_location = sub_location[0]
            if p.get('Children') is not None:
                children_scores = []
                for child in params.values()[0]:
                    if child.get('KPI ID') in self.children_to_int_list(p.get("Children")):
                        res = self.calculate_number_of_skus_in_single_scene_type(
                            params, child, kpi_fk)
                        children_scores.append(res)
                score = max(children_scores)
                # saving to level2
                attributes_for_table2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_kpi_results_old(attributes_for_table2, 'level2')
                return score
            else:
                res = self.calculate_availability(p, scenes=[scene])
                type_to_split_by = scene_type
                if p.get('Scenes to include'):  # by scene
                    type_to_split_by = scene_type
                elif sub_location and p.get('Sub locations to include'):
                    type_to_split_by = sub_location
                elif location and p.get('Locations to include'):
                    type_to_split_by = location
                if type_to_split_by not in results_dict:
                    results_dict[type_to_split_by] = 0
                results_dict[type_to_split_by] += res

        results_list = [self.calculate_score(res, p) for res in results_dict.values()]
        results_list = filter(bool, results_list)  # filtering the score=0
        if len(results_list) == 1:
            score = 100
        else:
            score = 0
        # Saving to old tables
        if p.get('level') == 2:  # saving also to level3 in case this KPI has only one level
            attributes_for_table3 = self.create_attributes_for_level3_df(
                p, score, kpi_fk, level=2, additional_level=3)
            self.write_to_kpi_results_old(attributes_for_table3, 'level3')
            attributes_for_table2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_kpi_results_old(attributes_for_table2, 'level2')
        else:
            attributes_for_table3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
            self.write_to_kpi_results_old(attributes_for_table3, 'level3')

        return score

    def get_relevant_products_by_type_and_values(self, params,
                                                 return_column='product_fk',
                                                 include_product_types='SKU'):
        value_type = \
            {
                'CAT': 'category',
                'SUB_CATEGORY': 'sub_category',
                'MAN in CAT': 'category',
                'MAN': 'manufacturer_name',
                'BRAND': 'brand_name',
                'SUB_BRAND': 'sub_brand_name',
                'SKUs': 'product_ean_code',
            }

        if not isinstance(include_product_types, (list, tuple, set)):
            include_product_types = [include_product_types]

        filtered_products = self.products

        if params.get('Type') in value_type.keys():
            filter_name = value_type[params.get('Type')]
            filter_values = [unicode(x).strip() for x in unicode(params.get('Values')).split(', ')]
            filtered_products = filtered_products[filtered_products[filter_name].isin(filter_values)]

        if include_product_types:
            filtered_products = filtered_products[filtered_products['product_type'].isin(include_product_types)]

        if return_column in filtered_products.columns:
            return_list = filtered_products[filtered_products[return_column].notnull()][return_column].unique().tolist()
        else:
            return_list = []

        return return_list

    def get_relevant_products_by_product_filters(self, params,
                                                 return_column='product_fk',
                                                 include_product_types='SKU'):
        filters = \
            {
                'Manufacturer': 'manufacturer_name',
                'Brand': 'brand_name',
                'Sub brand': 'sub_brand_name',
                'Product Category': 'category',
                'Sub category': 'sub_category',
                'Product': 'product_ean_code',
                'Form Factor': 'form_factor',
                'Size': 'size',

                'Products to exclude': 'product_ean_code',
                'Form factors to exclude': 'form_factor',

            }

        if not isinstance(include_product_types, (list, tuple, set)):
            include_product_types = [include_product_types]

        filtered_products = self.products

        for filter_name in filters.keys():
            if params.get(filter_name) is not None:
                if filters[filter_name] == 'size':
                    filter_values = [float(x) for x in str(params.get(filter_name)).split(', ')]
                    filter_values = [int(x) if int(x) == x else x for x in filter_values]
                else:
                    filter_values = [unicode(x).strip() for x in unicode(params.get(filter_name)).split(', ')]
                if filter_name.find('exclude') < 0:
                    filtered_products = filtered_products[filtered_products[filters[filter_name]].isin(filter_values)]
                else:
                    filtered_products = filtered_products[~filtered_products[filters[filter_name]].isin(filter_values)]

        if include_product_types:
            filtered_products = filtered_products[filtered_products['product_type'].isin(include_product_types)]

        if return_column in filtered_products.columns:
            return_list = filtered_products[filtered_products[return_column].notnull()][return_column].unique().tolist()
        else:
            return_list = []

        return return_list

    def calculate_lead_sku(self, params, scenes=None):
        if not scenes:
            scenes = self.get_relevant_scenes(params)
        relevant_products = self.get_relevant_products_by_product_filters(params,
                                                                          return_column='product_fk',
                                                                          include_product_types='SKU')
        relevant_products_and_facings = self.scif[
            (self.scif['scene_id'].isin(scenes)) &
            (self.scif['product_fk'].isin(relevant_products))]

        if params.get('Type') == 'SKUs':
            values = [unicode(x).strip()
                      for x in unicode(params.get('Values')).replace(' ', '').replace('=', '\n').split('\n')]
        else:
            values = [','.join(self.get_relevant_products_by_type_and_values(params,
                                                                             return_column='product_ean_code',
                                                                             include_product_types='SKU'))]
        partner_skus = []
        tested_skus = []
        if values:
            for skus in values:
                analogue_skus = [unicode(x).strip() for x in skus.split(',')]
                anchor_sku = analogue_skus.pop(0)
                partner_skus += [anchor_sku]
                relevant_products_and_facings.loc[
                    relevant_products_and_facings['product_ean_code'].isin(analogue_skus), [
                        'product_ean_code']] = anchor_sku

            tested_skus = [partner_skus.pop(0)]

        if tested_skus and not relevant_products_and_facings.empty:
            tested_facings = \
                relevant_products_and_facings[
                    relevant_products_and_facings['product_ean_code'].isin(tested_skus)]['facings'].sum()
            partner_facings_max = \
                relevant_products_and_facings[
                    relevant_products_and_facings['product_ean_code'].isin(partner_skus)].groupby(
                    'product_ean_code').agg({'facings': 'sum'}).max().sum()
            other_facings_max = \
                relevant_products_and_facings[
                    ~relevant_products_and_facings['product_ean_code'].isin(tested_skus + partner_skus)].groupby(
                    'product_ean_code').agg({'facings': 'sum'}).max().sum()
        else:
            tested_facings = 0
            partner_facings_max = 0
            other_facings_max = 0

        # # option 1:
        # # tested sku is lead if its facings are more or equal to any partner sku facings
        # # or they are more than any other sku facings
        # facings_target = max(partner_facings_max, other_facings_max + 1)

        # # option 2:
        # tested sku is lead if its facings are more or equal to any sku facings (either partner or any other sku)
        facings_target = max(partner_facings_max, other_facings_max)

        self.update_kpi_scores_and_results(params, {'result': tested_facings, 'target': facings_target})
        return tested_facings, facings_target

    def calculate_number_of_scenes(self, p):
        """
        This function is used to calculate number of scenes

        """
        kpi_total_res = 0
        scenes = self.get_relevant_scenes(p)
        if p.get('Type') == 'SCENES':
            if p.get('Values'):
                values_list = [unicode(x).strip() for x in unicode(p.get('Values')).split(', ')]
                for scene in scenes:
                    try:
                        scene_type = self.scif.loc[self.scif['scene_id']
                                                   == scene]['template_name'].values[0]
                        if scene_type in values_list:
                            res = 1
                        else:
                            res = 0
                        kpi_total_res += res
                    except IndexError as e:
                        continue
            else:
                return len(scenes)
        elif p.get('Type') == 'LOCATION_TYPE':
            values_list = [unicode(x).strip() for x in p.get('Values').split(', ')]
            for scene in scenes:
                try:
                    location_type = self.scif.loc[self.scif['scene_id']
                                                  == scene]['location_type'].values[0]
                    if location_type in values_list:
                        res = 1
                    else:
                        res = 0
                    kpi_total_res += res
                except IndexError as e:
                    continue
        elif p.get('Type') == 'SUB_LOCATION_TYPE':
            values_list = [p.get('Values')]
            for scene in scenes:
                try:
                    scene_type = self.scif.loc[self.scif['scene_id']
                                               == scene]['template_name'].values[0]
                    sub_location_type = int(self.templates.loc[self.templates['template_name'] == scene_type][
                        'additional_attribute_2'].values[0])
                    if sub_location_type in values_list:
                        res = 1
                    else:
                        res = 0
                    kpi_total_res += res
                except IndexError as e:
                    continue
        # checking for number of scenes with a complex condition (only certain products/brands/etc)
        else:
            p_copy = p.copy()
            p_copy["Formula"] = "number of facings"
            for scene in scenes:
                if self.calculate_availability(p_copy, scenes=[scene]) > 0:
                    res = 1
                else:
                    res = 0
                kpi_total_res += res
        score = self.calculate_score(kpi_total_res, p)
        return score

    def calculate_tccc_denominated(self, params, denominator):
        facings = self.calculate_availability(params)
        return float(facings) / denominator

    @kpi_runtime()
    def check_customer_cooler_doors(self, params, level=2, cooler_dict=None):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('level') != level:
                continue
            if p.get('Formula').strip() not in  ["facings TCCC/40", "facings TCCC/28"]:
                continue
            if p.get('Formula').strip() == "facings TCCC/40":
                d = 40
            elif p.get('Formula').strip() == "facings TCCC/28":
                d = 28
            else:
                continue
            kpi_total_res = self.calculate_tccc_denominated(p, d)
            score = self.calculate_score(kpi_total_res, p)

            score = round(float(score), 3)

            set_total_res += round(score * p.get('KPI Weight'), 3)
            # writing to DB
            if not cooler_dict:
                kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                if p.get('level') == 2:
                    attributes_for_level3 = self.create_attributes_for_level3_df(
                        p, score, kpi_fk, level=2, additional_level=3)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')
                    attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level2, 'level2')
                else:
                    attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')

            if cooler_dict is not None:
                kpi_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.COOLER_SCORE_LVL_2)
                kpi_ref_fk = self.common.get_kpi_fk_by_kpi_type(p['KPI name Eng'].upper())
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=cooler_dict[CCRUConsts.COOLER_FK],
                                               denominator_id=kpi_ref_fk, context_id=cooler_dict[ScifConsts.SCENE_FK],
                                               result=score, score=score * p.get('KPI Weight'),
                                               weight=p.get("KPI Weight"), identifier_parent=cooler_dict,
                                               should_enter=True)
        return set_total_res

    @kpi_runtime()
    def check_dummies(self, params, level=2, cooler_dict=None):
        total_score = 0
        for p in params.values()[0]:
            if p.get('level') != level:
                continue
            if p.get('Formula').strip() != 'DUMMY':
                continue
            score = 0
            total_score += round(score * p.get('KPI Weight'), 3)
            # writing to DB
            if not cooler_dict:
                kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                if p.get('level') == 2:
                    attributes_for_level3 = self.create_attributes_for_level3_df(
                        p, score, kpi_fk, level=2, additional_level=3)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')
                    attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level2, 'level2')
                else:
                    attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')

            if cooler_dict is not None:
                kpi_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.COOLER_SCORE_LVL_2)
                kpi_ref_fk = self.common.get_kpi_fk_by_kpi_type(p['KPI name Eng'].upper())
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=cooler_dict[CCRUConsts.COOLER_FK],
                                               denominator_id=kpi_ref_fk, context_id=cooler_dict[ScifConsts.SCENE_FK],
                                               result=score, score=score * p.get('KPI Weight'),
                                               weight=p.get("KPI Weight"), identifier_parent=cooler_dict,
                                               should_enter=True)
        return total_score

    @kpi_runtime()
    def check_sum_atomics(self, params, level=2, cooler_dict=None):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('level') != level:
                continue
            if p.get('Formula').strip() != "sum of atomic KPI result" or not p.get("Children"):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = self.children_to_int_list(p.get("Children"))
            kpi_total = 0
            atomic_result_total = 0
            for c in params.values()[0]:
                if c.get("KPI ID") in children:
                    if c.get("Formula").strip() == "number of facings":
                        atomic_res = self.calculate_availability(c)
                    elif c.get("Formula").strip() == "number of doors with more than Target facings":
                        atomic_res = self.calculate_number_of_doors_more_than_target_facings(c, 'sum of doors')
                    elif c.get("Formula").strip() == "facings TCCC/40":
                        atomic_res = self.calculate_tccc_denominated(c, 40)
                    elif c.get("Formula").strip() == "facings TCCC/28":
                        atomic_res = self.calculate_tccc_denominated(c, 28)
                    elif c.get("Formula").strip() == "number of doors of filled Coolers":
                        atomic_res = self.check_number_of_doors_of_filled_coolers(c)
                    elif c.get("Formula").strip() == "check_number_of_scenes_with_facings_target":
                        atomic_res = self.calculate_number_of_scenes_with_target(c)
                    elif c.get("Formula").strip() == "number of coolers with facings target and fullness target":
                        scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')
                        atomic_res = self.calculate_number_of_doors_of_filled_coolers(c, scenes, proportion_param=0.9)
                    else:
                        # print "sum of atomic KPI result:", c.get("Formula").strip()
                        atomic_res = 0

                    atomic_score = self.calculate_score(atomic_res, c)

                    kpi_total += atomic_res
                    # write to DB
                    kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                    atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(
                        c.get('KPI name Eng'), kpi_fk)
                    attributes_for_level3 = self.create_attributes_for_level3_df(
                        c, atomic_score, kpi_fk, atomic_kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')

                    atomic_result = attributes_for_level3['result']
                    if atomic_result.size > 0:
                        atomic_result_total += atomic_result.values[0]

            if p.get('Target'):
                if p.get('score_func') == 'PROPORTIONAL':
                        # score = (kpi_total/p.get('Target')) * 100
                    score = min((float(kpi_total) / p.get('Target')) * 100, 100)
                else:
                    if kpi_total >= p.get('Target'):
                        score = 100
                    else:
                        score = 0
            elif self.attr15:
                # if kpi_total >= self.attr15:
                score = (float(kpi_total)/self.attr15) * 100
                if score > 100:
                    score = 100
            else:
                score = 0

            score = round(float(score), 3)

            if p.get('KPI Weight') is None:
                set_total_res += score
            else:
                set_total_res += round(score * p.get('KPI Weight'), 3)
            # saving to DB
            if not cooler_dict:
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level2, 'level2')

                if p.get("KPI ID") in params.values()[2]["SESSION LEVEL"]:
                    self.write_to_kpi_facts_hidden(p.get("KPI ID"), None, atomic_result_total, score)

            if cooler_dict is not None:
                kpi_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.COOLER_SCORE_LVL_2)
                kpi_ref_fk = self.common.get_kpi_fk_by_kpi_type(p['KPI name Eng'].upper())
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=cooler_dict[CCRUConsts.COOLER_FK],
                                               denominator_id=kpi_ref_fk, context_id=cooler_dict[ScifConsts.SCENE_FK],
                                               result=score, score=score * p.get('KPI Weight'),
                                               weight=p.get("KPI Weight"), identifier_parent=cooler_dict,
                                               should_enter=True)

        return set_total_res

    @kpi_runtime()
    def check_atomic_passed(self, params, level=2, cooler_dict=None):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('level') != level:
                continue
            if p.get('Formula').strip() != "number of atomic KPI Passed" or not p.get("Children"):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = self.children_to_int_list(p.get("Children"))
            kpi_total = 0
            for c in params.values()[0]:
                if c.get("KPI ID") in children:
                    atomic_score = -1
                    atomic_target = 0
                    if c.get("Formula").strip() == "number of facings" or c.get("Formula").strip() == "number of SKUs":
                        atomic_res = self.calculate_availability(c, all_params=params)
                    elif c.get("Formula").strip() == "SOS":
                        atomic_res = self.calculate_facings_sos(c, all_params=params)
                    elif c.get("Formula").strip() == "number of sub atomic KPI Passed":
                        atomic_res = self.calculate_sub_atomic_passed(c, params, parent=p)
                    elif c.get("Formula").strip() == "Lead SKU":
                        atomic_res, atomic_target = self.calculate_lead_sku(c)
                        if atomic_res == 0 or atomic_res < atomic_target:
                            atomic_score = 0
                        else:
                            atomic_score = 100
                    elif c.get("Formula").strip() == "number of scenes":
                        atomic_res = self.calculate_number_of_scenes(c)
                    elif c.get("Formula").strip() == "number of facings near food":
                        atomic_res = self.calculate_number_facings_near_food(c, params)
                    elif c.get("Formula").strip() == "number of doors with more than Target facings":
                        atomic_res = self.calculate_number_of_doors_more_than_target_facings(
                            c, 'sum of doors')
                    elif c.get("Formula").strip() == "number of doors of filled Coolers":
                        atomic_res = self.check_number_of_doors_of_filled_coolers(c)
                    elif c.get("Formula").strip() == "number of pure Coolers":
                        scenes = self.get_relevant_scenes(c)
                        atomic_res = self.calculate_share_of_cch(c, scenes, sos=False)
                    elif c.get("Formula").strip() == "number of filled Coolers (scenes)":
                        scenes_list = self.check_number_of_doors_of_filled_coolers(
                            c, func='get scenes')
                        atomic_res = len(scenes_list)
                    elif c.get("Formula").strip() in ('number of SKU per Door RANGE',
                                                      'number of SKU per Door RANGE TOTAL'):
                        atomic_score = self.check_number_of_skus_per_door_range(params, level=3)
                        atomic_res = atomic_score
                    elif c.get("Formula").strip() == "Scenes with no tagging":
                        atomic_res = self.check_number_of_scenes_no_tagging(c, level=3)
                    elif c.get("Formula").strip() == "check_number_of_scenes_with_facings_target":
                        atomic_res = self.calculate_number_of_scenes_with_target(c)
                    elif c.get("Formula").strip() == "number of sub atomic KPI Passed on the same scene":
                        atomic_res = self.calculate_sub_atomic_passed_on_the_same_scene(
                            c, params, parent=p, scenes=[])
                    else:
                        # print "the atomic's formula is ", c.get('Formula').strip()
                        atomic_res = 0
                    if atomic_res == -1:
                        continue
                    if atomic_score == -1:
                        atomic_score = self.calculate_score(atomic_res, c)
                    # write to DB
                    atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(
                        c.get('KPI name Eng'), kpi_fk)
                    if c.get("Formula").strip() == "Lead SKU":
                        attributes_for_level3 = self.create_attributes_for_level3_df(
                            c, (atomic_score, atomic_res, atomic_target), kpi_fk, atomic_kpi_fk)
                    else:
                        attributes_for_level3 = self.create_attributes_for_level3_df(
                            c, atomic_score, kpi_fk, atomic_kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')
                    if atomic_score > 0:
                        kpi_total += 1

            score = self.calculate_score(kpi_total, p)

            score = round(float(score), 3)

            if p.get('KPI Weight') is None:
                set_total_res += score
            else:
                set_total_res += round(score * p.get('KPI Weight'), 3)

            # saving to DB
            if not cooler_dict:
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level2, 'level2')

                if p.get("KPI ID") in params.values()[2]["SESSION LEVEL"]:
                    self.write_to_kpi_facts_hidden(p.get("KPI ID"), None, kpi_total, score)

            if cooler_dict is not None:
                kpi_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.COOLER_SCORE_LVL_2)
                kpi_ref_fk = self.common.get_kpi_fk_by_kpi_type(p['KPI name Eng'].upper())
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=cooler_dict[CCRUConsts.COOLER_FK],
                                               denominator_id=kpi_ref_fk, context_id=cooler_dict[ScifConsts.SCENE_FK],
                                               result=score, score=score * p.get('KPI Weight'),
                                               weight=p.get("KPI Weight"), identifier_parent=cooler_dict,
                                               should_enter=True)

        return set_total_res

    @kpi_runtime()
    def check_atomic_passed_on_the_same_scene(self, params, level=2, cooler_dict=None):
        set_total_res = 0
        self.passed_scenes_per_kpi = {}
        for p in params.values()[0]:
            if p.get('level') != level:
                continue
            if p.get('Formula').strip() != "number of atomic KPI Passed on the same scene" or not p.get("Children"):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = self.children_to_int_list(p.get("Children"))
            info_by_kpi_id = self.build_dict(params.values()[0], 'KPI ID')
            if p.get('depends on'):
                depends_on_kpi_name = p.get('depends on')
                if depends_on_kpi_name in self.passed_scenes_per_kpi.keys():
                    relevant_scenes = self.passed_scenes_per_kpi[depends_on_kpi_name]
                else:
                    relevant_scenes = []
            else:
                relevant_scenes = self.get_relevant_scenes(p)
            scenes_kpi_info = {}
            favorite_scene = None
            for scene in relevant_scenes:
                scenes_kpi_info[scene] = {'num_passed_kpi': 0, 'total_row_no_passed': 0}
                kpi_total = 0
                index = 0
                for child in children:
                    atomic_score = -1
                    index += 1
                    c = info_by_kpi_id.get(child)
                    if c.get("Formula").strip() == "number of facings":
                        atomic_res = self.calculate_availability(c, [scene])
                    elif c.get("Formula").strip() == "number of sub atomic KPI Passed":
                        atomic_res = self.calculate_sub_atomic_passed(
                            c, params, [scene], parent=p, same_scene=True)
                    elif c.get("Formula").strip() == "Lead SKU":
                        atomic_res, atomic_target = self.calculate_lead_sku(c, [scene])
                        if atomic_res == 0 or atomic_res < atomic_target:
                            atomic_score = 0
                        else:
                            atomic_score = 100
                    elif c.get("Formula").strip() == "number of scenes":
                        list_of_scenes = self.get_relevant_scenes(c)
                        if scene in list_of_scenes:
                            atomic_res = 1
                        else:
                            atomic_res = 0
                    else:
                        atomic_res = 0
                    if atomic_res == -1:
                        continue
                    if atomic_score == -1:
                        atomic_score = self.calculate_score(atomic_res, c)
                    kpi_total += atomic_score / 100
                    if atomic_score:
                        scenes_kpi_info[scene]['num_passed_kpi'] += 1
                        scenes_kpi_info[scene]['total_row_no_passed'] += 1

                score = self.calculate_score(kpi_total, p)

                score = round(float(score), 3)

                if score:
                    if p.get('KPI name Eng') in self.passed_scenes_per_kpi:
                        self.passed_scenes_per_kpi[p.get('KPI name Eng')].append(scene)
                    else:
                        self.passed_scenes_per_kpi[p.get('KPI name Eng')] = [scene]

                if p.get("KPI ID") in params.values()[2]["SCENE LEVEL"]:
                    self.write_to_kpi_facts_hidden(p.get("KPI ID"), scene, None, score)

            if relevant_scenes:
                closest_to_pass_scenes = self.get_max_in_dict(scenes_kpi_info)
                if len(closest_to_pass_scenes) == 1:
                    favorite_scene = closest_to_pass_scenes[0]
                else:
                    favorite_scene = self.get_favorite_scene(
                        closest_to_pass_scenes, scenes_kpi_info)
            kpi_total = 0
            for child in children:
                atomic_score = -1
                c = info_by_kpi_id.get(child)
                if favorite_scene:
                    if c.get("Formula").strip() == "number of facings":
                        atomic_res = self.calculate_availability(c, [favorite_scene])
                    elif c.get("Formula").strip() == "number of sub atomic KPI Passed":
                        atomic_res = self.calculate_sub_atomic_passed(
                            c, params, [favorite_scene], parent=p)
                    elif c.get("Formula").strip() == "Lead SKU":
                        atomic_res, atomic_target = self.calculate_lead_sku(c, [favorite_scene])
                        if atomic_res == 0 or atomic_res < atomic_target:
                            atomic_score = 0
                        else:
                            atomic_score = 100
                    elif c.get("Formula").strip() == "number of scenes":
                        list_of_scenes = self.get_relevant_scenes(c)
                        if favorite_scene in list_of_scenes:
                            atomic_res = 1
                        else:
                            atomic_res = 0
                    else:
                        atomic_res = 0
                    if atomic_res == -1:
                        continue
                    if atomic_score == -1:
                        atomic_score = self.calculate_score(atomic_res, c)
                    kpi_total += atomic_score / 100
                else:
                    if c.get("Formula").strip() == "number of sub atomic KPI Passed":
                        sub_atomic_children = self.children_to_int_list(c.get("Children"))
                        for sub_atomic in sub_atomic_children:
                            sub_atomic_info = info_by_kpi_id.get(sub_atomic)
                            sub_atomic_res = 0
                            sub_atomic_score = self.calculate_score(sub_atomic_res, sub_atomic_info)
                            kpi_name = sub_atomic_info.get('KPI name Eng')
                            sub_atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(kpi_name, kpi_fk)
                            attributes_for_level4 = self.create_attributes_for_level3_df(
                                sub_atomic_info, sub_atomic_score, kpi_fk, sub_atomic_kpi_fk, level=4)
                            self.write_to_kpi_results_old(attributes_for_level4, 'level4')
                    atomic_res = 0
                    atomic_score = 0
                    self.calculate_score(atomic_res, c)

                kpi_name = c.get('KPI name Eng')
                atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(kpi_name, kpi_fk)
                attributes_for_level3 = self.create_attributes_for_level3_df(
                    c, atomic_score, kpi_fk, atomic_kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level3, 'level3')

            score = self.calculate_score(kpi_total, p)

            score = round(float(score), 3)

            if p.get('KPI Weight') is None:
                set_total_res += score
            else:
                set_total_res += round(score * p.get('KPI Weight'), 3)
            # saving to DB
            if not cooler_dict:
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level2, 'level2')

                if p.get("KPI ID") in params.values()[2]["SESSION LEVEL"]:
                    self.write_to_kpi_facts_hidden(p.get("KPI ID"), None, None, score)

            if cooler_dict is not None:
                kpi_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.COOLER_SCORE_LVL_2)
                kpi_ref_fk = self.common.get_kpi_fk_by_kpi_type(p['KPI name Eng'].upper())
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=cooler_dict[CCRUConsts.COOLER_FK],
                                               denominator_id=kpi_ref_fk, context_id=cooler_dict[ScifConsts.SCENE_FK],
                                               result=score, score=score * p.get('KPI Weight'),
                                               weight=p.get("KPI Weight"), identifier_parent=cooler_dict,
                                               should_enter=True)

        return set_total_res

    def calculate_sub_atomic_passed_on_the_same_scene(self, params, all_params, scenes=None, parent=None):
        total_res = [0]
        if not scenes:
            scenes = self.get_relevant_scenes(params)
        for scene in scenes:
            total_res += [self.calculate_sub_atomic_passed(params, all_params, scenes=[scene], parent=parent)]
        return max(total_res)

    def calculate_sub_atomic_passed(self, params, all_params, scenes=None, parent=None, same_scene=None):
        if not scenes:
            if params.get('depends on'):
                if params['depends on'] == 'scene type':
                    scenes = self.get_relevant_scenes(params)
                    if not scenes:
                        return -1
                else:
                    depends_on_kpi_name = params.get('depends on')
                    doors = 0
                    for c in all_params.values()[0]:
                        if c.get('KPI name Eng') == depends_on_kpi_name:
                            doors = self.calculate_number_of_doors_more_than_target_facings(
                                c, 'get doors')
                            break
                    if doors < 2:
                        return -1
            # else:
            #     scenes = self.get_relevant_scenes(params)
        children = self.children_to_int_list(params.get("Children"))
        total_res = 0
        for c in all_params.values()[0]:
            if c.get("KPI ID") in children:
                sub_atomic_score = -1
                sub_atomic_target = 0
                if c.get("Formula").strip() == "number of facings":
                    sub_atomic_res = self.calculate_availability(c, scenes, all_params=all_params)
                elif c.get("Formula").strip() == "number of facings near food":
                    sub_atomic_res = self.calculate_number_facings_near_food(c, params)
                elif c.get("Formula").strip() == "Lead SKU":
                    sub_atomic_res, sub_atomic_target = self.calculate_lead_sku(c, scenes)
                    if sub_atomic_res == 0 or sub_atomic_res < sub_atomic_target:
                        sub_atomic_score = 0
                    else:
                        sub_atomic_score = 100
                else:
                    # print "the sub-atomic's formula is ", c.get('Formula').strip()
                    sub_atomic_res = 0
                if sub_atomic_score == -1:
                    sub_atomic_score = self.calculate_score(sub_atomic_res, c)
                total_res += sub_atomic_score / 100
                if same_scene:
                    if parent.get('Formula').strip() == 'number of atomic KPI Passed on the same scene':
                        continue
                kpi_fk = self.kpi_fetcher.get_kpi_fk(parent.get('KPI name Eng'))
                sub_atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(
                    c.get('KPI name Eng'), kpi_fk)
                if c.get("Formula").strip() == "Lead SKU":
                    attributes_for_level4 = self.create_attributes_for_level3_df(
                        c, (sub_atomic_score, sub_atomic_res, sub_atomic_target), kpi_fk, sub_atomic_kpi_fk, level=4)
                else:
                    attributes_for_level4 = self.create_attributes_for_level3_df(
                        c, sub_atomic_score, kpi_fk, sub_atomic_kpi_fk, level=4)
                self.write_to_kpi_results_old(attributes_for_level4, 'level4')
        return total_res

    @kpi_runtime()
    def check_weighted_average(self, params, level=2, cooler_dict=None):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('level') != level:
                continue
            if not (p.get('Formula').strip() in ("Weighted Average", "average of atomic KPI Score", "Weighted Sum")
                    and p.get("Children")):
                continue
            scenes = []
            if p.get('depends on'):
                depends_on_kpi_name = p.get('depends on')
                for c in params.values()[0]:
                    if c.get('KPI name Eng') == depends_on_kpi_name:
                        if c.get('Formula').strip() == 'number of doors with more than Target facings':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(
                                c, 'get scenes')
                        elif c.get('Formula').strip() == 'number of coolers with facings target and fullness target':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(
                                c, 'get scenes')
                            scenes = self.calculate_number_of_doors_of_filled_coolers(c, scenes,
                                                                                      func='get scenes',
                                                                                      proportion_param=0.9)
                        break
            # else:
            #     scenes = self.get_relevant_scenes(p)
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = self.children_to_int_list(p.get("Children"))
            kpi_total = 0
            kpi_total_weight = 0
            for c in params.values()[0]:
                if c.get("KPI ID") in children:
                    atomic_res = -1
                    atomic_score = -1
                    if c.get("Formula").strip() in ("number of facings", "number of SKUs"):
                        atomic_res = self.calculate_availability(c, scenes=scenes, all_params=params)
                    elif c.get("Formula").strip() == "each SKU hits facings target":
                        atomic_res = self.calculate_availability(c, scenes=scenes, all_params=params)
                        atomic_score = 100 if atomic_res == 100 else 0
                    elif c.get("Formula").strip() == "number of sub atomic KPI Passed":
                        atomic_res = self.calculate_sub_atomic_passed(c, params, parent=p, scenes=scenes)
                    elif c.get("Formula").strip() == "number of sub atomic KPI Passed on the same scene":
                        atomic_res = self.calculate_sub_atomic_passed_on_the_same_scene(
                            c, params, parent=p, scenes=scenes)
                    elif c.get("Formula").strip() == "check_number_of_scenes_with_facings_target":
                        atomic_res = self.calculate_number_of_scenes_with_target(c, scenes=scenes)
                    elif c.get("Formula").strip() == "Scenes with no tagging":
                        atomic_res = self.check_number_of_scenes_no_tagging(c, level=3)
                    elif c.get("Formula").strip() == "SOS":
                        atomic_res = self.calculate_facings_sos(c, scenes=scenes, all_params=params)
                    elif c.get("Formula").strip() in ('number of SKU per Door RANGE',
                                                      'number of SKU per Door RANGE TOTAL'):
                        atomic_score = self.check_number_of_skus_per_door_range(params, level=3)
                        atomic_res = atomic_score
                    elif c.get('Formula').strip() == 'number of pure Coolers':
                        atomic_score = self.calculate_share_of_cch(c, scenes, sos=False, all_params=params)
                        atomic_res = atomic_score
                    elif c.get('Formula').strip() == 'Share of CCH doors which have 98% TCCC facings':
                        atomic_score = self.calculate_share_of_cch(c, scenes, all_params=params)
                        atomic_res = atomic_score
                    elif c.get('Formula').strip() == 'Share of CCH doors which have 98% TCCC facings and no FC packs':
                        atomic_score = self.calculate_share_of_cch(c, scenes, no_fc_packs=True, all_params=params)
                        atomic_res = atomic_score
                    elif c.get("Formula").strip() == "DUMMY":
                        atomic_res = 0
                    if atomic_res is None:
                        continue
                    if atomic_res == -1:
                        atomic_score = 0
                    else:
                        if atomic_score == -1:
                            atomic_score = self.calculate_score(atomic_res, c)
                        if p.get('Formula').strip() in ("Weighted Average", "Weighted Sum"):
                            kpi_total += atomic_score * \
                                (c.get('KPI Weight') if c.get('KPI Weight') is not None else 1)
                            kpi_total_weight += (c.get('KPI Weight')
                                                 if c.get('KPI Weight') is not None else 1)
                        else:
                            kpi_total += atomic_score
                            kpi_total_weight += 1

                    # write to DB
                    if not cooler_dict:
                        atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(
                            c.get('KPI name Eng'), kpi_fk)
                        if c.get("Formula").strip() == "each SKU hits facings target":
                            attributes_for_level3 = self.create_attributes_for_level3_df(c, (atomic_score, atomic_res,
                                                                                             100),kpi_fk, atomic_kpi_fk)
                        else:
                            attributes_for_level3 = self.create_attributes_for_level3_df(c, atomic_score,
                                                                                         kpi_fk, atomic_kpi_fk)
                        self.write_to_kpi_results_old(attributes_for_level3, 'level3')

            if kpi_total_weight:
                if p.get('Formula').strip() != "Weighted Sum":
                    kpi_total /= kpi_total_weight
            else:
                kpi_total = 0

            kpi_score = self.calculate_score(kpi_total, p)

            kpi_score = round(float(kpi_score), 3)

            if p.get('KPI Weight') is None:
                set_total_res += round(kpi_score * kpi_total_weight, 3)
                p['KPI Weight'] = kpi_total_weight
            else:
                set_total_res += kpi_score * p.get('KPI Weight')
            # saving to DB
            if not cooler_dict:
                if kpi_fk:
                    attributes_for_level2 = self.create_attributes_for_level2_df(p, kpi_score, kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level2, 'level2')
                self.write_to_db_category_kpis_for_mr(p, result=kpi_score, score=set_total_res)

            if cooler_dict is not None:
                kpi_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.COOLER_SCORE_LVL_2)
                kpi_ref_fk = self.common.get_kpi_fk_by_kpi_type(p['KPI name Eng'].upper())
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=cooler_dict[CCRUConsts.COOLER_FK],
                                               denominator_id=kpi_ref_fk, context_id=cooler_dict[ScifConsts.SCENE_FK],
                                               result=kpi_score, score=kpi_score * p.get('KPI Weight'),
                                               weight=p.get("KPI Weight"), identifier_parent=cooler_dict,
                                               should_enter=True)

        return set_total_res

    def write_to_db_category_kpis_for_mr(self, params, result, score):
        if params.get(CAT_KPI_TYPE) is not None and params.get(CAT_KPI_VALUE) is not None and params.get('level') == 2:
            kpi_type = POS_CAT_KPI_DICT.get(params.get(CAT_KPI_TYPE))
            if kpi_type is not None:
                cat_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type)
                category_fk_array = self.products[self.products['category'] ==
                                                  params.get(CAT_KPI_VALUE)]['category_fk'].values
                if len(category_fk_array) > 0:
                    category_fk = category_fk_array[0]
                    self.common.write_to_db_result(cat_kpi_fk, numerator_id=self.own_manufacturer_id,
                                                   denominator_id=category_fk, result=round(result), score=score)
                if len(category_fk_array) == 0:
                    Log.warning('Category {} does not exist in the DB. '
                                'Check kpi template'.format(params.get(CAT_KPI_VALUE)))

    @kpi_runtime()
    def check_number_of_scenes_no_tagging(self, params, level=2, cooler_dict=None):
        scenes_info = pd.merge(self.scenes_info, self.templates, on='template_fk')
        if level == 3:
            if params.get('Scenes to include'):
                values_list = [unicode(x).strip().encode('utf-8')
                               for x in params.get('Scenes to include').split('; ')]
                number_relevant_scenes = scenes_info['template_name'].isin(values_list).sum()
                return number_relevant_scenes
            else:
                return 0
        else:  # level 2
            set_total_res = 0
            number_relevant_scenes = 0
            scenes = []
            for p in params.values()[0]:
                if p.get('level') != level:
                    continue
                if p.get('Formula').strip() != "Scenes with no tagging":
                    continue
                if p.get('depends on'):
                    depends_on_kpi_name = p.get('depends on')
                    for c in params.values()[0]:
                        if c.get('KPI name Eng') == depends_on_kpi_name:
                            if c.get('Formula').strip() == 'number of doors with more than Target facings':
                                scenes = self.calculate_number_of_doors_more_than_target_facings(
                                    c, 'get scenes')
                            elif c.get('Formula').strip() == 'number of doors of filled Coolers':
                                scenes = self.check_number_of_doors_of_filled_coolers(
                                    c, 'get scenes')
                            break
                    if len(scenes) >= 1:
                        flag = 0
                        final_scenes = scenes_info
                        if p.get('Scenes to include'):
                            scenes_values_list = [unicode(x).strip().encode('utf-8')
                                                  for x in p.get('Scenes to include').split('; ')]
                            final_scenes = scenes_info['template_name'].isin(scenes_values_list)
                            flag = 1
                        if p.get('Locations to include'):
                            location_values_list = [unicode(x).strip() for x in p.get(
                                'Locations to include').split(', ')]
                            if flag:
                                if sum(final_scenes):
                                    final_scenes = scenes_info[final_scenes]['location_type'].isin(
                                        location_values_list)
                            else:
                                final_scenes = final_scenes['location_type'].isin(
                                    location_values_list)
                        number_relevant_scenes = final_scenes.sum()
                else:
                    if p.get('Scenes to include'):
                        values_list = [unicode(x).strip().encode('utf-8')
                                       for x in p.get('Scenes to include').split('; ')]
                        number_relevant_scenes = scenes_info['template_name'].isin(
                            values_list).sum()

                score = self.calculate_score(number_relevant_scenes, p)

                score = round(float(score), 3)

                if p.get('KPI Weight') is None:
                    set_total_res += score
                else:
                    set_total_res += round(score * p.get('KPI Weight'), 3)

                if not cooler_dict:
                    kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                    atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(p.get('KPI name Eng'), kpi_fk)

                    if level == 2:
                        attributes_for_level3 = self.create_attributes_for_level3_df(
                            p, score, kpi_fk, atomic_kpi_fk, level=2, additional_level=3)
                        self.write_to_kpi_results_old(attributes_for_level3, 'level3')
                        attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                        self.write_to_kpi_results_old(attributes_for_level2, 'level2')
                    # else:
                    #     attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk, atomic_kpi_fk)
                    #     self.write_to_kpi_results_old(attributes_for_level3, 'level3')

                    if p.get("KPI ID") in params.values()[2]["SESSION LEVEL"]:
                        self.write_to_kpi_facts_hidden(p.get("KPI ID"), None, None, score)

                if cooler_dict is not None:
                    kpi_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.COOLER_SCORE_LVL_2)
                    kpi_ref_fk = self.common.get_kpi_fk_by_kpi_type(p['KPI name Eng'].upper())
                    self.common.write_to_db_result(fk=kpi_fk, numerator_id=cooler_dict[CCRUConsts.COOLER_FK],
                                                   denominator_id=kpi_ref_fk,
                                                   context_id=cooler_dict[ScifConsts.SCENE_FK],
                                                   result=score, score=score * p.get('KPI Weight'),
                                                   weight=p.get("KPI Weight"), identifier_parent=cooler_dict,
                                                   should_enter=True)

            return set_total_res

    @staticmethod
    def build_dict(seq, key):
        return dict((d[key], dict(d, index=index)) for (index, d) in enumerate(seq))

    @staticmethod
    def get_max_in_dict(dictionary):
        max_value = 0
        closest_to_pass_scenes = []
        for row in dictionary.values():
            if row.get('num_passed_kpi') > max_value:
                max_value = row.get('num_passed_kpi')
        for scene, value in dictionary.items():
            if value.get('num_passed_kpi') == max_value:
                closest_to_pass_scenes.append(scene)
        return closest_to_pass_scenes

    @staticmethod
    def get_favorite_scene(potential_scenes, dictionary):
        favorite_scene = None
        for scene in potential_scenes:
            priority = 999
            if dictionary[scene]['total_row_no_passed'] < priority:
                favorite_scene = scene
        return favorite_scene

    def write_to_kpi_results_old(self, df=None, level=None):
        """
        This function writes KPI results to old tables

        """
        if self.kpi_set_type not in SKIP_OLD_KPIS_FROM_WRITING:

            if level == 'level4':
                if df['kpi_fk'].values[0] is None:
                    df['atomic_kpi_fk'] = self.kpi_fetcher.get_atomic_kpi_fk(df['name'][0])
                df['kpi_fk'] = df['kpi_fk'][0]
                df_dict = df.to_dict()
                df_dict['scope_value'] = {0: 'level 4'}
                df_dict.pop('name', None)
                query = insert(df_dict, KPI_RESULT)
                self.kpi_results_queries.append(query)
            elif level == 'level3':
                if df['kpi_fk'].values[0] is None:
                    df['atomic_kpi_fk'] = self.kpi_fetcher.get_atomic_kpi_fk(df['name'][0])
                df['kpi_fk'] = df['kpi_fk'][0]
                df_dict = df.to_dict()
                df_dict.pop('name', None)
                query = insert(df_dict, KPI_RESULT)
                self.kpi_results_queries.append(query)
            elif level == 'level2':
                kpi_name = df['kpk_name'][0].encode('utf-8')
                if df['kpi_fk'].values[0] is None:
                    df['kpi_fk'] = self.kpi_fetcher.get_kpi_fk(kpi_name)
                df_dict = df.to_dict()
                query = insert(df_dict, KPK_RESULT)
                self.kpi_results_queries.append(query)
            elif level == 'level1':
                if df['kpi_set_fk'].values[0] is None:
                    df['kpi_set_fk'] = self.kpi_fetcher.get_kpi_set_fk()
                df_dict = df.to_dict()
                query = insert(df_dict, KPS_RESULT)
                self.kpi_results_queries.append(query)

    @kpi_runtime()
    def commit_results_data_old(self):
        # self.rds_conn.disconnect_rds()
        self.rds_conn = self.rds_connection()
        cur = self.rds_conn.db.cursor()
        delete_queries = self.kpi_fetcher.get_delete_session_results(
            self.session_uid, self.session_fk)
        for query in delete_queries:
            cur.execute(query)
        # self.rds_conn.db.commit()
        # self.rds_conn.disconnect_rds()
        # self.rds_conn = self.rds_connection()
        # cur = self.rds_conn.db.cursor()
        for query in self.kpi_results_queries:
            cur.execute(query)
        # self.rds_conn.db.commit()
        # self.rds_conn.disconnect_rds()
        # self.rds_conn = self.rds_connection()
        # cur = self.rds_conn.db.cursor()
        for query in set(self.gaps_queries):
            cur.execute(query)
        # self.rds_conn.db.commit()
        # self.rds_conn.disconnect_rds()
        # self.rds_conn = self.rds_connection()
        # cur = self.rds_conn.db.cursor()
        top_sku_queries = self.merge_insert_queries(self.top_sku_queries)
        for query in top_sku_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
        # self.rds_conn.disconnect_rds()
        return

    def write_to_kpi_facts_hidden(self, kpi_id, scene, result, score):
        self.kpi_facts_hidden.append(
            {"KPI ID": kpi_id, "scene_fk": scene, "result": result, "score": score})
        return

    # @kpi_runtime()
    # def prepare_hidden_set(self, params, kpi_set_name):
    #     # table3 = pd.DataFrame([])  # for debugging
    #
    #     self.set_kpi_set(kpi_set_name, INTEGRATION)
    #     kpi_df = self.kpi_fetcher.kpi_static_data
    #
    #     kpi_facts = []
    #     for p in params.values()[1]:
    #         atomic_kpi_id = p.get("KPI ID")
    #         atomic_kpi_name = p.get("KPI name Eng").upper().replace(" ", "_")
    #         atomic_kpi = kpi_df[kpi_df["atomic_kpi_name"] == atomic_kpi_name]["atomic_kpi_fk"]
    #         if atomic_kpi.size > 0:
    #             atomic_kpi_fk = atomic_kpi.values[0]
    #         else:
    #             continue
    #
    #         if p.get("Formula").strip() == "Plan":
    #             result = self.check_planned_visit_flag()
    #             kpi_facts.append({"id": atomic_kpi_id, "name": atomic_kpi_name, "display_text": atomic_kpi_name,
    #                               "atomic_kpi_fk": atomic_kpi_fk, "result": result,
    #                               "format": p.get("Result Format")})
    #
    #         elif p.get("Formula").strip() == "number of KPI Passed" and p.get("Type") == "SESSION LEVEL":  # session level
    #             result = 0
    #             for k in self.kpi_facts_hidden:
    #                 if k.get("KPI ID") in p.get("Children List"):
    #                     if k.get("score") == 100:
    #                         result += 1
    #             kpi_facts.append({"id": atomic_kpi_id, "name": atomic_kpi_name, "display_text": atomic_kpi_name,
    #                               "atomic_kpi_fk": atomic_kpi_fk, "result": result,
    #                               "format": p.get("Result Format")})
    #
    #         elif p.get("Formula").strip() == "KPI score" and p.get("Type") == "SESSION LEVEL":  # session level
    #             for k in self.kpi_facts_hidden:
    #                 if k.get("KPI ID") in p.get("Children List"):
    #                     result = k.get("score")
    #                     kpi_facts.append({"id": atomic_kpi_id, "name": atomic_kpi_name, "display_text": atomic_kpi_name,
    #                                       "atomic_kpi_fk": atomic_kpi_fk, "result": result,
    #                                       "format": p.get("Result Format")})
    #
    #         elif p.get("Formula").strip() == "sum of KPI scores" and p.get("Type") == "SESSION LEVEL":  # session level
    #             result = 0
    #             for k in self.kpi_facts_hidden:
    #                 if k.get("KPI ID") in p.get("Children List"):
    #                     result += k.get("score")
    #             kpi_facts.append({"id": atomic_kpi_id, "name": atomic_kpi_name, "display_text": atomic_kpi_name,
    #                               "atomic_kpi_fk": atomic_kpi_fk, "result": result,
    #                               "format": p.get("Result Format")})
    #
    #         elif p.get("Formula").strip() == "KPI result" and p.get("Type") == "SESSION LEVEL":  # session level
    #             for k in self.kpi_facts_hidden:
    #                 if k.get("KPI ID") in p.get("Children List"):
    #                     result = k.get("result")
    #                     kpi_facts.append({"id": atomic_kpi_id, "name": atomic_kpi_name, "display_text": atomic_kpi_name,
    #                                       "atomic_kpi_fk": atomic_kpi_fk, "result": result,
    #                                       "format": p.get("Result Format")})
    #
    #         elif p.get("Formula").strip() == "sum of KPI results" and p.get("Type") == "SESSION LEVEL":  # session level
    #             result = 0
    #             for k in self.kpi_facts_hidden:
    #                 if k.get("KPI ID") in p.get("Children List"):
    #                     result += k.get("result")
    #             kpi_facts.append({"id": atomic_kpi_id, "name": atomic_kpi_name, "display_text": atomic_kpi_name,
    #                               "atomic_kpi_fk": atomic_kpi_fk, "result": result,
    #                               "format": p.get("Result Format")})
    #
    #         elif p.get("Formula").strip() == "Passed or Failed Value" and p.get("Type") == "SESSION LEVEL":  # session level
    #             for k in self.kpi_facts_hidden:
    #                 if k.get("KPI ID") in p.get("Children List"):
    #                     passed_failed = str(p.get("Values")).replace(" ", "").replace(
    #                         ",", "\n").replace("\n\n", "\n").split("\n")
    #                     if k.get("score") == 100:
    #                         result = passed_failed[0]
    #                     elif len(passed_failed) > 1:
    #                         result = passed_failed[1]
    #                     else:
    #                         result = ""
    #                     kpi_facts.append({"id": atomic_kpi_id, "name": atomic_kpi_name, "display_text": atomic_kpi_name,
    #                                       "atomic_kpi_fk": atomic_kpi_fk, "result": result,
    #                                       "format": p.get("Result Format")})
    #
    #         elif p.get("Formula").strip() == "Value" and p.get("Type") == "SCENE LEVEL":  # scene level
    #             scenes = self.get_relevant_scenes(params)
    #             for scene in scenes:
    #                 scene_uid = self.scenes_info[self.scenes_info['scene_fk']
    #                                              == scene]['scene_uid'].values[0]
    #                 kpi_facts.append({"id": atomic_kpi_id, "name": atomic_kpi_name,
    #                                   "display_text": atomic_kpi_name + "@" + scene_uid,
    #                                   "atomic_kpi_fk": atomic_kpi_fk, "result": p.get("Values"),
    #                                   "scene_uid": scene_uid, "scene_id": scene,
    #                                   "format": p.get("Result Format")})
    #
    #         elif p.get("Formula").strip() == "Attribute" and p.get("Type") == "SCENE LEVEL":  # scene level
    #             scenes = self.get_relevant_scenes(params)
    #             if p.get("Values") == 'template.additional_attribute_1':
    #                 for scene in scenes:
    #                     scene_uid = self.scenes_info[self.scenes_info['scene_fk']
    #                                                  == scene]['scene_uid'].values[0]
    #                     template = self.scenes_info[self.scenes_info['scene_fk']
    #                                                 == scene]['template_fk'].values[0]
    #                     result = self.templates[self.templates['template_fk']
    #                                             == template]['additional_attribute_1'].values[0]
    #                     kpi_facts.append({"id": atomic_kpi_id, "name": atomic_kpi_name,
    #                                       "display_text": atomic_kpi_name + "@" + scene_uid,
    #                                       "atomic_kpi_fk": atomic_kpi_fk, "result": result,
    #                                       "scene_uid": scene_uid, "scene_id": scene,
    #                                       "format": p.get("Result Format")})
    #
    #         elif p.get("Formula").strip() == "Passed or Failed Value" and p.get("Type") == "SCENE LEVEL":  # scene level
    #             scenes = self.get_relevant_scenes(params)
    #             for scene in scenes:
    #                 scene_uid = self.scenes_info[self.scenes_info['scene_fk']
    #                                              == scene]['scene_uid'].values[0]
    #                 for k in self.kpi_facts_hidden:
    #                     if k.get("KPI ID") in p.get("Children List") and k.get("scene_fk") == scene:
    #                         passed_failed = str(p.get("Values")).replace(" ", "").replace(
    #                             ",", "\n").replace("\n\n", "\n").split("\n")
    #                         if k.get("score") == 100:
    #                             result = passed_failed[0]
    #                         elif len(passed_failed) > 1:
    #                             result = passed_failed[1]
    #                         else:
    #                             result = ""
    #                         kpi_facts.append({"id": atomic_kpi_id, "name": atomic_kpi_name,
    #                                           "display_text": atomic_kpi_name + "@" + scene_uid,
    #                                           "atomic_kpi_fk": atomic_kpi_fk, "result": result,
    #                                           "scene_uid": scene_uid, "scene_id": scene,
    #                                           "format": p.get("Result Format")})
    #
    #     kpi_set_fk = kpi_df['kpi_set_fk'].values[0]
    #     kpi_fk = kpi_df['kpi_fk'].values[0]
    #     kpi_name = kpi_set_name
    #     for kf in kpi_facts:
    #         if not kf.get("result") is None:
    #             if kf.get("format") == "Integer":
    #                 result_format = 'NUM'
    #                 result_formatted = str(int(kf.get("result")))
    #             elif kf.get("format") == "Decimal.2":
    #                 result_format = 'NUM'
    #                 result_formatted = format(float(kf.get("result")), ".2f")
    #             else:
    #                 result_format = 'STR'
    #                 result_formatted = str(kf.get("result"))
    #
    #             atomic_kpi_name = kf.get("name")
    #             atomic_kpi_fk = kf.get("atomic_kpi_fk")
    #             if not atomic_kpi_fk and self.kpi_set_type not in SKIP_OLD_KPIS_FROM_WRITING:
    #                 Log.error(
    #                     'Atomic KPI Name <{}> is not found for KPI FK <{}> of KPI Set <{}> in static.atomic_kpi table'
    #                     ''.format(atomic_kpi_name, kpi_fk, self.kpi_set_name))
    #             attributes_for_table3 = pd.DataFrame([(kf.get("display_text"),
    #                                                    self.session_uid,
    #                                                    kpi_set_name,
    #                                                    self.store_id,
    #                                                    self.visit_date.isoformat(),
    #                                                    dt.datetime.utcnow().isoformat(),
    #                                                    None,
    #                                                    kpi_fk,
    #                                                    atomic_kpi_fk,
    #                                                    None,
    #                                                    result_formatted,
    #                                                    atomic_kpi_name)],
    #                                                  columns=['display_text',
    #                                                           'session_uid',
    #                                                           'kps_name',
    #                                                           'store_fk',
    #                                                           'visit_date',
    #                                                           'calculation_time',
    #                                                           'score',
    #                                                           'kpi_fk',
    #                                                           'atomic_kpi_fk',
    #                                                           'threshold',
    #                                                           'result',
    #                                                           'name'])
    #             self.write_to_kpi_results_old(attributes_for_table3, 'level3')
    #             self.update_kpi_scores_and_results(
    #                 {'KPI ID': kf.get('id'),
    #                  'KPI name Eng': kf.get('name'),
    #                  'KPI name Rus': kf.get('name'),
    #                  'Parent': 0,
    #                  'Sorting': 0},
    #                 {'scene_uid': kf.get('scene_uid'),
    #                  'scene_id': kf.get('scene_id'),
    #                  'result': result_formatted,
    #                  'format': result_format,
    #                  'level': 1})
    #
    #             # table3 = table3.append(attributes_for_table3)  # for debugging
    #
    #     if not kpi_fk and self.kpi_set_type not in SKIP_OLD_KPIS_FROM_WRITING:
    #         Log.error('KPI Name <{}> is not found for KPI Set <{}> in static.kpi table'
    #                   ''.format(kpi_name, self.kpi_set_name))
    #     attributes_for_table2 = pd.DataFrame([(self.session_uid,
    #                                            self.store_id,
    #                                            self.visit_date.isoformat(),
    #                                            kpi_fk,
    #                                            kpi_name,
    #                                            None)],
    #                                          columns=['session_uid',
    #                                                   'store_fk',
    #                                                   'visit_date',
    #                                                   'kpi_fk',
    #                                                   'kpk_name',
    #                                                   'score'])
    #     self.write_to_kpi_results_old(attributes_for_table2, 'level2')
    #
    #     if not kpi_set_fk and self.kpi_set_type not in SKIP_OLD_KPIS_FROM_WRITING:
    #         Log.error('KPI Set <{}> is not found in static.kpi_set table'
    #                   ''.format(self.kpi_set_name))
    #     attributes_for_table1 = pd.DataFrame([(kpi_set_name,
    #                                            self.session_uid,
    #                                            self.store_id,
    #                                            self.visit_date.isoformat(),
    #                                            None,
    #                                            kpi_set_fk)],
    #                                          columns=['kps_name',
    #                                                   'session_uid',
    #                                                   'store_fk',
    #                                                   'visit_date',
    #                                                   'score_1',
    #                                                   'kpi_set_fk'])
    #     self.write_to_kpi_results_old(attributes_for_table1, 'level1')
    #
    #     self.update_kpi_scores_and_results(
    #         {'KPI ID': 0,
    #          'KPI name Eng': kpi_set_name,
    #          'KPI name Rus': kpi_set_name,
    #          'Parent': 'root',
    #          'Sorting': 0},
    #         {'level': 0})
    #
    #     return

    @staticmethod
    def merge_insert_queries(insert_queries):
        query_groups = {}
        for query in insert_queries:
            static_data, inserted_data = query.split('VALUES ')
            if static_data not in query_groups:
                query_groups[static_data] = []
            query_groups[static_data].append(inserted_data)
        merged_queries = []
        for group in query_groups:
            merged_queries.append('{0} VALUES {1}'.format(group, ',\n'.join(query_groups[group])))
        return merged_queries

    def create_attributes_for_level2_df(self, param, score, kpi_fk, level=2):
        """
        This function creates a data frame with all attributes needed for saving in level 2 tables

        """
        # score = round(score)

        kpi_name = param.get('KPI name Eng')

        if not kpi_fk and self.kpi_set_type not in SKIP_OLD_KPIS_FROM_WRITING:
            Log.error('KPI Name <{}> is not found for KPI Set <{}> in static.kpi table'
                      ''.format(kpi_name, self.kpi_set_name))

        attributes_for_table2 = pd.DataFrame([(self.session_uid,
                                               self.store_id,
                                               self.visit_date.isoformat(),
                                               kpi_fk,
                                               param.get('KPI name Eng').replace("'", "\\'"),
                                               round(score),
                                               score)],
                                             columns=['session_uid',
                                                      'store_fk',
                                                      'visit_date',
                                                      'kpi_fk',
                                                      'kpk_name',
                                                      'score',
                                                      'score_2'])

        target = 100 * (param.get('KPI Weight') if param.get('KPI Weight') is not None else 1)
        if self.kpi_scores_and_results[self.kpi_set_type].get(str(param.get('KPI ID'))):
            if self.kpi_scores_and_results[self.kpi_set_type][str(param.get('KPI ID'))].get('target'):
                target = self.kpi_scores_and_results[self.kpi_set_type][str(
                    param.get('KPI ID'))]['target']

        self.update_kpi_scores_and_results(param, {'level': level,
                                                   'target': target,
                                                   'weight': param.get('KPI Weight'),
                                                   # 'result': round(score),
                                                   'score': score,
                                                   'weighted_score':
                                                       round(score * (param.get('KPI Weight')
                                                                      if param.get('KPI Weight') is not None
                                                                      else 1), 3)})

        return attributes_for_table2

    def create_attributes_for_level3_df(self, param, score, kpi_fk, atomic_kpi_fk=None, level=3, additional_level=None):
        """
        This function creates a data frame with all attributes needed for saving in level 3 tables

        """
        if isinstance(score, tuple):
            score, result, threshold = score
        else:
            result = threshold = None
        # score = round(score)
        self.update_kpi_scores_and_results(param, {})
        result = self.kpi_scores_and_results[self.kpi_set_type][str(param.get("KPI ID"))].get('result')\
            if result is None else result
        result = result if result else 0
        threshold = self.kpi_scores_and_results[self.kpi_set_type][str(param.get("KPI ID"))].get('target')\
            if threshold is None else threshold
        threshold = threshold if threshold else 0

        atomic_kpi_name = param.get('KPI name Eng')
        atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(atomic_kpi_name, kpi_fk)\
            if atomic_kpi_fk is None else atomic_kpi_fk

        if not atomic_kpi_fk and self.kpi_set_type not in SKIP_OLD_KPIS_FROM_WRITING:
            Log.error('Atomic KPI Name <{}> is not found for KPI FK <{}> of KPI Set <{}> in static.atomic_kpi table'
                      ''.format(atomic_kpi_name, kpi_fk, self.kpi_set_name))

        if param.get('KPI name Rus'):
            attributes_for_table3 = pd.DataFrame([(param.get('KPI name Rus').encode('utf-8').replace("'", "\\'"),
                                                   self.session_uid,
                                                   self.kpi_set_name,
                                                   self.store_id,
                                                   self.visit_date.isoformat(),
                                                   dt.datetime.utcnow().isoformat(),
                                                   round(score),
                                                   score,
                                                   kpi_fk,
                                                   atomic_kpi_fk,
                                                   threshold,
                                                   round(result, 5) if type(result) is float else result,
                                                   param.get('KPI name Eng').replace("'", "\\'"))],
                                                 columns=['display_text',
                                                          'session_uid',
                                                          'kps_name',
                                                          'store_fk',
                                                          'visit_date',
                                                          'calculation_time',
                                                          'score',
                                                          'score_2',
                                                          'kpi_fk',
                                                          'atomic_kpi_fk',
                                                          'threshold',
                                                          'result',
                                                          'name'])
        else:
            attributes_for_table3 = pd.DataFrame([(param.get('KPI name Eng').replace("'", "\\'"),
                                                   self.session_uid,
                                                   self.kpi_set_name,
                                                   self.store_id,
                                                   self.visit_date.isoformat(),
                                                   dt.datetime.utcnow().isoformat(),
                                                   round(score),
                                                   score,
                                                   kpi_fk,
                                                   atomic_kpi_fk,
                                                   threshold,
                                                   round(result, 5) if type(result) is float else result,
                                                   param.get('KPI name Eng').replace("'", "\\'"))],
                                                 columns=['display_text',
                                                          'session_uid',
                                                          'kps_name',
                                                          'store_fk',
                                                          'visit_date',
                                                          'calculation_time',
                                                          'score',
                                                          'score_2',
                                                          'kpi_fk',
                                                          'atomic_kpi_fk',
                                                          'threshold',
                                                          'result',
                                                          'name'])

        self.update_kpi_scores_and_results(param, {'level': level if additional_level is None else additional_level,
                                                   'target': threshold,
                                                   'weight': param.get('KPI Weight'),
                                                   'result': result,
                                                   'score': score,
                                                   'weighted_score':
                                                       round(score * (param.get('KPI Weight')
                                                                      if param.get('KPI Weight') is not None
                                                                      else 1), 3),
                                                   'additional_level': additional_level})

        return attributes_for_table3

    @kpi_runtime()
    def check_number_of_doors_given_sos(self, params):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula').strip() != "number of doors given sos" or not p.get("Children"):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = self.children_to_int_list(p.get("Children"))
            first_atomic_score = second_atomic_res = 0
            for c in params.values()[0]:
                if c.get("KPI ID") in children and c.get("Formula").strip() == "atomic sos":
                    first_atomic_res = self.calculate_facings_sos(c)
                    if first_atomic_res is None:
                        first_atomic_res = 0
                    first_atomic_score = self.calculate_score(first_atomic_res, c)
                    # write to DB
                    attributes_for_level3 = self.create_attributes_for_level3_df(
                        c, first_atomic_score, kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')
            for c in params.values()[0]:
                if c.get("KPI ID") in children and c.get("Formula").strip() == "atomic number of doors":
                    second_atomic_res = self.calculate_number_of_doors(c)
                    second_atomic_score = self.calculate_score(second_atomic_res, c)
                    # write to DB
                    attributes_for_level3 = self.create_attributes_for_level3_df(
                        c, second_atomic_score, kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')

            if first_atomic_score > 0:
                kpi_total_res = second_atomic_res
            else:
                kpi_total_res = 0
            score = self.calculate_score(kpi_total_res, p)
            score = round(score, 3)

            set_total_res += round(score * p.get('KPI Weight'), 3)
            # saving to DB
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_kpi_results_old(attributes_for_level2, 'level2')

        return set_total_res

    @kpi_runtime()
    def check_number_of_doors_given_number_of_sku(self, params):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula').strip() != "number of doors given number of SKUs" or not p.get("Children"):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = self.children_to_int_list(p.get("Children"))
            first_atomic_scores = []
            for c in params.values()[0]:
                if c.get("KPI ID") in children and c.get("Formula").strip() == "atomic number of SKUs":
                    first_atomic_res = self.calculate_availability(c)
                    first_atomic_score = self.calculate_score(first_atomic_res, c)
                    first_atomic_scores.append(first_atomic_score)
                    # write to DB
                    attributes_for_level3 = self.create_attributes_for_level3_df(
                        c, first_atomic_score, kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')
            second_atomic_res = 0
            for c in params.values()[0]:
                if c.get("KPI ID") in children and c.get("Formula").strip() == "atomic number of doors":
                    second_atomic_res = self.calculate_number_of_doors(c)
                    second_atomic_score = self.calculate_score(second_atomic_res, c)
                    # write to DB
                    attributes_for_level3 = self.create_attributes_for_level3_df(
                        c, second_atomic_score, kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')

            if 0 not in first_atomic_scores:  # if all assortment atomics have score > 0
                kpi_total_res = second_atomic_res
            else:
                kpi_total_res = 0
            score = self.calculate_score(kpi_total_res, p)
            score = round(score, 3)
            set_total_res += round(score * p.get('KPI Weight'), 3)
            # saving to DB
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_kpi_results_old(attributes_for_level2, 'level2')

        return set_total_res

    def get_pos_kpi_set_name(self, update_kpi_set=False):
        pos = self.get_pos_by_session()
        if pos not in self.ALLOWED_POS_SETS or update_kpi_set:
            if self.visit_type == self.GROWTH_FORMULA:
                if str(self.visit_date) < self.MIN_CALC_DATE:
                    pos = self.get_pos_by_store_attribute('additional_attribute_22')
                else:
                    pos = self.get_pos_by_store_attribute('additional_attribute_21')
            if pos not in self.ALLOWED_POS_SETS:
                if str(self.visit_date) < self.MIN_CALC_DATE:
                    pos = self.get_pos_by_store_attribute('additional_attribute_12')
                else:
                    pos = self.get_pos_by_store_attribute('additional_attribute_11')
        return pos

    def get_pos_by_session(self):
        query = """
                select s.name 
                from report.kps_results r
                join static.kpi_set s ON s.pk=r.kpi_set_fk
                WHERE session_uid='{}'
                AND s.name IN {};
                """.format(self.session_uid, tuple(ALLOWED_POS_SETS))
        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        res = cur.fetchall()
        pos = res[0][0] if res else None
        return pos

    def get_pos_by_store_attribute(self, pos_store_attribute):
        query = """
                select ss.{} 
                from static.stores ss 
                join probedata.session ps on ps.store_fk=ss.pk 
                where ps.session_uid = '{}';
                """.format(pos_store_attribute, self.session_uid)
        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        res = cur.fetchall()
        pos = res[0][0] if res else None
        return pos

    @kpi_runtime()
    def calculate_gaps_old(self, params):
        for param in params:
            kpi_name = param.get('KPI Name Eng')
            kpi_id = self.kpi_name_to_id[POS].get(kpi_name)
            if kpi_id is None:
                Log.warning('Gap KPI <{}> is not found in PoS KPI set <{}>'
                            ''.format(kpi_name, self.pos_kpi_set_name))
            else:
                kpi_name_local = self.kpi_scores_and_results[POS][kpi_id].get('rus_name')
                category = param.get('Gap Category Eng')
                category_local = param.get('Gap Category Rus')
                target = self.kpi_scores_and_results[POS][kpi_id].get('weight') * 100 \
                    if self.kpi_scores_and_results[POS][kpi_id].get('weight') else 0
                score = self.kpi_scores_and_results[POS][kpi_id].get('weighted_score') \
                    if self.kpi_scores_and_results[POS][kpi_id].get('weighted_score') else 0
                rank = param.get('Top Gaps Rank')
                if score is not None:
                    gap = target - score
                    gap = 0 if gap < 0 else gap
                    self.gaps_dict[category, kpi_name] = {'gap': round(gap, 2),
                                                          'rank': rank,
                                                          'kpi': kpi_name_local,
                                                          'group_local_name': category_local}
        self.write_gaps_old()
        return

    def write_gaps_old(self):
        gaps_dict_sorted_df = pd.DataFrame.from_dict(self.gaps_dict, orient='index')
        category_groups_kpis = gaps_dict_sorted_df.index.tolist()
        for index in category_groups_kpis:
            group = index[0]
            group_df = gaps_dict_sorted_df.loc[group]
            sorted_group_df = group_df.sort_values(by=['rank'], ascending=[True])
            counter = 0
            for i in range(len(sorted_group_df)):
                if sorted_group_df['gap'].iloc[i] < 1 or counter > self.gap_groups_limit[group]:
                    continue
                counter += 1
                attributes = pd.DataFrame([(self.session_fk,
                                            sorted_group_df['group_local_name'].iloc[i],
                                            sorted_group_df['kpi'].iloc[i],
                                            counter,
                                            sorted_group_df['gap'].iloc[i])],
                                          columns=['session_fk',
                                                   'gap_category',
                                                   'name',
                                                   'priority',
                                                   'impact'])
                query = insert(attributes.to_dict(), CUSTOM_GAPS_TABLE)
                self.gaps_queries.append(query)

    @kpi_runtime()
    def calculate_gaps_new(self, params, kpi_set_name):
        gaps = pd.DataFrame(params).sort_values(by=['Top Gaps Rank'], ascending=True)
        gaps['Gap'] = 0
        gap_groups_limit = self.gap_groups_limit
        for i, kpi in gaps.iterrows():
            if gap_groups_limit.get(kpi.get('Gap Category Eng')) > 0:
                kpi_id = self.kpi_name_to_id[POS].get(kpi.get('KPI Name Eng'))
                if kpi_id is None:
                    Log.warning('Gap KPI <{}> is not found in PoS KPI set <{}>'
                                ''.format(kpi.get('KPI Name Eng'), self.pos_kpi_set_name))
                else:
                    score = self.kpi_scores_and_results[POS][kpi_id].get('weighted_score') \
                        if self.kpi_scores_and_results[POS][kpi_id].get('weighted_score') else 0
                    target = self.kpi_scores_and_results[POS][kpi_id].get('weight') * 100 \
                        if self.kpi_scores_and_results[POS][kpi_id].get('weight') else 0
                    gap = (target if target else 0) - (score if score else 0)
                    if gap > 0:
                        gaps.loc[i, 'Gap'] = gap
                        gaps.loc[i, 'KPI Name Rus'] = self.kpi_scores_and_results[POS][kpi_id].get(
                            'rus_name')
                        gap_groups_limit[kpi.get('Gap Category Eng')] -= 1
        gaps = gaps[gaps['Gap'] > 0]

        counter = 0
        total_gap = 0
        for category in gaps['Gap Category Eng'].unique().tolist():
            counter += 1
            category_counter = counter
            category_local = category
            category_gap = 0
            for group in gaps[gaps['Gap Category Eng'] == category]['Gap Group Eng'].unique().tolist():
                counter += 1
                group_counter = counter
                group_local = group
                group_gap = 0
                for subgroup in gaps[gaps['Gap Group Eng'] == group]['Gap Subgroup Eng'].unique().tolist():
                    counter += 1
                    subgroup_counter = counter
                    subgroup_local = subgroup
                    subgroup_gap = 0
                    for i, kpi in gaps[gaps['Gap Subgroup Eng'] == subgroup]\
                            .sort_values(by=['Top Gaps Rank'], ascending=True)\
                            .reindex()\
                            .iterrows():

                        kpi_name = kpi.get('KPI Name Eng')
                        kpi_id = self.kpi_name_to_id[POS].get(kpi_name)
                        kpi_name_local = self.kpi_scores_and_results[POS][kpi_id].get('rus_name')
                        category_local = kpi['Gap Category Rus']
                        group_local = kpi['Gap Group Rus']
                        subgroup_local = kpi['Gap Subgroup Rus']
                        sort_order = kpi['Sorting']
                        score = self.kpi_scores_and_results[POS][kpi_id].get('weighted_score')
                        target = self.kpi_scores_and_results[POS][kpi_id].get('weight') * 100

                        if score is not None:
                            gap = target - score
                            gap = 0 if gap < 0 else gap
                            if gap > 0:
                                counter += 1
                                result = "+ " + format(gap, ".1f")
                                result = "0" if result == "+ 0.0" else result
                                result = "+ 100" if result == "+ 100.0" else result
                                self.update_kpi_scores_and_results(
                                    {'KPI ID': counter,
                                     'KPI name Eng': kpi_name,
                                     'KPI name Rus': kpi_name_local,
                                     'Parent': subgroup_counter,
                                     'Sorting': sort_order},
                                    {'target': 0,
                                     'result': result,
                                     'format': 'STR',
                                     'level': 4})

                                subgroup_gap += gap

                                # appending relevant atomics to the structure
                                atomics = self.kpi_children[POS].get(kpi_id)
                                if atomics:
                                    for atomic_id in atomics:
                                        self.update_kpi_scores_and_results(
                                            {'KPI ID': str(counter) + '_' + atomic_id,
                                             'KPI name Eng': self.kpi_scores_and_results[POS][atomic_id].get('eng_name'),
                                             'KPI name Rus': self.kpi_scores_and_results[POS][atomic_id].get('rus_name'),
                                             'Parent': counter,
                                             'Sorting': self.kpi_scores_and_results[POS][atomic_id].get('sort_order')},
                                            {'target': self.kpi_scores_and_results[POS][atomic_id].get('target'),
                                             'result': self.kpi_scores_and_results[POS][atomic_id].get('result'),
                                             'score': self.kpi_scores_and_results[POS][atomic_id].get('score'),
                                             'format': self.kpi_scores_and_results[POS][atomic_id].get('format'),
                                             'level': 5})

                                        # appending relevant sub-atomics to the structure
                                        sub_atomics = self.kpi_children[POS].get(atomic_id)
                                        if sub_atomics:
                                            for sub_atomic_id in sub_atomics:
                                                self.update_kpi_scores_and_results(
                                                    {'KPI ID': str(counter) + '_' + sub_atomic_id,
                                                     'KPI name Eng': self.kpi_scores_and_results[POS][sub_atomic_id].get('eng_name'),
                                                     'KPI name Rus': self.kpi_scores_and_results[POS][sub_atomic_id].get('rus_name'),
                                                     'Parent': str(counter) + '_' + atomic_id,
                                                     'Sorting': self.kpi_scores_and_results[POS][sub_atomic_id].get('sort_order')},
                                                    {'target': self.kpi_scores_and_results[POS][sub_atomic_id].get('target'),
                                                     'result': self.kpi_scores_and_results[POS][sub_atomic_id].get('result'),
                                                     'score': self.kpi_scores_and_results[POS][sub_atomic_id].get('score'),
                                                     'format': self.kpi_scores_and_results[POS][sub_atomic_id].get('format'),
                                                     'level': 6})

                    if subgroup_gap > 0:
                        result = "+ " + format(subgroup_gap, ".1f")
                        result = "0" if result == "+ 0.0" else result
                        result = "+ 100" if result == "+ 100.0" else result
                        self.update_kpi_scores_and_results(
                            {'KPI ID': subgroup_counter,
                             'KPI name Eng': subgroup,
                             'KPI name Rus': subgroup_local,
                             'Parent': group_counter,
                             'Sorting': subgroup_counter},
                            {'target': 0,
                             'result': result,
                             'format': 'STR',
                             'level': 3})
                        group_gap += subgroup_gap

                if group_gap > 0:
                    result = "+ " + format(group_gap, ".1f")
                    result = "0" if result == "+ 0.0" else result
                    result = "+ 100" if result == "+ 100.0" else result
                    self.update_kpi_scores_and_results(
                        {'KPI ID': group_counter,
                         'KPI name Eng': group,
                         'KPI name Rus': group_local,
                         'Parent': category_counter,
                         'Sorting': group_counter},
                        {'target': 0,
                         'result': result,
                         'format': 'STR',
                         'level': 2})
                    category_gap += group_gap

            if category_gap > 0:
                result = "+ " + format(category_gap, ".1f")
                result = "0" if result == "+ 0.0" else result
                result = "+ 100" if result == "+ 100.0" else result
                self.update_kpi_scores_and_results(
                    {'KPI ID': category_counter,
                     'KPI name Eng': category,
                     'KPI name Rus': category_local,
                     'Parent': 0,
                     'Sorting': category_counter},
                    {'target': 0,
                     'result': result,
                     'format': 'STR',
                     'level': 1})
                total_gap += category_gap

        result = "+ " + format(total_gap, ".1f")
        result = "0" if result == "+ 0.0" else result
        result = "+ 100" if result == "+ 100.0" else result
        self.update_kpi_scores_and_results(
            {'KPI ID': 0,
             'KPI name Eng': kpi_set_name,
             'KPI name Rus': kpi_set_name,
             'Parent': 'root',
             'Sorting': 0},
            {'target': 0,
             'result': result,
             'format': 'STR',
             'level': 0})

        return

    @kpi_runtime()
    def calculate_benchmark(self, params, kpi_set_name):
        kpi_set_fk = self.kpi_fetcher.get_kpi_set_fk()
        total_score = 0
        for param in params:
            if param.get('Value Type') != 'POS KPI':
                continue

            kpi_name = param.get('KPI Name')
            kpi_id = param.get('KPI Code')
            kpi_fk = self.kpi_fetcher.get_kpi_fk(kpi_name)
            # atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(kpi_name, kpi_fk)
            score = 0
            for pos_kpi_name in str(param.get("Values")).replace(", ", ",").replace(",", "\n").replace("\n\n", "\n").split("\n"):
                pos_kpi_id = self.kpi_name_to_id[POS].get(pos_kpi_name)
                if pos_kpi_id is None:
                    Log.warning(
                        'Benchmark POS KPI <{}> is not found in POS KPI set <{}>'
                        ''.format(pos_kpi_name, self.pos_kpi_set_name))
                else:

                    score += self.kpi_scores_and_results[POS][pos_kpi_id].get('weighted_score') \
                        if self.kpi_scores_and_results[POS][pos_kpi_id].get('weighted_score') else 0

            score = round(score*param.get('K'), 3)
            total_score += score

            # if not atomic_kpi_fk and self.kpi_set_type not in SKIP_OLD_KPIS_FROM_WRITING:
            #     Log.error(
            #         'Atomic KPI Name <{}> is not found for KPI FK <{}> of KPI Set <{}> in static.atomic_kpi table'
            #         ''.format(kpi_name, kpi_fk, self.kpi_set_name))
            # attributes_for_table3 = pd.DataFrame([(kpi_name,
            #                                        self.session_uid,
            #                                        kpi_set_name,
            #                                        self.store_id,
            #                                        self.visit_date.isoformat(),
            #                                        dt.datetime.utcnow().isoformat(),
            #                                        score,
            #                                        kpi_fk,
            #                                        atomic_kpi_fk,
            #                                        None,
            #                                        score,
            #                                        kpi_name)],
            #                                      columns=['display_text',
            #                                               'session_uid',
            #                                               'kps_name',
            #                                               'store_fk',
            #                                               'visit_date',
            #                                               'calculation_time',
            #                                               'score',
            #                                               'kpi_fk',
            #                                               'atomic_kpi_fk',
            #                                               'threshold',
            #                                               'result',
            #                                               'name'])
            # self.write_to_kpi_results_old(attributes_for_table3, 'level3')

            if not kpi_fk and self.kpi_set_type not in SKIP_OLD_KPIS_FROM_WRITING:
                Log.error('KPI Name <{}> is not found for KPI Set <{}> in static.kpi table'
                          ''.format(kpi_name, self.kpi_set_name))
            attributes_for_table2 = pd.DataFrame([(self.session_uid,
                                                   self.store_id,
                                                   self.visit_date.isoformat(),
                                                   kpi_fk,
                                                   kpi_name,
                                                   score)],
                                                 columns=['session_uid',
                                                          'store_fk',
                                                          'visit_date',
                                                          'kpi_fk',
                                                          'kpk_name',
                                                          'score_2'])
            self.write_to_kpi_results_old(attributes_for_table2, 'level2')

            self.update_kpi_scores_and_results(
                {'KPI ID': kpi_id,
                 'KPI name Eng': kpi_name,
                 'KPI name Rus': kpi_name,
                 'Parent': 0},
                {'result': score,
                 'score': score,
                 'level': 1})

        if not kpi_set_fk and self.kpi_set_type not in SKIP_OLD_KPIS_FROM_WRITING:
            Log.error('KPI Set <{}> is not found static.kpi_set table'
                      ''.format(self.kpi_set_name))
        attributes_for_table1 = pd.DataFrame([(kpi_set_name,
                                               self.session_uid,
                                               self.store_id,
                                               self.visit_date.isoformat(),
                                               total_score,
                                               kpi_set_fk)],
                                             columns=['kps_name',
                                                      'session_uid',
                                                      'store_fk',
                                                      'visit_date',
                                                      'score_1',
                                                      'kpi_set_fk'])
        self.write_to_kpi_results_old(attributes_for_table1, 'level1')

        self.update_kpi_scores_and_results(
            {'KPI ID': 0,
             'KPI name Eng': kpi_set_name,
             'KPI name Rus': kpi_set_name,
             'Parent': 'root'},
            {'result': total_score,
             'score': total_score,
             'level': 0})

        return

    @kpi_runtime()
    def calculate_equipment_execution(self, params, kpi_set_name, kpi_conversion_file, target_data):

        if target_data is not None:

            for field in (self.store_number, 'Start Date', 'End Date'):
                target_data.pop(field, None)

            kpi_conversion = self.get_kpi_conversion(kpi_conversion_file)

            total_score = 0
            total_weight = 0
            count_of_kpis = 0

            for param in params:
                if param.get('level') == 2 and param.get('KPI Set Type') == 'Equipment':

                    kpi_name = param.get('Channel') + '@' + param.get('KPI name Eng').strip()
                    kpi_fk = self.kpi_fetcher.kpi_static_data[self.kpi_fetcher.kpi_static_data['kpi_name']
                                                              == kpi_name]['kpi_fk'].values[0]
                    kpi_name = param.get('KPI name Eng')
                    kpi_weight = param.get('KPI Weight')
                    children = self.children_to_str_list(param.get('Children'))

                    sum_of_scores = 0
                    sum_of_weights = 0
                    count_of_targets = 0

                    for param_child in params:
                        if str(param_child.get('KPI ID')) in children and param_child.get('KPI Set Type') == 'Equipment':
                            atomic_kpi_name = param_child.get(
                                'Channel') + '@' + param_child.get('KPI name Eng')
                            atomic_kpi_fk = self.kpi_fetcher.kpi_static_data[self.kpi_fetcher.kpi_static_data[
                                'atomic_kpi_name'] == atomic_kpi_name]['atomic_kpi_fk'].values[0]
                            atomic_kpi_name = param_child.get('KPI name Eng')
                            target = target_data.get(kpi_conversion.get(atomic_kpi_name))
                            target, weight = target if target else (None, None)
                            weight = 1
                            if target:
                                if (type(target) is str or type(target) is unicode) and ',' in target:
                                    target = target.replace(',', '.')
                                if (type(target) is str or type(target) is unicode) and '%' in target:
                                    target = target.replace('%', '')
                                    target = float(target) / 100
                                target = round(float(target), 3)
                                if int(target) == target:
                                    target = int(target)
                                try:
                                    result = round(float(self.kpi_scores_and_results[TARGET][self.kpi_name_to_id[TARGET]
                                                         .get(atomic_kpi_name)].get('result')), 3)
                                    result = int(result) if result == int(result) else result
                                except:
                                    result = 0
                                score_func = param_child.get('score_func')
                                if score_func == PROPORTIONAL:
                                    score = round(result / float(target) * 100, 3) \
                                        if target \
                                        else 100
                                    score = 100 if score > 100 else score
                                else:
                                    score = 100 if result >= target else 0

                                attributes_for_level3 = self.create_attributes_for_level3_df(
                                    param_child, (score, result, target), kpi_fk, atomic_kpi_fk)
                                self.write_to_kpi_results_old(attributes_for_level3, 'level3')

                                sum_of_scores += score * weight
                                sum_of_weights += weight
                                count_of_targets += 1

                    if count_of_targets:
                        score = int(round(sum_of_scores / float(sum_of_weights)))
                        attributes_for_level2 = self.create_attributes_for_level2_df(
                            param, score, kpi_fk)
                        self.write_to_kpi_results_old(attributes_for_level2, 'level2')

                        total_score += score * kpi_weight
                        total_weight += kpi_weight
                        count_of_kpis += 1

            if count_of_kpis:
                total_weight = total_weight if total_weight > 0 else 1
                score = round(total_score / float(total_weight), 3)
                attributes_for_table1 = pd.DataFrame([(kpi_set_name,
                                                       self.session_uid,
                                                       self.store_id,
                                                       self.visit_date.isoformat(),
                                                       score,
                                                       None)],
                                                     columns=['kps_name',
                                                              'session_uid',
                                                              'store_fk',
                                                              'visit_date',
                                                              'score_1',
                                                              'kpi_set_fk'])
                self.write_to_kpi_results_old(attributes_for_table1, 'level1')

                self.update_kpi_scores_and_results(
                    {'KPI ID': 0,
                     'KPI name Eng': kpi_set_name,
                     'KPI name Rus': kpi_set_name,
                     'Parent': 'root'},
                    {'target': 100,
                     'weight': 1,
                     'result': score,
                     'score': score,
                     'weighted_score': score,
                     'level': 0})

                for kpi_id in self.kpi_scores_and_results[EQUIPMENT].keys():
                    if self.kpi_scores_and_results[EQUIPMENT][kpi_id]['level'] == 2:
                        self.kpi_scores_and_results[EQUIPMENT][kpi_id]['weight'] /= float(
                            total_weight)
                        self.kpi_scores_and_results[EQUIPMENT][kpi_id]['target'] = \
                            self.kpi_scores_and_results[EQUIPMENT][kpi_id]['weight'] * 100
                        self.kpi_scores_and_results[EQUIPMENT][kpi_id]['weighted_score'] = \
                            round(self.kpi_scores_and_results[EQUIPMENT][kpi_id]['score'] *
                                  self.kpi_scores_and_results[EQUIPMENT][kpi_id]['weight'], 3)

                self.equipment_execution_score = score

            else:

                self.equipment_execution_score = None

        return

    def get_equipment_target_data(self):
        target_data_raw = self.get_equipment_targets(str(self.store_id))
        if target_data_raw:
            Log.debug('Relevant Contract Execution target file for Store ID {} / Number {} is found'.format(
                self.store_id, self.store_number))
        target_data = None
        for data in target_data_raw:
            start_date = dt.datetime.strptime(data['Start Date'], '%Y-%m-%d').date()
            end_date = dt.datetime.now().date() if not data['End Date'] else \
                dt.datetime.strptime(data['End Date'], '%Y-%m-%d').date()
            if start_date <= self.visit_date <= end_date:
                target_data = data
        return target_data

    @kpi_runtime()
    def calculate_contract_execution(self, params, kpi_set_name):
        if self.top_sku_score is not None or self.equipment_execution_score is not None:

            total_score = 0
            total_weight = 0
            count_of_kpis = 0

            for param in params:
                if param.get('KPI Set Type') == 'Contract' and param.get('Formula'):
                    if param.get('Formula').strip() == 'OSA score':
                        # kpi_id = 'OSA'
                        # kpi_name = 'CONTRACT_OSA'

                        score = self.top_sku_score

                        if score is not None:
                            self.kpi_scores_and_results[TOPSKU][TOPSKU]['weight'] = \
                                param.get('KPI Weight') * 100
                            self.kpi_scores_and_results[TOPSKU][TOPSKU]['result'] = \
                                round(score * param.get('KPI Weight'), 3)

                    elif param.get('Formula').strip() == 'Equipment Execution score':
                        # kpi_id = 'EQUIPMENT'
                        # kpi_name = 'CONTRACT_EQUIPMENT'
                        score = self.equipment_execution_score

                        if score is not None:
                            self.kpi_scores_and_results[EQUIPMENT]['0']['weight'] = \
                                param.get('KPI Weight') * 100
                            self.kpi_scores_and_results[EQUIPMENT]['0']['result'] = \
                                round(score * param.get('KPI Weight'), 3)

                    else:
                        # kpi_id = None
                        # kpi_name = None
                        score = None

                    if score is not None:
                        target = 100
                        result = score
                        score_to_db = round(score, 3)

                        kpi_name_channel = param.get('Channel') + '@' + param.get('KPI name Eng')
                        kpi_fk = self.kpi_fetcher.kpi_static_data[
                            self.kpi_fetcher.kpi_static_data['kpi_name'] == kpi_name_channel]['kpi_fk'].values[0]
                        atomic_kpi_fk = self.kpi_fetcher.kpi_static_data[
                            self.kpi_fetcher.kpi_static_data['atomic_kpi_name'] == kpi_name_channel]['atomic_kpi_fk'].values[0]

                        kpi_weight = param.get('KPI Weight')

                        attributes_for_level3 = self.create_attributes_for_level3_df(
                            {'KPI ID': None, 'KPI name Eng': param.get('KPI name Eng')},
                            (score_to_db, result, target), kpi_fk, atomic_kpi_fk)
                        self.write_to_kpi_results_old(attributes_for_level3, 'level3')

                        attributes_for_level2 = self.create_attributes_for_level2_df(
                            {'KPI ID': None, 'KPI name Eng': param.get('KPI name Eng')},
                            score_to_db, kpi_fk)
                        self.write_to_kpi_results_old(attributes_for_level2, 'level2')

                        # self.update_kpi_scores_and_results(
                        #     {'KPI ID': kpi_id,
                        #      'KPI name Eng': kpi_name},
                        #     {'kpi_id': kpi_id,
                        #      'kpi_name': kpi_name,
                        #      'eng_name': param.get('KPI name Eng'),
                        #      'rus_name': param.get('KPI name Eng'),
                        #      'target': kpi_weight * 100,
                        #      'weight': kpi_weight,
                        #      'result': result,
                        #      'score': score,
                        #      'weighted_score': round(score * kpi_weight * 100, 2),
                        #      'level': 1,
                        #      'parent': '0'})

                        total_score += score * kpi_weight
                        total_weight += kpi_weight
                        count_of_kpis += 1

            if count_of_kpis:

                score = round(total_score / float(total_weight), 3)
                attributes_for_table1 = pd.DataFrame([(kpi_set_name,
                                                       self.session_uid,
                                                       self.store_id,
                                                       self.visit_date.isoformat(),
                                                       score,
                                                       None)],
                                                     columns=['kps_name',
                                                              'session_uid',
                                                              'store_fk',
                                                              'visit_date',
                                                              'score_1',
                                                              'kpi_set_fk'])
                self.write_to_kpi_results_old(attributes_for_table1, 'level1')

                set_target = self.mr_targets.get(CONTRACT) if self.mr_targets.get(CONTRACT) is not None else 100

                self.update_kpi_scores_and_results(
                    {'KPI ID': CONTRACT,
                     'KPI name Eng': kpi_set_name,
                     'KPI name Rus': kpi_set_name,
                     'Parent': 'root'},
                    {'target': set_target,
                     'weight': 1,
                     'result': score,
                     'score': score,
                     'weighted_score': score,
                     'level': 0})

    @staticmethod
    def get_kpi_conversion(kpi_conversion_file):
        data = pd.read_excel(
            os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', kpi_conversion_file))
        conversion = {}
        for i, row in data.iterrows():
            conversion[row['KPI Name']] = str(row['KPI ID'])
        return conversion

    @kpi_runtime()
    def calculate_top_sku(self, include_to_contract, kpi_set_name):

        top_skus = self.kpi_fetcher.get_top_skus_for_store(self.store_id, self.visit_date)
        if not top_skus['product_fks']:
            return

        top_sku_products = pd.DataFrame()
        in_assortment = True
        for scene_fk in self.scif['scene_id'].unique():

            scene_data = self.scif[(self.scif['scene_id'] == scene_fk) &
                                   (self.scif['manufacturer_name'] == 'TCCC') &
                                   (self.scif['product_type'] == 'SKU') &
                                   (self.scif['facings'] > 0)]
            facings_data = scene_data.groupby('product_fk')['facings'].sum().to_dict()
            for anchor_product_fk in top_skus['product_fks'].keys():
                min_facings = top_skus['min_facings'][anchor_product_fk]
                distributed_anchor_product_fk = False
                for product_fk in top_skus['product_fks'][anchor_product_fk].split(','):
                    product_fk = int(product_fk)
                    facings = facings_data.pop(product_fk, 0)
                    # if product_fk in facings_data.keys():
                    #     facings = facings_data[product_fk]
                    # else:
                    #     facings = 0
                    distributed_product_fk = True if facings >= min_facings else False
                    distributed_anchor_product_fk |= distributed_product_fk
                    top_sku_products = top_sku_products.append({'anchor_product_fk': anchor_product_fk,
                                                                'product_fk': product_fk,
                                                                'facings': facings,
                                                                'min_facings': min_facings,
                                                                'in_assortment': 1,
                                                                'distributed': 1 if distributed_product_fk else 0,
                                                                'distributed_extra': 0},
                                                               ignore_index=True)

                query = self.get_custom_scif_query(
                    self.session_fk, scene_fk, int(anchor_product_fk), in_assortment, distributed_anchor_product_fk)
                self.top_sku_queries.append(query)

            if facings_data:
                for product_fk in facings_data.keys():
                    facings = facings_data[product_fk] if facings_data[product_fk] else 0
                    if facings:
                        top_sku_products = top_sku_products.append({'anchor_product_fk': product_fk,
                                                                    'product_fk': product_fk,
                                                                    'facings': facings,
                                                                    'min_facings': 0,
                                                                    'in_assortment': 0,
                                                                    'distributed': 0,
                                                                    'distributed_extra': 1},
                                                                   ignore_index=True)

        if not top_sku_products.empty:
            top_sku_products = top_sku_products\
                .merge(self.products[['product_fk', 'product_name', 'category_fk', 'category']], on='product_fk')\
                .merge(self.products[['product_fk', 'product_name']], left_on='anchor_product_fk', right_on='product_fk', suffixes=('', '_anchor'))\
                .groupby(['category_fk',
                          'category',
                          'anchor_product_fk',
                          'product_fk',
                          'product_name',
                          'product_name_anchor'])\
                .agg({'facings': 'sum',
                      'min_facings': 'max',
                      'in_assortment': 'max',
                      'distributed': 'max',
                      'distributed_extra': 'max'}) \
                .sort_values(by=['category',
                                 'product_name_anchor',
                                 'product_name',
                                 'in_assortment',
                                 'distributed'],
                             ascending=[True,
                                        True,
                                        True,
                                        False,
                                        True])\
                .reset_index()

            product_name_anchor = None
            sort_order = 0
            for i, row in top_sku_products.iterrows():

                if product_name_anchor != row['product_name_anchor']:
                    product_name_anchor = row['product_name_anchor']
                    sort_order = 1
                else:
                    sort_order += 1

                identifier_result = self.common.get_dictionary(
                    set=self.kpi_set_type, level=3, kpi=row['product_fk'])
                identifier_parent = self.common.get_dictionary(
                    set=self.kpi_set_type, level=2, kpi=row['anchor_product_fk'])

                kpi_name = self.kpi_set_type + '_SKU'
                kpi_name = kpi_name.strip().replace("'", "").replace(
                    '"', '').replace(',', '.').replace('  ', ' ').upper()
                kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)

                numerator_id = row['product_fk']
                denominator_id = None
                context_id = None

                numerator_result = row['facings']
                denominator_result = row['min_facings']

                score = 100 if row['distributed'] or row['distributed_extra'] else 0
                weight = sort_order
                target = 100 if not row['distributed_extra'] else 0
                result = 'DISTRIBUTED' if row['distributed'] else (
                    'EXTRA' if row['distributed_extra'] else 'OOS')
                kpi_result_type_fk = self.common.kpi_static_data[
                    self.common.kpi_static_data['pk'] == kpi_fk]['kpi_result_type_fk'].values[0]
                if kpi_result_type_fk:
                    result = self.kpi_result_values[(self.kpi_result_values['result_type_fk'] == kpi_result_type_fk) &
                                                    (self.kpi_result_values['result_value'] == result)][
                        'result_value_fk'].values[0]

                self.common.write_to_db_result(fk=kpi_fk,
                                               numerator_id=numerator_id,
                                               numerator_result=numerator_result,
                                               denominator_id=denominator_id,
                                               denominator_result=denominator_result,
                                               context_id=context_id,
                                               result=result,
                                               score=score,
                                               weight=weight,
                                               target=target,
                                               identifier_result=identifier_result,
                                               identifier_parent=identifier_parent,
                                               should_enter=True)

            top_sku_anchor_products = top_sku_products\
                .groupby(['category_fk',
                          'category',
                          'anchor_product_fk',
                          'product_name_anchor'])\
                .agg({'product_fk': 'count',
                      'facings': 'sum',
                      'min_facings': 'max',
                      'in_assortment': 'max',
                      'distributed': 'max',
                      'distributed_extra': 'max'})\
                .sort_values(by=['category',
                                 'in_assortment',
                                 'distributed',
                                 'product_name_anchor'],
                             ascending=[True,
                                        False,
                                        True,
                                        True])\
                .reset_index()

            category = None
            for i, row in top_sku_anchor_products.iterrows():

                if category != row['category']:
                    category = row['category']
                    sort_order = 1
                else:
                    sort_order += 1

                identifier_result = self.common.get_dictionary(
                    set=self.kpi_set_type, level=2, kpi=row['anchor_product_fk'])
                identifier_parent = self.common.get_dictionary(
                    set=self.kpi_set_type, level=1, kpi=row['category_fk'])

                kpi_name = self.kpi_set_type + '_BUNDLE'
                kpi_name = kpi_name.strip().replace("'", "").replace(
                    '"', '').replace(',', '.').replace('  ', ' ').upper()
                kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)

                numerator_id = row['anchor_product_fk']
                denominator_id = None

                if self.kpi_entities.empty:
                    context_id = None
                else:
                    context_type_fk = self.common.kpi_static_data[
                        self.common.kpi_static_data['pk'] == kpi_fk]['context_type_fk'].values[0]
                    if row['product_fk'] == 1:
                        context_id = self.kpi_entities[(self.kpi_entities['type'] == context_type_fk)
                                                       & (self.kpi_entities['uid_field'] == 'SKU')]['fk'].values[0]
                    else:
                        context_id = self.kpi_entities[(self.kpi_entities['type'] == context_type_fk)
                                                       & (self.kpi_entities['uid_field'] == 'BUNDLE')]['fk'].values[0]

                numerator_result = row['facings']
                denominator_result = None if row['product_fk'] == 1 else row['product_fk']

                score = 100 if row['distributed'] or row['distributed_extra'] else 0
                weight = sort_order
                target = 100 if not row['distributed_extra'] else 0
                result = 'DISTRIBUTED' if row['distributed'] else (
                    'EXTRA' if row['distributed_extra'] else 'OOS')
                kpi_result_type_fk = self.common.kpi_static_data[
                    self.common.kpi_static_data['pk'] == kpi_fk]['kpi_result_type_fk'].values[0]
                if kpi_result_type_fk:
                    result = self.kpi_result_values[(self.kpi_result_values['result_type_fk'] == kpi_result_type_fk) &
                                                    (self.kpi_result_values['result_value'] == result)][
                        'result_value_fk'].values[0]

                self.common.write_to_db_result(fk=kpi_fk,
                                               numerator_id=numerator_id,
                                               numerator_result=numerator_result,
                                               denominator_id=denominator_id,
                                               denominator_result=denominator_result,
                                               context_id=context_id,
                                               result=result,
                                               score=score,
                                               weight=weight,
                                               target=target,
                                               identifier_result=identifier_result,
                                               identifier_parent=identifier_parent,
                                               should_enter=True)

            top_sku_categories = top_sku_anchor_products\
                .groupby(['category_fk',
                          'category'])\
                .agg({'in_assortment': 'sum',
                      'distributed': 'sum',
                      'distributed_extra': 'sum'})
            top_sku_categories['sos'] = top_sku_categories[top_sku_categories['in_assortment'] > 0]['distributed'] / \
                top_sku_categories[top_sku_categories['in_assortment'] > 0]['in_assortment']
            top_sku_categories = top_sku_categories\
                .sort_values(by=['sos', 'category'])\
                .reset_index()

            for i, row in top_sku_categories.iterrows():

                sort_order = i+1

                identifier_result = self.common.get_dictionary(
                    set=self.kpi_set_type, level=1, kpi=row['category_fk'])
                identifier_parent = self.common.get_dictionary(
                    set=self.kpi_set_type, level=0, kpi=TOPSKU)

                kpi_name = self.kpi_set_type + '_CATEGORY'
                kpi_name = kpi_name.strip().replace("'", "").replace(
                    '"', '').replace(',', '.').replace('  ', ' ').upper()
                kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)

                numerator_id = row['category_fk']
                denominator_id = None
                context_id = None

                numerator_result = row['distributed']
                denominator_result = row['in_assortment']

                result = round(numerator_result / float(denominator_result) * 100, 3) \
                    if denominator_result \
                    else 0
                score = result
                weight = sort_order
                target = 100 if denominator_result else 0

                self.common.write_to_db_result(fk=kpi_fk,
                                               numerator_id=numerator_id,
                                               numerator_result=numerator_result,
                                               denominator_id=denominator_id,
                                               denominator_result=denominator_result,
                                               context_id=context_id,
                                               result=result,
                                               score=score,
                                               weight=weight,
                                               target=target,
                                               identifier_result=identifier_result,
                                               identifier_parent=identifier_parent,
                                               should_enter=True)

                # category kpis MR
                cat_kpi_fk = self.common.get_kpi_fk_by_kpi_type(OSA_CAT_FOR_MR)
                if row['category'] in CATEGORIES_LIST:
                    self.common.write_to_db_result(fk=cat_kpi_fk, numerator_id=self.own_manufacturer_id,
                                                   denominator_id=numerator_id, context_id=context_id,
                                                   result=result, score=score)

            top_sku_total = top_sku_anchor_products\
                .agg({'in_assortment': 'sum',
                      'distributed': 'sum'})

            numerator_result = top_sku_total['distributed']
            denominator_result = top_sku_total['in_assortment']

        else:
            numerator_result = 0
            denominator_result = len(top_skus['product_fks'].keys())

        identifier_result = self.common.get_dictionary(set=self.kpi_set_type, level=0, kpi=TOPSKU)
        identifier_parent = None  # self.common.get_dictionary(set=CONTRACT, level=0, kpi='0')

        kpi_name = self.kpi_set_type + '_0'
        kpi_name = kpi_name.strip().replace("'", "").replace(
            '"', '').replace(',', '.').replace('  ', ' ').upper()
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)

        numerator_id = self.own_manufacturer_id
        denominator_id = self.store_id
        context_id = None

        result = round(numerator_result / float(denominator_result) * 100, 3) \
            if denominator_result \
            else 0
        score = result
        weight = 1
        target = 100 if denominator_result else 0

        if not include_to_contract:
            self.common.write_to_db_result(fk=kpi_fk,
                                           numerator_id=numerator_id,
                                           numerator_result=numerator_result,
                                           denominator_id=denominator_id,
                                           denominator_result=denominator_result,
                                           context_id=context_id,
                                           result=result,
                                           score=score,
                                           weight=weight,
                                           target=target,
                                           identifier_result=identifier_result,
                                           identifier_parent=identifier_parent,
                                           should_enter=True)
        else:
            target = self.mr_targets.get(TOPSKU) if self.mr_targets.get(TOPSKU) is not None else target
            self.update_kpi_scores_and_results(
                {'KPI ID': TOPSKU,
                 'KPI name Eng': kpi_set_name,
                 'KPI name Rus': kpi_set_name,
                 'Parent': 'root'},
                {'target': target,
                 'weight': weight,
                 'result': result,
                 'score': score,
                 'weighted_score': score,
                 'level': 0})

        self.top_sku_score = score

        return

    @kpi_runtime()
    def check_kpi_scores(self, params, level=2, cooler_dict=None):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('level') != level:
                continue
            if p.get('Formula').strip() in ('Check KPI score in POS', 'Number of KPIs passed in POS'):

                kpi_result = 0
                if p.get('Formula').strip() == 'Check KPI score in POS':
                    kpi_name = p.get('Values')
                    kpi_id = self.kpi_name_to_id[POS].get(kpi_name)
                    kpi_result = self.kpi_scores_and_results[POS][kpi_id].get(
                        'score') if kpi_id else None

                elif p.get('Formula').strip() == 'Number of KPIs passed in POS':
                    kpi_names = [unicode(x).strip() for x in unicode(p.get('Values')).split(', ')]
                    kpi_result = 0
                    for kpi_name in kpi_names:
                        kpi_id = self.kpi_name_to_id[POS].get(kpi_name)
                        kpi_score = self.kpi_scores_and_results[POS][kpi_id].get(
                            'score') if kpi_id else None
                        if kpi_score == 100:
                            kpi_result += 1

                kpi_score = self.calculate_score(kpi_result, p)

                kpi_score = round(float(kpi_score), 3)
                weighted_kpi_score = round(kpi_score * p.get('KPI Weight'), 3)

                if not cooler_dict:
                    kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))

                    # saving to DB
                    if level == 2:
                        attributes_for_level3 = self.create_attributes_for_level3_df(
                            p, kpi_score, kpi_fk, level=2, additional_level=3)
                        self.write_to_kpi_results_old(attributes_for_level3, 'level3')
                        attributes_for_level2 = self.create_attributes_for_level2_df(
                            p, kpi_score, kpi_fk)
                        self.write_to_kpi_results_old(attributes_for_level2, 'level2')
                    else:
                        attributes_for_level3 = self.create_attributes_for_level3_df(
                            p, kpi_score, kpi_fk)
                        self.write_to_kpi_results_old(attributes_for_level3, 'level3')

                set_total_res += weighted_kpi_score

                if cooler_dict is not None:
                    kpi_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.COOLER_SCORE_LVL_2)
                    kpi_ref_fk = self.common.get_kpi_fk_by_kpi_type(p['KPI name Eng'].upper())
                    self.common.write_to_db_result(fk=kpi_fk, numerator_id=cooler_dict[CCRUConsts.COOLER_FK],
                                                   denominator_id=kpi_ref_fk,
                                                   context_id=cooler_dict[ScifConsts.SCENE_FK],
                                                   result=kpi_score, score=weighted_kpi_score,
                                                   weight=p.get("KPI Weight"), identifier_parent=cooler_dict,
                                                   should_enter=True)

        return set_total_res

    def get_object_facings(self,
                           scenes,
                           objects,
                           object_type,
                           formula,
                           size=None,
                           form_factor=None,
                           shelves=None,
                           products_to_exclude=None,
                           form_factors_to_exclude=None,
                           product_categories=None,
                           product_sub_categories=None,
                           product_brands=None,
                           product_sub_brands=None,
                           product_manufacturers=None):
        object_type_conversion = {'SKUs': 'product_ean_code',
                                  'BRAND': 'brand_name',
                                  'CAT': 'category',
                                  'MAN in CAT': 'category',
                                  'MAN': 'manufacturer_name',
                                  'SUB_BRAND': 'sub_brand_name',
                                  'SUB_CATEGORY': 'sub_category'}
        object_field = object_type_conversion[object_type]
        if shelves is None:
            df = self.scif[self.scif['facings'] > 0]
        else:
            shelves_list = [int(shelf) for shelf in str(shelves).split(',')]
            df = self.matches[self.matches['shelf_number'].isin(shelves_list)]

        final_result = df[(df['scene_fk'].isin(scenes)) &
                          (df[object_field].isin(objects)) &
                          (~df['product_ean_code'].isin(products_to_exclude)) &
                          (~df['form_factor'].isin(form_factors_to_exclude)) &
                          (df['manufacturer_name'].isin(product_manufacturers)) &
                          (~df['product_type'].isin(['Empty']))]

        if form_factor:
            final_result = final_result[final_result['form_factor'].isin(form_factor)]
        if size:
            final_result = final_result[final_result['size'].isin(size)]
        if product_categories:
            final_result = final_result[final_result['category'].isin(product_categories)]
        if product_sub_categories:
            final_result = final_result[final_result['sub_category'].isin(product_sub_categories)]
        if product_brands:
            final_result = final_result[final_result['brand_name'].isin(product_brands)]
        if product_sub_brands:
            final_result = final_result[final_result['sub_brand_name'].isin(product_sub_brands)]

        try:
            if "number of SKUs" in formula:
                object_facings = len(final_result['product_ean_code'].unique())
            elif "each SKU hits facings target" in formula:
                object_facings = final_result.groupby(['scene_fk', 'product_ean_code']).agg(
                    {'facings': 'sum'})['facings'].values.tolist()
            else:
                if 'facings' in final_result.columns:
                    object_facings = final_result['facings'].sum()
                else:
                    object_facings = len(final_result)
        except IndexError:
            object_facings = 0
        return object_facings

    def calculate_cooler_kpis(self, kpi_data, group_model_map):
        cooler_ass_store = self.cooler_assortment
        if not cooler_ass_store.empty:
            cooler_audit_kpi_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.COOLER_AUDIT_SCORE)
            top_idenfifier_parent = {'kpi_fk': cooler_audit_kpi_fk}

            cooler_ass_store, visit_presence_score = self.calculate_cooler_presence(cooler_ass_store,
                                                                                    top_idenfifier_parent)
            visit_cooler_score = self.calculate_cooler_score(kpi_data, group_model_map, cooler_ass_store,
                                                             top_idenfifier_parent)
            audit_score = round((visit_presence_score + visit_cooler_score) / 2.0, 3)
            self.common.write_to_db_result(fk=cooler_audit_kpi_fk, numerator_id=self.own_manufacturer_id,
                                           denominator_id=self.store_id, score=audit_score, result=audit_score,
                                           identifier_result=top_idenfifier_parent, should_enter=True)

            cooler_audit_fk_old = self.kpi_fetcher.kpi_static_data['kpi_set_fk'].values[0]
            attributes_for_table1 = pd.DataFrame([(CCRUConsts.COOLER_AUDIT_SCORE, self.session_uid, self.store_id,
                                                   self.visit_date.isoformat(), audit_score, cooler_audit_fk_old)],
                                                 columns=['kps_name', 'session_uid', 'store_fk', 'visit_date',
                                                          'score_1', 'kpi_set_fk'])
            self.write_to_kpi_results_old(attributes_for_table1, 'level1')

    def get_presence_type_result(self, result):
        value = 'EXTRA'
        if result is not None:
            value = 'OOS' if not result else 'DISTRIBUTED'
        custom_result = self.get_kpi_result_value_pk_by_value(value)
        return custom_result

    def get_kpi_result_value_pk_by_value(self, value):
        pk = None
        try:
            pk = self.kpi_result_values[self.kpi_result_values['result_value'] == value]['result_value_fk'].values[0]
        except Exception as e:
            Log.error('Value {} does not exist: {}'.format(value, e))
        return pk

    @staticmethod
    def match_survey_to_assortment(row,  cooler_ass_arr, assigned_coolers):
        survey_ans = row[CCRUConsts.SURVEY_ANSWER]
        ans_len = len(survey_ans)
        for cooler in cooler_ass_arr:
            if len(cooler) > ans_len:
                if (cooler[-ans_len:] == survey_ans) and (cooler not in assigned_coolers):
                    assigned_coolers.append(cooler)
                    return cooler
        return None

    def write_extra_coolers_results(self, extra_coolers_df, identifier_parent):
        extra_score = None
        custom_result = self.get_presence_type_result(extra_score)
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.EXTRA_COOLER_PRESENCE)
        for i, row in extra_coolers_df.iterrows():
            numerator_id = row['pk']
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=numerator_id, context_id=row[ScifConsts.SCENE_FK],
                                           score=extra_score, result=custom_result, identifier_parent=identifier_parent,
                                           should_enter=True)

    # Code to be added once bar code reader solution is implemented
    def match_coolers_assortment_to_bar_code_reader(self, cooler_ass_store, visit_total_res, extra_coolers_df):
        # make sure that there is assigned cooler id column in the cooler_ass_store df
        return cooler_ass_store, visit_total_res, extra_coolers_df

    def match_coolers_assortment_to_survey(self, cooler_ass_store, visit_total_result, extra_coolers_df):
        coolers_survey = self.cooler_survey_responses
        if not coolers_survey.empty:
            coolers_survey['answ_len'] = coolers_survey[CCRUConsts.SURVEY_ANSWER].apply(lambda x: len(str(x)) \
                                                                                                if x is not None else 0)
            small_responses = coolers_survey[coolers_survey['answ_len'] < CCRUConsts.MIN_NUMBER_DIGITS] \
                                                                        [CCRUConsts.SURVEY_ANSWER].values.tolist()
            if len(small_responses) > 0:
                Log.warning('The answer lengths for the following coolers are below '
                            'the minimum of 3 digits: {}'.format(small_responses))

            valid_survey_answ = coolers_survey[~(coolers_survey[CCRUConsts.SURVEY_ANSWER].isin(small_responses))]
            valid_survey_answ = valid_survey_answ.reset_index(drop=True)
            valid_survey_answ['assigned_cooler_id'] = np.nan
            cooler_ass_arr = cooler_ass_store[CCRUConsts.COOLER_ID].values
            if not valid_survey_answ.empty:
                assigned_coolers = []
                valid_survey_answ['assigned_cooler_id'] = valid_survey_answ.apply(self.match_survey_to_assortment,
                                                                                  axis=1, args=(cooler_ass_arr,
                                                                                                assigned_coolers))
            cooler_ass_store = cooler_ass_store.merge(valid_survey_answ, right_on='assigned_cooler_id',
                                                      left_on=CCRUConsts.COOLER_ID, how='left')
            unassigned_responses = valid_survey_answ[valid_survey_answ['assigned_cooler_id'].isnull()]\
                                                                    [CCRUConsts.SURVEY_ANSWER].values.tolist()
            coolers_in_sess = cooler_ass_store[~(cooler_ass_store[CCRUConsts.SURVEY_ANSWER].isnull())]
            visit_total_result = len(coolers_in_sess) / float(len(cooler_ass_store)) * 100

            extra_coolers_df = coolers_survey[coolers_survey[CCRUConsts.SURVEY_ANSWER].isin(small_responses +
                                                                                                 unassigned_responses)]
        else:
            cooler_ass_store['assigned_cooler_id'] = None
            cooler_ass_store[ScifConsts.SCENE_FK] = None
        return cooler_ass_store, visit_total_result, extra_coolers_df

    def calculate_cooler_presence(self, cooler_ass_store, top_identif_parent):
        visit_kpi_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.VISIT_COOLER_AVAILABILITY)
        visit_identif_par = {'kpi_fk': visit_kpi_fk}
        visit_total_res = 0
        extra_coolers_df = pd.DataFrame(columns=['pk', ScifConsts.SCENE_FK])

        if not self.coolers_by_bar_code:
            cooler_ass_store, visit_total_res, extra_coolers_df = \
                self.match_coolers_assortment_to_survey(cooler_ass_store, visit_total_res, extra_coolers_df)
        else:
            cooler_ass_store, visit_total_res, extra_coolers_df = \
                self.match_coolers_assortment_to_bar_code_reader(cooler_ass_store, visit_total_res, extra_coolers_df)

        for i, row in cooler_ass_store.iterrows():
            score = 0 if (row['assigned_cooler_id'] is None) or (str(row['assigned_cooler_id']) == 'nan') else 100
            custom_result = self.get_presence_type_result(score)
            self.common.write_to_db_result(fk=row['kpi_level_2_fk'], numerator_id=row[CCRUConsts.COOLER_FK],
                                           denominator_id=row[CCRUConsts.COOLER_MODEL_FK],
                                           context_id=row[ScifConsts.SCENE_FK], result=custom_result, score=score,
                                           identifier_parent=visit_identif_par, should_enter=True)

        self.write_extra_coolers_results(extra_coolers_df, top_identif_parent)

        visit_total_res = round(visit_total_res, 3)

        self.common.write_to_db_result(fk=visit_kpi_fk, numerator_id=self.own_manufacturer_id,
                                       denominator_id=self.store_id, score=visit_total_res,
                                       result=visit_total_res, identifier_parent=top_identif_parent,
                                       identifier_result=visit_identif_par, should_enter=True)

        visit_fk_old = self.kpi_fetcher.kpi_static_data[self.kpi_fetcher.kpi_static_data['kpi_name'] \
                                                        == CCRUConsts.VISIT_COOLER_AVAILABILITY]['kpi_fk'].values[0]
        attributes_for_table2 = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                               visit_fk_old, CCRUConsts.VISIT_COOLER_AVAILABILITY,
                                               round(visit_total_res), visit_total_res)],
                                             columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name',
                                                      'score', 'score_2'])
        self.write_to_kpi_results_old(attributes_for_table2, 'level2')

        return cooler_ass_store, visit_total_res

    def calculate_cooler_score(self, kpi_data, group_model_map, cooler_ass_df, top_identif_parent):
        visit_kpi_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.VISIT_COOLER_SCORE)
        visit_identifier_parent = {'kpi_fk': visit_kpi_fk}
        cooler_visit_result = 0
        if not self.cooler_survey_responses.empty:
            cooler_ass_df = cooler_ass_df[~cooler_ass_df[ScifConsts.SCENE_FK].isnull()]
            cooler_scores = {}
            cooler_score_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.COOLER_SCORE)

            for i, cooler_row in cooler_ass_df.iterrows():
                cooler_scene = cooler_row[ScifConsts.SCENE_FK]
                cooler_fk = cooler_row[CCRUConsts.COOLER_FK]
                cooler_model_fk = cooler_row['cooler_model_fk']
                self.set_scif_and_matches_to_scene_lvl(cooler_scene)
                kpi_group_df = group_model_map[group_model_map['Cooler Model'] \
                                               == cooler_row[CCRUConsts.COOLER_MODEL_NAME]]
                if not kpi_group_df.empty:
                    kpi_group = kpi_group_df[CCRUConsts.KPI_GROUP].values[0]
                    kpi_group_data = copy.deepcopy(kpi_data)
                    kpi_group_data[0] = filter(lambda x: x['Cooler Group'] == kpi_group, kpi_data[0])

                    cooler_dict = {ScifConsts.SCENE_FK: cooler_scene, CCRUConsts.COOLER_FK: cooler_fk,
                                   'cooler_model_fk': cooler_model_fk, 'kpi_fk': cooler_score_fk}

                    score = 0
                    score += self.check_availability(kpi_group_data, cooler_dict=cooler_dict)
                    score += self.check_facings_sos(kpi_group_data, cooler_dict=cooler_dict)
                    score += self.check_share_of_cch(kpi_group_data, cooler_dict=cooler_dict)
                    score += self.check_number_of_skus_per_door_range(kpi_group_data, cooler_dict=cooler_dict)
                    score += self.check_number_of_doors(kpi_group_data, cooler_dict=cooler_dict)
                    score += self.check_number_of_scenes(kpi_group_data, cooler_dict=cooler_dict)
                    score += self.check_number_of_scenes_no_tagging(kpi_group_data, cooler_dict=cooler_dict)
                    score += self.check_customer_cooler_doors(kpi_group_data, cooler_dict=cooler_dict)
                    score += self.check_atomic_passed(kpi_group_data, cooler_dict=cooler_dict)
                    score += self.check_atomic_passed_on_the_same_scene(kpi_group_data, cooler_dict=cooler_dict)
                    score += self.check_sum_atomics(kpi_group_data, cooler_dict=cooler_dict)
                    score += self.check_dummies(kpi_group_data, cooler_dict=cooler_dict)
                    score += self.check_weighted_average(kpi_group_data, cooler_dict=cooler_dict)
                    score += self.check_kpi_scores(kpi_group_data, cooler_dict=cooler_dict)
                    score += self.calculate_availability_coolers(kpi_group_data, cooler_dict)
                    score += self.check_number_of_doors_of_filled_coolers_cooler_audit(kpi_group_data, cooler_dict)

                    score = round(score, 3)

                    cooler_scores.update({cooler_fk: score})
                    self.common.write_to_db_result(fk=cooler_score_fk, numerator_id=cooler_fk,
                                                   denominator_id=cooler_model_fk, identifier_result=cooler_dict,
                                                   identifier_parent=visit_identifier_parent, context_id=cooler_scene,
                                                   score=score, result=score, should_enter=True)
                if kpi_group_df.empty:
                    Log.warning('No Kpi Group is assigned for cooler model '
                                '{}. Please add it to template'.format(cooler_row[CCRUConsts.COOLER_MODEL_NAME]))

            cooler_visit_result = sum(cooler_scores.values()) / float(len(cooler_ass_df)) \
                if not cooler_ass_df.empty \
                else 0

            cooler_visit_result = round(cooler_visit_result, 3)

        self.common.write_to_db_result(fk=visit_kpi_fk, numerator_id=self.own_manufacturer_id,
                                       denominator_id=self.store_id, result=cooler_visit_result,
                                       score=cooler_visit_result, identifier_result=visit_identifier_parent,
                                       identifier_parent=top_identif_parent, should_enter=True)

        visit_fk_old = self.kpi_fetcher.kpi_static_data[self.kpi_fetcher.kpi_static_data['kpi_name'] \
                                                        == CCRUConsts.VISIT_COOLER_SCORE]['kpi_fk'].values[0]
        attributes_for_table2 = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                               visit_fk_old, CCRUConsts.VISIT_COOLER_SCORE,
                                               round(cooler_visit_result), cooler_visit_result)],
                                             columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name',
                                                      'score', 'score_2'])
        self.write_to_kpi_results_old(attributes_for_table2, 'level2')

        self.reset_scif_and_matches_to_initial_values()
        return cooler_visit_result

    def set_scif_and_matches_to_scene_lvl(self, scene_fk):
        self.scif = self.scif_session[self.scif_session['scene_fk'] == scene_fk]
        self.matches = self.matches_session[self.matches_session['scene_fk'] == scene_fk]
        self.scenes_info = self.scenes_session_initial[self.scenes_session_initial['scene_fk'] == scene_fk]
        self.data_provider._set_scene_item_facts(self.scif)
        self.data_provider._set_matches(self.matches)
        self.k_engine = BaseCalculationsGroup(self.data_provider, self.output)

    def reset_scif_and_matches_to_initial_values(self):
        self.scif = self.scif_session
        self.matches = self.matches_session
        self.scenes_info = self.scenes_session_initial
        self.data_provider._set_scene_item_facts(self.scif_session)
        self.data_provider._set_matches(self.matches_session)
        self.k_engine = BaseCalculationsGroup(self.data_provider, self.output)

    def check_number_of_doors_of_filled_coolers_cooler_audit(self, params, cooler_dict, level=2):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('level') != level:
                continue
            if not (p.get('Formula').strip() in ("number of doors of filled Coolers",)):
                continue
            scene = cooler_dict[ScifConsts.SCENE_FK]
            result = self.check_number_of_doors_of_filled_coolers(p)
            atomic_result = self.calculate_score(result, p)
            weight = p.get("KPI Weight")
            score = atomic_result * weight
            set_total_res += score
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.COOLER_SCORE_LVL_2)
            kpi_ref_fk = self.common.get_kpi_fk_by_kpi_type(p['KPI name Eng'].upper())
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=cooler_dict[CCRUConsts.COOLER_FK],
                                           denominator_id=kpi_ref_fk, context_id=scene, result=atomic_result,
                                           score=score, weight=weight, identifier_parent=cooler_dict, should_enter=True)
        return set_total_res

    def calculate_availability_coolers(self, params, cooler_dict, level=2):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('level') != level:
                continue
            if not (p.get('Formula').strip() in ("each SKU hits facings target",)):
                continue
            scene = cooler_dict[ScifConsts.SCENE_FK]
            result = self.calculate_availability(p, all_params=params)
            weight = p.get("KPI Weight")
            score = result * weight
            set_total_res += score
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(CCRUConsts.COOLER_SCORE_LVL_2)
            kpi_ref_fk = self.common.get_kpi_fk_by_kpi_type(p['KPI name Eng'].upper())
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=cooler_dict[CCRUConsts.COOLER_FK],
                                           denominator_id=kpi_ref_fk, context_id=scene, result=result, score=score,
                                           weight=weight, identifier_parent=cooler_dict, should_enter=True)
        return set_total_res

    def create_kpi_groups(self, kpi_data):
        if kpi_data:
            for kpi in kpi_data:
                if kpi.get('Formula') != 'Group':
                    continue
                else:
                    self.update_kpi_scores_and_results(kpi, {'level': 1})
        return

    def write_to_kpi_results_new(self):
        for kpi_set_type in NEW_KPIS_TO_WRITE_TO_DB:
            kpis = self.kpi_scores_and_results.get(kpi_set_type)
            if kpis:
                kpis = pd.DataFrame(kpis.values())
                kpis = kpis.where((pd.notnull(kpis)), None).sort_values(by=['sort_order'])
                if kpi_set_type in [EQUIPMENT, TOPSKU]:
                    identifier_parent = self.common.get_dictionary(
                        set=CONTRACT, level=0, kpi=CONTRACT)
                else:
                    identifier_parent = None
                # try:
                self.write_kpi_tree(kpi_set_type, kpis, identifier_parent=identifier_parent)
                # except Exception as e:
                #     Log.warning('write_to_kpi_results_new Exception Error ({}): {}'.format(kpi_set_type, e))

        return

    def write_kpi_tree(self, kpi_set_type, kpis, parent='root', identifier_parent=None):
        group_score = 0
        group_weight = 0
        sort_order = 0
        for i, kpi in kpis[kpis['parent'] == parent].iterrows():

            sort_order += 1

            kpi_name = (kpi_set_type + '_' + str(int(kpi['level']))
                        + (('_' + kpi['format']) if kpi['format'] else '')
                        + ('_SCENE' if kpi['scene_id'] else ''))
            kpi_name = kpi_name.strip().replace("  ", " ").replace(",", ".").upper()
            kpi_fk = self.common.kpi_static_data[self.common.kpi_static_data['type']
                                                 == kpi_name]['pk'].values[0]
            if not kpi_fk:
                Log.error('KPI Name <{}> is not found in static.kpi_level_2 table'
                          ''.format(kpi_name))

            scene_id = int(kpi['scene_id']) if kpi['scene_id'] else None

            identifier_result = self.common.get_dictionary(
                set=kpi_set_type, level=kpi['level'], kpi=kpi['kpi_id'])

            numerator_id = self.own_manufacturer_id if parent == 'root' \
                else self.common.get_kpi_fk_by_kpi_type(kpi['kpi_name'])
            denominator_id = self. store_id if parent == 'root' \
                else self.common.get_kpi_fk_by_kpi_type(kpis[kpis['kpi_id'] == kpi['parent']]['kpi_name'].values[0])
            context_id = self.common.get_kpi_fk_by_kpi_type(kpi['kpi_name']) if parent == 'root' \
                else (scene_id if scene_id else None)

            if kpi['kpi_id'] != parent:
                if kpi['additional_level'] is not None:
                    additional_level = kpi.copy()
                    additional_level['parent'] = additional_level['kpi_id']
                    additional_level['level'] = additional_level['additional_level']
                    additional_level['additional_level'] = None
                    kpis = kpis.append(additional_level)
                score, weight = self.write_kpi_tree(kpi_set_type, kpis, parent=kpi['kpi_id'],
                                                    identifier_parent=identifier_result)
            else:
                score = weight = 0

            if kpi_set_type in [POS, EQUIPMENT, SPIRITS]:
                score = score if kpi['weighted_score'] is None else kpi['weighted_score']
                weight = weight if kpi['weight'] is None else kpi['weight']
                target = weight*100 if weight is not None and kpi['level'] < 3 else kpi['target']
            else:
                score = score if kpi['score'] is None else kpi['score']
                weight = kpi['weight']
                target = kpi['target']

            if kpi['format'] == 'STR':
                result = kpi['result']
                kpi_result_type_fk = self.common.kpi_static_data[self.common.kpi_static_data['pk'] == kpi_fk][
                    'kpi_result_type_fk'].values[0]
                if kpi_result_type_fk:
                    result = self.kpi_result_values[(self.kpi_result_values['result_type_fk'] == kpi_result_type_fk) &
                                                    (self.kpi_result_values['result_value'] == result)][
                        'result_value_fk'].values[0]
            else:
                if kpi_set_type in [POS, EQUIPMENT, SPIRITS] and kpi['level'] == 1:
                    result = round(score/weight, 3) if score and weight \
                        else score
                else:
                    result = kpi['result'] if kpi['result'] is not None \
                        else (kpi['score'] if kpi['score'] is not None
                              else score)

            group_score += score if score else 0
            group_weight += weight if weight else 0

            if kpi['parent'] == 'root' and self.mr_targets.get(kpi_set_type) is not None:
                target = kpi['target']

            self.common.write_to_db_result(fk=kpi_fk,
                                           numerator_id=numerator_id,
                                           numerator_result=sort_order,
                                           denominator_id=denominator_id,
                                           context_id=context_id,
                                           target=target,
                                           weight=weight,
                                           result=result,
                                           score=score,
                                           identifier_result=identifier_result,
                                           identifier_parent=identifier_parent,
                                           should_enter=True)

        return group_score, group_weight

    @kpi_runtime()
    def commit_results_data_new(self):
        self.write_to_kpi_results_new()
        self.common.commit_results_data()

    def get_kpi_entities(self, entities):
        if type(entities) is str:
            entities = [entities]
        entities_df = pd.DataFrame()
        for entity in entities:
            entity_type_fk = self.kpi_entity_types[self.kpi_entity_types['name']
                                                   == entity]['pk'].values[0]
            entity_table_name = self.kpi_entity_types[self.kpi_entity_types['name']
                                                      == entity]['table_name'].values[0]
            entity_uid_field = self.kpi_entity_types[self.kpi_entity_types['name']
                                                     == entity]['uid_field'].values[0]
            entities_df = entities_df\
                .append(self.kpi_fetcher.get_kpi_entity(entity, entity_type_fk, entity_table_name, entity_uid_field),
                        ignore_index=True)
        if not entities_df.empty:
            entities_df = entities_df.reindex()
        return entities_df

    def check_planned_visit_flag(self):

        if self.external_session_id and self.external_session_id.find('EasyMerch-P') >= 0:
            result = 0
        elif self.session_user['user_position'] == 'External':
            result = 0
        elif self.session_user['user_role'] == 'Sales Rep' and self.session_user['user_position'] != 'SMC':
            if self.planned_visit_flag is not None:
                result = self.planned_visit_flag
            else:
                result = 0
        else:
            if self.planned_visit_flag is not None:
                result = self.planned_visit_flag
            else:
                result = 1

        return result

    @staticmethod
    def get_custom_scif_query(session_fk, scene_fk, product_fk, in_assortment, distributed):
        in_assortment = 1 if in_assortment else 0
        out_of_stock = 1 if not distributed else 0
        attributes = pd.DataFrame([(session_fk,
                                    scene_fk,
                                    product_fk,
                                    in_assortment,
                                    out_of_stock,
                                    0,
                                    0,
                                    0)],
                                  columns=['session_fk',
                                           'scene_fk',
                                           'product_fk',
                                           'in_assortment_osa',
                                           'oos_osa',
                                           'length_mm_custom',
                                           'mha_in_assortment',
                                           'mha_oos'])
        query = insert(attributes.to_dict(), 'pservice.custom_scene_item_facts')
        return query

    @staticmethod
    def get_equipment_targets(file_name):
        """
        This function receives a KPI set name and return its relevant template as a JSON.
        """
        cloud_path = os.path.join(EQUIPMENT_TARGETS_CLOUD_BASE_PATH, file_name)
        temp_path = os.path.join(os.getcwd(), 'TempFile')
        with open(temp_path, 'wb') as f:
            try:
                amz_conn = StorageFactory.get_connector(EQUIPMENT_TARGETS_BUCKET)
                amz_conn.download_file(cloud_path, f)
            except:
                f.write('{}')
        with open(temp_path, 'rb') as f:
            data = json.load(f)
        os.remove(temp_path)
        return data

    def calculate_promo_compliance_store(self, kpis):
        display_targets = KpiTargetFinder(self.project_name,
                                          [self.kpi_fetcher.get_kpi_level_2_fk(PROMO_KPI_LEVEL_2_TYPE)],
                                          self.visit_date,
                                          self.store_id,
                                          PROMO_KPI_OPERATION_TYPE).get_kpi_target_matches()
        if display_targets.empty:
            Log.debug('Warning. Session cannot be calculated. '
                      'Promo targets are not defined. '
                      'Store ID {}.'.format(self.store_id))
            return

        # Remove duplicate display targets by leaving the last uploaded
        display_targets_list = display_targets.groupby('DISPLAY_NAME').agg({'target_fk': 'max'})['target_fk'].tolist()
        display_targets = display_targets[display_targets['target_fk'].isin(display_targets_list)]

        kpis = pd.DataFrame(kpis)
        self.promo_displays = self.kpi_fetcher.get_custom_entity(PROMO_DISPLAY_ENTITY)
        self.promo_locations = self.kpi_fetcher.get_custom_entity(PROMO_LOCATION_ENTITY)
        self.promo_products = self.kpi_fetcher.get_custom_entity(PROMO_PRODUCT_GROUP_ENTITY)

        kpi_fk_0 = self.common.kpi_static_data[self.common.kpi_static_data['type']
                                               == PROMO_COMPLIANCE_STORE]['pk'].values[0]
        kpi_identifier_result_0 = self.common.get_dictionary(kpi_fk=kpi_fk_0)

        number_of_displays_target = 0
        number_of_displays_found = 0
        number_of_displays_scored = 0

        # Display level KPIs
        for i, display_target in display_targets.iterrows():

            number_of_displays_target += 1
            number_of_locations_calculated = 0

            kpi_fk = self.common.kpi_static_data[self.common.kpi_static_data['type']
                                                 == PROMO_COMPLIANCE_DISPLAY]['pk'].values[0]

            kpi_result_type_fk = self.common.kpi_static_data[self.common.kpi_static_data['pk'] == kpi_fk][
                'kpi_result_type_fk'].values[0]

            display_fk = display_target['DISPLAY_NAME']  # DISPLAY_NAME contains pk
            target_fk = display_target['target_fk']

            kpi_identifier_result = self.common.get_dictionary(kpi_fk=kpi_fk, display_fk=display_fk)

            # Keep relevant data for scif
            scif = self.scif[(self.scif['template_fk'] == display_target['SCENE_TYPE']) |
                             (self.scif['template_name'].isin(PROMO_SCENE_TYPES_MAIN_SHELF))]

            if scif.empty:
                result_value = 0
            else:

                number_of_displays_found += 1

                # Add promo product groups to scif
                product_groups = self.get_promo_product_groups_to_skus(display_target)
                scif = pd.merge(scif, product_groups, on='product_fk', how='inner')

                # Add median prices from match_product_in_scene table
                scene_list = scif['scene_fk'].unique().tolist()
                if scene_list:
                    prices = self.matches[self.matches['scene_fk'].isin(scene_list) &
                                          ~self.matches['outlier'].isin([-2, 2]) &
                                          (self.matches['price'].notnull() |
                                           self.matches['promotion_price'].notnull())][
                        ['scene_fk', 'product_fk', 'price', 'promotion_price']]
                    prices.loc[prices['price'].isnull(), 'price'] = prices['promotion_price']
                    prices = prices.astype({'price': float})
                    prices = prices[['scene_fk', 'product_fk', 'price']]\
                        .groupby(['scene_fk', 'product_fk'])\
                        .agg({'price': ['sum', 'count']})\
                        .reset_index()
                    prices['price', 'avg'] = prices['price', 'sum'] / prices['price', 'count']
                    prices.columns = ['scene_fk', 'product_fk', 'price_sum', 'price_count', 'price']
                    if not prices.empty:
                        scif = pd.merge(scif, prices[['scene_fk', 'product_fk', 'price']],
                                        on=['scene_fk', 'product_fk'],
                                        how='left')
                    else:
                        scif['price'] = pd.np.nan
                else:
                    scif['price'] = pd.np.nan

                # # Add prices from match_product_in_probe_price_attribute_value as in QV
                # # with outliers detection
                # scene_list = scif['scene_fk'].unique().tolist()
                # if scene_list:
                #     scip = self.kpi_fetcher.get_scene_item_prices(scene_list)
                #     if not scip.empty:
                #         scif = pd.merge(scif, scip, on=['scene_fk', 'product_fk'], how='left')
                #     else:
                #         scif['price'] = pd.np.nan
                # else:
                #     scif['price'] = pd.np.nan

                # Location level KPI
                # Calculate Promo Display Location Score
                score, target, location_calculated = \
                    self.check_promo_compliance_location(PROMO_LOC_DISPLAY,
                                                         scif, kpis, display_target,
                                                         kpi_identifier_result)
                number_of_locations_calculated += location_calculated

                if score >= target:
                    result_value = 1
                    number_of_displays_scored += 1
                else:

                    # Calculate Main Shelf Location Score
                    score, target, location_calculated = \
                        self.check_promo_compliance_location(PROMO_LOC_MAIN_SHELF,
                                                             scif, kpis, display_target,
                                                             kpi_identifier_result)

                    number_of_locations_calculated += location_calculated

                    if score >= target:
                        result_value = 2
                        number_of_displays_scored += 1
                    else:
                        result_value = 0

            # Write Display level KPI
            result = \
                self.kpi_result_values[(self.kpi_result_values['result_type_fk'] == kpi_result_type_fk) &
                                       (self.kpi_result_values['result_value'] == str(result_value))][
                    'result_value_fk'].values[0]
            score = result_value
            self.common.write_to_db_result(fk=kpi_fk,
                                           numerator_id=self.own_manufacturer_id,
                                           numerator_result=number_of_locations_calculated,
                                           denominator_id=display_fk,
                                           context_id=None,
                                           target=None,
                                           weight=None,
                                           result=result,
                                           score=score,
                                           identifier_result=kpi_identifier_result,
                                           identifier_parent=kpi_identifier_result_0,
                                           should_enter=True,
                                           kpi_external_targets_fk=target_fk,
                                           target_greater_than_or_equal=0,
                                           target_less_than_than_or_equal=0,
                                           target_list_of_values=[0])

        # Write Store level KPI
        # if number_of_displays > 0:
        self.common.write_to_db_result(fk=kpi_fk_0,
                                       numerator_id=self.own_manufacturer_id,
                                       numerator_result=None,
                                       denominator_id=self.store_id,
                                       context_id=None,
                                       target=number_of_displays_target,
                                       weight=None,
                                       result=number_of_displays_found,
                                       score=number_of_displays_scored,
                                       identifier_result=kpi_identifier_result_0,
                                       identifier_parent=None,
                                       should_enter=True)

        return

    def get_promo_product_groups_to_skus(self, target):
        product_groups = target['PRODUCT_GROUP']
        product_groups_df = pd.DataFrame()
        for p in product_groups.keys():
            query = PROMO_PRODUCT_GROUP_FILTER['QUERY']
            filters = product_groups[p].get('PROD_INCL')
            if filters:
                for f in filters.keys():
                    query += ' ' + PROMO_PRODUCT_GROUP_FILTER[f] \
                        .format('', "'" + "','".join([unicode(x) for x in filters[f]]) + "'")
            filters = product_groups[p].get('PROD_EXCL')
            if filters:
                for f in filters.keys():
                    query += ' ' + PROMO_PRODUCT_GROUP_FILTER[f] \
                        .format('not', "'" + "','".join([unicode(x) for x in filters[f]]) + "'")
            data = pd.read_sql_query(query, self.rds_conn.db)
            data['product_group'] = p
            product_groups_df = product_groups_df.append(data)
        return product_groups_df

    @staticmethod
    def get_relevant_promo_scif(location, kpi_target, scif):
        if location == PROMO_LOC_DISPLAY:
            # Filtering for the most relevant scene
            scif_loc = scif[scif['template_fk'] == kpi_target['SCENE_TYPE']]
            if not scif_loc.empty:
                scif_loc = scif_loc \
                    .groupby(['scene_fk', 'product_group']) \
                    .agg({'facings': 'sum', 'price': 'max'}) \
                    .reset_index() \
                    .groupby('scene_fk') \
                    .agg({'facings': ('count', 'sum'), 'price': 'count'}) \
                    .reset_index()
                scif_loc.columns = scif_loc.columns.map('_'.join).str.strip('_')
                scif_loc = scif_loc.sort_values(by=['facings_count', 'facings_sum', 'price_count'],
                                                ascending=[False, False, False])
                scenes = [scif_loc['scene_fk'].values[0]]
                scif_loc = scif[scif['scene_fk'].isin(scenes)]
        else:
            # Taking all relevant scenes
            scif_loc = scif[scif['template_name'].isin(PROMO_SCENE_TYPES_MAIN_SHELF)]
        return scif_loc

    def check_promo_compliance_location(self, location, scif, kpis, kpi_target, kpi_identifier_parent):

        kpi_fk = self.common.kpi_static_data[
            self.common.kpi_static_data['type'] == PROMO_COMPLIANCE_LOCATION]['pk'].values[0]
        display_fk = kpi_identifier_parent['display_fk']
        location_fk = self.promo_locations[self.promo_locations['name'] == location]['pk'].values[0]
        kpi_identifier_result = \
            self.common.get_dictionary(kpi_fk=kpi_fk, display_fk=display_fk, location_fk=location_fk)

        score = 0
        target = kpis[(kpis['Location'] == location) &
                      (kpis['KPI'] == PROMO_COMPLIANCE_LOCATION)]['Target'].values[0]

        scif_loc = self.get_relevant_promo_scif(location, kpi_target, scif)

        if scif_loc.empty:
            location_calculated = 0
        else:

            if location == PROMO_LOC_DISPLAY:
                scene_fk = scif_loc['scene_fk'].values[0]
            else:
                scene_fk = None

            location_calculated = 1

            score += self.check_promo_compliance_display_presence(
                location, scif_loc, kpis, kpi_target, kpi_identifier_result)
            score += self.check_promo_compliance_distribution_target(
                location, scif_loc, kpis, kpi_target, kpi_identifier_result)
            score += self.check_promo_compliance_facings_target(
                location, scif_loc, kpis, kpi_target, kpi_identifier_result)
            score += self.check_promo_compliance_price_availability(
                location, scif_loc, kpis, kpi_target, kpi_identifier_result)
            score += self.check_promo_compliance_price_availability_total(
                location, scif_loc, kpis, kpi_target, kpi_identifier_result)
            score += self.check_promo_compliance_price_target(
                location, scif_loc, kpis, kpi_target, kpi_identifier_result)

            score = round(score, 3)

            self.common.write_to_db_result(fk=kpi_fk,
                                           numerator_id=self.own_manufacturer_id,
                                           numerator_result=None,
                                           denominator_id=display_fk,
                                           denominator_result=scene_fk,
                                           context_id=location_fk,
                                           target=target,
                                           weight=None,
                                           result=score,
                                           score=score,
                                           identifier_result=kpi_identifier_result,
                                           identifier_parent=kpi_identifier_parent,
                                           should_enter=True)

        return score, target, location_calculated

    def check_promo_compliance_display_presence(self, location, scif, kpis, kpi_target, kpi_identifier_parent):
        kpi_fk = self.common.kpi_static_data[
            self.common.kpi_static_data['type'] == PROMO_COMPLIANCE_DISPLAY_PRESENCE]['pk'].values[0]
        # kpi_result_type_fk = self.common.kpi_static_data[self.common.kpi_static_data['pk'] == kpi_fk][
        #     'kpi_result_type_fk'].values[0]

        display_fk = kpi_identifier_parent['display_fk']
        location_fk = kpi_identifier_parent['location_fk']
        kpi_identifier_result = \
            self.common.get_dictionary(kpi_fk=kpi_fk, display_fk=display_fk, location_fk=location_fk)

        total_facings_fact = scif.agg({'facings': 'sum'})[0]
        total_facings_target = kpi_target['TOTAL_FACINGS']

        if total_facings_fact >= total_facings_target:
            result = 100
            score = 100
        else:
            result = 0
            score = 0
        # result = \
        #     self.kpi_result_values[(self.kpi_result_values['result_type_fk'] == kpi_result_type_fk) &
        #                            (self.kpi_result_values['result_value'] == result)][
        #         'result_value_fk'].values[0]

        weight = kpis[(kpis['Location'] == location) &
                      (kpis['KPI'] == PROMO_COMPLIANCE_DISPLAY_PRESENCE)]['Weight'].values[0]/100.0
        score = round(score * float(weight), 3)
        target = round(float(weight) * 100, 3)

        self.common.write_to_db_result(fk=kpi_fk,
                                       numerator_id=self.own_manufacturer_id,
                                       numerator_result=total_facings_fact,
                                       denominator_id=display_fk,
                                       denominator_result=total_facings_target,
                                       context_id=location_fk,
                                       target=target,
                                       weight=weight,
                                       result=result,
                                       score=score,
                                       identifier_result=kpi_identifier_result,
                                       identifier_parent=kpi_identifier_parent,
                                       should_enter=True)

        return score

    def check_promo_compliance_distribution_target(self, location, scif, kpis, kpi_target, kpi_identifier_parent):

        kpi_fk = self.common.kpi_static_data[
            self.common.kpi_static_data['type'] == PROMO_COMPLIANCE_DISTRIBUTION_TARGET]['pk'].values[0]
        kpi_fk_prod = self.common.kpi_static_data[
            self.common.kpi_static_data['type'] == (PROMO_COMPLIANCE_DISTRIBUTION_TARGET + '_PROD')]['pk'].values[0]
        kpi_fk_sku = self.common.kpi_static_data[
            self.common.kpi_static_data['type'] == (PROMO_COMPLIANCE_DISTRIBUTION_TARGET + '_SKU')]['pk'].values[0]

        kpi_result_type_fk_prod = self.common.kpi_static_data[self.common.kpi_static_data['pk'] == kpi_fk_prod][
            'kpi_result_type_fk'].values[0]

        display_fk = kpi_identifier_parent['display_fk']
        location_fk = kpi_identifier_parent['location_fk']
        kpi_identifier_result = \
            self.common.get_dictionary(kpi_fk=kpi_fk, display_fk=display_fk, location_fk=location_fk)

        product_groups_fact = []
        product_groups_target = []
        for k in kpi_target['PRODUCT_GROUP'].keys():
            if kpi_target['PRODUCT_GROUP'][k].get('FACINGS') is not None:
                product_groups_target += [k]

        # Product Group level KPIs
        for product_group_fk in product_groups_target:

            kpi_identifier_result_prod = \
                self.common.get_dictionary(kpi_fk=kpi_fk_prod, display_fk=display_fk, location_fk=location_fk,
                                           product_group_fk=product_group_fk)

            product_group_scif = scif[(scif['product_group'] == product_group_fk) & (scif['facings'] > 0)]

            if not product_group_scif.empty:
                product_groups_fact += [product_group_fk]
                result_prod_value = 1
                result_prod = 'DISTRIBUTED'
                score_prod = 100
                target_prod = 100
            else:
                result_prod_value = 0
                result_prod = 'OOS'
                score_prod = 0
                target_prod = 100
            result_prod = \
                self.kpi_result_values[(self.kpi_result_values['result_type_fk'] == kpi_result_type_fk_prod) &
                                       (self.kpi_result_values['result_value'] == result_prod)][
                    'result_value_fk'].values[0]

            self.common.write_to_db_result(fk=kpi_fk_prod,
                                           numerator_id=product_group_fk,
                                           numerator_result=result_prod_value,
                                           denominator_id=display_fk,
                                           denominator_result=1,
                                           context_id=location_fk,
                                           target=target_prod,
                                           weight=None,
                                           result=result_prod,
                                           score=score_prod,
                                           identifier_result=kpi_identifier_result_prod,
                                           identifier_parent=kpi_identifier_result,
                                           should_enter=True)

            # SKU level KPIs
            for i, row in product_group_scif.iterrows():

                product_fk = row['product_fk']
                facings = row['facings']

                kpi_identifier_result_sku = \
                    self.common.get_dictionary(
                        kpi_fk=kpi_fk_sku, display_fk=display_fk, location_fk=location_fk,
                        product_group_fk=product_group_fk, product_fk=product_fk)

                self.common.write_to_db_result(fk=kpi_fk_sku,
                                               numerator_id=product_fk,
                                               numerator_result=facings,
                                               denominator_id=display_fk,
                                               # denominator_result=None,
                                               context_id=location_fk,
                                               # target=None,
                                               # weight=None,
                                               # result=None,
                                               # score=None,
                                               identifier_result=kpi_identifier_result_sku,
                                               identifier_parent=kpi_identifier_result_prod,
                                               should_enter=True)

        result = round(len(product_groups_fact) / float(len(product_groups_target)) * 100, 3)
        weight = kpis[(kpis['Location'] == location) &
                      (kpis['KPI'] == PROMO_COMPLIANCE_DISTRIBUTION_TARGET)]['Weight'].values[0]/100.0
        score = round(result * float(weight), 3)
        target = round(float(weight) * 100, 3)

        self.common.write_to_db_result(fk=kpi_fk,
                                       numerator_id=self.own_manufacturer_id,
                                       numerator_result=len(product_groups_fact),
                                       denominator_id=display_fk,
                                       denominator_result=len(product_groups_target),
                                       context_id=location_fk,
                                       target=target,
                                       weight=weight,
                                       result=result,
                                       score=score,
                                       identifier_result=kpi_identifier_result,
                                       identifier_parent=kpi_identifier_parent,
                                       should_enter=True)

        return score

    def check_promo_compliance_facings_target(self, location, scif, kpis, kpi_target, kpi_identifier_parent):

        kpi_fk = self.common.kpi_static_data[
            self.common.kpi_static_data['type'] == PROMO_COMPLIANCE_FACINGS_TARGET]['pk'].values[0]
        kpi_fk_prod = self.common.kpi_static_data[
            self.common.kpi_static_data['type'] == (PROMO_COMPLIANCE_FACINGS_TARGET + '_PROD')]['pk'].values[0]
        kpi_fk_sku = self.common.kpi_static_data[
            self.common.kpi_static_data['type'] == (PROMO_COMPLIANCE_FACINGS_TARGET + '_SKU')]['pk'].values[0]

        display_fk = kpi_identifier_parent['display_fk']
        location_fk = kpi_identifier_parent['location_fk']
        kpi_identifier_result = \
            self.common.get_dictionary(kpi_fk=kpi_fk, display_fk=display_fk, location_fk=location_fk)

        product_groups_fact = []
        product_groups_target = []
        for k in kpi_target['PRODUCT_GROUP'].keys():
            if kpi_target['PRODUCT_GROUP'][k].get('FACINGS') is not None:
                product_groups_target += [k]

        # Product Group level KPIs
        for product_group_fk in product_groups_target:

            kpi_identifier_result_prod = \
                self.common.get_dictionary(
                    kpi_fk=kpi_fk_prod, display_fk=display_fk, location_fk=location_fk,
                    product_group_fk=product_group_fk)

            product_group_scif = scif[(scif['product_group'] == product_group_fk) & (scif['facings'] > 0)]

            product_group_facings_fact = product_group_scif.agg({'facings': 'sum'})[0]
            product_group_facings_target = kpi_target['PRODUCT_GROUP'][product_group_fk].get('FACINGS')

            if product_group_facings_fact >= product_group_facings_target:
                product_groups_fact += [product_group_fk]
                result_prod = 100
                score_prod = result_prod
                target_prod = 100
            else:
                result_prod = 0
                score_prod = result_prod
                target_prod = 100

            self.common.write_to_db_result(fk=kpi_fk_prod,
                                           numerator_id=product_group_fk,
                                           numerator_result=product_group_facings_fact,
                                           denominator_id=display_fk,
                                           denominator_result=product_group_facings_target,
                                           context_id=location_fk,
                                           target=product_group_facings_target,  # target_prod,
                                           # weight=None,
                                           result=product_group_facings_fact,  # result_prod,
                                           score=score_prod,
                                           identifier_result=kpi_identifier_result_prod,
                                           identifier_parent=kpi_identifier_result,
                                           should_enter=True)

            # SKU level KPIs
            for i, row in product_group_scif.iterrows():

                product_fk = row['product_fk']
                facings = row['facings']

                kpi_identifier_result_sku = \
                    self.common.get_dictionary(
                        kpi_fk=kpi_fk_sku, display_fk=display_fk, location_fk=location_fk,
                        product_group_fk=product_group_fk, product_fk=product_fk)

                self.common.write_to_db_result(fk=kpi_fk_sku,
                                               numerator_id=product_fk,
                                               numerator_result=facings,
                                               denominator_id=display_fk,
                                               # denominator_result=None,
                                               context_id=location_fk,
                                               # target=None,
                                               # weight=None,
                                               # result=None,
                                               # score=None,
                                               identifier_result=kpi_identifier_result_sku,
                                               identifier_parent=kpi_identifier_result_prod,
                                               should_enter=True)

        # Upper level KPI
        result = round(len(product_groups_fact) / float(len(product_groups_target)) * 100, 3)
        weight = kpis[(kpis['Location'] == location) &
                      (kpis['KPI'] == PROMO_COMPLIANCE_FACINGS_TARGET)]['Weight'].values[0]/100.0
        score = round(result * float(weight), 3)
        target = round(float(weight) * 100, 3)

        self.common.write_to_db_result(fk=kpi_fk,
                                       numerator_id=self.own_manufacturer_id,
                                       numerator_result=len(product_groups_fact),
                                       denominator_id=display_fk,
                                       denominator_result=len(product_groups_target),
                                       context_id=location_fk,
                                       target=target,
                                       weight=weight,
                                       result=result,
                                       score=score,
                                       identifier_result=kpi_identifier_result,
                                       identifier_parent=kpi_identifier_parent,
                                       should_enter=True)

        return score

    def check_promo_compliance_price_availability(self, location, scif, kpis, kpi_target, kpi_identifier_parent):

        kpi_fk = self.common.kpi_static_data[
            self.common.kpi_static_data['type'] == PROMO_COMPLIANCE_PRICE_AVAILABILITY]['pk'].values[0]
        kpi_fk_prod = self.common.kpi_static_data[
            self.common.kpi_static_data['type'] == (PROMO_COMPLIANCE_PRICE_AVAILABILITY + '_PROD')]['pk'].values[0]
        kpi_fk_sku = self.common.kpi_static_data[
            self.common.kpi_static_data['type'] == (PROMO_COMPLIANCE_PRICE_AVAILABILITY + '_SKU')]['pk'].values[0]

        kpi_result_type_fk_prod = self.common.kpi_static_data[self.common.kpi_static_data['pk'] == kpi_fk_prod][
            'kpi_result_type_fk'].values[0]

        display_fk = kpi_identifier_parent['display_fk']
        location_fk = kpi_identifier_parent['location_fk']
        kpi_identifier_result = \
            self.common.get_dictionary(kpi_fk=kpi_fk, display_fk=display_fk, location_fk=location_fk)

        product_groups_fact = []
        product_groups_target = []
        for k in kpi_target['PRODUCT_GROUP'].keys():
            if kpi_target['PRODUCT_GROUP'][k].get('PRICE') is not None:
                product_groups_target += [k]

        # Product Group level KPIs
        for product_group_fk in product_groups_target:

            kpi_identifier_result_prod = \
                self.common.get_dictionary(
                    kpi_fk=kpi_fk_prod, display_fk=display_fk, location_fk=location_fk,
                    product_group_fk=product_group_fk)

            product_group_scif = scif[(scif['product_group'] == product_group_fk) &
                                      (~scif['price'].isnull()) & (scif['price'] > 0)]

            if not product_group_scif.empty:
                product_groups_fact += [product_group_fk]
                result_prod_value = 1
                result_prod = 'DISTRIBUTED'
                score_prod = 100
                target_prod = 100
            else:
                result_prod_value = 0
                result_prod = 'OOS'
                score_prod = 0
                target_prod = 100
            result_prod = \
                self.kpi_result_values[(self.kpi_result_values['result_type_fk'] == kpi_result_type_fk_prod) &
                                       (self.kpi_result_values['result_value'] == result_prod)][
                    'result_value_fk'].values[0]

            self.common.write_to_db_result(fk=kpi_fk_prod,
                                           numerator_id=product_group_fk,
                                           numerator_result=result_prod_value,
                                           denominator_id=display_fk,
                                           denominator_result=1,
                                           context_id=location_fk,
                                           target=target_prod,
                                           # weight=None,
                                           result=result_prod,
                                           score=score_prod,
                                           identifier_result=kpi_identifier_result_prod,
                                           identifier_parent=kpi_identifier_result,
                                           should_enter=True)

            # SKU level KPIs
            for i, row in product_group_scif.iterrows():

                product_fk = row['product_fk']
                facings = row['facings']
                price = row['price']
                price_100 = price * 100 if price is not None else None

                kpi_identifier_result_sku = \
                    self.common.get_dictionary(
                        kpi_fk=kpi_fk_sku, display_fk=display_fk, location_fk=location_fk,
                        product_group_fk=product_group_fk, product_fk=product_fk)

                self.common.write_to_db_result(fk=kpi_fk_sku,
                                               numerator_id=product_fk,
                                               numerator_result=facings,
                                               denominator_id=display_fk,
                                               denominator_result=price_100,
                                               context_id=location_fk,
                                               # target=None,
                                               # weight=None,
                                               result=price,
                                               # score=None,
                                               identifier_result=kpi_identifier_result_sku,
                                               identifier_parent=kpi_identifier_result_prod,
                                               should_enter=True)

        # Upper level KPI
        if not product_groups_target:
            score = 0
        else:
            result = round(len(product_groups_fact) / float(len(product_groups_target)) * 100, 3)
            weight = kpis[(kpis['Location'] == location) &
                          (kpis['KPI'] == PROMO_COMPLIANCE_PRICE_AVAILABILITY)]['Weight'].values[0]/100.0
            score = round(result * float(weight), 3)
            target = round(float(weight) * 100, 3)

            self.common.write_to_db_result(fk=kpi_fk,
                                           numerator_id=self.own_manufacturer_id,
                                           numerator_result=len(product_groups_fact),
                                           denominator_id=display_fk,
                                           denominator_result=len(product_groups_target),
                                           context_id=location_fk,
                                           target=target,
                                           weight=weight,
                                           result=result,
                                           score=score,
                                           identifier_result=kpi_identifier_result,
                                           identifier_parent=kpi_identifier_parent,
                                           should_enter=True)

        return score

    def check_promo_compliance_price_availability_total(self, location, scif, kpis, kpi_target, kpi_identifier_parent):
        kpi_fk = self.common.kpi_static_data[
            self.common.kpi_static_data['type'] == PROMO_COMPLIANCE_PRICE_AVAILABILITY_TOTAL]['pk'].values[0]
        # kpi_result_type_fk = self.common.kpi_static_data[self.common.kpi_static_data['pk'] == kpi_fk][
        #     'kpi_result_type_fk'].values[0]

        display_fk = kpi_identifier_parent['display_fk']
        location_fk = kpi_identifier_parent['location_fk']
        kpi_identifier_result = \
            self.common.get_dictionary(kpi_fk=kpi_fk, display_fk=display_fk, location_fk=location_fk)

        product_groups_target = []
        for k in kpi_target['PRODUCT_GROUP'].keys():
            if kpi_target['PRODUCT_GROUP'][k].get('PRICE') is not None:
                product_groups_target += [k]

        if not product_groups_target:
            score = 0
        else:

            total_price_count = \
                scif[scif['product_group'].isin(product_groups_target) &
                     ~scif['price'].isnull() & (scif['price'] > 0)]['price'].agg({'price': 'count'})[0]

            if total_price_count > 0:
                result = 100
                score = 100
            else:
                result = 0
                score = 0
            # result = \
            #     self.kpi_result_values[(self.kpi_result_values['result_type_fk'] == kpi_result_type_fk) &
            #                            (self.kpi_result_values['result_value'] == result)][
            #         'result_value_fk'].values[0]

            weight = kpis[(kpis['Location'] == location) &
                          (kpis['KPI'] == PROMO_COMPLIANCE_PRICE_AVAILABILITY_TOTAL)]['Weight'].values[0]/100.0
            score = round(score * float(weight), 3)
            target = round(float(weight) * 100, 3)

            self.common.write_to_db_result(fk=kpi_fk,
                                           numerator_id=self.own_manufacturer_id,
                                           numerator_result=total_price_count,
                                           denominator_id=display_fk,
                                           denominator_result=1,
                                           context_id=location_fk,
                                           target=target,
                                           weight=weight,
                                           result=result,
                                           score=score,
                                           identifier_result=kpi_identifier_result,
                                           identifier_parent=kpi_identifier_parent,
                                           should_enter=True)

        return score

    def check_promo_compliance_price_target(self, location, scif, kpis, kpi_target, kpi_identifier_parent):

        kpi_fk = self.common.kpi_static_data[
            self.common.kpi_static_data['type'] == PROMO_COMPLIANCE_PRICE_TARGET]['pk'].values[0]
        kpi_fk_prod = self.common.kpi_static_data[
            self.common.kpi_static_data['type'] == (PROMO_COMPLIANCE_PRICE_TARGET + '_PROD')]['pk'].values[0]
        kpi_fk_sku = self.common.kpi_static_data[
            self.common.kpi_static_data['type'] == (PROMO_COMPLIANCE_PRICE_TARGET + '_SKU')]['pk'].values[0]

        display_fk = kpi_identifier_parent['display_fk']
        location_fk = kpi_identifier_parent['location_fk']
        kpi_identifier_result = \
            self.common.get_dictionary(kpi_fk=kpi_fk, display_fk=display_fk, location_fk=location_fk)

        total_price_facings_fact = 0
        total_price_facings_target = 0
        product_groups_target = []
        for k in kpi_target['PRODUCT_GROUP'].keys():
            if kpi_target['PRODUCT_GROUP'][k].get('PRICE') is not None:
                product_groups_target += [k]

        # Product Group level KPIs
        for product_group_fk in product_groups_target:

            kpi_identifier_result_prod = \
                self.common.get_dictionary(
                    kpi_fk=kpi_fk_prod, display_fk=display_fk, location_fk=location_fk,
                    product_group_fk=product_group_fk)

            product_group_scif = scif[(scif['product_group'] == product_group_fk)
                                      & (scif['facings'] > 0) & (scif['price'] > 0)]
            if not product_group_scif.empty:
                product_group_scif['price_facings'] = product_group_scif['price'] * product_group_scif['facings']
                product_group_price_facings_fact = product_group_scif.agg({'price_facings': 'sum'})[0]
                product_group_facings_fact = product_group_scif.agg({'facings': 'sum'})[0]
                product_group_price_fact = \
                    round(product_group_price_facings_fact / float(product_group_facings_fact), 3)
            else:
                product_group_price_facings_fact = 0
                product_group_facings_fact = 0
                product_group_price_fact = None
            product_group_price_target = kpi_target['PRODUCT_GROUP'][product_group_fk].get('PRICE')

            if product_group_price_fact:
                total_price_facings_fact += product_group_price_facings_fact
                # Deviation
                if product_group_price_target > 0:
                    total_price_facings_target += product_group_price_target * product_group_facings_fact
                    result_prod = \
                        round(abs((1 - product_group_price_fact / float(product_group_price_target)) * 100), 3)
                else:
                    total_price_facings_target += product_group_price_facings_fact
                    result_prod = 0
                score_prod = 100 - result_prod
            else:
                # result_prod = 0
                score_prod = 0

            product_group_price_fact_100 = product_group_price_fact * 100 if product_group_price_fact is not None \
                else None
            product_group_price_target_100 = product_group_price_target * 100
            self.common.write_to_db_result(fk=kpi_fk_prod,
                                           numerator_id=product_group_fk,
                                           numerator_result=product_group_price_fact_100,
                                           denominator_id=display_fk,
                                           denominator_result=product_group_price_target_100,
                                           context_id=location_fk,
                                           target=product_group_price_target,  # 100,
                                           # weight=None,
                                           result=product_group_price_fact,  # result_prod,
                                           score=score_prod,
                                           identifier_result=kpi_identifier_result_prod,
                                           identifier_parent=kpi_identifier_result,
                                           should_enter=True)

            # SKU level KPIs
            for i, row in product_group_scif.iterrows():

                product_fk = row['product_fk']
                facings = row['facings']
                price = row['price']
                price_100 = price * 100 if price else None

                kpi_identifier_result_sku = \
                    self.common.get_dictionary(
                        kpi_fk=kpi_fk_sku, display_fk=display_fk, location_fk=location_fk,
                        product_group_fk=product_group_fk, product_fk=product_fk)

                self.common.write_to_db_result(fk=kpi_fk_sku,
                                               numerator_id=product_fk,
                                               numerator_result=facings,
                                               denominator_id=display_fk,
                                               denominator_result=price_100,
                                               context_id=location_fk,
                                               # target=None,
                                               # weight=None,
                                               result=price,
                                               # score=None,
                                               identifier_result=kpi_identifier_result_sku,
                                               identifier_parent=kpi_identifier_result_prod,
                                               should_enter=True)

        # Upper level KPI
        if not product_groups_target:
            score = 0
        else:
            # Deviation
            deviation = \
                round(abs(1 - total_price_facings_fact / float(total_price_facings_target)) * 100, 3) \
                if total_price_facings_target else 100.0
            weight = kpis[(kpis['Location'] == location) &
                          (kpis['KPI'] == PROMO_COMPLIANCE_PRICE_TARGET)]['Weight'].values[0]/100.0
            result = round((100 - deviation), 3)
            score = round(result * float(weight), 3)
            target = round(float(weight) * 100, 3)

            self.common.write_to_db_result(fk=kpi_fk,
                                           numerator_id=self.own_manufacturer_id,
                                           numerator_result=deviation,
                                           denominator_id=display_fk,
                                           # denominator_result=None,
                                           context_id=location_fk,
                                           target=target,
                                           weight=weight,
                                           result=result,
                                           score=score,
                                           identifier_result=kpi_identifier_result,
                                           identifier_parent=kpi_identifier_parent,
                                           should_enter=True)

        return score







