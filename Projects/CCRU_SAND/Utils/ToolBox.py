# -*- coding: utf-8 -*-
import os
import numpy as np
import pandas as pd
import datetime as dt

from Trax.Algo.Calculations.Core.Constants import Fields as Fd
from Trax.Algo.Calculations.Core.DataProvider import Data, Keys
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime
from KPIUtils_v2.DB.CommonV2 import Common

from Projects.CCRU_SAND.Utils.Fetcher import CCRU_SANDCCHKPIFetcher
from Projects.CCRU_SAND.Utils.ExecutionContract import CCRU_SANDContract
from Projects.CCRU_SAND.Utils.TopSKU import CCRU_SANDTopSKUAssortment


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

SKIP_OLD_KPIS_FROM_WRITING = [TARGET]
SKIP_NEW_KPIS_FROM_WRITING = [TARGET, MARKETING, SPIRITS]

BINARY = 'BINARY'
PROPORTIONAL = 'PROPORTIONAL'
CONDITIONAL_PROPORTIONAL = 'CONDITIONAL PROPORTIONAL'
AVERAGE = 'AVERAGE'
KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
CUSTOM_GAPS_TABLE = 'pservice.custom_gaps'


class CCRU_SANDKPIToolBox:
    def __init__(self, data_provider, output, kpi_set_name=None, kpi_set_type=None):
        self.data_provider = data_provider
        self.output = output
        self.project_name = self.data_provider.project_name
        self.rds_conn = self.rds_connection()

        self.common = Common(self.data_provider)
        self.k_engine = BaseCalculationsGroup(self.data_provider, self.output)
        self.session_info = SessionInfo(self.data_provider)

        self.session_uid = self.data_provider.session_uid
        self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.own_manufacturer_id = int(self.data_provider[Data.OWN_MANUFACTURER][self.data_provider[Data.OWN_MANUFACTURER]['param_name'] == 'manufacturer_id']['param_value'].tolist()[0])
        self.sales_rep_fk = self.data_provider[Data.SESSION_INFO]['s_sales_rep_fk'].iloc[0]
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]

        self.products = self.data_provider[Data.ALL_PRODUCTS]
        self.templates = self.data_provider[Data.ALL_TEMPLATES]
        self.scenes_info = self.data_provider[Data.SCENES_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.matches = self.data_provider[Data.MATCHES].merge(self.products, on='product_fk')

        self.pos_kpi_set_name = self.get_pos_kpi_set_name()
        self.kpi_set_name = kpi_set_name if kpi_set_name else self.pos_kpi_set_name
        self.kpi_set_type = kpi_set_type if kpi_set_type else POS
        self.kpi_name_to_id = {kpi_set_type: {}}
        self.kpi_scores_and_results = {kpi_set_type: {}}

        self.kpi_fetcher = CCRU_SANDCCHKPIFetcher(self.project_name)
        self.external_session_id = self.kpi_fetcher.get_external_session_id(self.session_uid)
        self.store_number = self.kpi_fetcher.get_store_number(self.store_id)
        self.test_store = self.kpi_fetcher.get_test_store(self.store_id)
        self.attr15 = self.kpi_fetcher.get_attr15_store(self.store_id)
        self.kpi_result_values = self.kpi_fetcher.get_kpi_result_values()
        self.kpi_entity_types = self.kpi_fetcher.get_kpi_entity_types()
        self.kpi_entities = self.get_kpi_entities('top_sku_type')
        self.store_areas = self.kpi_fetcher.get_store_area_df(self.session_uid)

        self.execution_contract = CCRU_SANDContract(rds_conn=self.rds_conn)
        self.top_sku = CCRU_SANDTopSKUAssortment(rds_conn=self.rds_conn)

        self.passed_scenes_per_kpi = {}
        self.kpi_facts_hidden = []
        self.kpi_results_queries = []
        self.gaps_dict = {}
        self.gaps_queries = []
        self.gap_groups_limit = {'Availability': 2, 'Cooler/Cold Availability': 1, 'Shelf/Displays/Activation': 3}
        self.top_sku_queries = []
        self.equipment_execution_score = None
        self.top_sku_score = None

    def set_kpi_set(self, kpi_set_name, kpi_set_type, empty_kpi_scores_and_results=True):
        self.kpi_set_name = kpi_set_name
        self.kpi_set_type = kpi_set_type
        self.kpi_fetcher.kpi_set_name = kpi_set_name
        self.kpi_fetcher.kpi_static_data = self.kpi_fetcher.get_static_kpi_data(kpi_set_name)
        if empty_kpi_scores_and_results:
            self.kpi_name_to_id[kpi_set_type] = {}
            self.kpi_scores_and_results[kpi_set_type] = {}

    def update_kpi_scores_and_results(self, params, kpi_scores_and_results, duplicate=False):
        if not duplicate:
            self.kpi_name_to_id[self.kpi_set_type][params.get('KPI name Eng')] = params.get('KPI ID')
            if not self.kpi_scores_and_results[self.kpi_set_type].get(params.get('KPI ID')):
                self.kpi_scores_and_results[self.kpi_set_type][params.get('KPI ID')] = \
                    {'kpi_id': params.get('KPI ID'),
                     'eng_name': params.get('KPI name Eng'),
                     'rus_name': params.get('KPI name Rus'),
                     'score_func': params.get('score_func'),
                     'weight': params.get('KPI Weight'),
                     'threshold': None,
                     'result': None,
                     'format': None,
                     'score': None,
                     'scene_id': None,
                     'scene_uid': None,
                     'level': params.get('level'),
                     'parent': params.get('Parent') if params.get('Parent') else 0}
            self.kpi_scores_and_results[self.kpi_set_type][params.get('KPI ID')].update(kpi_scores_and_results)

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
                sub_location = sub_location[0]
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
                scenes_to_include = params.get('Scenes to include').split(', ')
                for scene in scenes_to_include:
                    if scene in scenes_data.keys():
                        include_list_candidate.extend(scenes_data[scene])
                if not include_list_candidate:
                    return relevant_scenes
                include_list = list(set(include_list_candidate))

            if params.get('Locations to include'):
                include_list_candidate = []
                locations_to_include = params.get('Locations to include').split(', ')
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
                    sub_locations_to_include = str(int(params.get('Sub locations to include'))).split(', ')
                else:
                    sub_locations_to_include = str(params.get('Sub locations to include')).split(', ')
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
                store_areas_to_include = str(params.get('Zone to include')).split(', ')
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
            scenes_to_exclude = params.get('Scenes to exclude').split(', ')
            for scene in scenes_to_exclude:
                if scene in scenes_data.keys():
                    exclude_list.extend(scenes_data[scene])

        if params.get('Locations to exclude'):
            locations_to_exclude = params.get('Locations to exclude').split(', ')
            for location in locations_to_exclude:
                if location in location_data.keys():
                    exclude_list.extend(location_data[location])

        if params.get('Sub locations to exclude'):
            sub_locations_to_exclude = str(params.get('Sub locations to exclude')).split(', ')
            for sub_location in sub_locations_to_exclude:
                if sub_location in sub_location_data.keys():
                    exclude_list.extend(sub_location_data[sub_location])
        exclude_list = list(set(exclude_list))

        for scene in include_list:
            if scene not in exclude_list:
                relevant_scenes.append(scene)
        return relevant_scenes

    @kpi_runtime()
    def check_availability(self, params):
        """
        This function is used to calculate availability given a set pf parameters

        """
        set_total_res = 0
        availability_types = ['SKUs', 'BRAND', 'MAN', 'CAT', 'MAN in CAT', 'SUB_BRAND', 'SUB_CATEGORY']
        formula_types = ['number of SKUs', 'number of facings']
        atomic_result_total = 0
        for p in params.values()[0]:
            if p.get('Type') not in availability_types or p.get('Formula').strip() not in formula_types:
                continue
            if p.get('level') != 2:
                continue
            is_atomic = False
            kpi_total_res = 0
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))

            if p.get('Children') is not None:
                is_atomic = True
                children = [int(child) for child in str(p.get('Children')).split(', ')]
                atomic_scores = []
                atomic_result_total = 0
                for child in params.values()[0]:
                    if child.get('KPI ID') in children:

                        if child.get('Children') is not None:  # atomic of atomic
                            atomic_score = 0
                            atomic_children = [int(a_child) for a_child in str(child.get('Children')).split(', ')]
                            for atomic_child in params.values()[0]:
                                if atomic_child.get('KPI ID') in atomic_children:
                                    atomic_child_res = self.calculate_availability(atomic_child)
                                    atomic_child_score = self.calculate_score(atomic_child_res, atomic_child)
                                    atomic_score += atomic_child.get('additional_weight',
                                                                     1.0 / len(atomic_children)) * atomic_child_score

                        else:
                            atomic_res = self.calculate_availability(child)
                            atomic_score = self.calculate_score(atomic_res, child)

                        # saving to DB
                        attributes_for_level3 = self.create_attributes_for_level3_df(child, atomic_score, kpi_fk)
                        self.write_to_kpi_results_old(attributes_for_level3, 'level3')

                        atomic_result = attributes_for_level3['result']
                        if atomic_result.size > 0:
                            atomic_result_total += atomic_result.values[0]

                        if p.get('Logical Operator') in ('OR', 'AND', 'MAX'):
                            atomic_scores.append(atomic_score)
                        elif p.get('Logical Operator') == 'SUM':
                            kpi_total_res += child.get('additional_weight', 1 / float(len(children))) * atomic_score

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
                    score = round(kpi_total_res) / 100.0
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

            # Saving to old tables
            attributes_for_table2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_kpi_results_old(attributes_for_table2, 'level2')

            if p.get("KPI ID") in params.values()[2]["SESSION LEVEL"]:
                self.write_to_kpi_facts_hidden(p.get("KPI ID"), None, atomic_result_total, score)

            if not is_atomic:  # saving also to level3 in case this KPI has only one level
                attributes_for_table3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
                self.write_to_kpi_results_old(attributes_for_table3, 'level3')

            set_total_res += round(score) * p.get('KPI Weight')

        return set_total_res

    def calculate_availability(self, params, scenes=None, all_params=None):
        values_list = str(params.get('Values')).split(', ')
        if not scenes:
            if 'depends on' in params.keys():
                depends_on_kpi_name = params.get('depends on')
                for c in all_params.values()[0]:
                    if c.get('KPI name Eng') == depends_on_kpi_name:
                        if c.get('Formula').strip() == 'number of coolers with facings target and fullness target':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')
                            scenes = self.calculate_number_of_doors_of_filled_coolers(c, scenes,
                                                                                      func='get scenes',
                                                                                      proportion_param=0.9)
                        else:
                            scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')

                if not scenes:
                    return 0
            else:
                scenes = self.get_relevant_scenes(params)

        if params.get("Form Factor"):
            form_factors = [str(form_factor) for form_factor in params.get("Form Factor").split(", ")]
        else:
            form_factors = []
        if params.get("Size"):
            sizes = [float(size) for size in str(params.get('Size')).split(", ")]
            sizes = [int(size) if int(size) == size else size for size in sizes]
        else:
            sizes = []
        if params.get("Products to exclude"):
            products_to_exclude = [int(float(product)) for product in \
                                   str(params.get("Products to exclude")).split(", ")]
        else:
            products_to_exclude = []
        if params.get("Form factors to exclude"):
            form_factors_to_exclude = str(params.get("Form factors to exclude")).split(", ")
        else:
            form_factors_to_exclude = []
        if params.get("Product Category"):
            product_categories = str(params.get("Product Category")).split(", ")
        else:
            product_categories = []
        if params.get("Sub category"):
            product_sub_categories = str(params.get("Sub category")).split(", ")
        else:
            product_sub_categories = []
        if params.get("Brand"):
            product_brands = str(params.get("Brand")).split(", ")
        else:
            product_brands = []
        if params.get("Manufacturer"):
            product_manufacturers = str(params.get("Manufacturer")).split(", ")
        else:
            product_manufacturers = []
        if not product_manufacturers:
            product_manufacturers = ['TCCC']

        object_facings = self.get_object_facings(scenes=scenes,
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
                                                 product_manufacturers=product_manufacturers)

        return object_facings

    def calculate_number_facings_near_food(self, params, all_params):
        total_res = 0
        if 'depends on' in params.keys():
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
            if 'depends on' in params.keys():
                depends_on_kpi_name = params.get('depends on')
                for c in params.values()[0]:
                    if c.get('KPI name Eng') == depends_on_kpi_name:
                        scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')
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
    def check_number_of_scenes(self, params):
        """
        This function is used to calculate number of scenes

        """
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula').strip() != 'number of scenes' or p.get('level') == 3:
                continue

            kpi_total_res = 0
            if 'depends on' in p.keys():
                depends_scenes = []
                depends_on_kpi_name = p.get('depends on')
                for c in params.values()[0]:
                    if c.get('KPI name Eng') == depends_on_kpi_name:
                        if c.get('Formula').strip() == 'number of doors with more than Target facings':
                            depends_scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')
                        elif c.get('Formula').strip() == 'number of doors of filled Coolers':
                            depends_scenes = self.check_number_of_doors_of_filled_coolers(c, 'get scenes')
                        elif c.get('Formula').strip() == 'number of coolers with facings target and fullness target':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')
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
                values_list = [str(s) for s in p.get('Values').split(', ')]
                for scene in scenes:
                    try:
                        scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
                        if scene_type in values_list:
                            res = 1
                        else:
                            res = 0
                        kpi_total_res += res
                    except IndexError as e:
                        continue
            elif p.get('Type') == 'LOCATION_TYPE':
                values_list = [str(s) for s in p.get('Values').split(', ')]
                for scene in scenes:
                    try:
                        location_type = self.scif.loc[self.scif['scene_id'] == scene]['location_type'].values[0]
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
                        scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
                        sub_location_type = int(self.templates.loc[self.templates['template_name'] == scene_type][
                                                    'additional_attribute_2'].values[0])
                        if sub_location_type in values_list:
                            res = 1
                        else:
                            res = 0
                        kpi_total_res += res
                    except IndexError as e:
                        continue
            else:  # checking for number of scenes with a complex condition (only certain products/brands/etc)
                p_copy = p.copy()
                p_copy["Formula"] = "number of facings"
                for scene in scenes:
                    if self.calculate_availability(p_copy, scenes=[scene]) > 0:
                        res = 1
                    else:
                        res = 0
                    kpi_total_res += res

            score = self.calculate_score(kpi_total_res, p)
            if 'KPI Weight' in p.keys():
                set_total_res += round(score) * p.get('KPI Weight')
            else:
                set_total_res += round(score)
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            if p.get('level') == 2:
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level2, 'level2')

                if p.get("KPI ID") in params.values()[2]["SESSION LEVEL"]:
                    self.write_to_kpi_facts_hidden(p.get("KPI ID"), None, None, score)

            if not p.get('Children'):
                atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(p.get('KPI name Eng'))
                attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk, atomic_kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level3, 'level3')
        return set_total_res

    @kpi_runtime()
    def check_number_of_doors(self, params):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Type') not in ('DOORS', 'SUB_LOCATION_TYPE', 'LOCATION_TYPE') or p.get('Formula').strip() != 'number of doors':
                continue
            kpi_total_res = self.calculate_number_of_doors(p)
            score = self.calculate_score(kpi_total_res, p)
            set_total_res += round(score) * p.get('KPI Weight')
            # saving to DB
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
            self.write_to_kpi_results_old(attributes_for_level3, 'level3')
            if p.get('level') == 2:
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level2, 'level2')
        return set_total_res

    def calculate_number_of_doors(self, params):
        total_res = 0
        relevant_scenes = self.get_relevant_scenes(params)
        for scene in relevant_scenes:
            res = 0
            scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
            num_of_doors = \
                self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1'].values[0]
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
                    float(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1'].values[0])
                facings = self.calculate_availability(params, scenes=[scene])
                if num_of_doors is not None:
                    res = facings / float(num_of_doors)
                    if res >= int(params.get('target_min')):
                        total_res += num_of_doors
                        scenes_passed.append(scene)
        if scenes_passed:
            if params.get('depends on') == 'filled collers target':
                total_res = 0
                scenes_passed_filled = self.check_number_of_doors_of_filled_coolers(params,
                                                                                    func='get scenes',
                                                                                    proportion=0.9)
                total_scenes_passed = list(set(scenes_passed_filled) & set(scenes_passed))
                for scene in total_scenes_passed:
                    scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
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
            survey_data = self.survey_response.loc[self.survey_response['question_text'] == p.get('Values')]
            if not survey_data['selected_option_text'].empty:
                result = survey_data['selected_option_text'].values[0]
                targets = [d.get(target) if target in d.keys() else target
                           for target in unicode(p.get('Target')).split(", ")]
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
            set_total_res += round(score) * p.get('KPI Weight')
            if p.get('level') == 3:  # todo should be a separate generic function
                kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level3, 'level3')
            elif p.get('level') == 2:
                kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level3, 'level3')
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level2, 'level2')
            else:
                Log.warning('No level indicated for this KPI')

        return set_total_res

    @kpi_runtime()
    def check_facings_sos(self, params):
        """
        This function is used to calculate facing share of shelf

        """
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Type') in ('MAN in CAT', 'MAN', 'BRAND', 'BRAND_IN_CAT', 'SUB_BRAND_IN_CAT') and \
                            p.get('Formula').strip() in ['sos', 'SOS', 'sos with empty']:
                ratio = self.calculate_facings_sos(p)
            else:
                continue
            if p.get('depends on'):
                scenes_info = pd.merge(self.scenes_info, self.templates, on='template_fk')
                values_list = [str(s) for s in p.get('depends on').split(', ')]
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
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
            self.write_to_kpi_results_old(attributes_for_level3, 'level3')
            if p.get('level') == 2:
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level2, 'level2')
            set_total_res += round(score) * p.get('KPI Weight')

            atomic_result = attributes_for_level3['result']
            if p.get("KPI ID") in params.values()[2]["SESSION LEVEL"]:
                self.write_to_kpi_facts_hidden(p.get("KPI ID"), None, atomic_result, score)

        return set_total_res

    def calculate_facings_sos(self, params):
        relevant_scenes = self.get_relevant_scenes(params)
        if params.get('Formula').strip() == 'sos with empty':
            if params.get('Type') == 'MAN':
                pop_filter = (self.scif['scene_id'].isin(relevant_scenes))
                subset_filter = (self.scif[Fd.M_NAME].isin(self.kpi_fetcher.TCCC))
            elif params.get('Type') == 'MAN in CAT':
                pop_filter = ((self.scif[Fd.CAT].isin(params.get('Values'))) &
                              (self.scif['scene_id'].isin(relevant_scenes)))
                subset_filter = (self.scif[Fd.M_NAME].isin(self.kpi_fetcher.TCCC))
            else:
                return 0
        else:
            try:
                values_list = params.get('Values').split(', ')
            except Exception as e:
                values_list = [params.get('Values')]
            if params.get('Type') == 'MAN':
                pop_filter = ((self.scif['scene_id'].isin(relevant_scenes)) &
                              (~self.scif['product_type'].isin(['Empty'])))
                subset_filter = ((self.scif[Fd.M_NAME].isin(self.kpi_fetcher.TCCC)) &
                                 (~self.scif['product_type'].isin(['Empty'])))
            elif params.get('Type') == 'MAN in CAT':
                pop_filter = ((self.scif[Fd.CAT].isin(values_list)) &
                              (self.scif['scene_id'].isin(relevant_scenes)) &
                              (~self.scif['product_type'].isin(['Empty'])))
                subset_filter = ((self.scif[Fd.M_NAME].isin(self.kpi_fetcher.TCCC)) &
                                 (~self.scif['product_type'].isin(['Empty'])))
            elif params.get('Type') == 'SUB_BRAND_IN_CAT':
                pop_filter = ((self.scif[Fd.CAT] == params.get('Category')) &
                              (self.scif['scene_id'].isin(relevant_scenes)) &
                              (~self.scif['product_type'].isin(['Empty'])))
                subset_filter = ((self.scif['sub_brand_name'].isin(values_list)) &
                                 (~self.scif['product_type'].isin(['Empty'])))
            elif params.get('Type') == 'BRAND_IN_CAT':
                pop_filter = ((self.scif[Fd.CAT] == params.get('Category')) &
                              (self.scif['scene_id'].isin(relevant_scenes)) &
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
        return ratio

    def calculate_score(self, kpi_total_res, params):
        """
        This function calculates score according to predefined score functions

        """
        self.update_kpi_scores_and_results(params, {'result': kpi_total_res})
        if 'Target' not in params.keys():
            return kpi_total_res

        if params.get('Target') == 'range of targets':
            if not (params.get('target_min', 0) < kpi_total_res <= params.get('target_max', 100)):
                score = 0
                if kpi_total_res < params.get('target_min', 0):
                    self.update_kpi_scores_and_results(params, {'threshold': params.get('target_min')})
                else:
                    self.update_kpi_scores_and_results(params, {'threshold': params.get('target_max')})
            else:
                self.update_kpi_scores_and_results(params, {'threshold': params.get('target_min')})
                numerator = kpi_total_res - params.get('target_min', 0)
                denominator = params.get('target_max', 1) - params.get('target_min', 0)
                score = (numerator / float(denominator)) * 100
            return score

        elif params.get('Target') == 'targets by guide':
            target = self.kpi_fetcher.get_category_target_by_region(params.get('Values'), self.store_id)
        else:
            target = params.get('Target')
        self.update_kpi_scores_and_results(params, {'threshold': target})
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
            else:
                if kpi_total_res >= target:
                    score = 100
                else:
                    score = 0
        return score

    @kpi_runtime()
    def check_share_of_cch(self, params):
        """
        This function calculates number of SKUs per single scene type

        """
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula').strip() != 'Share of CCH doors which have 98% TCCC facings' and p.get('Formula').strip() != 'number of pure Coolers':
                continue
            scenes = None
            if 'depends on' in p.keys():
                depends_on_kpi_name = p.get('depends on')
                for c in params.values()[0]:
                    if c.get('KPI name Eng') == depends_on_kpi_name:
                        if c.get('Formula').strip() == 'number of doors of filled Coolers':
                            scenes = self.check_number_of_doors_of_filled_coolers(c, 'get scenes')
                        elif c.get('Formula').strip() == 'number of doors with more than Target facings':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')
                        elif c.get('Formula').strip() == 'number of coolers with facings target and fullness target':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')
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
                score = self.calculate_share_of_cch(p, scenes, sos=False)
                # if score >= 1:
                #     score=1
            else:
                score = self.calculate_share_of_cch(p, scenes)
            atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(p.get('KPI name Eng'))
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            if 'KPI Weight' in p.keys():
                set_total_res += round(score) * p.get('KPI Weight')
            else:
                set_total_res += round(score)
            # saving to DB
            attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk, atomic_kpi_fk)
            self.write_to_kpi_results_old(attributes_for_level3, 'level3')
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_kpi_results_old(attributes_for_level2, 'level2')

            if p.get("KPI ID") in params.values()[2]["SESSION LEVEL"]:
                self.write_to_kpi_facts_hidden(p.get("KPI ID"), None, None, score)

        return set_total_res

    def check_number_of_doors_of_filled_coolers(self, params, func=None, proportion=None):
        """
        This function calculates number of doors of filled Coolers

        """
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
        """
        This function calculates number of doors of filled Coolers

        """
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula').strip() != 'number of scenes with have at least target amount of facings':
                continue
            scenes = self.get_relevant_scenes(p)
            score = self.calculate_number_of_scenes_given_facings(p, scenes)
            atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(p.get('KPI name Eng'))
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            if 'KPI Weight' in p.keys():
                set_total_res += round(score) * p.get('KPI Weight')
            else:
                set_total_res += round(score)
            # saving to DB
            attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk, atomic_kpi_fk)
            self.write_to_kpi_results_old(attributes_for_level3, 'level3')
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_kpi_results_old(attributes_for_level2, 'level2')
        return set_total_res

    def calculate_share_of_cch(self, p, scenes, sos = True):
        sum_of_passed_doors = 0
        sum_of_passed_scenes = 0
        sum_of_all_doors = 0
        for scene in scenes:
            products_of_tccc = self.scif[(self.scif['scene_id'] == scene) &
                                         (self.scif['manufacturer_name'] == 'TCCC') &
                                         (self.scif['location_type'] == p.get('Locations to include')) &
                                         (self.scif['product_type']!='Empty')]['facings'].sum()
            all_products = self.scif[(self.scif['scene_id'] == scene) &
                                     (self.scif['location_type'] == p.get('Locations to include')) &
                                     (self.scif['product_type'] != 'Empty')]['facings'].sum()
            if products_of_tccc == 0:
                proportion = 0
            else:
                proportion = products_of_tccc / all_products
            scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
            if any(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1']):
                num_of_doors = \
                    float(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1'].values[0])
                sum_of_all_doors += num_of_doors
            else:
                num_of_doors = 1
                sum_of_all_doors += num_of_doors
            if proportion > 0.98:
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
                proportion = products_of_tccc / all_products
            else:
                proportion = 0
            scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
            if any(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1']):
                num_of_doors = \
                    float(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1'].values[0])
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
    def check_number_of_skus_per_door_range(self, params):
        """
        This function calculates number of SKUs per single scene type

        """
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula').strip() != 'number of SKU per Door RANGE':
                continue
            scenes = None
            if 'depends on' in p.keys():
                depends_on_kpi_name = p.get('depends on')
                for c in params.values()[0]:
                    if c.get('KPI name Eng') == depends_on_kpi_name:
                        if c.get('Formula').strip() == 'number of doors of filled Coolers':
                            scenes = self.check_number_of_doors_of_filled_coolers(c, 'get scenes')
                        elif c.get('Formula').strip() == 'number of coolers with facings target and fullness target':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')
                            scenes = self.calculate_number_of_doors_of_filled_coolers(c, scenes,
                                                                                      func='get scenes',
                                                                                      proportion_param=0.9)
                        else:
                            scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')
                if not scenes:
                    if p.get('level') == 2:
                        scenes = []
                    else:
                        return 0
            else:
                scenes = self.get_relevant_scenes(p)
            score = self.calculate_number_of_skus_per_door_range(p, scenes)
            atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(p.get('KPI name Eng'))
            if p.get('level') == 3:
                return score
            # saving to DB
            if p.get('level') == 2:
                kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                if 'KPI Weight' in p.keys():
                    set_total_res += round(score) * p.get('KPI Weight')
                else:
                    set_total_res += round(score)
                attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk, atomic_kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level3, 'level3')
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level2, 'level2')

                if p.get("KPI ID") in params.values()[2]["SESSION LEVEL"]:
                    self.write_to_kpi_facts_hidden(p.get("KPI ID"), None, None, score)

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
                    float(self.templates[self.templates['template_name'] == scene_type]['additional_attribute_1'].values[0])
            else:
                num_of_doors = 1
            sum_of_all_doors += num_of_doors
            if p.get('target_min') <= eans_count / num_of_doors <= p.get('target_max'):
                sum_of_passed_doors += num_of_doors
        self.update_kpi_scores_and_results(p, {'result': sum_of_passed_doors})
        if sum_of_all_doors:
            return (sum_of_passed_doors / sum_of_all_doors) * 100
        return 0

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
            set_total_res += round(score) * p.get('KPI Weight')

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
                    if child.get('KPI ID') in [int(kpi) for kpi in p.get('Children').split(', ')]:
                        res = self.calculate_number_of_skus_in_single_scene_type(params, child, kpi_fk)
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
            attributes_for_table2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_kpi_results_old(attributes_for_table2, 'level2')
        attributes_for_table3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
        self.write_to_kpi_results_old(attributes_for_table3, 'level3')

        return score

    def calculate_lead_sku(self, params, scenes=None):
        if not scenes:
            scenes = self.get_relevant_scenes(params)
        if params.get('Product Category'):
            category = [str(params.get('Product Category'))]
            relevant_products_and_facings = self.scif[
                (self.scif['scene_id'].isin(scenes)) & ~(self.scif['product_type'].isin(['Empty', 'Other'])) &
                (self.scif['category'].isin(category))]
        else:
            relevant_products_and_facings = self.scif[
                (self.scif['scene_id'].isin(scenes)) & ~(self.scif['product_type'].isin(['Empty', 'Other']))]
        all_products_by_ean_code = relevant_products_and_facings.groupby(['product_ean_code'])['facings'].sum()
        if ', ' not in str(params.get('Values')):
            tested_sku = [str(params.get('Values'))]
        else:
            tested_sku = [str(s) for s in params.get('Values').split(', ')]
        tested_facings = \
            relevant_products_and_facings[relevant_products_and_facings['product_ean_code'].isin(tested_sku)][
                'facings'].sum()
        self.update_kpi_scores_and_results(params, {'result': tested_facings})
        if not all_products_by_ean_code.empty:
            if sum(tested_facings < all_products_by_ean_code) == 0:
                return tested_facings
            else:
                return 0
        else:
            return 0

    def calculate_number_of_scenes(self, p):
        """
        This function is used to calculate number of scenes

        """
        kpi_total_res = 0
        scenes = self.get_relevant_scenes(p)
        if p.get('Type') == 'SCENES':
            if p.get('Values'):
                values_list = [str(s) for s in p.get('Values').split(', ')]
                for scene in scenes:
                    try:
                        scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
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
            values_list = [str(s) for s in p.get('Values').split(', ')]
            for scene in scenes:
                try:
                    location_type = self.scif.loc[self.scif['scene_id'] == scene]['location_type'].values[0]
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
                    scene_type = self.scif.loc[self.scif['scene_id'] == scene]['template_name'].values[0]
                    sub_location_type = int(self.templates.loc[self.templates['template_name'] == scene_type][
                                                'additional_attribute_2'].values[0])
                    if sub_location_type in values_list:
                        res = 1
                    else:
                        res = 0
                    kpi_total_res += res
                except IndexError as e:
                    continue
        else:  # checking for number of scenes with a complex condition (only certain products/brands/etc)
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

    def calculate_tccc_40(self, params):
        facings = self.calculate_availability(params)
        return float(facings) / 40

    @kpi_runtime()
    def check_customer_cooler_doors(self, params):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula').strip() != "facings TCCC/40":
                continue
            kpi_total_res = self.calculate_tccc_40(p)
            score = self.calculate_score(kpi_total_res, p)
            set_total_res += round(score) * p.get('KPI Weight')
            # writing to DB
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
            self.write_to_kpi_results_old(attributes_for_level3, 'level3')
            if p.get('level') == 2:
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level2, 'level2')
        return set_total_res

    @kpi_runtime()
    def check_sum_atomics(self, params):
        """

        :param params:
        :return:
        """
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula').strip() != "sum of atomic KPI result" or not p.get("Children"):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = map(int, str(p.get("Children")).replace(" ", "").replace(",", "\n").replace("\n\n", "\n").split("\n"))
            kpi_total = 0
            atomic_result_total = 0
            for c in params.values()[0]:
                if c.get("KPI ID") in children:
                    if c.get("Formula").strip() == "number of facings":
                        atomic_res = self.calculate_availability(c)
                    elif c.get("Formula").strip() == "number of doors with more than Target facings":
                        atomic_res = self.calculate_number_of_doors_more_than_target_facings(c, 'sum of doors')
                    elif c.get("Formula").strip() == "facings TCCC/40":
                        atomic_res = self.calculate_tccc_40(c)
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
                    atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(c.get('KPI name Eng'))
                    attributes_for_level3 = self.create_attributes_for_level3_df(c, atomic_score, kpi_fk, atomic_kpi_fk)
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
                score = (kpi_total/self.attr15) * 100
                if score > 100:
                    score = 100
            else:
                score = 0
            if 'KPI Weight' in p.keys():
                set_total_res += round(score) * p.get('KPI Weight')
            else:
                set_total_res += score
            # saving to DB
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_kpi_results_old(attributes_for_level2, 'level2')

            if p.get("KPI ID") in params.values()[2]["SESSION LEVEL"]:
                self.write_to_kpi_facts_hidden(p.get("KPI ID"), None, atomic_result_total, score)

        return set_total_res

    @kpi_runtime()
    def check_atomic_passed(self, params):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula').strip() != "number of atomic KPI Passed" or not p.get("Children"):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = map(int, str(p.get("Children")).replace(" ", "").replace(",", "\n").replace("\n\n", "\n").split("\n"))
            kpi_total = 0
            for c in params.values()[0]:
                if c.get("KPI ID") in children:
                    atomic_score = -1
                    if c.get("Formula").strip() == "number of facings" or c.get("Formula").strip() == "number of SKUs":
                        atomic_res = self.calculate_availability(c, all_params =params)
                    elif c.get("Formula").strip() == "number of sub atomic KPI Passed":
                        atomic_res = self.calculate_sub_atomic_passed(c, params, parent = p)
                    elif c.get("Formula").strip() == "Lead SKU":
                        atomic_res = self.calculate_lead_sku(c)
                        if not atomic_res:
                            atomic_score = 0
                        else:
                            atomic_score = 100
                    elif c.get("Formula").strip() == "number of scenes":
                        atomic_res = self.calculate_number_of_scenes(c)
                    elif c.get("Formula").strip() == "number of facings near food":
                        atomic_res = self.calculate_number_facings_near_food(c, params)
                    elif c.get("Formula").strip() == "number of doors with more than Target facings":
                        atomic_res = self.calculate_number_of_doors_more_than_target_facings(c, 'sum of doors')
                    elif c.get("Formula").strip() == "number of doors of filled Coolers":
                        atomic_res = self.check_number_of_doors_of_filled_coolers(c)
                    elif c.get("Formula").strip() == "number of pure Coolers":
                        scenes = self.get_relevant_scenes(c)
                        atomic_res = self.calculate_share_of_cch(c, scenes, sos=False)
                    elif c.get("Formula").strip() == "number of filled Coolers (scenes)":
                        scenes_list = self.check_number_of_doors_of_filled_coolers(c, func='get scenes')
                        atomic_res = len(scenes_list)
                    elif c.get("Formula").strip() == "number of SKU per Door RANGE":
                        atomic_score = self.check_number_of_skus_per_door_range(params)
                    elif c.get("Formula").strip() == "Scenes with no tagging":
                        atomic_res = self.check_number_of_scenes_no_tagging(c, level=3)
                    else:
                        # print "the atomic's formula is ", c.get('Formula').strip()
                        atomic_res = 0
                    if atomic_res == -1:
                        continue
                    if atomic_score == -1:
                        atomic_score = self.calculate_score(atomic_res, c)
                    # write to DB
                    atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(c.get('KPI name Eng'))
                    attributes_for_level3 = self.create_attributes_for_level3_df(c, atomic_score, kpi_fk, atomic_kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')
                    if atomic_score > 0:
                        kpi_total += 1
            score = self.calculate_score(kpi_total, p)
            if 'KPI Weight' in p.keys():
                set_total_res += round(score) * p.get('KPI Weight')
            else:
                set_total_res += score

            # saving to DB
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_kpi_results_old(attributes_for_level2, 'level2')

            if p.get("KPI ID") in params.values()[2]["SESSION LEVEL"]:
                self.write_to_kpi_facts_hidden(p.get("KPI ID"), None, kpi_total, score)

        return set_total_res

    @kpi_runtime()
    def check_atomic_passed_on_the_same_scene(self, params):
        set_total_res = 0
        self.passed_scenes_per_kpi = {}
        for p in params.values()[0]:
            if p.get('Formula').strip() != "number of atomic KPI Passed on the same scene" or not p.get("Children"):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = map(int, str(p.get("Children")).replace(" ", "").replace(",", "\n").replace("\n\n", "\n").split("\n"))
            info_by_kpi_id = self.build_dict(params.values()[0], 'KPI ID')
            if 'depends on' in p.keys():
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
                        atomic_res = self.calculate_sub_atomic_passed(c, params, [scene], parent = p, same_scene = True)
                    elif c.get("Formula").strip() == "Lead SKU":
                        atomic_res = self.calculate_lead_sku(c, [scene])
                        if not atomic_res:
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
                    favorite_scene = self.get_favorite_scene(closest_to_pass_scenes, scenes_kpi_info)
            kpi_total = 0
            for child in children:
                atomic_score = -1
                c = info_by_kpi_id.get(child)
                if favorite_scene:
                    if c.get("Formula").strip() == "number of facings":
                        atomic_res = self.calculate_availability(c, [favorite_scene])
                    elif c.get("Formula").strip() == "number of sub atomic KPI Passed":
                        atomic_res = self.calculate_sub_atomic_passed(c, params, [favorite_scene], parent=p)
                    elif c.get("Formula").strip() == "Lead SKU":
                        atomic_res = self.calculate_lead_sku(c, [favorite_scene])
                        if not atomic_res:
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
                        sub_atomic_children = map(int, str(c.get("Children")).replace(" ", "").replace(",", "\n").replace("\n\n", "\n").split("\n"))
                        for sub_atomic in sub_atomic_children:
                            sub_atomic_info = info_by_kpi_id.get(sub_atomic)
                            atomic_res = 0
                            atomic_score = 0
                            self.calculate_score(atomic_res, sub_atomic_info)
                            kpi_name = sub_atomic_info.get('KPI name Eng')
                            atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(kpi_name)
                            attributes_for_level3 = self.create_attributes_for_level3_df(sub_atomic_info, atomic_score, kpi_fk,
                                                                                         atomic_kpi_fk)
                            self.write_to_kpi_results_old(attributes_for_level3, 'level4')
                    atomic_res = 0
                    atomic_score = 0
                    self.calculate_score(atomic_res, c)

                kpi_name = c.get('KPI name Eng')
                atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(kpi_name)
                attributes_for_level3 = self.create_attributes_for_level3_df(c, atomic_score, kpi_fk,
                                                                             atomic_kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level3, 'level3')
            score = self.calculate_score(kpi_total, p)
            if 'KPI Weight' in p.keys():
                set_total_res += round(score) * p.get('KPI Weight')
            else:
                set_total_res += score
            # saving to DB
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_kpi_results_old(attributes_for_level2, 'level2')

            if p.get("KPI ID") in params.values()[2]["SESSION LEVEL"]:
                self.write_to_kpi_facts_hidden(p.get("KPI ID"), None, None, score)

        return set_total_res

    def calculate_sub_atomic_passed(self, params, all_params, scenes=[], parent=None, same_scene=None):
        if not scenes:
            if 'depends on' in params.keys():
                if params['depends on'] == 'scene type':
                    scenes = self.get_relevant_scenes(params)
                    if not scenes:
                        return -1
                else:
                    depends_on_kpi_name = params.get('depends on')
                    doors = 0
                    for c in all_params.values()[0]:
                        if c.get('KPI name Eng') == depends_on_kpi_name:
                            doors = self.calculate_number_of_doors_more_than_target_facings(c, 'get doors')
                            break
                    if doors < 2:
                        return -1
        children = map(int, str(params.get("Children")).replace(" ", "").replace(",", "\n").replace("\n\n", "\n").split("\n"))
        total_res = 0
        for c in all_params.values()[0]:
            if c.get("KPI ID") in children:
                sub_atomic_score = -1
                if c.get("Formula").strip() == "number of facings":
                    sub_atomic_res = self.calculate_availability(c, scenes)
                elif c.get("Formula").strip() == "Lead SKU":
                    sub_atomic_res = self.calculate_lead_sku(c, scenes)
                    if not sub_atomic_res:
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
                sub_atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(c.get('KPI name Eng'))
                attributes_for_level4 = self.create_attributes_for_level3_df(c, sub_atomic_score, kpi_fk,
                                                                             sub_atomic_kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level4, 'level4')
        return total_res

    @kpi_runtime()
    def check_weighted_average(self, params):
        set_total_res = 0
        for p in params.values()[0]:
            if not (p.get('Formula').strip() in ("Weighted Average", "average of atomic KPI Score", "Weighted Sum")
                    and p.get("Children")):
                continue
            scenes = []
            if 'depends on' in params.keys():
                depends_on_kpi_name = params.get('depends on')
                for c in params.values()[0]:
                    if c.get('KPI name Eng') == depends_on_kpi_name:
                        if c.get('Formula').strip() == 'number of doors with more than Target facings':
                            scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = map(int, str(p.get("Children")).replace(" ", "").replace(",", "\n").replace("\n\n", "\n").split("\n"))
            kpi_total = 0
            kpi_total_weight = 0
            for c in params.values()[0]:
                if c.get("KPI ID") in children:
                    if c.get("Formula").strip() == "number of facings":
                        atomic_res = self.calculate_availability(c, scenes=scenes)
                    elif c.get("Formula").strip() == "number of sub atomic KPI Passed":
                        atomic_res = self.calculate_sub_atomic_passed(c, params, parent=p, scenes=scenes)
                    elif c.get("Formula").strip() == "check_number_of_scenes_with_facings_target":
                        atomic_res = self.calculate_number_of_scenes_with_target(c, scenes=scenes)
                    else:
                        atomic_res = -1
                        # print "Weighted Average", c.get("Formula").strip()
                    if atomic_res == -1:
                        continue
                    atomic_score = self.calculate_score(atomic_res, c)
                    if p.get('Formula').strip() in ("Weighted Average", "Weighted Sum"):
                        kpi_total += atomic_score * c.get('KPI Weight')
                        kpi_total_weight += c.get('KPI Weight')
                    else:
                        kpi_total += atomic_score
                        kpi_total_weight += 1
                    # write to DB
                    atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(c.get('KPI name Eng'))
                    attributes_for_level3 = self.create_attributes_for_level3_df(c, atomic_score, kpi_fk, atomic_kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')
            if kpi_total_weight:
                if p.get('Formula').strip() != "Weighted Sum":
                    kpi_total /= kpi_total_weight
            else:
                kpi_total = 0
            kpi_score = self.calculate_score(kpi_total, p)
            if 'KPI Weight' in p.keys():
                set_total_res += round(kpi_score) * p.get('KPI Weight')
            else:
                set_total_res += round(kpi_score) * kpi_total_weight
            # saving to DB
            if kpi_fk:
                attributes_for_level2 = self.create_attributes_for_level2_df(p, kpi_score, kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level2, 'level2')
        return set_total_res

    @kpi_runtime()
    def check_number_of_scenes_no_tagging(self, params, level=None):
        scenes_info = pd.merge(self.scenes_info, self.templates, on='template_fk')
        if level == 3:
            if params.get('Scenes to include'):
                values_list = [str(s) for s in params.get('Scenes to include').split(', ')]
                number_relevant_scenes = scenes_info['template_name'].isin(values_list).sum()
                return number_relevant_scenes
            else:
                return 0
        else:  # level 2
            set_total_res = 0
            number_relevant_scenes = 0
            scenes = []
            for p in params.values()[0]:
                if p.get('Formula').strip() != "Scenes with no tagging":
                    continue
                if 'depends on' in p.keys():
                    depends_on_kpi_name = p.get('depends on')
                    for c in params.values()[0]:
                        if c.get('KPI name Eng') == depends_on_kpi_name:
                            if c.get('Formula').strip() == 'number of doors with more than Target facings':
                                scenes = self.calculate_number_of_doors_more_than_target_facings(c, 'get scenes')
                            elif c.get('Formula').strip() == 'number of doors of filled Coolers':
                                scenes = self.check_number_of_doors_of_filled_coolers(c, 'get scenes')
                            break
                    if len(scenes) >= 1:
                        flag = 0
                        final_scenes = scenes_info
                        if p.get('Scenes to include'):
                            scenes_values_list = [str(s) for s in p.get('Scenes to include').split(', ')]
                            final_scenes = scenes_info['template_name'].isin(scenes_values_list)
                            flag = 1
                        if p.get('Locations to include'):
                            location_values_list = [str(s) for s in p.get('Locations to include').split(', ')]
                            if flag:
                                if sum(final_scenes):
                                    final_scenes = scenes_info[final_scenes]['location_type'].isin(location_values_list)
                            else:
                                final_scenes = final_scenes['location_type'].isin(location_values_list)
                        number_relevant_scenes = final_scenes.sum()
                else:
                    if p.get('Scenes to include'):
                        values_list = [str(s) for s in p.get('Scenes to include').split(', ')]
                        number_relevant_scenes = scenes_info['template_name'].isin(values_list).sum()

                score = self.calculate_score(number_relevant_scenes, p)
                if 'KPI Weight' in p.keys():
                    set_total_res += round(score) * p.get('KPI Weight')
                else:
                    set_total_res += round(score)
                kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)

                if p.get("KPI ID") in params.values()[2]["SESSION LEVEL"]:
                    self.write_to_kpi_facts_hidden(p.get("KPI ID"), None, None, score)

                self.write_to_kpi_results_old(attributes_for_level2, 'level2')
                # save to level 3
                atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(p.get('KPI name Eng'))
                attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk, atomic_kpi_fk)
                self.write_to_kpi_results_old(attributes_for_level3, 'level3')
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
        self.rds_conn.disconnect_rds()
        self.rds_conn = self.rds_connection()
        cur = self.rds_conn.db.cursor()
        delete_queries = self.kpi_fetcher.get_delete_session_results(self.session_uid, self.session_fk)
        for query in delete_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
        self.rds_conn.disconnect_rds()
        self.rds_conn = self.rds_connection()
        cur = self.rds_conn.db.cursor()
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
        self.rds_conn.disconnect_rds()
        self.rds_conn = self.rds_connection()
        cur = self.rds_conn.db.cursor()
        for query in set(self.gaps_queries):
            cur.execute(query)
        self.rds_conn.db.commit()
        self.rds_conn.disconnect_rds()
        self.rds_conn = self.rds_connection()
        cur = self.rds_conn.db.cursor()
        top_sku_queries = self.merge_insert_queries(self.top_sku_queries)
        for query in top_sku_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
        self.rds_conn.disconnect_rds()
        return

    def write_to_kpi_facts_hidden(self, kpi_id, scene, result, score):
        self.kpi_facts_hidden.append({"KPI ID": kpi_id, "scene_fk": scene, "result": result, "score": score})
        return

    @kpi_runtime()
    def prepare_hidden_set(self, params, kpi_set_name):
        # table3 = pd.DataFrame([])  # for debugging

        self.set_kpi_set(kpi_set_name, INTEGRATION)
        kpi_df = self.kpi_fetcher.kpi_static_data

        kpi_facts = []
        for p in params.values()[1]:
            atomic_kpi_id = p.get("KPI ID")
            atomic_kpi_name = p.get("KPI name Eng").upper().replace(" ", "_")
            atomic_kpi = kpi_df[kpi_df["atomic_kpi_name"] == atomic_kpi_name]["atomic_kpi_fk"]
            if atomic_kpi.size > 0:
                atomic_kpi_fk = atomic_kpi.values[0]
            else:
                continue

            if p.get("Formula").strip() == "number of KPI Passed" and p.get("Type") == "SESSION LEVEL":  # session level
                result = 0
                for k in self.kpi_facts_hidden:
                    if k.get("KPI ID") in p.get("Children List"):
                        if k.get("score") == 100:
                            result += 1
                kpi_facts.append({"id": atomic_kpi_id, "name": atomic_kpi_name, "display_text": atomic_kpi_name,
                                  "atomic_kpi_fk": atomic_kpi_fk, "result": result,
                                  "format": p.get("Result Format")})

            elif p.get("Formula").strip() == "KPI score" and p.get("Type") == "SESSION LEVEL":  # session level
                for k in self.kpi_facts_hidden:
                    if k.get("KPI ID") in p.get("Children List"):
                        result = k.get("score")
                        kpi_facts.append({"id": atomic_kpi_id, "name": atomic_kpi_name, "display_text": atomic_kpi_name,
                                          "atomic_kpi_fk": atomic_kpi_fk, "result": result,
                                          "format": p.get("Result Format")})

            elif p.get("Formula").strip() == "sum of KPI scores" and p.get("Type") == "SESSION LEVEL":  # session level
                result = 0
                for k in self.kpi_facts_hidden:
                    if k.get("KPI ID") in p.get("Children List"):
                        result += k.get("score")
                kpi_facts.append({"name": atomic_kpi_name, "display_text": atomic_kpi_name,
                                  "atomic_kpi_fk": atomic_kpi_fk, "result": result,
                                  "format": p.get("Result Format")})

            elif p.get("Formula").strip() == "KPI result" and p.get("Type") == "SESSION LEVEL":  # session level
                for k in self.kpi_facts_hidden:
                    if k.get("KPI ID") in p.get("Children List"):
                        result = k.get("result")
                        kpi_facts.append({"id": atomic_kpi_id, "name": atomic_kpi_name, "display_text": atomic_kpi_name,
                                          "atomic_kpi_fk": atomic_kpi_fk, "result": result,
                                          "format": p.get("Result Format")})

            elif p.get("Formula").strip() == "sum of KPI results" and p.get("Type") == "SESSION LEVEL":  # session level
                result = 0
                for k in self.kpi_facts_hidden:
                    if k.get("KPI ID") in p.get("Children List"):
                        result += k.get("result")
                kpi_facts.append({"id": atomic_kpi_id, "name": atomic_kpi_name, "display_text": atomic_kpi_name,
                                  "atomic_kpi_fk": atomic_kpi_fk, "result": result,
                                  "format": p.get("Result Format")})

            elif p.get("Formula").strip() == "Passed or Failed Value" and p.get("Type") == "SESSION LEVEL":  # session level
                for k in self.kpi_facts_hidden:
                    if k.get("KPI ID") in p.get("Children List"):
                        passed_failed = str(p.get("Values")).replace(" ", "").replace(",", "\n").replace("\n\n", "\n").split("\n")
                        if k.get("score") == 100:
                            result = passed_failed[0]
                        elif len(passed_failed) > 1:
                            result = passed_failed[1]
                        else:
                            result = ""
                        kpi_facts.append({"id": atomic_kpi_id, "name": atomic_kpi_name, "display_text": atomic_kpi_name,
                                          "atomic_kpi_fk": atomic_kpi_fk, "result": result,
                                          "format": p.get("Result Format")})

            elif p.get("Formula").strip() == "Value" and p.get("Type") == "SCENE LEVEL":  # scene level
                scenes = self.get_relevant_scenes(params)
                for scene in scenes:
                    scene_uid = self.scenes_info[self.scenes_info['scene_fk'] == scene]['scene_uid'].values[0]
                    kpi_facts.append({"id": atomic_kpi_id, "name": atomic_kpi_name, "display_text": atomic_kpi_name + "@" + scene_uid,
                                      "atomic_kpi_fk": atomic_kpi_fk, "result": p.get("Values"),
                                      "scene_uid": scene_uid, "scene_id": scene,
                                      "format": p.get("Result Format")})

            elif p.get("Formula").strip() == "Attribute" and p.get("Type") == "SCENE LEVEL":  # scene level
                scenes = self.get_relevant_scenes(params)
                if p.get("Values") == 'template.additional_attribute_1':
                    for scene in scenes:
                        scene_uid = self.scenes_info[self.scenes_info['scene_fk'] == scene]['scene_uid'].values[0]
                        template = self.scenes_info[self.scenes_info['scene_fk'] == scene]['template_fk'].values[0]
                        result = self.templates[self.templates['template_fk'] == template]['additional_attribute_1'].values[0]
                        kpi_facts.append({"id": atomic_kpi_id, "name": atomic_kpi_name, "display_text": atomic_kpi_name + "@" + scene_uid,
                                          "atomic_kpi_fk": atomic_kpi_fk, "result": result,
                                          "scene_uid": scene_uid, "scene_id": scene,
                                          "format": p.get("Result Format")})

            elif p.get("Formula").strip() == "Passed or Failed Value" and p.get("Type") == "SCENE LEVEL":  # scene level
                scenes = self.get_relevant_scenes(params)
                for scene in scenes:
                    scene_uid = self.scenes_info[self.scenes_info['scene_fk'] == scene]['scene_uid'].values[0]
                    for k in self.kpi_facts_hidden:
                        if k.get("KPI ID") in p.get("Children List") and k.get("scene_fk") == scene:
                            passed_failed = str(p.get("Values")).replace(" ", "").replace(",", "\n").replace("\n\n", "\n").split("\n")
                            if k.get("score") == 100:
                                result = passed_failed[0]
                            elif len(passed_failed) > 1:
                                result = passed_failed[1]
                            else:
                                result = ""
                            kpi_facts.append({"id": atomic_kpi_id, "name": atomic_kpi_name, "display_text": atomic_kpi_name + "@" + scene_uid,
                                              "atomic_kpi_fk": atomic_kpi_fk, "result": result,
                                              "scene_uid": scene_uid, "scene_id": scene,
                                              "format": p.get("Result Format")})

        kpi_set_fk = kpi_df['kpi_set_fk'].values[0]
        kpi_fk = kpi_df['kpi_fk'].values[0]
        kpi_name = kpi_set_name
        for kf in kpi_facts:
            if not kf.get("result") is None:
                if kf.get("format") == "Integer":
                    result_format = 'NUM'
                    result_formatted = str(int(kf.get("result")))
                elif kf.get("format") == "Decimal.2":
                    result_format = 'NUM'
                    result_formatted = format(float(kf.get("result")), ".2f")
                else:
                    result_format = 'STR'
                    result_formatted = str(kf.get("result"))

                attributes_for_table3 = pd.DataFrame([(kf.get("display_text"),
                                                       self.session_uid,
                                                       kpi_set_name,
                                                       self.store_id,
                                                       self.visit_date.isoformat(),
                                                       dt.datetime.utcnow().isoformat(),
                                                       None,
                                                       kpi_fk,
                                                       kf.get("atomic_kpi_fk"),
                                                       None,
                                                       result_formatted,
                                                       kf.get("name"))],
                                                     columns=['display_text',
                                                              'session_uid',
                                                              'kps_name',
                                                              'store_fk',
                                                              'visit_date',
                                                              'calculation_time',
                                                              'score',
                                                              'kpi_fk',
                                                              'atomic_kpi_fk',
                                                              'threshold',
                                                              'result',
                                                              'name'])
                self.write_to_kpi_results_old(attributes_for_table3, 'level3')
                self.update_kpi_scores_and_results(
                    {'KPI ID': kf.get('id'),
                     'KPI name Eng': kf.get('name'),
                     'KPI name Rus': kf.get('name')},
                    {'scene_uid': kf.get('scene_uid'),
                     'scene_id': kf.get('scene_id'),
                     'result': result_formatted,
                     'format': result_format,
                     'level': 1,
                     'parent': 0})

                # table3 = table3.append(attributes_for_table3)  # for debugging

        attributes_for_table2 = pd.DataFrame([(self.session_uid,
                                               self.store_id,
                                               self.visit_date.isoformat(),
                                               kpi_fk,
                                               kpi_name,
                                               None)],
                                             columns=['session_uid',
                                                      'store_fk',
                                                      'visit_date',
                                                      'kpi_fk',
                                                      'kpk_name',
                                                      'score'])
        self.write_to_kpi_results_old(attributes_for_table2, 'level2')

        attributes_for_table1 = pd.DataFrame([(kpi_set_name,
                                               self.session_uid,
                                               self.store_id,
                                               self.visit_date.isoformat(),
                                               None,
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
             'KPI name Rus': kpi_set_name},
            {'level': 0,
             'parent': None})
        self.write_to_kpi_results_new()

        return

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

    def create_attributes_for_level2_df(self, params, score, kpi_fk, update_kpi_scores_and_results=True):
        """
        This function creates a data frame with all attributes needed for saving in level 2 tables

        """
        score = round(score)
        attributes_for_table2 = pd.DataFrame([(self.session_uid, self.store_id,
                                               self.visit_date.isoformat(), kpi_fk,
                                               params.get('KPI name Eng').replace("'", "\\'"), score)],
                                             columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk',
                                                      'kpk_name', 'score'])

        if update_kpi_scores_and_results:
            self.update_kpi_scores_and_results(params, {'threshold': params.get('Weight'),
                                                        'result': score,
                                                        'weight': params.get('Weight'),
                                                        'score': score if params.get('Weight') is None
                                                        else score*params.get('Weight')})

        return attributes_for_table2

    def create_attributes_for_level3_df(self, params, score, kpi_fk, atomic_kpi_fk=None, update_kpi_scores_and_results=True):
        """
        This function creates a data frame with all attributes needed for saving in level 3 tables

        """
        if isinstance(score, tuple):
            score, result, threshold = score
        else:
            result = threshold = None
        score = round(score)
        self.update_kpi_scores_and_results(params, {})
        result = self.kpi_scores_and_results[self.kpi_set_type][params.get("KPI ID")].get('result')\
            if result is None else result
        result = result if result else 0
        threshold = self.kpi_scores_and_results[self.kpi_set_type][params.get("KPI ID")].get('threshold')\
            if threshold is None else threshold
        threshold = threshold if threshold else 0

        atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(params.get('KPI name Eng'))\
            if atomic_kpi_fk is None else atomic_kpi_fk
        if params.get('KPI name Rus'):
            attributes_for_table3 = pd.DataFrame([(params.get('KPI name Rus').encode('utf-8').replace("'", "\\'"),
                                                   self.session_uid,
                                                   self.kpi_set_name,
                                                   self.store_id,
                                                   self.visit_date.isoformat(),
                                                   dt.datetime.utcnow().isoformat(),
                                                   score,
                                                   kpi_fk,
                                                   atomic_kpi_fk,
                                                   threshold,
                                                   result,
                                                   params.get('KPI name Eng').replace("'", "\\'"))],
                                                 columns=['display_text',
                                                          'session_uid',
                                                          'kps_name',
                                                          'store_fk',
                                                          'visit_date',
                                                          'calculation_time',
                                                          'score',
                                                          'kpi_fk',
                                                          'atomic_kpi_fk',
                                                          'threshold',
                                                          'result',
                                                          'name'])
        else:
            attributes_for_table3 = pd.DataFrame([(params.get('KPI name Eng').replace("'", "\\'"),
                                                   self.session_uid,
                                                   self.kpi_set_name,
                                                   self.store_id,
                                                   self.visit_date.isoformat(),
                                                   dt.datetime.utcnow().isoformat(),
                                                   score,
                                                   kpi_fk,
                                                   atomic_kpi_fk,
                                                   threshold,
                                                   result,
                                                   params.get('KPI name Eng').replace("'", "\\'"))],
                                                 columns=['display_text',
                                                          'session_uid',
                                                          'kps_name',
                                                          'store_fk',
                                                          'visit_date',
                                                          'calculation_time',
                                                          'score',
                                                          'kpi_fk',
                                                          'atomic_kpi_fk',
                                                          'threshold',
                                                          'result',
                                                          'name'])

        if update_kpi_scores_and_results:
            self.update_kpi_scores_and_results(params, {'threshold': threshold,
                                                        'result': result,
                                                        'weight': params.get('Weight'),
                                                        'score': score})
            if params.get('KPI name Eng') != 3:
                self.update_kpi_scores_and_results(params, {'level': 3,
                                                            'parent': params.get('KPI ID')})

        return attributes_for_table3

    @kpi_runtime()
    def check_number_of_doors_given_sos(self, params):
        set_total_res = 0
        for p in params.values()[0]:
            if p.get('Formula').strip() != "number of doors given sos" or not p.get("Children"):
                continue
            kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
            children = map(int, str(p.get("Children")).replace(" ", "").replace(",", "\n").replace("\n\n", "\n").split("\n"))
            first_atomic_score = second_atomic_res = 0
            for c in params.values()[0]:
                if c.get("KPI ID") in children and c.get("Formula").strip() == "atomic sos":
                    first_atomic_res = self.calculate_facings_sos(c)
                    first_atomic_score = self.calculate_score(first_atomic_res, c)
                    # write to DB
                    attributes_for_level3 = self.create_attributes_for_level3_df(c, first_atomic_score, kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')
            for c in params.values()[0]:
                if c.get("KPI ID") in children and c.get("Formula").strip() == "atomic number of doors":
                    second_atomic_res = self.calculate_number_of_doors(c)
                    second_atomic_score = self.calculate_score(second_atomic_res, c)
                    # write to DB
                    attributes_for_level3 = self.create_attributes_for_level3_df(c, second_atomic_score, kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')

            if first_atomic_score > 0:
                kpi_total_res = second_atomic_res
            else:
                kpi_total_res = 0
            score = self.calculate_score(kpi_total_res, p)
            set_total_res += round(score) * p.get('KPI Weight')
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
            children = map(int, str(p.get("Children")).replace(" ", "").replace(",", "\n").replace("\n\n", "\n").split("\n"))
            first_atomic_scores = []
            for c in params.values()[0]:
                if c.get("KPI ID") in children and c.get("Formula").strip() == "atomic number of SKUs":
                    first_atomic_res = self.calculate_availability(c)
                    first_atomic_score = self.calculate_score(first_atomic_res, c)
                    first_atomic_scores.append(first_atomic_score)
                    # write to DB
                    attributes_for_level3 = self.create_attributes_for_level3_df(c, first_atomic_score, kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')
            second_atomic_res = 0
            for c in params.values()[0]:
                if c.get("KPI ID") in children and c.get("Formula").strip() == "atomic number of doors":
                    second_atomic_res = self.calculate_number_of_doors(c)
                    second_atomic_score = self.calculate_score(second_atomic_res, c)
                    # write to DB
                    attributes_for_level3 = self.create_attributes_for_level3_df(c, second_atomic_score, kpi_fk)
                    self.write_to_kpi_results_old(attributes_for_level3, 'level3')

            if 0 not in first_atomic_scores:  # if all assortment atomics have score > 0
                kpi_total_res = second_atomic_res
            else:
                kpi_total_res = 0
            score = self.calculate_score(kpi_total_res, p)
            set_total_res += round(score) * p.get('KPI Weight')
            # saving to DB
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_kpi_results_old(attributes_for_level2, 'level2')

        return set_total_res

    def get_pos_kpi_set_name(self):
        if str(self.visit_date) < '2017-11-25':  # todo: change the date to the relevant one before deployment
            query = """
                    select ss.pk , ss.additional_attribute_12
                    from static.stores ss
                    join probedata.session ps on ps.store_fk=ss.pk
                    where ss.delete_date is null and ps.session_uid = '{}';
                    """.format(self.session_uid)
        else:
            query = """
                    select ss.pk , ss.additional_attribute_11
                    from static.stores ss
                    join probedata.session ps on ps.store_fk=ss.pk
                    where ss.delete_date is null and ps.session_uid = '{}';
                    """.format(self.session_uid)

        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        res = cur.fetchall()

        df = pd.DataFrame(list(res), columns=['store_fk', 'channel'])

        return df['channel'][0]

    @kpi_runtime()
    def calculate_gaps_old(self, params):
        for param in params:
            kpi_name = param.get('KPI Name Eng')
            kpi_id = self.kpi_name_to_id[POS].get(kpi_name)
            kpi_name_local = self.kpi_scores_and_results[POS][kpi_id].get('rus_name')
            category = param.get('Gap Category Eng')
            category_local = param.get('Gap Category Rus')
            score = self.kpi_scores_and_results[POS][kpi_id].get('score')
            target = self.kpi_scores_and_results[POS][kpi_id].get('threshold')*100
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

    @kpi_runtime()
    def calculate_gaps_new(self, params):
        gaps = pd.DataFrame(params)
        total_gap = 0
        category_counter = 0
        for category in gaps['Gap Category Eng'].tolist().unique():
            category_counter += 1
            category_local = category
            category_gap = 0
            group_counter = 0
            for group in gaps[gaps['Gap Category Eng'] == category]['Gap Group Eng'].tolist().unique():
                group_counter += 1
                group_local = group
                group_gap = 0
                subgroup_counter = 0
                for subgroup in gaps[gaps['Gap Group Eng'] == group]['Gap Subgroup Eng'].tolist().unique():
                    subgroup_counter += 1
                    subgroup_local = subgroup
                    subgroup_gap = 0
                    counter = 0
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
                        score = self.kpi_scores_and_results[POS][kpi_id].get('score')
                        target = self.kpi_scores_and_results[POS][kpi_id].get('threshold') * 100

                        if score is not None:
                            counter += 1
                            gap = target - score
                            gap = 0 if gap < 0 else gap
                            if gap > 0:
                                result = format(gap, "+.1f")
                                self.update_kpi_scores_and_results(
                                    {'KPI ID': counter,
                                     'KPI name Eng': kpi_name,
                                     'KPI name Rus': kpi_name_local},
                                    {'weight': counter,
                                     'result': result,
                                     'level': 4,
                                     'parent': subgroup_counter})
                                subgroup_gap += gap

                    if subgroup_gap > 0:
                        result = format(subgroup_gap, "+.1f")
                        self.update_kpi_scores_and_results(
                            {'KPI ID': subgroup_counter,
                             'KPI name Eng': subgroup,
                             'KPI name Rus': subgroup_local},
                            {'weight': None,
                             'result': result,
                             'level': 3,
                             'parent': group_counter})
                        group_gap += subgroup_gap

                if group_gap > 0:
                    result = format(group_gap, "+.1f")
                    self.update_kpi_scores_and_results(
                        {'KPI ID': group_counter,
                         'KPI name Eng': group,
                         'KPI name Rus': group_local},
                        {'weight': None,
                         'result': result,
                         'level': 2,
                         'parent': category_counter})
                    category_gap += group_gap

            if category_gap > 0:
                result = format(category_gap, "+.1f")
                self.update_kpi_scores_and_results(
                    {'KPI ID': category_counter,
                     'KPI name Eng': category,
                     'KPI name Rus': category_local},
                    {'weight': None,
                     'result': result,
                     'level': 1,
                     'parent': 0})
                total_gap += category_gap

        result = format(total_gap, "+.1f")
        self.update_kpi_scores_and_results(
            {'KPI ID': 0,
             'KPI name Eng': self.kpi_set_name,
             'KPI name Rus': self.kpi_set_name},
            {'weight': None,
             'result': result,
             'level': 0,
             'parent': None})

        self.write_to_kpi_results_new()
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
    def calculate_equipment_execution(self, params, kpi_set_name, kpi_conversion_file):

        target_data_raw = self.execution_contract.get_json_file_content(str(self.store_id))
        if target_data_raw:
            Log.info('Relevant Contract Execution target file for Store ID {} / Number {} is found'.format(self.store_id, self.store_number))

        target_data = None
        for data in target_data_raw:
            start_date = dt.datetime.strptime(data['Start Date'], '%Y-%m-%d').date()
            end_date = dt.datetime.now().date() if not data['End Date'] else \
                dt.datetime.strptime(data['End Date'], '%Y-%m-%d').date()
            if start_date <= self.visit_date <= end_date:
                target_data = data

        if target_data is not None:

            for field in (self.store_number, 'Start Date', 'End Date'):
                target_data.pop(field, None)

            kpi_conversion = self.get_kpi_conversion(kpi_conversion_file)

            total_score = 0
            total_weight = 0
            count_of_kpis = 0

            for param in params:
                if param.get('level') == 2 and param.get('KPI Set Type') == 'Equipment':

                    kpi_name = param.get('Channel') + '@' + param.get('KPI name Eng')
                    kpi_fk = self.kpi_fetcher.kpi_static_data[self.kpi_fetcher.kpi_static_data['kpi_name'] == kpi_name]['kpi_fk'].values[0]
                    kpi_name = param.get('KPI name Eng')
                    kpi_weight = param.get('KPI Weight')
                    children = param.get('Children').replace('\n','').replace(' ', '').split(',')

                    sum_of_scores = 0
                    sum_of_weights = 0
                    count_of_targets = 0

                    for param_child in params:
                        if str(param_child.get('KPI ID')) in children and param_child.get('KPI Set Type') == 'Equipment':
                            atomic_kpi_name = param_child.get('Channel') + '@' + param_child.get('KPI name Eng')
                            atomic_kpi_fk = self.kpi_fetcher.kpi_static_data[self.kpi_fetcher.kpi_static_data['atomic_kpi_name'] == atomic_kpi_name]['atomic_kpi_fk'].values[0]
                            atomic_kpi_name = param_child.get('KPI name Eng')
                            target, weight = target_data.get(kpi_conversion.get(atomic_kpi_name))
                            target = target if target else None
                            weight = 1
                            if target:
                                if type(target) is unicode and ',' in target:
                                    target = target.replace(',', '.')
                                if type(target) is unicode and '%' in target:
                                    target = target.replace('%', '')
                                    target = float(target) / 100
                                target = round(float(target), 2)
                                if int(target) == target:
                                    target = int(target)
                                try:
                                    result = round(float(self.kpi_scores_and_results[TARGET][param_child.get('KPI ID')].get('result')), 2)
                                    result = int(result) if result == int(result) else result
                                except:
                                    result = 0
                                score_func = param_child.get('score_func')
                                if score_func == PROPORTIONAL:
                                    score = int(round(result / float(target) * 100)) if target else 100
                                    score = 100 if score > 100 else score
                                else:
                                    score = 100 if result >= target else 0

                                attributes_for_level3 = self.create_attributes_for_level3_df(
                                    {'KPI name Eng': atomic_kpi_name}, (score, result, target), kpi_fk, atomic_kpi_fk)
                                self.write_to_kpi_results_old(attributes_for_level3, 'level3')

                                sum_of_scores += score * weight
                                sum_of_weights += weight
                                count_of_targets += 1

                    if count_of_targets:
                        score = int(round(sum_of_scores / float(sum_of_weights)))
                        attributes_for_level2 = self.create_attributes_for_level2_df(
                            {'KPI name Eng': kpi_name}, score, kpi_fk)
                        self.write_to_kpi_results_old(attributes_for_level2, 'level2')

                        total_score += score * kpi_weight
                        total_weight += kpi_weight
                        count_of_kpis += 1

            if count_of_kpis:
                score = round(total_score / float(total_weight), 2)
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
                     'KPI name Rus': kpi_set_name},
                    {'level': 1,
                     'parent': None})

                self.equipment_execution_score = score

            else:
                self.equipment_execution_score = None

        return

    @kpi_runtime()
    def calculate_contract_execution(self, params, kpi_set_name):
        if self.top_sku_score is not None or self.equipment_execution_score is not None:

            total_score = 0
            total_weight = 0
            count_of_kpis = 0

            score = None

            self.kpi_scores_and_results[CONTRACT] = self.kpi_scores_and_results[EQUIPMENT]
            kpi_id = None
            eng_name = None

            for param in params:
                if param.get('KPI Set Type') == 'Contract':
                    if param.get('Formula').strip() == 'OSA score':
                        kpi_id = 'OSA'
                        eng_name = 'CONTRACT_OSA'
                        score = self.top_sku_score

                    elif param.get('Formula').strip() == 'Equipment Execution score':
                        kpi_id = 'EQUIPMENT'
                        eng_name = 'CONTRACT_EQUIPMENT'
                        score = self.equipment_execution_score

                    if score is not None:
                        target = 100
                        result = score
                        score_to_db = int(round(score*100))

                        kpi_name = param.get('Channel') + '@' + param.get('KPI name Eng')
                        kpi_fk = self.kpi_fetcher.kpi_static_data[self.kpi_fetcher.kpi_static_data['kpi_name'] == kpi_name]['kpi_fk'].values[0]
                        atomic_kpi_fk = self.kpi_fetcher.kpi_static_data[self.kpi_fetcher.kpi_static_data['atomic_kpi_name'] == kpi_name]['atomic_kpi_fk'].values[0]
                        kpi_weight = param.get('KPI Weight')

                        kpi_name = param.get('KPI name Eng')

                        attributes_for_level3 = self.create_attributes_for_level3_df(
                            {'KPI name Eng': kpi_name}, (score_to_db, result, target), kpi_fk, atomic_kpi_fk,
                            update_kpi_scores_and_results=False)
                        self.write_to_kpi_results_old(attributes_for_level3, 'level3')

                        attributes_for_level2 = self.create_attributes_for_level2_df(
                            {'KPI name Eng': kpi_name}, score_to_db, kpi_fk,
                            update_kpi_scores_and_results=False)
                        self.write_to_kpi_results_old(attributes_for_level2, 'level2')

                        self.update_kpi_scores_and_results(
                            {'KPI ID': kpi_id},
                            {'kpi_id': kpi_id,
                             'eng_name': eng_name,
                             'result': result,
                             'threshold': kpi_weight,
                             'score': round(result * kpi_weight, 3),
                             'level': 1,
                             'parent': 0})

                        total_score += score * kpi_weight
                        total_weight += kpi_weight
                        count_of_kpis += 1

            if count_of_kpis:

                score = round(total_score / float(total_weight), 2)
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
                     'KPI name Rus': kpi_set_name},
                    {'weight': 1,
                     'score': score,
                     'level': 0,
                     'parent': None})
                self.write_to_kpi_results_new()

    @staticmethod
    def get_kpi_conversion(kpi_conversion_file):
        data = pd.read_excel(
            os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', kpi_conversion_file))
        conversion = {}
        for x, row in data.iterrows():
            conversion[row['KPI Name']] = str(row['KPI ID'])
        return conversion

    @kpi_runtime()
    def calculate_top_sku(self, params, kpi_set_name):

        top_skus = self.top_sku.get_top_skus_for_store(self.store_id, self.visit_date)
        if not top_skus:
            return

        top_sku_products = pd.DataFrame()
        in_assortment = True
        for scene_fk in self.scif['scene_id'].unique():

            scene_data = self.scif[(self.scif['scene_id'] == scene_fk) & (self.scif['facings'] > 0)]
            facings_data = scene_data.groupby('product_fk')['facings'].sum().to_dict()
            for anchor_product_fk in top_skus['product_fks'].keys():
                min_facings = top_skus['min_facings'][anchor_product_fk]
                distributed = False
                for product_fk in top_skus['product_fks'][anchor_product_fk].split(','):
                    product_fk = int(product_fk)
                    if product_fk in facings_data.keys():
                        facings = facings_data[product_fk]
                    else:
                        facings = 0
                    if facings >= min_facings:
                        distributed = True
                        top_sku_products = top_sku_products.append({'anchor_product_fk': anchor_product_fk,
                                                                    'product_fk': product_fk,
                                                                    'min_facings': min_facings,
                                                                    'facings': facings,
                                                                    'in_assortment': 1,
                                                                    'distributed': 1 if distributed else 0})
                query = self.top_sku.get_custom_scif_query(self.session_fk, scene_fk, int(anchor_product_fk), in_assortment, distributed)
                self.top_sku_queries.append(query)

        score = None

        top_sku_products = top_sku_products.merge(self.products[['product_fk', 'category_fk']], on='product_fk')
        for row in top_sku_products.iterrows():

            identifier_result = self.common.get_dictionary(set=self.kpi_set_type, level=3, kpi=row['product_fk'])
            identifier_parent = self.common.get_dictionary(set=self.kpi_set_type, level=2, kpi=row['anchor_product_fk'])

            kpi_name = self.kpi_set_type + '_SKU'
            kpi_fk = self.common.kpi_static_data[self.common.kpi_static_data['type'] == kpi_name]['pk'].values[0]

            numerator_id = row['product_fk']
            denominator_id = None
            context_id = None

            numerator_result = row['facings']
            denominator_result = row['min_facings']

            result = 'DISTRIBUTED' if row['distributed'] else 'OOS'
            kpi_result_type_fk = self.common.kpi_static_data[self.common.kpi_static_data['pk'] == kpi_fk][
                'kpi_result_type_fk'].values[0]
            if kpi_result_type_fk:
                result = self.kpi_result_values[(self.kpi_result_values['result_type_fk'] == kpi_result_type_fk) &
                                                (self.kpi_result_values['result_value'] == result)][
                    'result_value_fk'].values[0]

            score = 1 if row['distributed'] else 0
            weight = None
            target = 1

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
            .groupby(['category_fk', 'anchor_product_fk'])\
            .agg({'product_fk': 'count', 'facings': 'sum', 'in_assortment': 'max', 'distributed': 'max'})
        for row in top_sku_anchor_products.iterrows():

            identifier_result = self.common.get_dictionary(set=self.kpi_set_type, level=2, kpi=row['anchor_product_fk'])
            identifier_parent = self.common.get_dictionary(set=self.kpi_set_type, level=1, kpi=row['category_fk'])

            kpi_name = self.kpi_set_type + '_BUNDLE'
            kpi_fk = self.common.kpi_static_data[self.common.kpi_static_data['type'] == kpi_name]['pk'].values[0]

            numerator_id = row['anchor_product_fk']
            denominator_id = None

            if self.kpi_entities.empty:
                context_id = None
            else:
                kpi_context_type_fk = self.common.kpi_static_data[self.common.kpi_static_data['pk'] == kpi_fk][
                    'kpi_context_type_fk'].values[0]
                if row['product_fk'] == 1:
                    context_id = self.kpi_entities[(self.kpi_entities['type'] == kpi_context_type_fk)
                                                   & (self.kpi_entities['uid_field'] == 'SKU')]['fk'].values[0]
                else:
                    context_id = self.kpi_entities[(self.kpi_entities['type'] == kpi_context_type_fk)
                                                   & (self.kpi_entities['uid_field'] == 'BUNDLE')]['fk'].values[0]

            numerator_result = row['facings']
            denominator_result = None if row['product_fk'] == 1 else row['product_fk']

            result = 'DISTRIBUTED' if row['distributed'] else 'OOS'
            kpi_result_type_fk = self.common.kpi_static_data[self.common.kpi_static_data['pk'] == kpi_fk][
                'kpi_result_type_fk'].values[0]
            if kpi_result_type_fk:
                result = self.kpi_result_values[(self.kpi_result_values['result_type_fk'] == kpi_result_type_fk) &
                                                (self.kpi_result_values['result_value'] == result)][
                    'result_value_fk'].values[0]

            score = 1 if row['distributed'] else 0
            weight = None
            target = 1

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
            .groupby(['category_fk'])\
            .agg({'in_assortment': 'sum', 'distributed': 'sum'})
        for row in top_sku_categories.iterrows():

            identifier_result = self.common.get_dictionary(set=self.kpi_set_type, level=1, kpi=row['category_fk'])
            identifier_parent = self.common.get_dictionary(set=self.kpi_set_type, level=0, kpi=0)

            kpi_name = self.kpi_set_type + '_CATEGORY'
            kpi_fk = self.common.kpi_static_data[self.common.kpi_static_data['type'] == kpi_name]['pk'].values[0]

            numerator_id = row['anchor_product_fk']
            denominator_id = None
            context_id = None

            numerator_result = row['distributed']
            denominator_result = row['in_assortment']

            result = round(numerator_result / float(denominator_result), 3)
            score = result
            weight = None
            target = 1

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

        top_sku_total = top_sku_anchor_products\
            .agg({'in_assortment': 'sum', 'distributed': 'sum'})
        for row in top_sku_total.iterrows():

            identifier_result = self.common.get_dictionary(set=self.kpi_set_type, level=0, kpi=0)
            identifier_parent = self.common.get_dictionary(set=CONTRACT, level=1, kpi='OSA')

            kpi_name = self.kpi_set_type
            kpi_fk = self.common.kpi_static_data[self.common.kpi_static_data['type'] == kpi_name]['pk'].values[0]

            numerator_id = self.own_manufacturer_id
            denominator_id = self.store_id
            context_id = None

            numerator_result = row['distributed']
            denominator_result = row['in_assortment']

            result = round(numerator_result / float(denominator_result), 3)
            score = result
            weight = 1
            target = 1

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

        self.top_sku_score = score

        return

    @kpi_runtime()
    def check_kpi_scores(self, params):
        set_total_res = 0
        for p in params.values()[0]:

            if p.get('Formula').strip() == "Check KPI score":
                kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                kpi_id = p.get('KPI ID')
                if kpi_id in self.kpi_scores_and_results[self.kpi_set_type].keys():
                    kpi_score = self.kpi_scores_and_results[self.kpi_set_type][kpi_id].get('score')
                    kpi_result = kpi_score
                else:
                    continue

            elif p.get('Formula').strip() == "Number of KPI passed":
                kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('KPI name Eng'))
                kpi_id = p.get('KPI ID')
                if kpi_id in self.kpi_scores_and_results[self.kpi_set_type].keys():
                    kpi_score = self.kpi_scores_and_results[self.kpi_set_type][kpi_id].get('score')
                    if kpi_score == 100:
                        kpi_result = 1
                    else:
                        kpi_result = 0
                else:
                    continue

            else:
                continue

            score = self.calculate_score(kpi_result, p)
            set_total_res += round(score) * p.get('KPI Weight')

            # saving to DB
            attributes_for_level2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
            self.write_to_kpi_results_old(attributes_for_level2, 'level2')
            attributes_for_level3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
            self.write_to_kpi_results_old(attributes_for_level3, 'level3')

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
            shelves_list = [int(shelf) for shelf in shelves.split(',')]
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

        try:
            if "number of SKUs" in formula:
                object_facings = len(final_result['product_ean_code'].unique())
            else:
                if 'facings' in final_result.columns:
                    object_facings = final_result['facings'].sum()
                else:
                    object_facings = len(final_result)
        except IndexError:
            object_facings = 0
        return object_facings

    def write_to_kpi_results_new(self, kpi_data=None):
        if self.kpi_set_type not in SKIP_NEW_KPIS_FROM_WRITING:
            if kpi_data:
                for kpi in kpi_data.values()[0]:
                    if kpi.get('Formula') != 'Group':
                        continue
                    else:
                        self.update_kpi_scores_and_results(kpi, {})
            kpis = pd.DataFrame(self.kpi_scores_and_results[self.kpi_set_type].values())\
                .fillna({'parent': -1})  # -1 is to mark the root kpi in the tree
            self.write_kpi_tree(kpis)
        return

    def write_kpi_tree(self, kpis, parent=-1, parent_level=-1):
        group_score = 0
        for i, kpi in kpis[kpis['parent' == parent]].iterrows():
            score = self.write_kpi_tree(kpis, parent=kpi['kpi_id'], parent_level=kpi['level'])
            group_score += 0 if score is None else score

            identifier_result = self.common.get_dictionary(set=self.kpi_set_type, level=kpi['level'], kpi=kpi['kpi_id'])
            identifier_parent = self.common.get_dictionary(set=self.kpi_set_type, level=parent_level, kpi=parent)

            kpi_name = self.kpi_set_type + '_' + str(kpi['level'])\
                + (('_' + kpi['format']) if kpi['format'] else '') \
                + ('_SCENE' if kpi['scene_id'] else '')
            kpi_fk = self.common.kpi_static_data[self.common.kpi_static_data['type'] == kpi_name]['pk'].values[0]

            numerator_id = self.own_manufacturer_id if parent == -1 \
                else self.common.get_kpi_fk_by_kpi_type(kpi['eng_name'])
            denominator_id = self. store_id if parent == -1 \
                else self.common.get_kpi_fk_by_kpi_type(kpis[kpis['kpi_id'] == kpi['parent']]['eng_name'].values[0])
            context_id = self.kpi_set_name

            result = kpi['result']
            if result is str or result is unicode:
                kpi_result_type_fk = self.common.kpi_static_data[self.common.kpi_static_data['pk'] == kpi_fk][
                    'kpi_result_type_fk'].values[0]
                if kpi_result_type_fk:
                    result = self.kpi_result_values[(self.kpi_result_values['result_type_fk'] == kpi_result_type_fk) &
                                                    (self.kpi_result_values['result_value'] == result)][
                        'result_value_fk'].values[0]

            score = score if kpi['level'] == 1 else kpi['score']
            weight = kpi['weight']
            target = kpi['threshold']

            scene_id = kpi['scene_id']

            if scene_id:
                self.common.write_to_db_result(fk=kpi_fk,
                                               numerator_id=numerator_id,
                                               denominator_id=denominator_id,
                                               context_id=context_id,
                                               result=result,
                                               score=score,
                                               weight=weight,
                                               target=target,
                                               identifier_result=identifier_result,
                                               identifier_parent=identifier_parent,
                                               should_enter=True,
                                               by_scene=True,
                                               scene_result_fk=scene_id)
            else:
                self.common.write_to_db_result(fk=kpi_fk,
                                               numerator_id=numerator_id,
                                               denominator_id=denominator_id,
                                               context_id=context_id,
                                               result=result,
                                               score=score,
                                               weight=weight,
                                               target=target,
                                               identifier_result=identifier_result,
                                               identifier_parent=identifier_parent,
                                               should_enter=True)

        return group_score

    @kpi_runtime()
    def commit_results_data_new(self):
        self.common.commit_results_data()

    def get_kpi_entities(self, entities):
        if entities is str:
            entities = [entities]
        entities_df = pd.DataFrame()
        for entity in entities:
            entity_type_fk = self.kpi_entity_types[self.kpi_entity_types['name'] == entity]['pk'].values[0]
            entity_table_name = self.kpi_entity_types[self.kpi_entity_types['name'] == entity]['table_name'].values[0]
            entity_uid_field = self.kpi_entity_types[self.kpi_entity_types['name'] == entity]['uid_field'].values[0]
            entities_df = entities_df\
                .append(self.kpi_fetcher.get_kpi_entity(entity, entity_type_fk, entity_table_name, entity_uid_field),
                        ignore_index=True).reindex()
        return entities_df
