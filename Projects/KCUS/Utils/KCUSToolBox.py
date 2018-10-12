# -*- coding: utf-8 -*-
import json
import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup
from Trax.Data.Orm.OrmCore import OrmSession
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Logging.Logger import Log
from Projects.KCUS.KCUSFetcher import KCUSFetcher
from Projects.KCUS.Utils.KCUS_GENERAL_TOOLBOX import KCUSGENERALToolBox
from datetime import datetime, timedelta

__author__ = 'ortalk'

UNLIMITED_DISTANCE = 'General'
TOP_DISTANCE = 'Above distance (shelves)'
BOTTOM_DISTANCE = 'Below distance (shelves)'
LEFT_DISTANCE = 'Left distance (facings)'
RIGHT_DISTANCE = 'Right distance (facings)'
PACKAGE_COLOR = 'Package color'
SUB_BRAND = 'sub_brand'
SUB_CATEGORY = 'sub_category'
SUB = {'sub_brand': 'sub_brand_x'}
QUALITY_TIER = 'quality_tier'
TARGET_GROUP = 'target_group'
PACKAGE_SIZE = 'package_size'
TITLE = {'Sub Category': 'sub_category', 'Package color': 'package_color',
         'Sub Brand': 'sub_brand', 'Package Size': 'package_size'}
TRAINING_YOUTH_SWIM_PANTS = 'TRAINING, YOUTH & SWIM PANTS'
YES = 'Yes'
NO = 'No'
BINARY = 'BINARY'
PROPORTIONAL = 'PROPORTIONAL'
CONDITIONAL_PROPORTIONAL = 'CONDITIONAL PROPORTIONAL'
MARS = 'Mars'
KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
EMPTY = 'Empty'
ALLOWED_EMPTIES_RATIO = 0.2
ALLOWED_DEVIATION = 2
NEGATIVE_ADJACENCY_RANGE = (2, 1000)
POSITIVE_ADJACENCY_RANGE = (0, 1)


class KCUSToolBox:
    def __init__(self, data_provider, output, set_name=None):
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.data_provider = data_provider
        self.output = output
        self.products = self.data_provider[Data.ALL_PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.templates = self.data_provider[Data.ALL_TEMPLATES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.scenes_info = self.data_provider[Data.SCENES_INFO]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.session_info = SessionInfo(data_provider)
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_data = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif = self.scif.merge(self.data_provider[Data.STORE_INFO], how='left', left_on='store_id',
                                    right_on='store_fk')
        self.download_time = timedelta(0)
        for sub in SUB:
            if sub in (SUB.keys()):
                self.scif = self.scif.rename(columns={sub: SUB.get(sub)})

        for title in TITLE:
            if title in (self.scif.columns.unique().tolist()):
                self.scif = self.scif.rename(columns={title: TITLE.get(title)})
        self.generaltoolbox = KCUSGENERALToolBox(data_provider, output, geometric_kpi_flag=True)
        # self.scif = self.scif.replace(' ', '', regex=True)
        self.set_name = set_name
        self.kpi_fetcher = KCUSFetcher(self.project_name, self.scif, self.match_product_in_scene,
                                            self.set_name, self.products, self.session_uid)
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.sales_rep_fk = self.data_provider[Data.SESSION_INFO]['s_sales_rep_fk'].iloc[0]
        self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]
        self.store_type = self.data_provider[Data.STORE_INFO]['store_type'].iloc[0]
        self.region = self.data_provider[Data.STORE_INFO]['region_name'].iloc[0]
        self.thresholds_and_results = {}
        self.result_df = []
        self.writing_to_db_time = timedelta(0)
        # self.match_product_in_probe_details = self.kpi_fetcher.get_match_product_in_probe_details(self.session_uid)
        self.kpi_results_queries = []
        # self.position_graphs = MARSRU_PRODPositionGraphs(self.data_provider)
        self.potential_products = {}
        self.get_atts()
        self.shelf_square_boundaries = {}
        # self.object_type_conversion = {'SKUs': 'product_ean_code',
        #                                'BRAND': 'brand_name',
        #                                'BRAND in CAT': 'brand_name',
        #                                'CAT': 'category',
        #                                'MAN in CAT': 'category',
        #                                'MAN': 'manufacturer_name'}


        # def insert_new_kpis_old(self, project, kpi_list=None):
        #     """
        #     This function inserts KPI metadata to static tables
        #     """
        #     # session = OrmSession(project, writable=True)
        #     try:
        #         voting_process_pk_dic = {}
        #         # with session.begin(subtransactions=True):
        #         for kpi in kpi_list.values()[0]:
        #             if kpi.get('first_calc?') == 2:
        #                 Log.info('Trying to write KPI {}'.format(kpi.get('#Mars KPI NAME')))
        #
        #                 insert_trans_level2 = """
        #                                 INSERT INTO static.kpi (kpi_set_fk, display_text)
        #                                VALUES ('{0}', '{1}');""".format(3, kpi.get('#Mars KPI NAME'))
        #
        #                 kpi_fk = self.insert_results_data(insert_trans_level2)
        #
        #
        #                 insert_trans_level3 = """
        #                                 INSERT INTO static.atomic_kpi (kpi_fk,
        #                                name, description, display_text, presentation_order, display)
        #                                VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');""".format(kpi_fk, kpi.get(
        #                     '#Mars KPI NAME'),
        #                                                                                             kpi.get(
        #                                                                                                 '#Mars KPI NAME'),
        #                                                                                             kpi.get(
        #                                                                                                 'KPI Display name RU').encode(
        #                                                                                                 'utf-8'),
        #                                                                                             1, 'Y')
        #
        #                 self.insert_results_data(insert_trans_level3)
        #
        #         return
        #     except Exception as e:
        #         Log.error('Caught exception while inserting new voting process to SQL: {}'.
        #                   format(str(e)))
        #         return -1



        # def check_availability(self, params):
        #     """
        #     This function is used to calculate availability given a set pf parameters
        #relati
        #     set_total_res = 0
        #     availability_types = ['SKUs', 'BRAND', 'MAN', 'CAT', 'MAN in CAT', 'BRAND in CAT']
        #     formula_types = ['number of SKUs', 'number of facings']
        #     for p in params.values()[0]:
        #         if p.get('Type') not in availability_types or p.get('Formula') not in formula_types:
        #             continue
        #         if p.get('level') == 3:
        #             continue
        #         is_atomic = False
        #         kpi_total_res = 0
        #         kpi_fk = self.kpi_fetcher.get_kpi_fk(p.get('#Mars KPI NAME'))
        #
        #
        #
        #         kpi_total_res = self.calculate_anchor(p)
        #         score = self.calculate_score(kpi_total_res, p)
        #         # Saving to old tables
        #         attributes_for_table2 = self.create_attributes_for_level2_df(p, score, kpi_fk)
        #         self.write_to_db_result(attributes_for_table2, 'level2', kpi_fk)
        #         if not is_atomic:  # saving also to level3 in case this KPI has only one level
        #             attributes_for_table3 = self.create_attributes_for_level3_df(p, score, kpi_fk)
        #             self.write_to_db_result(attributes_for_table3, 'level3', kpi_fk)

        # set_total_res += score * p.get('KPI Weight')

        # return set_total_res
    def get_atts(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        query = KCUSFetcher.get_store_atts(self.store_id)
        store_att8 = pd.read_sql_query(query, self.rds_conn.db)
        self.store_data = self.store_data.merge(store_att8, how='left', left_on='store_fk',
                                    right_on='store_fk', suffixes= ['', '_1'])

    def get_new_products(self):
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        query = KCUSFetcher.get_static_new_products(self.store_id)
        self.new_products = pd.read_sql_query(query, self.rds_conn.db)


    def calculate_anchor(self, params):
        filters = {}
        store_type = params.get('Store_Type')
        if store_type is not None:
            store_type = [str(s) for s in params.get('Store_Type').split(', ')]
            filters['store_type'] = store_type
        scene_type = params.get('Scene_Type')
        if scene_type is not None:
            scene_type = [str(s) for s in params.get('Scene_Type').split(', ')]
            filters['template_name'] = scene_type
        param1 = params.get('Param1')
        if param1 is not None:
            value1 = [str(s) for s in params.get('Value1').split(', ')]
            filters[param1] = value1
        param2 = params.get('Param2')
        if param2 is not None:
            value2 = [str(s) for s in params.get('Value2').split(', ')]
            filters[param2] = value2

        min_number_of_facings = int(params.get('min_number_of_facings'))
        min_number_of_shelves = params.get('min_number_of_shelves')
        if min_number_of_shelves is None:
            res = self.generaltoolbox.calculate_products_on_edge(min_number_of_facings, min_number_of_shelves=1,
                                                                 **filters)
        else:
            res = self.generaltoolbox.calculate_products_on_edge(min_number_of_facings, int(min_number_of_shelves),
                                                                 **filters)
        if res[0] >= 1:
            score = 1
        else:
            score = 0
        kpi_fk = self.kpi_fetcher.get_kpi_fk(params.get('KPI Level 2 Name'))
        attributes_for_level2 = self.create_attributes_for_level2_df(params, score, kpi_fk)
        self.write_to_db_result(attributes_for_level2, 'level2', kpi_fk)
        attributes_for_level3 = self.create_attributes_for_level3_df(params, score, kpi_fk)
        self.write_to_db_result(attributes_for_level3, 'level3', kpi_fk)

    def calculate_eye_level(self, params):
        filters = {}
        store_type = params.get('Store_Type')
        if store_type is not None:
            store_type = [str(s) for s in params.get('Store_Type').split(', ')]
            filters['store_type'] = store_type
        scene_type = params.get('Scene_Type')
        if scene_type is not None:
            scene_type = [str(s) for s in params.get('Scene_Type').split(', ')]
            filters['template_name'] = scene_type
        param1 = params.get('Param1')
        if param1 is not None:
            value1 = [str(s) for s in params.get('Value1').split(', ')]
            filters[param1] = value1
        shelf_target = [int(s) for s in params.get('Value2').split(', ')]
        min_number_of_facings = params.get('min_number_of_facings')
        res = self.generaltoolbox.shelf_level_assortment(min_number_of_facings, shelf_target, **filters)
        kpi_fk = self.kpi_fetcher.get_kpi_fk(params.get('KPI Level 2 Name'))
        attributes_for_level2 = self.create_attributes_for_level2_df(params, res, kpi_fk)
        self.write_to_db_result(attributes_for_level2, 'level2', kpi_fk)
        attributes_for_level3 = self.create_attributes_for_level3_df(params, res, kpi_fk)
        self.write_to_db_result(attributes_for_level3, 'level3', kpi_fk)

    def calculate_flow(self, params):
        general_filters = {}
        store_type = params.get('Store_Type')
        if store_type is not None:
            store_type = [str(s) for s in params.get('Store_Type').split(', ')]
            general_filters['store_type'] = store_type
        scene_type = params.get('Scene_Type')
        if scene_type is not None:
            scene_type = [str(s) for s in params.get('Scene_Type').split(', ')]
            general_filters['template_name'] = scene_type
        direction = params.get('Value2')
        values = [str(s) for s in params.get('Value1').split(', ')]
        entity = params.get('Param1')
        sequence_filters = (entity, values)
        res = self.generaltoolbox.calculate_product_sequence(sequence_filters=sequence_filters,
                                                             direction=direction, **general_filters)
        if res:
            score = 1
        else:
            score = 0
        kpi_fk = self.kpi_fetcher.get_kpi_fk(params.get('KPI Level 2 Name'))
        attributes_for_level2 = self.create_attributes_for_level2_df(params, score, kpi_fk)
        self.write_to_db_result(attributes_for_level2, 'level2', kpi_fk)
        attributes_for_level3 = self.create_attributes_for_level3_df(params, score, kpi_fk)
        self.write_to_db_result(attributes_for_level3, 'level3', kpi_fk)

    def calculate_assort(self,params):
        filters = {}
        filter_by1 = {}
        filter_by2 = {}
        score = 0
        scene_type = params.get('Scene_Type')
        if scene_type is not None:
            filters['template_name'] = scene_type
        store_type = params.get('Store_Type')
        if store_type is not None:
            store_type = [str(s) for s in params.get('Store_Type').split(', ')]
            filters['store_type'] = store_type
        scenes = self.scif[self.generaltoolbox.get_filter_condition(self.scif, **filters)]['scene_id'].unique()
        tested1 = params.get('Entity 1')
        if tested1 is not None:
            filter_by1[tested1] = [str(s) for s in params.get('Tested Entity 1').split(', ')]
        tested2 = params.get('Entity 2')
        if tested2 is not None:
            filter_by1[tested2] = [str(s) for s in params.get('Tested Entity 2').split(', ')]
        anchor1 = params.get('Anchor Entity 1')
        if anchor1 is not None:
            filter_by2[anchor1] = [str(s) for s in params.get('Anchor param 1').split(', ')]
        anchor2 = params.get('Anchor Entity 2')
        if anchor2 is not None:
            filter_by2[anchor2] = [str(s) for s in params.get('Anchor param 2').split(', ')]
        if scenes is not None:
            for scene in scenes:
                filter_by1['scene_id'] = scene
                res1 = self.generaltoolbox.calculate_assortment(assortment_entity='brand_name', **filter_by1)
                if res1 > 0:
                    filter_by2['scene_id'] = scene
                    res2 = self.generaltoolbox.calculate_assortment(assortment_entity='brand_name', **filter_by2)
                    if res2 > 0:
                        score = 1
                        break
        kpi_fk = self.kpi_fetcher.get_kpi_fk(params.get('KPI Level 2 Name'))
        attributes_for_level2 = self.create_attributes_for_level2_df(params, score, kpi_fk)
        self.write_to_db_result(attributes_for_level2, 'level2', kpi_fk)
        attributes_for_level3 = self.create_attributes_for_level3_df(params, score, kpi_fk)
        self.write_to_db_result(attributes_for_level3, 'level3', kpi_fk)

    def calculate_flow_between(self, params):
        general_filters = {}
        store_type = params.get('Store_Type')
        if store_type is not None:
            store_type = [str(s) for s in params.get('Store_Type').split(', ')]
            general_filters['store_type'] = store_type
        scene_type = params.get('Scene_Type')
        if scene_type is not None:
            scene_type = [str(s) for s in params.get('Scene_Type').split(', ')]
            general_filters['template_name'] = scene_type
        entity = params.get('Entity')
        values1 = [str(s) for s in params.get('Value1').split(', ')]
        entity1 = params.get('Param1')
        values2 = [str(s) for s in params.get('Value2').split(', ')]
        entity2 = params.get('Param2')
        values3 = [str(s) for s in params.get('Value3').split(', ')]
        entity3 = params.get('Param3')
        # sequence_filters = [{entity1: values1},{entity2: values2},{entity3: values3}]
        general_filters[entity1] = values1
        general_filters[entity2] = values2
        general_filters[entity3] = values3
        val = [['None'],values1,values2,values3]
        scenes = self.scif[self.generaltoolbox.get_filter_condition(self.scif, **general_filters)]['scene_id'].unique()
        count = 0
        changes = params.get('Change_Matrix')
        if changes is not None:
            change = True
        else:
            change = False
        score = 0
        for scene in scenes:
            if score == 1:
                break
            brand_table = self.generaltoolbox.position_graph_data.get_entity_matrix(scene, entity1,
                                                                                    changes_matrix=change)
            for row in brand_table:
                current_val = 0
                max_val = len(val)-1
                for i in xrange(0, len(row) - 1):
                    if current_val < max_val:
                        if row[i] in val[current_val]:
                            continue
                        elif row[i] in val[current_val+1]:
                            current_val += 1
                            continue
                        else:
                            current_val = 0
                    else:
                        break
                if current_val == max_val:
                    score = 1
                    break
                else:
                    score = 0

        if score == 1:
            self.thresholds_and_results[params.get('KPI Level 2 Name')] = {'result': 'TRUE'}
        else:
            self.thresholds_and_results[params.get('KPI Level 2 Name')] = {'result': 'FALSE'}
        kpi_fk = self.kpi_fetcher.get_kpi_fk(params.get('KPI Level 2 Name'))
        attributes_for_level2 = self.create_attributes_for_level2_df(params, score, kpi_fk)
        self.write_to_db_result(attributes_for_level2, 'level2', kpi_fk)
        attributes_for_level3 = self.create_attributes_for_level3_df(params, score, kpi_fk)
        self.write_to_db_result(attributes_for_level3, 'level3', kpi_fk)

    def check_survey_answer(self, param):
        """
        This function is used to calculate survey answer according to given target

        """
        score = 0
        survey_data = self.survey_response.loc[self.survey_response['question_text'] == param.get('KPI Level 2 Name')]
        if survey_data['selected_option_text'] is not None:
            result = survey_data['selected_option_text'].values[0]
            if result == YES:
                self.thresholds_and_results[param.get('KPI Level 2 Name')] = {'result': 'TRUE'}
                score = 1
            else:
                self.thresholds_and_results[param.get('KPI Level 2 Name')] = {'result': 'FALSE'}
            kpi_fk = self.kpi_fetcher.get_kpi_fk(param.get('KPI Level 2 Name'))
            attributes_for_level3 = self.create_attributes_for_level3_df(param, score, kpi_fk)
            self.write_to_db_result(attributes_for_level3, 'level3')
            attributes_for_level2 = self.create_attributes_for_level2_df(param, score, kpi_fk)
            self.write_to_db_result(attributes_for_level2, 'level2')
        else:
            Log.warning('No survey data for this session')
        return score

    def calculate_block_together(self, param):
        filters = {}
        score = 0
        att_8 = param.get('additional_attribute_8')
        if att_8 is not None:
            att_8 = [str(s) for s in param.get('additional_attribute_8').split(', ')]
            if self.store_data['additional_attribute_8'][0] not in att_8:
                    return
        scene_type = param.get('Scene_Type')
        if scene_type is not None:
            scene_type = [str(s) for s in param.get('Scene_Type').split(', ')]
            filters['template_name'] = scene_type
        param1 = param.get('Entity')
        if param1 is not None:
            if param.get('Tested Product Group') == TRAINING_YOUTH_SWIM_PANTS:
                filters[param1] = TRAINING_YOUTH_SWIM_PANTS
            else:
                filters[param1] = [str(s) for s in param.get('Tested Product Group').split(', ')]
        param2 = param.get('Entity 2')
        if param2 is not None:
            filters[param2] = [str(s) for s in param.get('Tested Product Group 2').split(', ')]
        param3 = param.get('Entity 3')
        if param3 is not None:
            filters[param3] = [str(s) for s in param.get('Tested Product Group 3').split(', ')]
        ignore_empty = param.get('Ignore Empty')
        include_empty = True
        if ignore_empty is None:
            include_empty = True
        elif ignore_empty == YES:
            include_empty = False
        res = self.generaltoolbox.calculate_block_together(include_empty=include_empty, **filters)
        if res:
            vertical = param.get('Vertical')
            if vertical is not None:
                shelves = self.generaltoolbox.calculate_assortment(assortment_entity='shelf_number', **filters)
                if shelves > 1:
                    score = 1
            else:
                score = 1
        kpi_fk = self.kpi_fetcher.get_kpi_fk(param.get('KPI Level 2 Name'))
        attributes_for_level2 = self.create_attributes_for_level2_df(param, score, kpi_fk)
        self.write_to_db_result(attributes_for_level2, 'level2', kpi_fk)
        attributes_for_level3 = self.create_attributes_for_level3_df(param, score, kpi_fk)
        self.write_to_db_result(attributes_for_level3, 'level3', kpi_fk)
        return score

    def calculate_block_together_relative(self, param):
        result = False
        filters = {}
        scene_filter_block_1 = {}
        scene_filter_block_2 = {}
        tested_filters = {}
        anchor_filters = {}
        scene_type = param.get('Scene_Type')
        if scene_type is not None:
            filters['template_name'] = scene_type
            scenes = self.scif[self.generaltoolbox.get_filter_condition(self.scif, **filters)]['scene_id'].unique()
            for scene in scenes:
                scene_filter_block_1['scene_id'] = scene
                param1 = param.get('Entity')
                if param1 is not None:
                    scene_filter_block_1[param1] = [str(s) for s in param.get('Tested Product Group').split(', ')]
                res = self.generaltoolbox.calculate_block_together(include_empty=False, **scene_filter_block_1)
                if res:
                    scene_filter_block_2['scene_id'] = scene
                    param2 = param.get('Entity 2')
                    if param2 is not None:
                        scene_filter_block_2[param2] = [str(s) for s in param.get('Tested Product Group').split(', ')]
                    res = self.generaltoolbox.calculate_block_together(include_empty=False, **scene_filter_block_2)
                    if res:
                        tested_filters[param1] = [str(s) for s in param.get('Tested Product Group').split(', ')]
                        anchor_filters[param2] = [str(s) for s in param.get('Tested Product Group').split(', ')]
                        general_filters = filters
                        direction_data = {'top': 0,
                                          'bottom': 0,
                                          'left': 0,
                                          'right': 1}
                        result = self.generaltoolbox.calculate_relative_position(tested_filters, anchor_filters,
                                                                                 direction_data, **general_filters)
        if result:
            score = 1
        else:
            score = 0
        kpi_fk = self.kpi_fetcher.get_kpi_fk(param.get('KPI Level 2 Name'))
        attributes_for_level2 = self.create_attributes_for_level2_df(param, score, kpi_fk)
        self.write_to_db_result(attributes_for_level2, 'level2', kpi_fk)
        attributes_for_level3 = self.create_attributes_for_level3_df(param, score, kpi_fk)
        self.write_to_db_result(attributes_for_level3, 'level3', kpi_fk)
        return score

    def calculate_relative_position(self, params):
        """
        This function calculates every relative-position-typed KPI from the relevant sets, and returns the set final score.
        """
        tested2 = params.get('Tested Entity 2')
        anchor2 = params.get('Anchor Entity 2')
        if tested2 is None:
            tested_filters = {params.get('Entity 1'): [params.get('Tested Entity 1')]}
        else:
            tested_filters = {params.get('Entity 1'): [params.get('Tested Entity 1')],
                              params.get('Entity 2'): [params.get('Tested Entity 2')]}
        if anchor2 is None:
            anchor_filters = {params.get('Anchor Entity 1'): [params.get('Anchor param 1')]}
        else:
            anchor_filters = {params.get('Anchor Entity 1'): [params.get('Anchor param 1')],
                              params.get('Anchor Entity 2'): [params.get('Anchor param 2')]}

        direction_data = {'top': self._get_direction_for_relative_position(params.get(TOP_DISTANCE)),
                          'bottom': self._get_direction_for_relative_position(
                              params.get(BOTTOM_DISTANCE)),
                          'left': self._get_direction_for_relative_position(
                              params.get(LEFT_DISTANCE)),
                          'right': self._get_direction_for_relative_position(
                              params.get(RIGHT_DISTANCE))}
        store_type = params.get('Store_Type')
        if store_type is None:
            general_filters = {'template_name': [params.get('Scene_Type')]}
        else:
            general_filters = {'template_name': [params.get('Scene_Type')],
                               'store_type': [params.get('Store_Type')]}
        strict = params.get('Strict')
        if strict is not None:
            min_required_to_pass = 'all'
        else:
            min_required_to_pass = 1
        result = self.generaltoolbox.calculate_relative_position(tested_filters, anchor_filters, direction_data,
                                                                 min_required_to_pass, **general_filters)
        score = self.calculate_score(result, params)
        kpi_fk = self.kpi_fetcher.get_kpi_fk(params.get('KPI Level 2 Name'))
        attributes_for_level2 = self.create_attributes_for_level2_df(params, score, kpi_fk)
        self.write_to_db_result(attributes_for_level2, 'level2', kpi_fk)
        attributes_for_level3 = self.create_attributes_for_level3_df(params, score, kpi_fk)
        self.write_to_db_result(attributes_for_level3, 'level3', kpi_fk)
        return score

    def _get_direction_for_relative_position(self, value):
        """
        This function converts direction data from the template (as string) to a number.
        """
        if value == UNLIMITED_DISTANCE:
            value = 1000
        elif not value or not str(value).isdigit():
            value = 0
        else:
            value = int(value)
        return value

    @staticmethod
    def filter_vertices_from_graph(graph, **filters):
        """
        This function is given a graph and returns a set of vertices calculated by a given set of filters.
        """
        vertices_indexes = None
        for field in filters.keys():
            field_vertices = set()
            values = filters[field] if isinstance(filters[field], (list, tuple)) else [filters[field]]
            for value in values:
                vertices = [v.index for v in graph.vs.select(**{field: value})]
                field_vertices = field_vertices.union(vertices)
            if vertices_indexes is None:
                vertices_indexes = field_vertices
            else:
                vertices_indexes = vertices_indexes.intersection(field_vertices)
        vertices_indexes = vertices_indexes if vertices_indexes is not None else [v.index for v in graph.vs]
        return vertices_indexes

    @staticmethod
    def validate_moves(moves, direction_data):
        """
        This function checks whether the distance between the anchor and the tested SKUs fits the requirements.
        """
        for direction in moves.keys():
            if moves[direction] > 0:
                if direction_data[direction][0] <= moves[direction] <= direction_data[direction][1]:
                    return False
        return True

    def calculate_score(self, kpi_total_res, params):
        """
        This function calculates score according to predefined score functions

        """
        kpi_name = params.get('KPI Level 2 Name')
        if kpi_total_res:
            score = 1
            self.thresholds_and_results[kpi_name] = {'result': 1}
        else:
            score = 0
            self.thresholds_and_results[kpi_name] = {'result': 0}
        return score

    def write_to_db_result(self, df=None, level=None, kps_name_temp=None):
        """
        This function writes KPI results to old tables

        """
        if level == 'level3':
            df['atomic_kpi_fk'] = self.kpi_fetcher.get_atomic_kpi_fk(df['name'][0])
            df['kpi_fk'] = df['kpi_fk'][0]
            df_dict = df.to_dict()
            df_dict.pop('name', None)
            query = insert(df_dict, KPI_RESULT)
            self.kpi_results_queries.append(query)
        elif level == 'level2':
            kpi_name = df['kpk_name'][0]
            df['kpi_fk'] = self.kpi_fetcher.get_kpi_fk(kpi_name)
            df_dict = df.to_dict()
            # df_dict.pop("kpk_name", None)
            query = insert(df_dict, KPK_RESULT)
            self.kpi_results_queries.append(query)
        elif level == 'level1':
            df['kpi_set_fk'] = self.kpi_fetcher.get_kpi_set_fk()
            df_dict = df.to_dict()
            query = insert(df_dict, KPS_RESULT)
            self.kpi_results_queries.append(query)

    def insert_results_data(self, query):
        start_time = datetime.datetime.utcnow()
        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        self.rds_conn.db.commit()
        self.writing_to_db_time += datetime.datetime.utcnow() - start_time
        return cur.lastrowid

    def create_attributes_for_level2_df(self, params, score, kpi_fk):
        """
        This function creates a data frame with all attributes needed for saving in level 2 tables

        """
        attributes_for_table2 = pd.DataFrame([(self.session_uid, self.store_id,
                                               self.visit_date.isoformat(), kpi_fk,
                                               params.get('KPI Level 2 Name').encode('utf-8'),
                                               score)],
                                             columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk',
                                                      'kpk_name', 'score'])

        return attributes_for_table2

    def create_attributes_for_level3_df(self, params, score, kpi_fk):
        """
        This function creates a data frame with all attributes needed for saving in level 3 tables

        """
        if self.thresholds_and_results.get(params.get('KPI Level 2 Name')):
            result = self.thresholds_and_results[params.get('KPI Level 2 Name')]['result']
            # threshold = self.thresholds_and_results[params.get('#Mars KPI NAME')]['threshold']
        elif len(self.thresholds_and_results) == 0:
            result = score
        else:
            result = threshold = 0
        attributes_for_table3 = pd.DataFrame([(params.get('KPI Level 2 Name').encode('utf-8'),
                                               self.session_uid, self.set_name, self.store_id,
                                               self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                               score, kpi_fk, None, result, params.get('KPI Level 2 Name'))],
                                             columns=['display_text', 'session_uid', 'kps_name',
                                                      'store_fk', 'visit_date',
                                                      'calculation_time', 'score', 'kpi_fk',
                                                      'atomic_kpi_fk', 'result', 'name'])

        return attributes_for_table3

    def get_set(self):
        kpi_set = self.kpi_fetcher.get_session_set(self.session_uid)

        return kpi_set['additional_attribute_12'][0]

    def commit_results_data(self):
        cur = self.rds_conn.db.cursor()
        delete_queries = self.kpi_fetcher.get_delete_session_results(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()

    def add_new_kpi_to_static_tables(self, set_fk, new_kpi_list):
        """
        :param set_fk: The relevant KPI set FK.
        :param new_kpi_list: a list of all new KPI's parameters.
        This function adds new KPIs to the DB ('Static' table) - both to level2 (KPI) and level3 (Atomic KPI).
        """
        session = OrmSession(self.project_name, writable=True)
        with session.begin(subtransactions=True):
            for kpi in new_kpi_list:
                level2_query = """
                               INSERT INTO static.kpi (kpi_set_fk, display_text)
                               VALUES ('{0}', '{1}');""".format(set_fk, kpi)
                result = session.execute(level2_query)
                kpi_fk = result.lastrowid
                level3_query = """
                               INSERT INTO static.atomic_kpi (kpi_fk, name, description, display_text,
                                                              presentation_order, display)
                               VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');""".format(kpi_fk,
                                                                                            kpi,
                                                                                            kpi,
                                                                                            kpi,
                                                                                            1, 'Y')

                session.execute(level3_query)
        session.close()
        return

    def add_kpi_sets_to_static(self, set_names):
        """
        This function is to be ran at a beginning of a projects - and adds the constant KPI sets data to the DB.
        """
        session = OrmSession(self.project_name, writable=True)
        with session.begin(subtransactions=True):
            for set_name in set_names:
                level1_query = """
                               INSERT INTO static.kpi_set (name, missing_kpi_score, enable, normalize_weight,
                                                           expose_to_api, is_in_weekly_report)
                               VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}');""".format(set_name, 'Bad', 'Y',
                                                                                            'N', 'N', 'N')
                session.execute(level1_query)
        session.close()
        return
