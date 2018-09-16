#!/usr/bin/env python
# -*- coding: utf-8 -*

import pandas as pd
import os

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.Common import Common, log_runtime
from Projects.INBEVMX_SAND.Data.Const import Const
from Projects.INBEVMX_SAND.Utils.Fetcher import INBEVMXQueries
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from KPIUtils.DB.Common import Common


from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence


__author__ = 'ilays'

KPI_NEW_TABLE = 'report.kpi_level_2_results'
PATH_SURVEY_AND_SOS_TARGET = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                    '..', 'Data', 'inbevmx_survey_and_sos_target_template_v1.1.xlsx')


class INBEVMXToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.session_id = self.data_provider.session_id
        self.products = self.data_provider[Data.PRODUCTS]
        self.common_db = Common(self.data_provider)
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.survey = Survey(self.data_provider, self.output)
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.kpi_results_new_tables_queries = []
        self.store_info = self.data_provider[Data.STORE_INFO]
        try:
            self.store_type_filter = self.store_info['store_type'].values[0].strip()
        except:
            Log.error("there is no store type in the db")
            return
        try:
            self.region_name_filter = self.store_info['region_name'].values[0].strip()
        except:
            Log.error("there is no region in the db")
            return
        self.sos_target_sheet = pd.read_excel(PATH_SURVEY_AND_SOS_TARGET, Const.SOS_TARGET).fillna("")
        self.survey_sheet = pd.read_excel(PATH_SURVEY_AND_SOS_TARGET, Const.SURVEY).fillna("")


    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        kpis_sheet = pd.read_excel(PATH_SURVEY_AND_SOS_TARGET, Const.KPIS).fillna("")
        for index, row in kpis_sheet.iterrows():
            self.handle_atomic(row)
        self.commit_results_data()

    def handle_atomic(self, row):
        atomic_id = row[Const.TEMPLATE_KPI_ID]
        atomic_name = row[Const.TEMPLATE_ENGLISH_KPI_NAME].strip()
        store_type_template = row[Const.TEMPLATE_STORE_TYPE].strip()

        # if cell in template is not empty
        if store_type_template != "":
            store_types = store_type_template.split(",")
            store_types = [item.strip() for item in store_types]
            if self.store_type_filter not in store_types:
                return

        kpi_type = row[Const.TEMPLATE_KPI_TYPE].strip()
        if kpi_type == Const.SOS_TARGET:
            self.handle_sos_target_atomics(atomic_id, atomic_name)
        elif kpi_type == Const.SURVEY:
            self.handle_survey_atomics(atomic_id, atomic_name)

    def handle_sos_target_atomics(self,atomic_id, atomic_name):

        denominator_number_of_total_facings = 0
        count_result = -1

        # bring the kpi rows from the sos sheet
        rows = self.sos_sheet.loc[self.sos_sheet[Const.KPI_ID] == atomic_id]

        # get a single row
        row = self.find_row(rows)
        if row.empty:
            return

        target = row[Const.TARGET].values[0]
        weight = row[Const.WEIGHT].values[0]
        df = self.scif.copy()

        product_size = row[Const.PRODUCT_SIZE].values[0]
        if product_size != "":
            df = self.filter_product_size(df, product_size)

        # get the filters
        filters = self.get_filters_from_row(row.squeeze())
        numerator_number_of_facings = self.count_of_facings(df, filters)
        if numerator_number_of_facings != 0 and count_result == -1:
            if 'manufacturer_name' in filters.keys():
                for f in ['manufacturer_name', 'brand_name']:
                    if f in filters:
                        del filters[f]
                denominator_number_of_total_facings = self.count_of_facings(df, filters)
                percentage = 100 * (numerator_number_of_facings / denominator_number_of_total_facings)
                count_result = weight if percentage >= target else -1

        if count_result == -1:
            return

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_name_new_tables(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return

        self.write_to_db_result_new_tables(fk=atomic_pk, numerator_id=self.session_id,
                                       numerator_result=numerator_number_of_facings, denominator_id=3,
                                       denominator_result=denominator_number_of_total_facings, result=count_result)


    def find_row(self, rows):
        temp = rows[Const.STORE_TYPE_TEMPLATE]
        rows_stores_filter = rows[(temp == self.store_type_filter) | (temp == "")]
        temp = rows_stores_filter[Const.REGION_TEMPLATE]
        rows_regions_filter = rows_stores_filter[(temp == self.region_name_filter) | (temp == "")]
        temp = rows_regions_filter[Const.STATE_TEMPLATE]
        row_result = rows_regions_filter[(temp.apply(lambda r: self.state_name_filter in r.split(",")))
                                                                                               | (temp == "")]
        return row_result

    def get_filters_from_row(self, row):
        filters = dict(row)

        # no need to be accounted for
        for field in Const.DELETE_FIELDS:
            if field in filters:
                del filters[field]

        # filter all the empty cells
        for key in filters.keys():
            if (filters[key] == ""):
                del filters[key]
            elif isinstance(filters[key], tuple):
                filters[key] = (filters[key][0].split(","), filters[key][1])
            else:
                filters[key] = filters[key].split(",")
                filters[key] = [item.strip() for item in filters[key]]

        return self.create_filters_according_to_scif(filters)

    def create_filters_according_to_scif(self, filters):
        convert_from_scif =    {Const.TEMPLATE_GROUP: 'template_group',
                                Const.TEMPLATE_BRAND: 'brand_name',
                                Const.TEMPLATE_MANUFACTURER_DENOMINATOR: 'manufacturer_name',
                                Const.TEMPLATE_ADDITIONAL_ATTRIBUTE_6: 'additional_attribute_6'}

        for key in filters.keys():
            filters[convert_from_scif[key]] = filters.pop(key)
        return filters

    def count_of_facings(self, df, filters):

        facing_data = df[self.tools.get_filter_condition(df, **filters)]
        number_of_facings = facing_data['facings'].sum()
        return number_of_facings

    def count_of_scenes_packs(self, df, filters):
        all_scene_info = pd.merge(self.scene_info,self.data_provider[Data.ALL_TEMPLATES],on='template_fk')
        df = pd.merge(df, all_scene_info, on="scene_fk")
        df = df[self.tools.get_filter_condition(df, **filters)]
        df = df.groupby(['template_name', 'scene_fk']).size().reset_index(name='num_packs')
        return df

    def count_of_scenes(self, df, filters):
        facing_data = df[self.tools.get_filter_condition(df, **filters)]

        # filter by scene_id and by template_name (scene type)
        scene_types_groupby = facing_data.groupby(['template_name', 'scene_id'])['facings'].sum().reset_index()
        return scene_types_groupby


    def count_of_scenes_facings(self, df, filters):
        all_scene_info = pd.merge(self.scene_info,self.data_provider[Data.ALL_TEMPLATES],on='template_fk')
        df = pd.merge(df, all_scene_info, on="scene_fk")
        df = df[self.tools.get_filter_condition(df, **filters)]
        df['face_count'] = df['face_count'].fillna(1)
        df = df.groupby(['template_name', 'scene_fk'])['face_count'].sum().reset_index()
        return df

    def handle_survey_atomics(self, atomic_id, atomic_name):
        # bring the kpi rows from the survey sheet
        row = self.survey_sheet.loc[self.survey_sheet[Const.TEMPLATE_KPI_ID] == atomic_id]

        if row.empty:
            return
        else:
            # find the answer to the survey in session
            question_id = row[Const.TEMPLATE_SURVEY_QUESTION_ID]
            question_answer_template = row[Const.TEMPLATE_TARGET_ANSWER].values[0]

            survey_result = self.survey.get_survey_answer(('question_fk', question_id))
            if not survey_result:
                return
            if '-' in question_answer_template:
                numbers = question_answer_template.split('-')
                if survey_result < int(numbers[0]) or survey_result > int(numbers[1]):
                    return
                condition = row[Const.TEMPLATE_CONDITION].values[0]
                if condition != "":
                    if condition == ">=":
                        second_question_id = row[Const.TEMPLATE_SECOND_SURVEY_ID]
                        second_survey_result = self.survey.get_survey_answer(('question_fk', second_question_id))
                        survey_result = 1 if survey_result > second_survey_result else -1
            else:
                answer = self.survey.check_survey_answer(('question_fk', question_id), question_answer_template)
                survey_result = 1 if answer else -1

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_name_new_tables(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return

        self.write_to_db_result_new_tables(fk=atomic_pk, numerator_id=self.session_id, numerator_result=0,
                                           denominator_result=0, denominator_id=1, result=survey_result)


    def get_new_kpi_static_data(self):
        """
            This function extracts the static new KPI data (new tables) and saves it into one global data frame.
            The data is taken from static.kpi_level_2.
            """
        query = INBEVMXQueries.get_new_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def write_to_db_result_new_tables(self, fk, numerator_id, numerator_result, result, denominator_id=None,
                                      denominator_result=None, score=None):
        """
            This function creates the result data frame of new rables KPI,
            and appends the insert SQL query into the queries' list, later to be written to the DB.
            """
        table = KPI_NEW_TABLE
        attributes = self.create_attributes_dict_new_tables(fk, numerator_id, numerator_result, denominator_id,
                                                            denominator_result, result, score)
        query = insert(attributes, table)
        self.kpi_results_new_tables_queries.append(query)

    def create_attributes_dict_new_tables(self, kpi_fk, numerator_id, numerator_result, denominator_id,
                                          denominator_result, result, score):
        """
        This function creates a data frame with all attributes needed for saving in KPI results new tables.
        """
        attributes = pd.DataFrame([(kpi_fk, self.session_id, numerator_id, numerator_result, denominator_id,
                                    denominator_result, result, score)], columns=['kpi_level_2_fk', 'session_fk',
                                                                                  'numerator_id', 'numerator_result',
                                                                                  'denominator_id',
                                                                                  'denominator_result', 'result',
                                                                                  'score'])
        return attributes.to_dict()



    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        insert_queries = self.merge_insert_queries(self.kpi_results_new_tables_queries)
        self.rds_conn.disconnect_rds()
        self.rds_conn.connect_rds()
        cur = self.rds_conn.db.cursor()
        delete_query = INBEVMXQueries.get_delete_session_results_query(self.session_uid, self.session_id)
        cur.execute(delete_query)
        for query in insert_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
        self.rds_conn.disconnect_rds()

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
