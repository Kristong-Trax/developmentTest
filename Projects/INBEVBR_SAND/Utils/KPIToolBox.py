#!/usr/bin/env python
# -*- coding: utf-8 -*

import pandas as pd
import os

from datetime import datetime
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from KPIUtils.Calculations.Survey import Survey
from Projects.INBEVBR_SAND.Utils.Fetcher import INBEVBRQueries
from Projects.INBEVBR_SAND.Utils.GeneralToolBox import INBEVBRGENERALToolBox
from Projects.INBEVBR_SAND.Data.Const import Const
from KPIUtils.GlobalDataProvider.PsDataProvider import PsDataProvider
from Projects.INBEVBR_SAND.Utils.PositionGraph import INBEVBR_SANDPositionGraphs
from KPIUtils.DB.Common import Common

__author__ = 'ilays'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
KPI_NEW_TABLE = 'report.kpi_level_2_results'
PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Ambev template v1.0 - KENGINE.xlsx')

def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result
        return wrapper
    return decorator


class INBEVBRToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = INBEVBRGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type_filter = self.store_info['store_type'].values[0].strip()
        self.region_name_filter = self.store_info['region_name'].values[0].strip()
        self.state_name_filter = self.store_info['additional_attribute_2'].values[0].strip()
        self.kpi_results_queries = []
        self.survey = Survey(self.data_provider, self.output)
        self.kpi_results_new_tables_queries = []
        self.New_kpi_static_data = self.get_new_kpi_static_data()
        self.session_id = self.data_provider.session_id
        self.prices_per_session = PsDataProvider(self.data_provider, self.output).get_price(self.session_id)
        self.common_db = Common(self.data_provider)
        self.sos_sheet = pd.read_excel(PATH, Const.SOS).fillna("")
        self.count_sheet = pd.read_excel(PATH, Const.COUNT).fillna("")
        self.group_count_sheet = pd.read_excel(PATH, Const.GROUP_COUNT).fillna("")
        self.survey_sheet = pd.read_excel(PATH, Const.SURVEY).fillna("")
        self.prod_seq_sheet = pd.read_excel(PATH, Const.PROD_SEQ).fillna("")
        self.match_product_in_scene = self.data_provider[Data.MATCHES]

    @property
    def position_graphs(self):
        if not hasattr(self, '_position_graphs'):
            self._position_graphs = INBEVBR_SANDPositionGraphs(self.data_provider, rds_conn=self.rds_conn)
        return self._position_graphs

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        kpis_sheet = pd.read_excel(PATH, Const.KPIS).fillna("")
        for index, row in kpis_sheet.iterrows():
            self.handle_atomic(row)
        # self.handle_simon_kpis()
        # self.commit_results_data()

    def handle_atomic(self, row):
        atomic_id = row[Const.KPI_ID]
        atomic_name = row[Const.ENGLISH_KPI_NAME].strip()
        store_type_template = row[Const.STORE_TYPE_TEMPLATE].strip()

        # if cell in template is not empty
        if store_type_template != "":
            store_types = store_type_template.split(",")
            store_types = [item.strip() for item in store_types]
            if self.store_type_filter not in store_types:
                return 0

        kpi_type = row[Const.KPI_TYPE].strip()
        if kpi_type == Const.SOS:
            self.handle_sos_atomics(atomic_id, atomic_name)
        elif kpi_type == Const.COUNT:
            self.handle_count_atomics(atomic_id, atomic_name)
        elif kpi_type == Const.GROUP_COUNT:
            self.handle_group_count_atomics(atomic_id, atomic_name)
        elif kpi_type == Const.SURVEY:
            self.handle_survey_atomics(atomic_id, atomic_name)
        elif kpi_type == Const.PROD_SEQ:
            self.handle_prod_seq_atomics(atomic_id, atomic_name)


    def handle_sos_atomics(self,atomic_id, atomic_name):

        target = 0
        count_result = 0
        numerator_number_of_facings = 0

        # bring the kpi rows from the sos sheet
        rows = self.sos_sheet.loc[self.sos_sheet[Const.KPI_ID] == atomic_id]

        # get a single row
        row = self.find_row(rows)

        if len(row) != 1:
            Log.warning("Dataframe is not correct, wrong number of lines: " + str(len(row)))
            row = -1

        # enter only if there is a matching store, region and state
        if isinstance(row, pd.DataFrame):
            target = row[Const.TARGET].values[0]
            weight = row[Const.WEIGHT].values[0]
            target_operator = row[Const.TARGET_OPERATOR].values[0].strip()
            count_type = row[Const.COUNT_TYPE].values[0].strip()

            # get the filters
            filters = self.get_filters_from_row(row.squeeze())

            if count_type == Const.FACING:
                numerator_number_of_facings = self.count_of_facings(self.scif, filters)
                if numerator_number_of_facings != 0:
                    del filters['manufacturer_name']
                    denominator_number_of_total_facings = self.count_of_facings(self.scif, filters)
                    if target_operator == '%':
                        percentage = 100 * (numerator_number_of_facings / denominator_number_of_total_facings)
                        count_result = weight if percentage >= target else 0

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_name_new_tables(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return

        self.write_to_db_result_new_tables(fk=atomic_pk, numerator_id=self.session_id,
                                           numerator_result=numerator_number_of_facings,
                                           denominator_result=target, result=count_result)

    def handle_count_atomics(self, atomic_id, atomic_name):

        target = 0
        count_result = 0
        numerator_number_of_facings = 0

        # bring the kpi rows from the count sheet
        rows = self.count_sheet.loc[self.count_sheet[Const.KPI_ID] == atomic_id]

        # get a single row
        row = self.find_row(rows)

        if len(row) != 1:
            Log.warning("Dataframe is not correct, wrong number of lines: " + str(len(row)))
            return

        # enter only if there is a matching store, region and state
        if isinstance(row, pd.DataFrame):
            target = row[Const.TARGET].values[0]
            weight = row[Const.WEIGHT].values[0]
            count_type = row[Const.COUNT_TYPE].values[0].strip()
            container_type = row[Const.CONTAINER_TYPE].values[0].strip()

            if container_type != "":
                df = self.scif[self.scif['att1'] == container_type]
            else:
                df = self.scif.copy()

            # get the filters
            filters = self.get_filters_from_row(row.squeeze())

            if count_type == Const.FACING:
                number_of_facings = self.count_of_facings(df, filters)
                count_result = weight if number_of_facings >= target else 0

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_name_new_tables(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return

        self.write_to_db_result_new_tables(fk=atomic_pk, numerator_id=self.session_id,
                                           numerator_result=numerator_number_of_facings,
                                           denominator_result=target, result=count_result)

    def handle_group_count_atomics(self, atomic_id, atomic_name):

        target = 0
        count_result = 0
        numerator_number_of_facings = 0
        group_score = 0

        # bring the kpi rows from the group_count sheet
        rows = self.group_count_sheet.loc[self.group_count_sheet[Const.KPI_ID] == atomic_id]

        # get a single row
        matching_rows = self.find_row(rows)

        for index, row in matching_rows.iterrows():
            target = row[Const.TARGET]
            weight = row[Const.WEIGHT]
            score = row[Const.SCORE]
            count_type = row[Const.COUNT_TYPE].strip()

            # get the filters
            del row[Const.GROUP_KPI_NAME]
            del row[Const.SCORE]

            # get the filters
            filters = self.get_filters_from_row(row)

            if count_type == Const.FACING:
                number_of_facings = self.count_of_facings(self.scif, filters)
                if number_of_facings >= target:
                    group_score += score
                    if group_score >= 100:
                        count_result = weight
                        break

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_name_new_tables(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return


        self.write_to_db_result_new_tables(fk=atomic_pk, numerator_id=self.session_id,
                                           numerator_result=numerator_number_of_facings,
                                           denominator_result=target, result=count_result)

    def find_row(self, rows):

        if len(rows) == 1:
            row = rows.copy()
            store_type_template = row[Const.STORE_TYPE_TEMPLATE].values[0].strip()
            if store_type_template != "":
                store_types = store_type_template.split(",")
                store_types = [item.strip() for item in store_types]
                if self.store_type_filter not in store_types:
                    return -1

            region_template = row[Const.REGION_TEMPLATE].values[0].strip()
            if region_template != "":
                regions = region_template.split(",")
                regions = [item.strip() for item in regions]
                if self.region_name_filter not in regions:
                    return -1

            state_template = row[Const.STATE_TEMPLATE].values[0].strip()
            if state_template != "":
                states = state_template.split(",")
                states = [item.strip() for item in states]
                if self.state_name_filter not in states:
                    return -1

        else:
            temp = rows[Const.STORE_TYPE_TEMPLATE]
            rows_stores_filter = rows[(temp == self.store_type_filter) | (temp == "")]
            temp = rows_stores_filter[Const.REGION_TEMPLATE]
            rows_regions_filter = rows_stores_filter[(temp == self.region_name_filter) | (temp == "")]
            temp = rows_regions_filter[Const.STATE_TEMPLATE]
            row = rows_regions_filter[(temp == self.state_name_filter) | (temp == "")]

        return row

    def get_filters_from_row(self, row):
        filters = dict(row)

        # no need to be accounted for
        for field in Const.DELETE_FIELDS:
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
                                Const.BRAND: 'brand_name',
                                Const.SUB_BRAND: 'Sub Brand',
                                Const.CATEGORY: 'category',
                                Const.SUB_CATEGORY: 'sub_category',
                                Const.TEMPLATE_NAME: 'template_name',
                                Const.MANUFACTURER: 'manufacturer_name'}

        for key in filters.keys():
            filters[convert_from_scif[key]] = filters.pop(key)
        return filters

    def count_of_facings(self, df, filters):

        facing_data = df[self.tools.get_filter_condition(df, **filters)]
        number_of_facings = facing_data['facings'].sum()
        return number_of_facings

    def handle_survey_atomics(self, atomic_id, atomic_name):

        # bring the kpi rows from the survey sheet
        rows = self.survey_sheet.loc[self.survey_sheet[Const.KPI_ID] == atomic_id]

        # get a the correct rows
        row = self.find_row(rows)

        if row == -1:
            survey_result = -1
        else:
            # find the answer to the survey in session
            question_id = row[Const.SURVEY_QUESTION_ID].values[0]
            question_answer_template = row[Const.TARGET_ANSWER].values[0]

            survey_result = self.survey.get_survey_answer(('question_fk', question_id))
            if question_answer_template == Const.NUMERIC:
                if not survey_result:
                    survey_result = -1
                if not isinstance(survey_result, (int, long, float)):
                    Log.warning("question id " + str(question_id) + " in template is not a number")
                    survey_result = -1

            else:
                answer = self.survey.check_survey_answer(('question_fk', question_id), question_answer_template)
                survey_result = 1 if answer else 0

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_name_new_tables(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return

        self.write_to_db_result_new_tables(fk=atomic_pk, numerator_id=self.session_id, numerator_result=survey_result,
                                           result=survey_result)

    def handle_prod_seq_atomics(self, atomic_id, atomic_name):

        target = 0
        count_result = 0
        numerator_number_of_facings = 0
        group_score = 0
        rows_filter_stores = pd.DataFrame

        # bring the kpi rows in the PROD_SEQ sheet
        rows = self.prod_seq_sheet.loc[self.prod_seq_sheet[Const.KPI_ID] == atomic_id]

        # filter the relevant store lines
        for index, row in rows.iterrows():
            store_type_template = row[Const.STORE_TYPE_TEMPLATE].strip()
            store_types = store_type_template.split(",")
            store_types = [item.strip() for item in store_types]
            if self.store_type_filter in store_types:
                temp = rows[Const.STORE_TYPE_TEMPLATE]
                rows_filter_stores = rows[(temp == store_type_template)]
                break

        row_example = rows_filter_stores.iloc[0]
        row_example[Const.CATEGORY] = row_example[Const.SUB_CATEGORY] = row_example[Const.BRAND] = row_example[Const.SUB_BRAND] = ""
        del row_example['Left or Right']
        del row_example['Exclude Brand']
        del row_example['Left Brand']
        del row_example['Right Brand']
        del row_example['Score']
        filters = self.get_filters_from_row(row_example)

        scenes = self.scif[self.tools.get_filter_condition(self.scif, **filters)]['scene_id'].drop_duplicates().tolist()

        # for i in xrange(len(self.relative_positioning)):
        #     params = self.relative_positioning.iloc[i]
        #     tested_filters = {'brand_name': params.get('Tested Brand Name')}
        #     anchor_filters = {'brand_name': params.get('Anchor Brand Name')}
        #     direction_data = {'top': self._get_direction_for_relative_position(params.get(self.TOP_DISTANCE)),
        #                       'bottom': self._get_direction_for_relative_position(
        #                           params.get(self.BOTTOM_DISTANCE)),
        #                       'left': self._get_direction_for_relative_position(
        #                           params.get(self.LEFT_DISTANCE)),
        #                       'right': self._get_direction_for_relative_position(
        #                           params.get(self.RIGHT_DISTANCE))}
        #     general_filters = {'template_display_name': params.get(self.LOCATION)}
        #     result = self.tools.calculate_relative_position(tested_filters, anchor_filters, direction_data,
        #                                                     **general_filters)
        #     score = 1 if result else 0



        #
        #
        # for index, row in rows_filter_stores.iterrows():
        #     target = row[Const.TARGET]
        #     weight = row[Const.WEIGHT]
        #     score = row[Const.SCORE]
        #     count_type = row[Const.COUNT_TYPE].strip()
        #
        #     # get the filters
        #     del row[Const.GROUP_KPI_NAME]
        #     del row[Const.SCORE]
        #
        #     # get the filters
        #     filters = self.get_filters_from_row(row)
        #
        #     if count_type == Const.FACING:
        #         number_of_facings = self.count_of_facings(self.scif, filters)
        #         if number_of_facings >= target:
        #             group_score += score
        #             if group_score >= 100:
        #                 count_result = weight
        #                 break

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_name_new_tables(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return

        self.write_to_db_result_new_tables(fk=atomic_pk, numerator_id=self.session_id,
                                           numerator_result=numerator_number_of_facings,
                                           denominator_result=target, result=count_result)

    def get_new_kpi_static_data(self):
        """
            This function extracts the static new KPI data (new tables) and saves it into one global data frame.
            The data is taken from static.kpi_level_2.
            """
        query = INBEVBRQueries.get_new_kpi_data()
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
        delete_query = INBEVBRQueries.get_delete_session_results_query(self.session_uid, self.session_id)
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

    #
    # def calculate_availability(self, active_products):
    #     active_products_pks = active_products['product_fk'].unique().tolist()
    #     filters = {'product_fk': active_products_pks}
    #     filtered_df = self.scif[self.tools.get_filter_condition(self.scif, **filters)]
    #     facing_filtered = filtered_df.loc[filtered_df['facings'] > 0][['template_fk','product_fk', 'facings']]
    #     facing_filtered_pks = facing_filtered['product_fk'].unique().tolist()
    #     for product in facing_filtered_pks:
    #         product_df = facing_filtered.loc[facing_filtered['product_fk'] == product]
    #         product_template_fks = product_df['template_fk'].unique().tolist()
    #         for template_fk in product_template_fks:
    #             sum_facing = product_df.loc[product_df['template_fk'] == template_fk]['facings'].sum()
    #             self.write_to_db_result_new_tables(fk=Const.AVAILABILITY_PK, numerator_id=product, score='1',
    #                                                denominator_id=template_fk, numerator_result='1', result=sum_facing)
    #
    # def calculate_pricing(self, all_products):
    #     only_sku_type_products = all_products.loc[all_products['product_type'] == 'SKU']
    #     all_products_fks_size = only_sku_type_products[['product_fk', 'size']].fillna("")
    #     product_fks_and_prices = self.prices_per_session
    #     merge_size_and_price = pd.merge(all_products_fks_size, product_fks_and_prices, how='left', on='product_fk')
    #     merge_size_and_price['value'] = merge_size_and_price['value'].fillna('0')
    #     for row in merge_size_and_price.itertuples():
    #         product = row[1] # row['product_fk']
    #         size = row[2] # row['size']
    #         price = row[3] # row['value']
    #         if size == '':
    #             size = 0
    #         if price > 0:
    #             self.write_to_db_result_new_tables(fk=Const.PRICING_PK, numerator_id=product,
    #                                            numerator_result=size, result=price)
