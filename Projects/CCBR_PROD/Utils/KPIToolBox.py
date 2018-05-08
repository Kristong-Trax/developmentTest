#!/usr/bin/env python
# -*- coding: utf-8 -*

import time
import pandas as pd
import os

from datetime import datetime
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from KPIUtils.Calculations.Survey import Survey
from Projects.CCBR_PROD.Utils.Fetcher import CCBRQueries
from Projects.CCBR_PROD.Utils.GeneralToolBox import CCBRGENERALToolBox
from Projects.CCBR_PROD.Data.Const import Const
from KPIUtils.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils.DB.Common import Common

__author__ = 'ilays'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
KPI_NEW_TABLE = 'report.kpi_level_2_results'
PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Femsa template v2.6 - KENGINE.xlsx')


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


class CCBRToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = CCBRGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.kpi_results_queries = []
        self.survey = Survey(self.data_provider, self.output)
        self.kpi_results_new_tables_queries = []
        self.New_kpi_static_data = self.get_new_kpi_static_data()
        self.session_id = self.data_provider.session_id
        self.prices_per_session = PsDataProvider(self.data_provider, self.output).get_price(self.session_id)
        self.common_db = Common(self.data_provider)
        self.count_sheet = pd.read_excel(PATH, Const.COUNT).fillna("")
        self.group_count_sheet = pd.read_excel(PATH, Const.GROUP_COUNT).fillna("")
        self.survey_sheet = pd.read_excel(PATH, Const.SURVEY).fillna("")
        self.sos_sheet = pd.read_excel(PATH, Const.SOS).fillna("")

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        kpis_sheet = pd.read_excel(PATH, Const.KPIS).fillna("")
        for index, row in kpis_sheet.iterrows():
            self.handle_atomic(row)
        self.handle_simon_kpis()
        self.commit_results_data()

    def handle_simon_kpis(self):
        active_products = self.all_products.loc[self.all_products["is_active"] > 0]
        self.calculate_availability(active_products)
        self.calculate_pricing(active_products)

    def calculate_availability(self, active_products):
        active_products_pks = active_products['product_fk'].unique().tolist()
        filters = {'product_fk': active_products_pks}
        filtered_df = self.scif[self.tools.get_filter_condition(self.scif, **filters)]
        facing_filtered = filtered_df.loc[filtered_df['facings'] > 0][['product_fk', 'facings']]
        facing_filtered_pks = facing_filtered['product_fk'].unique().tolist()
        for product in active_products_pks:
            if product in facing_filtered_pks:
                sum_facing = facing_filtered.loc[facing_filtered['product_fk'] == product]['facings'].values[0]
                self.write_to_db_result_new_tables(fk=Const.AVAILABILITY_PK, numerator_id=product,
                                                   numerator_result='1', result=sum_facing, score='1')

    def calculate_pricing(self, active_products):
        active_products_fks_size = active_products[['product_fk', 'size']].fillna("")
        product_fks_and_prices = self.prices_per_session
        merge_size_and_price = pd.merge(active_products_fks_size, product_fks_and_prices, how='left', on='product_fk')
        merge_size_and_price['value'] = merge_size_and_price['value'].fillna('0')
        for index, row in merge_size_and_price.iterrows():
            product = row['product_fk']
            price = row['value']
            size = row['size']
            if size == '':
                size = -1
            self.write_to_db_result_new_tables(fk=Const.PRICING_PK, numerator_id=product,
                                                   numerator_result=size, result=price)

    def handle_atomic(self, row):
        atomic_name = row[Const.ENGLISH_KPI_NAME]
        kpi_type = row[Const.KPI_TYPE]
        if kpi_type == Const.SURVEY:
            self.handle_survey_atomics(atomic_name)
        elif kpi_type == Const.COUNT:
            self.handle_count_atomics(atomic_name)
        elif kpi_type == Const.GROUP_COUNT:
            self.handle_group_count_atomics(atomic_name)
        elif kpi_type == Const.SOS:
            self.handle_sos_atomics(atomic_name)

    def handle_survey_atomics(self, atomic_name):
        row = self.survey_sheet.loc[self.survey_sheet[Const.ENGLISH_KPI_NAME] == atomic_name]
        if row.empty:
            Log.warning("Dataframe is empty, wrong kpi name: " + atomic_name)
            return
        store_type_filter = self.store_info['store_type'].values[0]
        store_type_template = row[Const.STORE_TYPE_TEMPLATE].values[0]

        # if cell in template is not empty
        if store_type_template != "":
            store_types = store_type_template.split(",")
            if store_type_filter not in store_types:
                return

        # find the answer to the survey in session
        question_id = row[Const.SURVEY_QUESTION_ID].values[0]
        question_answer_template = row[Const.TARGET_ANSWER].values[0]

        survey_result = self.survey.get_survey_answer(('question_fk', question_id))
        if question_answer_template == Const.NUMERIC:
            if not survey_result:
                survey_result = 0
            if not isinstance(survey_result, (int, long, float)):
                Log.warning("question id " + str(question_id) + " in template is not a number")
                survey_result = 0

        else:
            answer = self.survey.check_survey_answer(('question_fk', question_id), question_answer_template)
            survey_result = 1 if answer else -1

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_name_new_tables(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return

        self.write_to_db_result_new_tables(fk=atomic_pk, numerator_id=self.session_id, numerator_result=survey_result,
                                           result=survey_result)


    def handle_count_atomics(self, atomic_name):
        sum_of_count = 0
        target = 0
        count_result = 0
        row = self.count_sheet.loc[self.count_sheet[Const.ENGLISH_KPI_NAME] == atomic_name]
        if row.empty:
            Log.warning("Dataframe is empty, wrong kpi name: " + atomic_name)
            return

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_name_new_tables(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return
        for index, row in row.iterrows():
            sum_of_count, target, count_result = self.handle_count_row(row)
        self.write_to_db_result_new_tables(fk=atomic_pk, numerator_id=self.session_id,
                                           numerator_result=sum_of_count,
                                           denominator_result=target, result=count_result)

    def handle_group_count_atomics(self, atomic_name):
        rows = self.group_count_sheet.loc[self.group_count_sheet[Const.GROUP_KPI_NAME] == atomic_name]
        group_weight = 0
        group_result = 0
        group_target = 0
        group_sum_of_count = 0
        if rows.empty:
            Log.warning("Dataframe is empty, wrong kpi name: " + atomic_name)
            return

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_name_new_tables(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return

        for index, row in rows.iterrows():
            weight = row[Const.WEIGHT]
            sum_of_count, target, count_result = self.handle_count_row(row)
            group_sum_of_count += sum_of_count
            if count_result == 100:
                group_target += target
                group_weight += weight
                if group_weight > 100:
                    group_result = 1
                    continue

        self.write_to_db_result_new_tables(fk=atomic_pk, numerator_id=self.session_id,
                                           numerator_result=group_sum_of_count,
                                           denominator_result=group_target, result=group_result)

    def handle_count_row(self, row):
        count_type = row[Const.COUNT_TYPE]
        target = row[Const.TARGET]
        target_operator = row[Const.TARGET_OPERATOR]
        products_filter = self.products['product_name'].values.tolist()
        product_template = row[Const.PRODUCT]
        store_type_filter = self.store_info['store_type'].values[0]
        store_type_template = row[Const.STORE_TYPE_TEMPLATE]
        product_size = row[Const.PRODUCT_SIZE]
        product_size_operator = row[Const.PRODUCT_SIZE_OPERATOR]
        product_measurement_unit = row[Const.MEASUREMENT_UNIT]

        # filter store type
        if store_type_template != "":
            store_types = store_type_template.split(",")
            if store_type_filter not in store_types:
                return 0,0,0

        # filter product
        if product_template != "":
            check = 0
            products_to_check = product_template.split(",")
            for p in products_to_check:
                if p in products_filter:
                    check = 1
                    continue
            if check == 0:
                return 0,0,0

        # filter product size
        filtered_df = self.scif
        if product_size != "":
            if product_measurement_unit == 'l':
                product_size *= 1000
            if product_size_operator == '<': filtered_df = self.scif[self.scif['size'] < product_size]
            elif product_size_operator == '<=': filtered_df = self.scif[self.scif['size'] <= product_size]
            elif product_size_operator == '>': filtered_df = self.scif[self.scif['size'] > product_size]
            elif product_size_operator == '>=': filtered_df = self.scif[self.scif['size'] >= product_size]
            elif product_size_operator == '=': filtered_df = self.scif[self.scif['size'] == product_size]

        filters = self.get_filters_from_row(row)
        count_of_units = 0
        if count_type == Const.SCENE:
            count_of_units = self.count_of_scenes(filtered_df, filters)
        elif count_type == Const.UNIQUE_SKU:
            count_of_units = self.count_of_unique_skus(filtered_df, filters)
        elif count_type == Const.FACING:
            count_of_units = self.count_of_facings(filtered_df, filters)
        else:
            Log.warning("Couldn't find a correct COUNT variable in template")

        if (target_operator == '<='):
            count_result = 1 if (target <= count_of_units) else 0
        else:
            count_result = 1 if (target >= count_of_units) else 0
        return count_of_units, target, count_result

    # todo: implement SOS, pass if there is at least one that have more than 50% of scenes (description in template)
    def handle_sos_atomics(self, atomic_name):
        row = self.sos_sheet.loc[self.sos_sheet[Const.ENGLISH_KPI_NAME] == atomic_name]
        result = 0
        sum_of_count = 0
        target = 0
        if row.empty:
            Log.warning("Dataframe is empty, wrong kpi name: " + atomic_name)
            return
        scene_types = row[Const.TEMPLATE_GROUP].values[0].split(",")
        for scene in scene_types:
            for index, r in row.iterrows():
                r[Const.TEMPLATE_GROUP] = scene
                sum_of_count, target, count_result = self.handle_count_row(r)
                total_products_in_scene = len(self.scif[self.scif['template_group'] == scene])
                if sum_of_count > total_products_in_scene * 0.5:
                    result += sum_of_count

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_name_new_tables(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return
        self.write_to_db_result_new_tables(fk=atomic_pk, numerator_id=self.session_id,
                                           numerator_result=sum_of_count,
                                           denominator_result=target, result=sum_of_count)

    def get_filters_from_row(self, row):
        filters = dict(row)

        # no need to be accounted for
        for field in Const.DELETE_FIELDS:
            del filters[field]

        if Const.WEIGHT in filters.keys():
            del filters[Const.WEIGHT]
        if Const.GROUP_KPI_NAME in filters.keys():
            del filters[Const.GROUP_KPI_NAME]

        exclude_manufacturer = filters[Const.EXCLUDE_MANUFACTURER]
        if exclude_manufacturer != "":
            filters[Const.MANUFACTURER] = (exclude_manufacturer, Const.EXCLUDE_FILTER)
            del filters[Const.EXCLUDE_MANUFACTURER]

        # filter all the empty cells
        for key in filters.keys():
            if (filters[key] == ""):
                del filters[key]
            elif isinstance(filters[key], tuple):
                filters[key] = (filters[key][0].split(","), filters[key][1])
            else:
                filters[key] = filters[key].split(",")
        return self.create_filters_according_to_scif(filters)

    def create_filters_according_to_scif(self, filters):
        convert_from_scif =    {Const.TEMPLATE_GROUP: 'template_group',
                                Const.TEMPLATE_NAME: 'template_name',
                                Const.BRAND: 'brand_name',
                                Const.CATEGORY: 'category',
                                Const.MANUFACTURER: 'manufacturer_name',
                                Const.PRODUCT_TYPE: 'product_type'}
        for key in filters.keys():
            filters[convert_from_scif[key]] = filters.pop(key)
        return filters

    def count_of_scenes(self, filtered_df, filters):
        scene_data = filtered_df[self.tools.get_filter_condition(filtered_df, **filters)]
        number_of_scenes = len(scene_data['scene_id'].unique())
        return number_of_scenes

    def count_of_unique_skus(self, filtered_df, filters):

        unique_skus_data = filtered_df[self.tools.get_filter_condition(filtered_df, **filters)]
        number_of_unique_skus = len(unique_skus_data['product_ean_code'].unique())
        return number_of_unique_skus

    def count_of_facings(self, filtered_df, filters):

        facing_data = filtered_df[self.tools.get_filter_condition(filtered_df, **filters)]
        number_of_facings = facing_data['facings'].sum()
        return number_of_facings

    def get_new_kpi_static_data(self):
        """
            This function extracts the static new KPI data (new tables) and saves it into one global data frame.
            The data is taken from static.kpi_level_2.
            """
        query = CCBRQueries.get_new_kpi_data()
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
        delete_query = CCBRQueries.get_delete_session_results_query(self.session_uid, self.session_id)
        cur.execute(delete_query)
        for query in insert_queries:
            cur.execute(query)
        self.rds_conn.db.commit()

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