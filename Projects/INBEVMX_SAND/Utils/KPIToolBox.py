#!/usr/bin/env python
# -*- coding: utf-8 -*

import pandas as pd
import os
import json

from pandas.io.json import json_normalize
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.CommonV2 import Common as Common_V2, log_runtime
from Projects.INBEVMX_SAND.Data.Const import Const
from Projects.INBEVMX_SAND.Utils.Fetcher import INBEVMXQueries
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox


__author__ = 'ilays'

KPI_NEW_TABLE = 'report.kpi_level_2_results'
PATH_SURVEY_AND_SOS_TARGET = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                          '..', 'Data', 'inbevmx_template_v1.4.xlsx')


class INBEVMXToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.session_id = self.data_provider.session_id
        self.products = self.data_provider[Data.PRODUCTS]
        self.common_v2 = Common_V2(self.data_provider)
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.tools = GENERALToolBox(self.data_provider)
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.survey = Survey(self.data_provider, self.output)
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common_v2.get_kpi_static_data()
        self.kpi_results_queries = []
        self.kpi_results_new_tables_queries = []
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.oos_policies = self.get_policies()


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
        try:
            self.att6_filter = self.store_info['additional_attribute_6'].values[0].strip()
        except:
            Log.error("there is no additional attribute 6 in the db")
            return
        self.sos_target_sheet = pd.read_excel(
            PATH_SURVEY_AND_SOS_TARGET, Const.SOS_TARGET).fillna("")
        self.survey_sheet = pd.read_excel(PATH_SURVEY_AND_SOS_TARGET, Const.SURVEY).fillna("")

    def get_policies(self):
        query = INBEVMXQueries.get_policies()
        policies = pd.read_sql_query(query, self.rds_conn.db)
        return policies

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        kpis_sheet = pd.read_excel(PATH_SURVEY_AND_SOS_TARGET, Const.KPIS).fillna("")
        self.calculate_oos_target()
        for index, row in kpis_sheet.iterrows():
            self.handle_atomic(row)
        self.common_v2.commit_results_data()


    def calculate_oos_target(self):
        all_data = pd.merge(self.scif[["store_id","product_fk","facings","template_name"]], self.store_info, left_on="store_id",right_on="store_fk")
        if all_data.empty:
            return
        json_policies = self.oos_policies.copy()
        json_policies[Const.POLICY] = self.oos_policies[Const.POLICY].apply(lambda line: json.loads(line))
        diff_policies = json_policies[Const.POLICY].drop_duplicates().reset_index()
        diff_table = json_normalize(diff_policies[Const.POLICY].tolist())

        # remove all lists from df
        diff_table = diff_table.applymap(lambda x: x[0] if isinstance(x, list) else x)
        for col in diff_table.columns:
            att = all_data.iloc[0][col]
            if att is None:
                return
            diff_table = diff_table[diff_table[col] == att]
            all_data = all_data[all_data[col] == att]
        if len(diff_table) > 1:
            Log.warning ("There is more than one possible match")
            return
        if diff_table.empty:
            return
        selected_row = diff_policies.iloc[diff_table.index[0]][Const.POLICY]
        json_policies = json_policies[json_policies[Const.POLICY] == selected_row]
        products_to_check = json_policies['product_fk'].tolist()
        products_df = all_data[(all_data['product_fk'].isin(products_to_check))][['product_fk','facings']].fillna(0)
        products_df = products_df.groupby('product_fk').sum()
        try:
            atomic_pk_sku = self.common_v2.get_kpi_fk_by_kpi_name(Const.OOS_SKU_KPI)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + Const.OOS_SKU_KPI)
            return
        for index, row in products_df.iterrows():
            result = 1 if row['facings'] > 0 else 0
            self.common_v2.write_to_db_result(fk=atomic_pk_sku, numerator_id=row['product_fk'],
                                        numerator_result=row['facings'], denominator_id=self.store_id,
                                        result=result, score=result, identifier_parent=Const.OOS_KPI, should_enter=True)

        existing_products_len = len(products_df[products_df['facings'] > 0])
        result = existing_products_len / float(len(products_to_check))
        try:
            atomic_pk = self.common_v2.get_kpi_fk_by_kpi_name(Const.OOS_KPI)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + Const.OOS_KPI)
            return
        self.common_v2.write_to_db_result(fk=atomic_pk, numerator_id=self.session_id,
                                           numerator_result=existing_products_len, denominator_id=self.store_id,
                                           denominator_result=len(products_to_check), result=result, score=result,
                                          identifier_result=Const.OOS_KPI)

    def handle_atomic(self, row):
        atomic_id = row[Const.TEMPLATE_KPI_ID]
        atomic_name = row[Const.TEMPLATE_ENGLISH_KPI_NAME].strip()
        kpi_type = row[Const.TEMPLATE_KPI_TYPE].strip()
        if kpi_type == Const.SOS_TARGET:
            self.handle_sos_target_atomics(atomic_id, atomic_name)
        elif kpi_type == Const.SURVEY:
            self.handle_survey_atomics(atomic_id, atomic_name)

    def handle_sos_target_atomics(self, atomic_id, atomic_name):

        denominator_number_of_total_facings = 0
        count_result = -1

        # bring the kpi rows from the sos sheet
        rows = self.sos_target_sheet.loc[self.sos_target_sheet[Const.TEMPLATE_KPI_ID] == atomic_id]

        # get a single row
        row = self.find_row(rows)
        if row.empty:
            return

        target = row[Const.TEMPLATE_TARGET_PRECENT].values[0]
        weight = row[Const.TEMPLATE_SCORE].values[0]
        df = self.scif.copy()
        df = pd.merge(self.scif, self.store_info, how="left",
                      left_on="store_id", right_on="store_fk")

        # get the filters
        filters = self.get_filters_from_row(row.squeeze())
        numerator_number_of_facings = self.count_of_facings(df, filters)
        if numerator_number_of_facings != 0 and count_result == -1:
            if 'manufacturer_name' in filters.keys():
                deno_manufacturer = row[Const.TEMPLATE_TARGET_PRECENT].values[0].split()
                filters['manufacturer_name'] = [item.strip() for item in deno_manufacturer]
                denominator_number_of_total_facings = self.count_of_facings(df, filters)
                percentage = 100 * (numerator_number_of_facings /
                                    denominator_number_of_total_facings)
                count_result = weight if percentage >= target else -1

        if count_result == -1:
            return

        try:
            atomic_pk = self.common_v2.get_kpi_fk_by_kpi_name(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return

        self.common_v2.write_to_db_result(fk=atomic_pk, numerator_id=self.session_id,
                                           numerator_result=numerator_number_of_facings, denominator_id=self.store_id,
                                           denominator_result=denominator_number_of_total_facings, result=count_result,
                                          score=count_result)

    def find_row(self, rows):
        temp = rows[Const.TEMPLATE_STORE_TYPE]
        rows_stores_filter = rows[(temp.apply(lambda r: self.store_type_filter in [item.strip()
                                                                                   for item in r.split(",")])) | (temp == "")]
        temp = rows_stores_filter[Const.TEMPLATE_REGION]
        rows_regions_filter = rows_stores_filter[(temp.apply(lambda r: self.region_name_filter in [item.strip()
                                                                                                   for item in r.split(",")])) | (temp == "")]
        temp = rows_regions_filter[Const.TEMPLATE_ADDITIONAL_ATTRIBUTE_6]
        rows_att6_filter = rows_regions_filter[(temp.apply(lambda r: self.att6_filter in [item.strip()
                                                                                          for item in r.split(",")])) | (temp == "")]
        return rows_att6_filter

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
        convert_from_scif = {Const.TEMPLATE_GROUP: 'template_group',
                             Const.TEMPLATE_MANUFACTURER_NOMINATOR: 'manufacturer_name',
                             Const.TEMPLATE_ADDITIONAL_ATTRIBUTE_6: 'additional_attribute_6'}

        for key in filters.keys():
            if key in convert_from_scif:
                filters[convert_from_scif[key]] = filters.pop(key)
        return filters

    def count_of_facings(self, df, filters):

        facing_data = df[self.tools.get_filter_condition(df, **filters)]
        number_of_facings = facing_data['facings'].sum()
        return number_of_facings

    def handle_survey_atomics(self, atomic_id, atomic_name):
        # bring the kpi rows from the survey sheet
        rows = self.survey_sheet.loc[self.survey_sheet[Const.TEMPLATE_KPI_ID] == atomic_id]
        temp = rows[Const.TEMPLATE_STORE_TYPE]
        row_store_filter = rows[(temp.apply(lambda r: self.store_type_filter in [item.strip() for item in
                                                                                 r.split(",")])) | (temp == "")]

        if row_store_filter.empty:
            return
        else:
            # find the answer to the survey in session
            question_id = row_store_filter[Const.TEMPLATE_SURVEY_QUESTION_ID].values[0]
            question_answer_template = row_store_filter[Const.TEMPLATE_TARGET_ANSWER].values[0]

            survey_result = self.survey.get_survey_answer(('question_fk', question_id))
            if not survey_result:
                return
            if '-' in question_answer_template:
                numbers = question_answer_template.split('-')
                try:
                    numeric_survey_result = int(survey_result)
                except:
                    Log.warning("Survey doesn't have a numeric result")
                    return
                if numeric_survey_result < int(numbers[0]) or numeric_survey_result > int(numbers[1]):
                    return
                condition = row_store_filter[Const.TEMPLATE_CONDITION].values[0]
                if condition != "":
                    second_question_id = row_store_filter[Const.TEMPLATE_SECOND_SURVEY_ID].values[0]
                    second_survey_result = self.survey.get_survey_answer(
                        ('question_fk', second_question_id))
                    second_numeric_survey_result = int(second_survey_result)
                    survey_result = 1 if numeric_survey_result >= second_numeric_survey_result else -1
                else:
                    survey_result = 1
            else:
                question_answer_template = question_answer_template.split(',')
                question_answer_template = [item.strip() for item in question_answer_template]
                if survey_result in question_answer_template:
                    survey_result = 1
                else:
                    survey_result = -1

        try:
            atomic_pk = self.common_v2.get_kpi_fk_by_kpi_name(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return
        self.common_v2.write_to_db_result(fk=atomic_pk, numerator_id=self.session_id, numerator_result=0,
                                           denominator_result=0, denominator_id=self.store_id, result=survey_result,
                                          score=survey_result)

    def get_new_kpi_static_data(self):
        """
            This function extracts the static new KPI data (new tables) and saves it into one global data frame.
            The data is taken from static.kpi_level_2.
            """
        query = INBEVMXQueries.get_new_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data
