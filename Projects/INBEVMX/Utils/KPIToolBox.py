#!/usr/bin/env python
# -*- coding: utf-8 -*

import pandas as pd
import os
import json

from pandas.io.json import json_normalize
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.CommonV2 import Common as Common_V2, log_runtime
from Projects.INBEVMX.Data.Const import Const
from Projects.INBEVMX.Utils.Fetcher import INBEVMXQueries
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox


__author__ = 'ilays'

KPI_NEW_TABLE = 'report.kpi_level_2_results'
PATH_SURVEY_AND_SOS_TARGET = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                          '..', 'Data', 'inbevmx_template_v3.6.xlsx')


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
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common_v2.get_kpi_static_data()
        self.kpi_results_queries = []
        self.kpi_results_new_tables_queries = []
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.oos_policies = self.get_policies()
        self.result_dict = {}
        self.hierarchy_dict = {}
        self.sos_target_sheet = pd.read_excel(
            PATH_SURVEY_AND_SOS_TARGET, Const.SOS_TARGET).fillna("")
        self.survey_sheet = pd.read_excel(PATH_SURVEY_AND_SOS_TARGET, Const.SURVEY).fillna("")
        self.survey_combo_sheet = pd.read_excel(PATH_SURVEY_AND_SOS_TARGET, Const.SURVEY_COMBO).fillna("")
        self.oos_sheet = pd.read_excel(PATH_SURVEY_AND_SOS_TARGET, Const.OOS_KPI).fillna("")

        try:
            self.store_type_filter = self.store_info['store_type'].values[0].strip()
        except:
            Log.warning("There is no store type in the db for store_fk: {}").format(str(self.store_id))
        try:
            self.region_name_filter = self.store_info['region_name'].values[0].strip()
            self.region_fk = self.store_info['region_fk'].values[0]
        except:
            Log.warning("There is no region in the db for store_fk: {}").format(str(self.store_id))
        try:
            self.att6_filter = self.store_info['additional_attribute_6'].values[0].strip()
        except:
            Log.warning("There is no additional attribute 6 in the db for store_fk: {}").format(str(self.store_id))

    def get_policies(self):
        query = INBEVMXQueries.get_policies()
        policies = pd.read_sql_query(query, self.rds_conn.db)
        return policies

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        kpis_sheet = pd.read_excel(PATH_SURVEY_AND_SOS_TARGET, Const.KPIS).fillna("")
        for index, row in kpis_sheet.iterrows():
            self.handle_atomic(row)
        self.save_parent_kpis()
        self.common_v2.commit_results_data()


    def calculate_oos_target(self):
        temp = self.oos_sheet[Const.TEMPLATE_STORE_TYPE]
        rows_stores_filter = self.oos_sheet[
            temp.apply(lambda r: self.store_type_filter in [item.strip() for item in r.split(",")])]
        if rows_stores_filter.empty:
            weight = 0
        else:
            weight = rows_stores_filter[Const.TEMPLATE_SCORE].values[0]
        all_data = pd.merge(self.scif[["store_id","product_fk","facings","template_name"]], self.store_info, left_on="store_id",right_on="store_fk")
        if all_data.empty:
            return 0
        json_policies = self.oos_policies.copy()
        json_policies[Const.POLICY] = self.oos_policies[Const.POLICY].apply(lambda line: json.loads(line))
        diff_policies = json_policies[Const.POLICY].drop_duplicates().reset_index()
        diff_table = json_normalize(diff_policies[Const.POLICY].tolist())

        # remove all lists from df
        diff_table = diff_table.applymap(lambda x: x[0] if isinstance(x, list) else x)
        for col in diff_table.columns:
            att = all_data.iloc[0][col]
            if att is None:
                return 0
            diff_table = diff_table[diff_table[col] == att]
            all_data = all_data[all_data[col] == att]
        if len(diff_table) > 1:
            Log.warning("There is more than one possible match")
            return 0
        if diff_table.empty:
            return 0
        selected_row = diff_policies.iloc[diff_table.index[0]][Const.POLICY]
        json_policies = json_policies[json_policies[Const.POLICY] == selected_row]
        products_to_check = json_policies['product_fk'].tolist()
        products_df = all_data[(all_data['product_fk'].isin(products_to_check))][['product_fk','facings']].fillna(0)
        products_df = products_df.groupby('product_fk').sum().reset_index()
        try:
            atomic_pk_sku = self.common_v2.get_kpi_fk_by_kpi_name(Const.OOS_SKU_KPI)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + Const.OOS_SKU_KPI)
            return 0
        for product in products_to_check:
            if product not in products_df['product_fk'].values:
                products_df = products_df.append({'product_fk': product, 'facings': 0.0}, ignore_index=True)
        for index, row in products_df.iterrows():
            result = 0 if row['facings'] > 0 else 1
            self.common_v2.write_to_db_result(fk=atomic_pk_sku, numerator_id=row['product_fk'],
                                        numerator_result=row['facings'], denominator_id=self.store_id,
                                        result=result, score=result, identifier_parent=Const.OOS_KPI,
                                        should_enter=True, parent_fk=3)

        not_existing_products_len = len(products_df[products_df['facings'] == 0])
        result = not_existing_products_len / float(len(products_to_check))
        try:
            atomic_pk = self.common_v2.get_kpi_fk_by_kpi_name(Const.OOS_KPI)
            result_oos_pk = self.common_v2.get_kpi_fk_by_kpi_name(Const.OOS_RESULT_KPI)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + Const.OOS_KPI)
            return 0
        score = result * weight
        self.common_v2.write_to_db_result(fk=atomic_pk, numerator_id=self.region_fk,
                                           numerator_result=not_existing_products_len, denominator_id=self.store_id,
                                           denominator_result=len(products_to_check), result=result, score=score,
                                          identifier_result=Const.OOS_KPI, parent_fk=3)
        self.common_v2.write_to_db_result(fk=result_oos_pk, numerator_id=self.region_fk,
                                          numerator_result=not_existing_products_len, denominator_id=self.store_id,
                                          denominator_result=len(products_to_check), result=result, score=result,
                                          parent_fk=3)
        return score

    def save_parent_kpis(self):
        for kpi in self.result_dict.keys():
            try:
                kpi_fk = self.common_v2.get_kpi_fk_by_kpi_name(kpi)
            except IndexError:
                Log.warning("There is no matching Kpi fk for kpi name: " + kpi)
                continue
            if kpi not in self.hierarchy_dict:
                self.common_v2.write_to_db_result(fk=kpi_fk, numerator_id=self.region_fk, denominator_id=self.store_id,
                                                  result=self.result_dict[kpi], score=self.result_dict[kpi],
                                                    identifier_result=kpi, parent_fk=1)
            else:
                self.common_v2.write_to_db_result(fk=kpi_fk, numerator_id=self.region_fk,
                                                  denominator_id=self.store_id,
                                                  result=self.result_dict[kpi], score=self.result_dict[kpi],
                                                  identifier_result=kpi, identifier_parent=self.hierarchy_dict[kpi],
                                                  should_enter=True, parent_fk=2)

    def handle_atomic(self, row):
        result = 0
        atomic_id = row[Const.TEMPLATE_KPI_ID]
        atomic_name = row[Const.KPI_LEVEL_3].strip()
        kpi_name = row[Const.KPI_LEVEL_2].strip()
        set_name = row[Const.KPI_LEVEL_1].strip()
        kpi_type = row[Const.TEMPLATE_KPI_TYPE].strip()
        if atomic_name != kpi_name:
            parent_name = kpi_name
        else:
            parent_name = set_name
        if kpi_type == Const.SOS_TARGET:
            if self.scene_info['number_of_probes'].sum() > 1:
                result = self.handle_sos_target_atomics(atomic_id, atomic_name, parent_name)
        elif kpi_type == Const.SURVEY:
            result = self.handle_survey_atomics(atomic_id, atomic_name, parent_name)
        elif kpi_type == Const.SURVEY_COMBO:
            result = self.handle_survey_combo(atomic_id, atomic_name, parent_name)
        elif kpi_type == Const.OOS_KPI:
            result = self.calculate_oos_target()

        # Update kpi results
        if atomic_name != kpi_name:
            if kpi_name not in self.result_dict.keys():
                self.result_dict[kpi_name] = result
                self.hierarchy_dict[kpi_name] = set_name
            else:
                self.result_dict[kpi_name] += result

        # Update set results
        if set_name not in self.result_dict.keys():
            self.result_dict[set_name] = result
        else:
            self.result_dict[set_name] += result

    def handle_sos_target_atomics(self, atomic_id, atomic_name, parent_name):

        denominator_number_of_total_facings = 0
        count_result = -1

        # bring the kpi rows from the sos sheet
        rows = self.sos_target_sheet.loc[self.sos_target_sheet[Const.TEMPLATE_KPI_ID] == atomic_id]

        # get a single row
        row = self.find_row(rows)
        if row.empty:
            return 0

        target = row[Const.TEMPLATE_TARGET_PRECENT].values[0]
        score = row[Const.TEMPLATE_SCORE].values[0]
        df = pd.merge(self.scif, self.store_info, how="left",
                      left_on="store_id", right_on="store_fk")

        # get the filters
        filters = self.get_filters_from_row(row.squeeze())
        numerator_number_of_facings = self.count_of_facings(df, filters)
        if numerator_number_of_facings != 0 and count_result == -1:
            if 'manufacturer_name' in filters.keys():
                deno_manufacturer = row[Const.TEMPLATE_MANUFACTURER_DENOMINATOR].values[0].strip()
                deno_manufacturer = deno_manufacturer.split(",")
                filters['manufacturer_name'] = [item.strip() for item in deno_manufacturer]
                denominator_number_of_total_facings = self.count_of_facings(df, filters)
                percentage = 100 * (numerator_number_of_facings /
                                    denominator_number_of_total_facings)
                count_result = score if percentage >= target else -1


        if count_result == -1:
            return 0

        try:
            atomic_pk = self.common_v2.get_kpi_fk_by_kpi_name(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return 0

        self.common_v2.write_to_db_result(fk=atomic_pk, numerator_id=self.region_fk,
                                           numerator_result=numerator_number_of_facings, denominator_id=self.store_id,
                                           denominator_result=denominator_number_of_total_facings, result=count_result,
                                          score=count_result, identifier_result=atomic_name, identifier_parent=parent_name,
                                          should_enter=True, parent_fk=3)
        return count_result

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

    def handle_survey_combo(self, atomic_id, atomic_name, parent_name):
        # bring the kpi rows from the survey sheet
        numerator = denominator = 0
        rows = self.survey_combo_sheet.loc[self.survey_combo_sheet[Const.TEMPLATE_KPI_ID] == atomic_id]
        temp = rows[Const.TEMPLATE_STORE_TYPE]
        row_store_filter = rows[(temp.apply(lambda r: self.store_type_filter in [item.strip() for item in
                                                                                 r.split(",")])) | (temp == "")]
        if row_store_filter.empty:
            return 0

        condition = row_store_filter[Const.TEMPLATE_CONDITION].values[0]
        condition_type = row_store_filter[Const.TEMPLATE_CONDITION_TYPE].values[0]
        score = row_store_filter[Const.TEMPLATE_SCORE].values[0]

        # find the answer to the survey in session
        for i, row in row_store_filter.iterrows():
            question_text = row[Const.TEMPLATE_SURVEY_QUESTION_TEXT]
            question_answer_template = row[Const.TEMPLATE_TARGET_ANSWER]

            survey_result = self.survey.get_survey_answer(('question_text', question_text))
            if not survey_result:
                continue
            if '-' in question_answer_template:
                numbers = question_answer_template.split('-')
                try:
                    numeric_survey_result = int(survey_result)
                except:
                    Log.warning("Survey question - " + str(question_text) + " - doesn't have a numeric result")
                    continue
                if numeric_survey_result < int(numbers[0]) or numeric_survey_result > int(numbers[1]):
                    continue
                numerator_or_denominator = row_store_filter[Const.NUMERATOR_OR_DENOMINATOR].values[0]
                if numerator_or_denominator == Const.DENOMINATOR:
                    denominator += numeric_survey_result
                else:
                    numerator += numeric_survey_result
            else:
                continue
        if condition_type == '%':
            if denominator != 0:
                fraction = 100 * (float(numerator) / float(denominator))
            else:
                if numerator > 0:
                    fraction = 100
                else:
                    fraction = 0
            result = score if fraction >= condition else 0
        else:
            return 0

        try:
            atomic_pk = self.common_v2.get_kpi_fk_by_kpi_name(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return 0
        self.common_v2.write_to_db_result(fk=atomic_pk, numerator_id=self.region_fk, numerator_result=numerator,
                                          denominator_result=denominator, denominator_id=self.store_id, result=result,
                                          score=result, identifier_result=atomic_name, identifier_parent=parent_name,
                                          should_enter=True, parent_fk=3)
        return result

    def handle_survey_atomics(self, atomic_id, atomic_name, parent_name):
        # bring the kpi rows from the survey sheet
        rows = self.survey_sheet.loc[self.survey_sheet[Const.TEMPLATE_KPI_ID] == atomic_id]
        temp = rows[Const.TEMPLATE_STORE_TYPE]
        row_store_filter = rows[(temp.apply(lambda r: self.store_type_filter in [item.strip() for item in
                                                                                 r.split(",")])) | (temp == "")]

        if row_store_filter.empty:
            return 0
        else:
            # find the answer to the survey in session
            question_text = row_store_filter[Const.TEMPLATE_SURVEY_QUESTION_TEXT].values[0]
            question_answer_template = row_store_filter[Const.TEMPLATE_TARGET_ANSWER].values[0]
            score = row_store_filter[Const.TEMPLATE_SCORE].values[0]

            survey_result = self.survey.get_survey_answer(('question_text', question_text))
            if not survey_result:
                return 0
            if '-' in question_answer_template:
                numbers = question_answer_template.split('-')
                try:
                    numeric_survey_result = int(survey_result)
                except:
                    Log.warning("Survey question - " + str(question_text) + " - doesn't have a numeric result")
                    return 0
                if numeric_survey_result < int(numbers[0]) or numeric_survey_result > int(numbers[1]):
                    return 0
                condition = row_store_filter[Const.TEMPLATE_CONDITION].values[0]
                if condition != "":
                    second_question_text = row_store_filter[Const.TEMPLATE_SECOND_SURVEY_QUESTION_TEXT].values[0]
                    second_survey_result = self.survey.get_survey_answer(
                        ('question_text', second_question_text))
                    if not second_survey_result:
                        second_survey_result = 0
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
        final_score = score if survey_result == 1 else 0

        try:
            atomic_pk = self.common_v2.get_kpi_fk_by_kpi_name(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return 0
        self.common_v2.write_to_db_result(fk=atomic_pk, numerator_id=self.region_fk, numerator_result=0,
                                        denominator_result=0, denominator_id=self.store_id, result=survey_result,
                                        score=final_score, identifier_result=atomic_name, identifier_parent=parent_name,
                                        should_enter=True, parent_fk=3)
        return final_score

    def get_new_kpi_static_data(self):
        """
            This function extracts the static new KPI data (new tables) and saves it into one global data frame.
            The data is taken from static.kpi_level_2.
            """
        query = INBEVMXQueries.get_new_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data
