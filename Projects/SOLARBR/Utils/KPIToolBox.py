#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import pandas as pd
import re
import numpy as np
import json
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Projects.SOLARBR.Utils.Const import Const
from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from Trax.Algo.Calculations.Core.Shortcuts import BaseCalculationsGroup
from Trax.Algo.Calculations.Core.Constants import Fields as Fd
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox
from Trax.Algo.Calculations.Core.Utils import ToolBox as TBox
from Trax.Algo.Calculations.Core.Utils import Validation
from KPIUtils_v2.Calculations.SOSCalculations import SOS

__author__ = 'nicolaske'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

SCORE_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Score Template_Solar_2019_DH_1.1.xlsx')
MAIN_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'KPI Template 2019_DH_1.1.xlsx')


class SOLARBRToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    EXCLUDE_EMPTY = False
    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2

    EMPTY = 'Empty'

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.commonV2 = CommonV2(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.products = self.data_provider[Data.PRODUCTS]
        # self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.commonV2.get_kpi_static_data()
        self.kpi_results_queries = []
        self.templates = {}
        self.all_products = self.commonV2.data_provider[Data.ALL_PRODUCTS]
        self.session_id = self.data_provider.session_id
        self.score_templates = {}
        self.get_templates()
        self.get_score_template()
        self.manufacturer_fk = self.all_products[
            self.all_products['manufacturer_name'] == 'Coca Cola'].iloc[0]
        self.sos = SOS(self.data_provider, self.output)
        self.total_score = 0
        self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]
        self.toolbox = GENERALToolBox(self.data_provider)
        self.scenes_info = self.data_provider[Data.SCENES_INFO]
        self.kpi_results_new_tables_queries = []

    def get_templates(self):

        for sheet in Const.SHEETS_MAIN:
            self.templates[sheet] = pd.read_excel(MAIN_TEMPLATE_PATH, sheetname=sheet.decode("utf-8"),
                                                  keep_default_na=False)

    def get_score_template(self):
        for sheet in Const.SHEETS_SCORE:
            self.score_templates[sheet] = pd.read_excel(SCORE_TEMPLATE_PATH, sheetname=sheet.decode("utf-8"),
                                                        keep_default_na=False, encoding="utf-8")

    def main_calculation(self, *args, **kwargs):
        main_template = self.templates[Const.KPIS]
        for i, main_line in main_template.iterrows():
            self.calculate_main_kpi(main_line)
        self.commonV2.commit_results_data()

    def calculate_main_kpi(self, main_line):
        kpi_name = main_line[Const.KPI_NAME]
        kpi_type = main_line[Const.Type]
        template_groups = self.does_exist(main_line, Const.TEMPLATE_GROUP)

        general_filters = {}

        scif_template_groups = self.scif['template_group'].unique().tolist()
        # encoding_fixed_list = [template_group.replace("\u2013","-") for template_group in scif_template_groups]
        # scif_template_groups = encoding_fixed_list

        store_type = self.store_info["store_type"].iloc[0]
        store_types = self.does_exist_store(main_line, Const.STORE_TYPES)
        if store_type in store_types:

            if template_groups:
                if ('All' in template_groups) or bool(set(scif_template_groups) & set(template_groups)):
                    if not ('All' in template_groups):
                        general_filters['template_group'] = template_groups
                    if kpi_type == Const.SOVI:
                        relevant_template = self.templates[kpi_type]
                        relevant_template = relevant_template[relevant_template[Const.KPI_NAME] == kpi_name]

                        if relevant_template["numerator param 1"].all() and relevant_template[
                            "denominator param 1"].all():
                            function = self.get_kpi_function(kpi_type)
                            for i, kpi_line in relevant_template.iterrows():
                                result, score = function(kpi_line, general_filters)
                    else:
                        pass

            else:

                pass

    @staticmethod
    def does_exist(kpi_line, column_name):
        """
        checks if kpi_line has values in this column, and if it does - returns a list of these values
        :param kpi_line: line from template
        :param column_name: str
        :return: list of values if there are, otherwise None
        """
        if column_name in kpi_line.keys() and kpi_line[column_name] != "":
            cell = kpi_line[column_name]
            if type(cell) in [int, float]:
                return [cell]
            elif type(cell) in [unicode, str]:
                return cell.split(", ")
        return None

    @staticmethod
    def does_exist_store(kpi_line, column_name):
        """
        checks if kpi_line has values in this column, and if it does - returns a list of these values
        :param kpi_line: line from template
        :param column_name: str
        :return: list of values if there are, otherwise None
        """
        if column_name in kpi_line.keys() and kpi_line[column_name] != "":
            cell = kpi_line[column_name]
            if type(cell) in [int, float]:
                return [cell]
            elif type(cell) in [unicode, str]:
                return cell.split(",")
        return None

    def calculate_sos(self, kpi_line, general_filters):
        kpi_name = kpi_line[Const.KPI_NAME]

        # get denominator filters
        for den_column in [col for col in kpi_line.keys() if Const.DEN_TYPE in col]:  # get relevant den columns
            if kpi_line[den_column]:  # check to make sure this kpi has this denominator param
                general_filters[kpi_line[den_column]] = \
                    kpi_line[den_column.replace(Const.DEN_TYPE, Const.DEN_VALUE)].split(',')  # get associated values

        general_filters = self.convert_operators_to_values(general_filters)

        sos_filters = {}
        # get numerator filters
        for num_column in [col for col in kpi_line.keys() if Const.NUM_TYPE in col]:  # get relevant numerator columns
            if kpi_line[num_column]:  # check to make sure this kpi has this numerator param
                sos_filters[kpi_line[num_column]] = \
                    kpi_line[num_column.replace(Const.NUM_TYPE, Const.NUM_VALUE)].split(',')  # get associated values

        sos_filters = self.convert_operators_to_values(sos_filters)

        sos_value = self.sos.calculate_share_of_shelf(sos_filters, **general_filters)
        # sos_value *= 100
        sos_value = round(sos_value, 2)

        score = self.get_score_from_range(kpi_name, sos_value)

        manufacturer_products = self.all_products[
            self.all_products['manufacturer_name'] == sos_filters['manufacturer_name'][0]].iloc[0]

        manufacturer_fk = manufacturer_products["manufacturer_fk"]

        filtered_kpi_list = self.kpi_static_data[self.kpi_static_data['type'] == kpi_name]
        kpi_fk = filtered_kpi_list['pk'].iloc[0]

        numerator_res, denominator_res = self.get_numerator_and_denominator(sos_filters, **general_filters)

        if numerator_res is None:
            numerator_res = 0

        denominator_fk = None
        if general_filters.keys()[0] == 'category':
            category_fk = self.all_products["category_fk"][
                self.all_products['category'] == general_filters['category'][0]].iloc[0]
            denominator_fk = category_fk

        elif general_filters.keys()[0] == 'sub_category':
            try:
                sub_category_fk = self.all_products["sub_category_fk"][
                    self.all_products['sub_category'] == general_filters['sub_category'][0]].iloc[0]
                denominator_fk = sub_category_fk
            except:
                sub_brand_fk = 999
                denominator_fk = sub_brand_fk

        elif general_filters.keys()[0] == 'sub_brand':
            # sub brand table is empty, update when table is updated

            try:
                sub_brand_fk = self.all_products["sub_category_fk"][
                    self.all_products['sub_brand'] == general_filters['sub_brand'][0]].iloc[0]
            except:

                sub_brand_fk = 999

            denominator_fk = sub_brand_fk

        self.commonV2.write_to_db_result(fk=kpi_fk,
                                         numerator_id=manufacturer_fk,
                                         numerator_result=numerator_res,
                                         denominator_id=denominator_fk,
                                         denominator_result=denominator_res,
                                         result=sos_value,
                                         score=score,
                                         score_after_actions=score,
                                         context_id=kpi_fk)

        return sos_value, score

    def get_score_from_range(self, kpi_name, sos_value):
        store_type = str(self.store_info["store_type"].iloc[0].encode("utf-8"))
        self.score_templates[store_type] = self.score_templates[store_type].replace(kpi_name,
                                                                                    kpi_name.encode("utf-8").rstrip())
        score_range = self.score_templates[store_type].query('Kpi == "' + str(kpi_name.encode("utf-8")) +
                                                             '" & Low <= ' + str(sos_value) +
                                                             ' & High >= ' + str(sos_value) + '')
        try:
            score = score_range['Score'].iloc[0]
        except IndexError:
            Log.error('No score data found for KPI name {} in store type {}'.format(kpi_name.encode("utf-8"), store_type.encode("utf-8")))
            return 0

        return score

    def convert_operators_to_values(self, filters):
        if 'number_of_sub_packages' in filters.keys():
            value = filters['number_of_sub_packages']
            operator, number = [x.strip() for x in re.split('(\d+)', value[0]) if x != '']
            if operator == '>=':
                subpackages_num = self.scif[self.scif['number_of_sub_packages'] >= int(number)]['number_of_sub_packages'].unique().tolist()
                filters['number_of_sub_packages'] = subpackages_num
            elif operator == '<=':
                subpackages_num = self.scif[self.scif['number_of_sub_packages'] <= int(number)]['number_of_sub_packages'].unique().tolist()
                filters['number_of_sub_packages'] = subpackages_num
            elif operator == '>':
                subpackages_num = self.scif[self.scif['number_of_sub_packages'] > int(number)]['number_of_sub_packages'].unique().tolist()
                filters['number_of_sub_packages'] = subpackages_num
            elif operator == '<':
                subpackages_num = self.scif[self.scif['number_of_sub_packages'] < int(number)]['number_of_sub_packages'].unique().tolist()
                filters['number_of_sub_packages'] = subpackages_num
        return filters

    def get_kpi_function(self, kpi_type):
        """
        transfers every kpi to its own function    .encode('utf-8')
        :param kpi_type: value from "sheet" column in the main sheet
        :return: function
        """
        if kpi_type == Const.SOVI:
            return self.calculate_sos
        else:
            Log.warning("The value '{}' in column sheet in the template is not recognized".format(kpi_type))
            return None

    @staticmethod
    def round_result(result):
        return round(result, 3)

    def get_numerator_and_denominator(self, sos_filters=None, include_empty=False, **general_filters):

        if include_empty == self.EXCLUDE_EMPTY and 'product_type' not in sos_filters.keys() + general_filters.keys():
            general_filters['product_type'] = (self.EMPTY, self.EXCLUDE_FILTER)
        pop_filter = self.toolbox.get_filter_condition(self.scif, **general_filters)
        subset_filter = self.toolbox.get_filter_condition(self.scif, **sos_filters)
        try:
            pop = self.scif

            filtered_population = pop[pop_filter]
            if filtered_population.empty:
                return 0, 0
            else:
                subset_population = filtered_population[subset_filter]

                df = filtered_population
                subset_df = subset_population
                sum_field = Fd.FACINGS
                try:
                    Validation.is_empty_df(df)
                    Validation.is_empty_df(subset_df)
                    Validation.is_subset(df, subset_df)
                    Validation.df_columns_equality(df, subset_df)
                    Validation.validate_columns_exists(df, [sum_field])
                    Validation.validate_columns_exists(subset_df, [sum_field])
                    Validation.is_none(sum_field)
                except Exception, e:
                    msg = "Data verification failed: {}.".format(e)

                default_value = 0

                numerator = TBox.calculate_frame_column_sum(subset_df, sum_field, default_value)
                denominator = TBox.calculate_frame_column_sum(df, sum_field, default_value)

                return numerator, denominator

        except Exception as e:

            Log.error(e.message)

        return True
