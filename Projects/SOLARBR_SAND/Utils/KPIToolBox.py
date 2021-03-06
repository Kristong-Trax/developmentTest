#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import pandas as pd
import numpy as np
import json
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Projects.SOLARBR_SAND.Utils.Const import Const
from KPIUtils_v2.DB.Common import Common
from Projects.SOLARBR_SAND.Utils.Fetcher import SOLARBRQueries
from Trax.Algo.Calculations.Core.Shortcuts import BaseCalculationsGroup
from Trax.Algo.Calculations.Core.Constants import Fields as Fd
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox
from Trax.Algo.Calculations.Core.Utils import ToolBox as TBox
from Trax.Algo.Calculations.Core.Utils import Validation
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'nicolaske'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


SCORE_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Score Template.xlsx')
MAIN_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'KPI Template v0.1.xlsx')





class SOLARBRToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    EXCLUDE_EMPTY = False
    EXCLUDE_FILTER = 0
    EMPTY = 'Empty'

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.templates = {}
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
        # self.store_type = self.data_provider.store_type



    def get_templates(self):

        for sheet in Const.SHEETS_MAIN:
            self.templates[sheet] = pd.read_excel(MAIN_TEMPLATE_PATH, sheetname=sheet.decode("utf-8"), keep_default_na=False)


    def get_score_template(self):
        for sheet in Const.SHEETS_SCORE:
            self.score_templates[sheet] = pd.read_excel(SCORE_TEMPLATE_PATH, sheetname=sheet.decode("utf-8"), keep_default_na=False, encoding = "utf-8")


    def main_calculation(self, *args, **kwargs):
        main_template = self.templates[Const.KPIS]
        for i, main_line in main_template.iterrows():
            self.calculate_main_kpi(main_line)
        self.commit_results()





    def calculate_main_kpi(self, main_line):
        kpi_name = main_line[Const.KPI_NAME]
        kpi_type = main_line[Const.Type]
        scene_types = self.does_exist(main_line, Const.SCENE_TYPES)

        result = score = 0
        general_filters = {}


        scif_scene_types = self.scif['template_name'].unique().tolist()
        store_type = str(self.store_info["store_type"].iloc[0])
        store_types = self.does_exist_store(main_line, Const.STORE_TYPES)
        if store_type in store_types:

            if scene_types:
                if (('All' in scene_types) or bool(set(scif_scene_types) & set(scene_types))) :
                    if not ('All' in scene_types):
                        general_filters['template_name'] = scene_types
                    if kpi_type == Const.SOVI:
                        relevant_template = self.templates[kpi_type]
                        relevant_template = relevant_template[relevant_template[Const.KPI_NAME] == kpi_name]

                        if relevant_template["numerator param 1"].all() and relevant_template["denominator param"].all():
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





    def calculate_sos(self, kpi_line,  general_filters):
        kpi_name = kpi_line[Const.KPI_NAME]
        den_type = kpi_line[Const.DEN_TYPES_1]
        den_value = kpi_line[Const.DEN_VALUES_1].split(',')

        num_type = kpi_line[Const.NUM_TYPES_1]
        num_value = kpi_line[Const.NUM_VALUES_1].split(',')

        general_filters[den_type] = den_value

        sos_filters = {num_type : num_value}

        if kpi_line[Const.NUM_TYPES_2]:
            num_type_2 = kpi_line[Const.NUM_TYPES_2]
            num_value_2 = kpi_line[Const.NUM_VALUES_2].split(',')
            sos_filters[num_type_2] = num_value_2

        sos_value = self.sos.calculate_share_of_shelf(sos_filters, **general_filters)
        # sos_value *= 100
        sos_value = round(sos_value, 2)

        score = self.get_score_from_range(kpi_name, sos_value)

        manufacturer_products = self.all_products[
            self.all_products['manufacturer_name'] == num_value[0]].iloc[0]

        manufacturer_fk = manufacturer_products["manufacturer_fk"]

        all_products = self.all_products[
            self.all_products['category'] == den_value[0]].iloc[0]

        category_fk = all_products["category_fk"]



        numerator_res, denominator_res = self.get_numerator_and_denominator(sos_filters, **general_filters)

        self.common.write_to_db_result_new_tables(fk = 1,
                                                  numerator_id=manufacturer_fk,
                                                  numerator_result= numerator_res,
                                                  denominator_id=category_fk,
                                                  denominator_result= denominator_res,
                                                  result=sos_value,
                                                  score= score,
                                                  score_after_actions= score)
        return sos_value, score

    def get_score_from_range(self, kpi_name, sos_value):
        store_type = str(self.store_info["store_type"].iloc[0])
        self.score_templates[store_type] = self.score_templates[store_type].replace(kpi_name, kpi_name.encode("utf-8"))
        score_range = self.score_templates[store_type].query('Kpi == "' + str(kpi_name.encode("utf-8")) +
                                                          '" & Low <= ' + str(sos_value) +
                                                          ' & High >= ' + str(sos_value)+'')
        score = score_range['Score'].iloc[0]
        return score


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
                return 0,0
            else:
                subset_population = filtered_population[subset_filter]
                # ratio = TBox.calculate_ratio_sum_field_in_rows(filtered_population, subset_population, Fd.FACINGS)

                df = filtered_population
                subset_df = subset_population
                sum_field  = Fd.FACINGS
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
                    # raise Exception(msg)

                default_value = 0

                numerator = TBox.calculate_frame_column_sum(subset_df, sum_field, default_value)
                denominator = TBox.calculate_frame_column_sum(df, sum_field, default_value)

                return numerator, denominator

        except Exception as e:

             Log.error(e.message)


        return True

    def commit_results(self):
        insert_queries = self.merge_insert_queries(self.kpi_results_new_tables_queries)
        self.rds_conn.disconnect_rds()
        self.rds_conn.connect_rds()
        cur = self.rds_conn.db.cursor()
        delete_query = SOLARBRQueries.get_delete_session_results_query(self.session_uid, self.session_id)
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