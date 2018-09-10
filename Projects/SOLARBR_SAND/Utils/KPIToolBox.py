#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import pandas as pd
import numpy as np
import json
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
from Projects.SOLARBR_SAND.Utils.Const import Const
from KPIUtils_v2.DB.Common import Common
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

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.templates = {}
        self.score_templates = {}
        self.get_templates()
        self.get_score_template()
        self.manufacturer_fk = self.all_products[
            self.all_products['manufacturer_name'] == 'Coca Cola'].iloc[0]
        self.sos = SOS(self.data_provider, self.output)
        self.total_score = 0
        # self.store_type = self.data_provider.store_type



    def get_templates(self):

        for sheet in Const.SHEETS_MAIN:
            self.templates[sheet] = pd.read_excel(MAIN_TEMPLATE_PATH, sheetname=sheet.decode("utf-8"), keep_default_na=False)


    def get_score_template(self):
        for sheet in Const.SHEETS_SCORE:
            self.score_templates[sheet] = pd.read_excel(SCORE_TEMPLATE_PATH, sheetname=sheet.decode("utf-8"), keep_default_na=False)


    def main_calculation(self, *args, **kwargs):
        main_template = self.templates[Const.KPIS]
        for i, main_line in main_template.iterrows():
            self.calculate_main_kpi(main_line)
        # self.write_to_db_result(
        #     self.common_db.get_kpi_fk_by_kpi_name(CMA_COMPLIANCE, 1), score=self.total_score, level=1)

        # self.common.write_to_db_result(
        #     fk=total_kpi_fk, numerator_id=self.manufacturer_fk, result=self.round_result(self.total_score),
        #     denominator_id=self.store_id,
        #     identifier_result=result, score=self.round_result(self.total_store))




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

                        function = self.get_kpi_function(kpi_type)
                        for i, kpi_line in relevant_template.iterrows():
                            result, score = function(kpi_line, general_filters)
                    else:
                        pass

                    if score > 0:
                            self.total_score += score
                    else:
                        pass

               # self.write_to_all_levels(kpi_name=kpi_name, result=result, score=score)
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


        return sos_value, score

    def get_score_from_range(self, kpi_name, sos_value):
        store_type = str(self.store_info["store_type"].iloc[0])
        score_range = self.score_templates[store_type].query('Kpi == "' + str(kpi_name) +
                                                          '" & Low <= ' + str(sos_value) +
                                                          ' & High >= ' + str(sos_value)+'')
        score = score_range['Score'].iloc[0]
        return score


    def get_kpi_function(self, kpi_type):
        """
        transfers every kpi to its own function
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


    def write_to_all_levels(self, kpi_name, result, score, target=None, scene_fk=None, reuse_scene=False):
        """
        Writes the final result in the "all" DF, add the score to the red score and writes the KPI in the DB
        :param kpi_name: str
        :param result: int
        :param display_text: str
        :param weight: int/float
        :param scene_fk: for the scene's kpi
        :param reuse_scene: this kpi can use scenes that were used
        """
        result_dict = {Const.KPI_NAME: kpi_name, Const.RESULT: result, Const.SCORE: score, Const.THRESHOLD: target}
        # self.all_results = self.all_results.append(result_dict, ignore_index=True)
        print(kpi_name, score, result)
        # self.write_to_db(kpi_name, score, result=result)