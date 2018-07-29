import os
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Projects.CCBOTTLERSUS_SAND.REDSCORE.Const import Const
from Projects.CCBOTTLERSUS_SAND.REDSCORE.SceneKPIToolBox import CCBOTTLERSUS_SANDSceneRedToolBox
from KPIUtils_v2.DB.Common import Common as Common
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
# from KPIUtils_v2.DB.CommonV2 import Common as Common2

__author__ = 'Elyashiv'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'KPITemplateV1.xlsx')
SURVEY_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'SurveyTemplateV1.xlsx')


class CCBOTTLERSUS_SANDREDToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
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
        self.survey = Survey(self.data_provider, self.output)
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.templates = {}
        for sheet in Const.SHEETS:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheetname=sheet)
        self.region = self.store_info['region_name'].iloc[0]
        self.store_type = self.store_info['store_type'].iloc[0]
        self.common = Common(self.data_provider)
        self.kpi_static_data_session = self.common.kpi_static_data
        self.scene_calculator = CCBOTTLERSUS_SANDSceneRedToolBox(data_provider, output, self.templates, self.common)
        self.scenes_results = None

    def main_calculation(self, *args, **kwargs):
        """
            :param kwargs: dict - kpi line from the template.
            the function gets the kpi (level 2) row, and calculates its children.
            :return: float - score of the kpi.
        """
        self.scenes_results = self.scene_calculator.main_calculation()
        main_template = self.templates[Const.KPIS]
        main_template = main_template[(main_template[Const.REGION] == self.region) &
                                      (main_template[Const.STORE_TYPE] == self.store_type)]
        for i, line in main_template.iterrows():
            weight = line[Const.WEIGHT]
            if line[Const.SHEET] == Const.SURVEY:
                self.calculate_survey_specific(line)

    def calculate_main_kpi(self, kpi_main_line, scene_fk):
        kpi_name = kpi_main_line[Const.KPI_NAME]
        target = kpi_main_line[Const.GROUP_TARGET]
        kpi_type = kpi_main_line[Const.SHEET]
        relevant_template = self.templates[kpi_type]
        relevant_template = relevant_template[relevant_template[Const.KPI_NAME] == kpi_name]
        if target == Const.ALL:
            target = len(relevant_template)
        passed_counter = 0
        function = self.get_kpi_function(kpi_type)
        for i, line in relevant_template.iterrows():
            answer = function(line)
            if answer:
                passed_counter += 1
        return passed_counter >= target

    def get_kpi_function(self, kpi_type):
        if kpi_type == Const.SURVEY:
            return self.calculate_survey_specific
        else:
            return None

    def calculate_survey_specific(self, kpi_line):
        question = kpi_line[Const.Q_TEXT]
        question_id = None
        if not question:
            question_id = kpi_line[Const.Q_ID]
        answers = kpi_line[Const.ACCEPTED_ANSWER].split(',')
        if question:
            for answer in answers:
                if self.survey.check_survey_answer(survey_text=question, target_answer=answer):
                    return True
        elif question_id:
            for answer in answers:
                if self.survey.check_survey_answer(survey_text=('question_fk', int(question_id)), target_answer=answer):
                    return True
            return False
