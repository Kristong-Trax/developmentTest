
import pandas as pd
import os
import re

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.Common import Common
from Projects.CCBZA.Utils.ParseTemplates import parse_template


from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
from KPIUtils_v2.Calculations.SurveyCalculations import Survey

from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'sergey'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

KPIS_TEMPLATE_NAME = 'MOLSONCOORS_CE_KPIs.xlsx'
KPIS_TEMPLATE_SHEET = 'KPIs'

SOS_VS_TARGET = 'sos vs target'
FACINGS_VS_TARGET = 'facings vs target'
DISTRIBUTION = 'distribution'
SUM_OF_SCORES = 'weighted sum of scores'
CONDITIONAL_PROPORTIONAL = 'conditional proportional'
WEIGHTED_AVERAGE = 'weighted average'
WEIGHTED_SCORE = 'weighted score'


class MOLSONCOORSHRToolBox:
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
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []

        self.template_path = self.get_template_path()
        self.template_data = self.get_template_data()

        self.scores = pd.DataFrame()

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        if not self.template_data or KPIS_TEMPLATE_SHEET not in self.template_data:
            Log.error('KPIs template sheet is empty or not found')
            return

        self.kpis_calculation()

        # kpis_0 = self.template_data['KPIs'][self.template_data['KPIs']['KPI Group'] == '']
        # for index_0, kpi_0 in kpis_0.iterrows():
        #     child_kpi_group_0 = kpi_0['Child KPI Group']
        #
        #     if not child_kpi_group_0:
        #         kpis_1 = self.template_data['KPIs'][self.template_data['KPIs']['KPI Group'] == child_kpi_group_0]
        #         for index_1, kpi_1 in kpis_1.iterrows():
        #
        #             child_kpi_group_1 = kpi_1['Child KPI Group']
        #             if not child_kpi_group_1:
        #                 kpis_2 = self.template_data['KPIs'][self.template_data['KPIs']['KPI Group'] == child_kpi_group_1]
        #                 for index_2, kpi_2 in kpis_2.iterrows():
        #
        #                     print kpi_2

    def kpis_calculation(self, kpi_group='', parent_kpi_type=''):

        total_score = 0
        kpis = self.template_data['KPIs'][self.template_data['KPIs']['KPI Group'] == kpi_group]
        for index, kpi in kpis.iterrows():
            child_kpi_group = kpi['Child KPI Group']
            kpi_type = kpi['KPI Type'].lower()
            kpi_score_formula = kpi['Score Function'].lower()
            kpi_weight = kpi['Weight']
            if not child_kpi_group:
                if kpi_type in [SOS_VS_TARGET]:
                    score = 10
                elif kpi_type in [FACINGS_VS_TARGET]:
                    score = 15
                elif kpi_type in [DISTRIBUTION]:
                    score = 40
                else:
                    Log.error("KPI of type '{}' is not supported".format(kpi_type))
                    score = 0
            else:
                score = self.kpis_calculation(child_kpi_group)

            if kpi_score_formula in [WEIGHTED_SCORE]:
                total_score += score * 1 #kpi_weight
            else:
                total_score += score

            self.scores = self.scores.append({'KPI': kpi['KPI name Eng'], 'Score': score}, ignore_index=True)

        return total_score


        # red_score = 0
        # red_target = 0
        # identifier_result_red_score = self.get_identifier_result_red_score()
        # for kpi_set_name in self.kpi_sets:
        #     self.current_kpi_set_name = kpi_set_name
        #     set_score = 0
        #     set_target = 0
        #     is_bonus = False
        #     kpi_data = self.template_data[KPI_TAB][self.template_data[KPI_TAB][SET_NAME] == kpi_set_name]
        #     identifier_result_set = self.get_identifier_result_set(kpi_set_name)
        #     for index, kpi in kpi_data.iterrows():
        #         kpi_types = self.get_kpi_types_by_kpi(kpi)
        #         identifier_result_kpi = self.get_identifier_result_kpi(kpi)
        #         is_bonus = self.is_bonus_kpi(kpi)
        #         for kpi_type in kpi_types:
        #             atomic_kpis_data = self.get_atomic_kpis_data(kpi_type, kpi)
        #             if not atomic_kpis_data.empty:
        #                 if kpi_type == 'Survey':
        #                     self.calculate_survey_new(atomic_kpis_data, identifier_result_kpi)
        #                 elif kpi_type == 'Availability':
        #                     self.calculate_availability_session_new(atomic_kpis_data, identifier_result_kpi)
        #                 elif kpi_type == 'Count':
        #                     self.calculate_count(atomic_kpis_data, identifier_result_kpi)
        #                 elif kpi_type == 'Price':
        #                     self.calculate_price(atomic_kpis_data, identifier_result_kpi)
        #                 elif kpi_type == 'SOS':
        #                     self.calculate_sos(atomic_kpis_data, identifier_result_kpi)
        #                 elif kpi_type == 'Planogram':
        #                     self.calculate_planogram_compliance(atomic_kpis_data, identifier_result_kpi)
        #                 else:
        #                     Log.warning("KPI of type '{}' is not supported".format(kpi_type))
        #                     continue
        #         kpi_result, kpi_target = self.calculate_kpi_result(kpi, identifier_result_set, identifier_result_kpi)
        #         set_score += kpi_result
        #         set_target += kpi_target
        #     set_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_set_name)
        #     self.common.write_to_db_result(fk=set_kpi_fk, numerator_id=self.ko_id, score=set_score,
        #                                    denominator_id=self.store_id,
        #                                    identifier_parent=identifier_result_red_score,
        #                                    identifier_result=identifier_result_set,
        #                                    target=set_target, should_enter=True)
        #     red_score += set_score
        #     if not is_bonus:
        #         red_target += set_target
        #
        # red_score_percent = float(red_score) / red_target if red_target != 0 else 0
        # red_score_kpi_fk = self.common.get_kpi_fk_by_kpi_type(RED_SCORE)
        # self.common.write_to_db_result(fk=red_score_kpi_fk, numerator_id=self.ko_id, result=red_score,
        #                                score=red_score_percent, identifier_result=identifier_result_red_score,
        #                                denominator_id=self.store_id, target=red_target, should_enter=True)
        # self.common.commit_results_data()

    def get_template_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', KPIS_TEMPLATE_NAME)

    def get_template_data(self):
        template_data = {}
        try:
            sheet_names = pd.ExcelFile(self.template_path).sheet_names
            for sheet in sheet_names:
                template_data[sheet] = parse_template(self.template_path, sheet, lower_headers_row_index=0)
        except IOError as e:
            Log.error('Template {} does not exist. {}'.format(KPIS_TEMPLATE_NAME, repr(e)))
        return template_data

