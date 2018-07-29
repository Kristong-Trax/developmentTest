import os
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Projects.CCBOTTLERSUS_SAND.REDSCORE.Const import Const
from KPIUtils_v2.DB.Common import Common as Common
from KPIUtils_v2.DB.CommonV2 import Common as Common2


__author__ = 'Elyashiv'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'KPITemplateV1.xlsx')
SURVEY_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'SurveyTemplateV1.xlsx')


class CCBOTTLERSUS_SANDSceneRedToolBox:

    def __init__(self, data_provider, output, templates, common):
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
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.templates = templates
        self.region = self.store_info['region_name'].iloc[0]
        self.store_type = self.store_info['store_type'].iloc[0]
        self.common = common
        self.kpi_static_data_session = self.common.kpi_static_data
        self.scenes_results = pd.DataFrame(columns=Const.COLUMNS_OF_SCENE)

    def main_calculation(self, *args, **kwargs):
        """
            :param kwargs: dict - kpi line from the template.
            the function gets the kpi (level 2) row, and calculates its children.
            :return: float - score of the kpi.
        """
        main_template = self.templates[Const.KPIS]
        main_template = main_template[(main_template[Const.REGION] == self.region) &
                                      (main_template[Const.STORE_TYPE] == self.store_type) &
                                      (main_template[Const.SESSION_LEVEL].isnull())]
        for i, line in main_template.iterrows():
            self.calculate_main_kpi(line)
        return self.scenes_results

    def write_to_scene_level(self, kpi_name, kpi_fk, scene_fk, result=0):
        result_dict = {Const.KPI_FK: kpi_fk, Const.KPI_NAME: kpi_name, Const.SCENE_FK: scene_fk, Const.RESULT: result}
        self.scenes_results = self.scenes_results.append(result_dict, ignore_index=True)

    def calculate_main_kpi(self, kpi_main_line):
        kpi_name = kpi_main_line[Const.KPI_NAME]
        target = kpi_main_line[Const.GROUP_TARGET]
        kpi_type = kpi_main_line[Const.SHEET]
        relevant_template = self.templates[kpi_type]
        relevant_template = relevant_template[relevant_template[Const.KPI_NAME] == kpi_name]
        if target == Const.ALL:
            target = len(relevant_template)
        passed_counter = 0
        function = self.get_kpi_function(kpi_type)
        for i, kpi_line in relevant_template.iterrows():
            answer = function(kpi_line)
            if answer:
                passed_counter += 1
        return passed_counter >= target

    def get_kpi_function(self, kpi_type):
        if kpi_type == Const.AVAILABILITY:
            return self.calculate_availability_sku
        else:
            return None

    def calculate_availability_sku(self, kpi_line):
        ssd_still = kpi_line[Const.SSD_STILL]
        scene_type = kpi_line[Const.SCENE_TYPE]
        scene_type_group = kpi_line[Const.SCENE_TYPE_GROUP]
        manu = kpi_line[Const.SSD_STILL]
        ssd_still = kpi_line[Const.SSD_STILL]

