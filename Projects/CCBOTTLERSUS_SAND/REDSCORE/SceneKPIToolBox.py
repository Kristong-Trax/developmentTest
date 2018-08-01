import os
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Projects.CCBOTTLERSUS_SAND.REDSCORE.Const import Const

__author__ = 'Elyashiv'


class CCBOTTLERSUS_SANDSceneRedToolBox:

    def __init__(self, data_provider, output, templates, common, toolbox):
        self.output = output
        self.data_provider = data_provider
        self.toolbox = toolbox
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
        self.rds_conn = toolbox.rds_conn
        self.store_attr = toolbox.store_attr
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
        main_template = main_template[main_template[Const.SESSION_LEVEL] != Const.V]
        main_template = main_template[main_template[Const.SHEET].isin([Const.AVAILABILITY, Const.SCENE_AVAILABILITY, Const.SURVEY])]
        for i, main_line in main_template.iterrows():
            self.calculate_main_kpi(main_line)
        return self.scenes_results

    def write_to_scene_level(self, kpi_name, scene_fk, result=0):
        result_dict = {Const.KPI_NAME: kpi_name, Const.SCENE_FK: scene_fk, Const.RESULT: result}
        self.scenes_results = self.scenes_results.append(result_dict, ignore_index=True)

    def calculate_main_kpi(self, main_line):
        kpi_name = main_line[Const.KPI_NAME]
        target = main_line[Const.GROUP_TARGET]
        kpi_type = main_line[Const.SHEET]
        relevant_scif = self.scif
        scene_types = self.toolbox.does_exist(main_line, Const.SCENE_TYPE)
        if scene_types:
            relevant_scif = relevant_scif[relevant_scif['template_name'].isin(scene_types)]
        scene_groups = self.toolbox.does_exist(main_line, Const.SCENE_TYPE_GROUP)
        if scene_groups:
            relevant_scif = relevant_scif[relevant_scif['template_group'].isin(scene_groups)]
        relevant_template = self.templates[kpi_type]
        relevant_template = relevant_template[relevant_template[Const.KPI_NAME] == kpi_name]
        if target == Const.ALL:
            target = len(relevant_template)
        function = self.toolbox.get_kpi_function(kpi_type)
        for scene_fk in relevant_scif['scene_fk'].unique().tolist():
            passed_counter = 0
            for i, kpi_line in relevant_template.iterrows():
                answer = function(kpi_line, relevant_scif[relevant_scif['scene_fk'] == scene_fk])
                if answer:
                    passed_counter += 1
            self.write_to_scene_level(
                kpi_name=kpi_name, scene_fk=scene_fk, result=passed_counter >= target)
