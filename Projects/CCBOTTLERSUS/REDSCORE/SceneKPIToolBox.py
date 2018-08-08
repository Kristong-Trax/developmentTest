import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCBOTTLERSUS.REDSCORE.Const import CCBOTTLERSUSConst

__author__ = 'Elyashiv'


class CCBOTTLERSUSSceneRedToolBox:

    def __init__(self, data_provider, output, templates, toolbox):
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
        self.store_attr = toolbox.store_attr
        self.templates = templates
        self.scenes_results = pd.DataFrame(columns=CCBOTTLERSUSConst.COLUMNS_OF_SCENE)

    def main_calculation(self, *args, **kwarg):
        """
            This function makes the calculation for the scene's KPI and returns their answers to the session's calc
        """
        main_template = self.templates[CCBOTTLERSUSConst.KPIS]
        main_template = main_template[main_template[CCBOTTLERSUSConst.SESSION_LEVEL] != CCBOTTLERSUSConst.V]
        for i, main_line in main_template.iterrows():
            self.calculate_main_kpi(main_line)
        return self.scenes_results

    def write_to_scene_level(self, kpi_name, scene_fk, result=False, parent=""):
        """
        Writes a result in the DF (and "tells" its parent if it passed)
        :param kpi_name: string
        :param scene_fk: for the main script will know which scenes to choose
        :param result: boolean
        :param parent: if the kpi is a condition kpi and it passed, we want the parent to know that
                        because we want the kpi to choose the scene with the most passed children
        """
        if parent and result:
            self.scenes_results.loc[(self.scenes_results[CCBOTTLERSUSConst.KPI_NAME] == parent) &
                                    (self.scenes_results[CCBOTTLERSUSConst.RESULT] > 0) &
                                    (self.scenes_results[CCBOTTLERSUSConst.SCENE_FK] == scene_fk), CCBOTTLERSUSConst.RESULT] += 1
        result_dict = {CCBOTTLERSUSConst.KPI_NAME: kpi_name, CCBOTTLERSUSConst.SCENE_FK: scene_fk, CCBOTTLERSUSConst.RESULT: result * 1}
        self.scenes_results = self.scenes_results.append(result_dict, ignore_index=True)

    def calculate_main_kpi(self, main_line):
        """
        This function gets a line from the main_sheet, transfers it to the match function, and checks all of the
        KPIs in the same name in the match sheet, scene after scene.
        :param main_line: series from the template of the main_sheet.
        """
        kpi_name = main_line[CCBOTTLERSUSConst.KPI_NAME]
        kpi_type = main_line[CCBOTTLERSUSConst.SHEET]
        relevant_scif = self.scif[self.scif['scene_id'].isin(self.toolbox.united_scenes)]
        scene_types = self.toolbox.does_exist(main_line, CCBOTTLERSUSConst.SCENE_TYPE)
        if scene_types:
            relevant_scif = relevant_scif[relevant_scif['template_name'].isin(scene_types)]
        scene_groups = self.toolbox.does_exist(main_line, CCBOTTLERSUSConst.SCENE_TYPE_GROUP)
        if scene_groups:
            relevant_scif = relevant_scif[relevant_scif['template_group'].isin(scene_groups)]
        relevant_template = self.templates[kpi_type]
        relevant_template = relevant_template[relevant_template[CCBOTTLERSUSConst.KPI_NAME] == kpi_name]
        target = len(relevant_template) if main_line[CCBOTTLERSUSConst.GROUP_TARGET] == CCBOTTLERSUSConst.ALL else main_line[CCBOTTLERSUSConst.GROUP_TARGET]
        isnt_dp = True if self.store_attr != CCBOTTLERSUSConst.DP and main_line[CCBOTTLERSUSConst.STORE_ATTRIBUTE] == CCBOTTLERSUSConst.DP else False
        function = self.toolbox.get_kpi_function(kpi_type)
        parent = main_line[CCBOTTLERSUSConst.CONDITION]
        for scene_fk in relevant_scif['scene_fk'].unique().tolist():
            if main_line[CCBOTTLERSUSConst.SAME_PACK] == CCBOTTLERSUSConst.V:
                result = self.toolbox.calculate_availability_with_same_pack(relevant_template, relevant_scif, isnt_dp)
            else:
                passed_counter = 0
                for i, kpi_line in relevant_template.iterrows():
                    answer = function(kpi_line, relevant_scif[relevant_scif['scene_fk'] == scene_fk], isnt_dp)
                    if answer:
                        passed_counter += 1
                result = passed_counter >= target
            self.write_to_scene_level(
                kpi_name=kpi_name, scene_fk=scene_fk, result=result, parent=parent)
