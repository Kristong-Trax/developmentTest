import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCBOTTLERSUS_SAND.REDSCORE.Const import Const
from Projects.CCBOTTLERSUS_SAND.REDSCORE.FunctionsToolBox import FunctionsToolBox

__author__ = 'Elyashiv'


class CCBOTTLERSUS_SANDSceneRedToolBox:

    def __init__(self, data_provider, output, common):
        self.output = output
        self.data_provider = data_provider
        self.common = common
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
        self.scif = self.scif[self.scif['product_type'] != "Irrelevant"]
        self.store_attr = self.store_info['additional_attribute_15'].iloc[0]
        self.store_type = self.store_info['store_type'].iloc[0]
        if self.store_type in Const.STORE_TYPES:
            self.store_type = Const.STORE_TYPES[self.store_type]
        self.templates = {}
        for sheet in Const.SHEETS:
            self.templates[sheet] = pd.read_excel(Const.TEMPLATE_PATH, sheetname=sheet).fillna('')
        self.region = self.store_info['region_name'].iloc[0]
        self.toolbox = FunctionsToolBox(self.data_provider, self.output, self.templates, self.store_attr)
        main_template = self.templates[Const.KPIS]
        self.templates[Const.KPIS] = main_template[(main_template[Const.REGION] == self.region) &
                                                   (main_template[Const.STORE_TYPE] == self.store_type)]
        self.scenes_results = pd.DataFrame(columns=Const.COLUMNS_OF_RESULTS)

    def main_calculation(self):
        """
            This function makes the calculation for the scene's KPI and returns their answers to the session's calc
        """
        if self.scif[self.scif['United Deliver'] == 'Y'].empty:  # if it's not united scene we don't need to calculate
            return False
        main_template = self.templates[Const.KPIS]
        main_template = main_template[main_template[Const.SESSION_LEVEL] != Const.V]
        for i, main_line in main_template.iterrows():
            self.calculate_main_kpi(main_line)
        self.write_results_to_db()
        return True

    def write_results_to_db(self):
        """
        Now we are just writing all the scene results to db
        """
        for i, scene_result in self.scenes_results.iterrows():
            self.common.write_to_db_result(
                fk=self.common.get_kpi_fk_by_kpi_name(scene_result[Const.KPI_NAME] + Const.SCENE_SUFFIX),
                result=round(scene_result[Const.DB_RESULT], 2), by_scene=True,
                numerator_id=Const.MANUFACTURER_FK, denominator_id=self.store_id)

    def write_to_scene_level(self, kpi_name, result=False, parent=""):
        """
        Writes a result in the DF (and "tells" its parent if it passed)
        :param kpi_name: string
        :param result: boolean
        :param parent: if the kpi is a condition kpi and it passed, we want the parent to know that
                        because we want the kpi to choose the scene with the most passed children
        """
        if parent and result:
            self.scenes_results.loc[(self.scenes_results[Const.KPI_NAME] == parent) &
                                    (self.scenes_results[Const.DB_RESULT] > 0), Const.DB_RESULT] += 1
        result_dict = {Const.KPI_NAME: kpi_name, Const.DB_RESULT: result * 1}
        self.scenes_results = self.scenes_results.append(result_dict, ignore_index=True)

    def calculate_main_kpi(self, main_line):
        """
        This function gets a line from the main_sheet, transfers it to the match function, and checks all of the
        KPIs in the same name in the match sheet, scene after scene.
        :param main_line: series from the template of the main_sheet.
        """
        scene_types = self.toolbox.does_exist(main_line, Const.SCENE_TYPE)
        scene_groups = self.toolbox.does_exist(main_line, Const.SCENE_TYPE_GROUP)
        if (scene_types and self.scif['template_name'].iloc[0] not in scene_types) or \
                (scene_groups and self.scif['template_group'].iloc[0] not in scene_groups):
            return
        kpi_name = main_line[Const.KPI_NAME]
        parent = main_line[Const.CONDITION]
        result = self.toolbox.calculate_kpi_by_type(main_line, self.scif)
        if result is not None:
            self.write_to_scene_level(
                kpi_name=kpi_name, result=result, parent=parent)
