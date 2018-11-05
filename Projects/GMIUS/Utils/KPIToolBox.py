from datetime import datetime
import os
import pandas as pd
from collections import defaultdict

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from Projects.GOOGLEKR.Utils.Const import Const
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

__author__ = 'Sam'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../Data', 'GMIUS Template v0.1.xlsx')


class ToolBox:

    def __init__(self, data_provider, output, common):
        self.common = common
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.templates = self.data_provider.all_templates
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.scenes = self.scene_info['scene_fk'].tolist()
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.all_products = self.ps_data_provider.get_sub_category(self.all_products)
        self.store_assortment = self.ps_data_provider.get_store_assortment()
        self.store_sos_policies = self.ps_data_provider.get_store_policies()
        self.labels = self.ps_data_provider.get_labels()
        self.scene_results = self.ps_data_provider.get_scene_results(self.scenes)
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_info = self.ps_data_provider.get_ps_store_info(self.store_info)
        self.template = {}
        for sheet in Const.SHEETS:
            self.template[sheet] = pd.read_excel(TEMPLATE_PATH, sheet)
        self.hierarchy = self.templates[Const.KPIS].set_index(Const.KPI_NAME)[Const.PARENT].to_dict()
        self.sub_scores = defaultdict(int)
        self.sub_totals = defaultdict(int)

    # main functions:
    def main_calculation(self, *args, **kwargs):
        """
            This function gets all the scene results from the SceneKPI, after that calculates every session's KPI,
            and in the end it calls "filter results" to choose every KPI and scene and write the results in DB.
        """
        main_template = self.templates[Const.KPIS]
        for i, main_line in main_template.iterrows():
            self.calculate_main_kpi(main_line)

    def calculate_main_kpi(self, main_line):
        kpi_name = main_line[Const.KPI_NAME]
        kpi_type = main_line[Const.TYPE]

        function = self.get_kpi_function(kpi_type)

        for i, kpi_line in self.template.iterrows():
            result, num, den, score, target = function(kpi_line)
            if (result is None and score is None and target is None) or not den:
                continue
            # self.update_parents(kpi_name, score)
            self.write_to_db(kpi_name, kpi_type, score, result=result, threshold=target, num=num, den=den)

    def write_to_db(self, kpi_name, kpi_type, score, result=None, threshold=None, num=None, den=None):
        """
        writes result in the DB
        :param kpi_name: str
        :param score: float
        :param display_text: str
        :param result: str
        :param threshold: int
        """
        kpi_fk = self.common_db2.get_kpi_fk_by_kpi_type(self.get_kpi_name(kpi_name, kpi_type))
        parent = self.get_parent(kpi_name)
        self.common_db2.write_to_db_result(fk=kpi_fk, score=score, result=result, should_enter=True, target=threshold,
                                           numerator_result=num, denominator_result=den, weight=delta,
                                           identifier_parent=self.common_db2.get_dictionary(parent_name=parent))

    def get_parent(self, kpi_name):
        try:
            parent = self.hierarchy[kpi_name]
        except Exception as e:
            parent = None
            Log.warning("Warning, Parent KPI not found in column '{}' on template page '{}'"
                        .format(Const.KPI_NAME, Const.KPIS))
        return parent

    def update_parents(self, kpi, score):
        parent = self.get_parent(kpi)
        while parent:
            self.update_sub_score(parent, score=score)
            parent = self.get_parent(parent)

    def update_sub_score(self, parent, score=0):
        self.sub_totals[parent] += 1
        self.sub_scores[parent] += score


    # def kpi_parent_result(self, parent, num, den):
    #     if parent in Const.PARENT_RATIO:
    #         if den:
    #             result = round((float(num) / den)*100, 2)
    #         else:
    #             result = 0
    #     else:
    #         result = num
    #     return result
    #
    # def write_family_tree(self):
    #     for sub_parent in self.sub_totals.keys():
    #         # for sub_parent in set(Const.KPI_FAMILY_KEY.values()):
    #         kpi_type = sub_parent
    #         if sub_parent != SUB_PROJECT:
    #             kpi_type = '{} {}'.format(SUB_PROJECT, sub_parent)
    #         kpi_fk = self.common_db2.get_kpi_fk_by_kpi_type(kpi_type)
    #         num = self.sub_scores[sub_parent]
    #         den = self.sub_totals[sub_parent]
    #         result, score = self.ratio_score(num, den, 1)
    #         self.common_db2.write_to_db_result(fk=kpi_fk, numerator_result=num, numerator_id=Const.MANUFACTURER_FK,
    #                                            denominator_id=self.store_id,
    #                                            denominator_result=den, result=result, score=num, target=den,
    #                                            identifier_result=self.common_db2.get_dictionary(
    #                                                parent_name=sub_parent),
    #                                            identifier_parent=self.common_db2.get_dictionary(
    #                                                parent_name=self.get_parent(sub_parent)),
    #                                            should_enter=True)