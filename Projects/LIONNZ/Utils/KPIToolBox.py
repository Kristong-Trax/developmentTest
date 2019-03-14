import os
import pandas as pd

from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector

__author__ = 'nidhin'

TEMPLATE_PARENT_FOLDER = 'Data'
TEMPLATE_NAME = 'Template.xlsx'
KPI_NAMES_SHEET = 'kpis'
KPI_DETAILS_SHEET = 'details'
KPI_INC_EXC_SHEET = 'include_exclude'


class LIONNZToolBox:
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
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.templates_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                           '..', TEMPLATE_PARENT_FOLDER,
                                           TEMPLATE_NAME)
        self.kpi_template = pd.ExcelFile(self.templates_path)

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        score = 0
        self.calculate_kpi()
        # self.common.commit_results_data()
        # self.common.write_to_db_result(fk=kpi_fk,
        #                                numerator_id=product_fk,
        #                                denominator_id=self.store_id,
        #                                context_id=scene_fk,
        #                                numerator_result=bay_number,
        #                                denominator_result=shelf_number,
        #                                result=facings_count_in_cell,
        #                                score=scene_fk)
        return score

    def calculate_kpi(self):
        kpi_sheet = self.kpi_template.parse(KPI_NAMES_SHEET)
        kpi_details = self.kpi_template.parse(KPI_DETAILS_SHEET)
        kpi_include_esclude = self.kpi_template.parse(KPI_INC_EXC_SHEET)

        for index, kpi_sheet_row in kpi_sheet.iterrows():
            kpi = self.kpi_static_data[(self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
                                       & (self.kpi_static_data[TYPE] == kpi_sheet_row[KPI_TYPE])
                                       & (self.kpi_static_data['delete_time'].isnull())]
            if kpi.empty:
                print("KPI Name:{} not found in DB".format(kpi_sheet_row[KPI_NAME]))
            else:
                print("KPI Name:{} found in DB".format(kpi_sheet_row[KPI_NAME]))

    def get_template(self):
        template = pd.ExcelFile(self.excel_file_path)
        return template
