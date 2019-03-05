import os
import pandas as pd

from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector

__author__ = 'nidhin'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

KPI_SHEET_NAME = 'kpi_list'
PS_KPI_FAMILY = 19
KPI_FAMILY = 'kpi_family_fk'
TYPE = 'type'
KPI_TYPE = 'kpi_type'
KPI_NAME = 'kpi_name'


class JRIJPToolBox:
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
        self.templates_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        self.excel_file_path = os.path.join(self.templates_path, 'Template.xlsx')

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        self.calculate_facings_in_cell_per_product()
        self.common.commit_results_data()
        return

    def calculate_facings_in_cell_per_product(self):
        kpi_sheet = self.get_template_details(KPI_SHEET_NAME)
        for index, kpi_sheet_row in kpi_sheet.iterrows():
            kpi = self.kpi_static_data[(self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
                                       & (self.kpi_static_data[TYPE] == kpi_sheet_row[KPI_TYPE])
                                       & (self.kpi_static_data['delete_time'].isnull())]
            if kpi.empty:
                print("KPI Name:{} not found in DB".format(kpi_sheet_row[KPI_NAME]))
            else:
                print("KPI Name:{} found in DB".format(kpi_sheet_row[KPI_NAME]))
                kpi_fk = kpi.pk.values[0]
                grouped_data = self.match_product_in_scene.query(
                    'stacking_layer==1'
                ).groupby(
                    ['scene_fk', 'bay_number', 'shelf_number', 'product_fk']
                )
                for data_tup, scene_data_df in grouped_data:
                    scene_fk, bay_number, shelf_number, product_fk = data_tup
                    facings_count_in_cell = len(scene_data_df)
                    self.common.write_to_db_result(fk=kpi_fk,
                                                   numerator_id=product_fk,
                                                   denominator_id=self.store_id,
                                                   context_id=scene_fk,
                                                   numerator_result=bay_number,
                                                   denominator_result=shelf_number,
                                                   result=facings_count_in_cell,
                                                   score=scene_fk)

    def get_template_details(self, sheet_name):
        template = pd.read_excel(self.excel_file_path, sheetname=sheet_name)
        return template

