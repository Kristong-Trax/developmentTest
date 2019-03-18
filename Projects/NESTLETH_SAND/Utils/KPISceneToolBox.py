import pandas as pd
import os
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils.GlobalProjects.NESTLE.SceneKPIGenerator import SceneGenerator
from Trax.Utils.Logging.Logger import Log

__author__ = 'limorc'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class NESTLETHSceneToolBox:
    ERROR_LOC = -1
    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.templates = self.data_provider[Data.TEMPLATES]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_type = self.data_provider.store_type
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.nestle_generator = SceneGenerator(self.data_provider, self.output, self.common)

    def main_function(self):
        shelf_placement_template = pd.read_excel(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                                              'Placement.xlsx'), sheetname='Minimum Shelf',
                                                 keep_default_na=False)
        shelf_placement_dict = self.nestle_generator.nestle_global_shelf_placement_function(shelf_placement_template)
        if shelf_placement_dict == self.ERROR_LOC:
            Log.warning("Shelf placement failed for scene - {0} "
                        "Match product in scene doesnt contain Nestle product in layer 1 ".format(self.scene_info['scene_fk'].iloc[0]))
            return 0
        self.common.save_json_to_new_tables(shelf_placement_dict)
        return 0
