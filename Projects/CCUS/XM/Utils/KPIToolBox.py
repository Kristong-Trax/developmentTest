import os
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from Projects.CCUS.XM.Utils.Const import Const
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

# from Trax.Cloud.Services.Connector.Keys import DbUsers
# from Trax.Data.Projects.Connector import ProjectConnector
# from KPIUtils_v2.DIAGEOUSCalculations.AvailabilityDIAGEOUSCalculations import Availability
# from KPIUtils_v2.DIAGEOUSCalculations.NumberOfScenesDIAGEOUSCalculations import NumberOfScenes
# from KPIUtils_v2.DIAGEOUSCalculations.PositionGraphsDIAGEOUSCalculations import PositionGraphs
# from KPIUtils_v2.DIAGEOUSCalculations.SOSDIAGEOUSCalculations import SOS
# from KPIUtils_v2.DIAGEOUSCalculations.SequenceDIAGEOUSCalculations import Sequence
# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'Elyashiv'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')


class CCUSToolBox_XM:

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
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.ps_data = PsDataProvider(self.data_provider, self.output)
        self.store_area_df = self.ps_data.get_store_area_df()
        self.point_index = 1

    # main functions:

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        for scene_id in self.scif['scene_id'].unique().tolist():
            scene_type = self.scif[self.scif['scene_id'] == scene_id]['template_name'][0]
            if scene_type in Const.BAY_COUNTS:
                self.calculate_by_bays(scene_id)
            elif scene_type in Const.SCENE_TYPE_COUNTS:
                self.calculate_by_scene_types(scene_id)
            else:
                Log.warning("The scene_type {} has no definition".format(scene_id))

    def calculate_by_scene_types(self, scene_id):
        relevant_scif = self.scif[self.scif['scene_fk'] == scene_id]
        for i, product in relevant_scif.iterrows():
            result = product['facings']
            POC = self.point_index
        self.point_index += 1

    def calculate_by_bays(self, scene_id):
        relevant_match_product = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene_id]
        for bay_number in relevant_match_product['bay_number'].unique().tolist():
            bay_match_product = relevant_match_product[relevant_match_product['bay_number'] == bay_number]
            for product_fk in bay_match_product['product_fk'].unique().tolist():
                facings = len(bay_match_product[bay_match_product['product_fk'] == product_fk])
                POC = self.point_index
        self.point_index += 1
