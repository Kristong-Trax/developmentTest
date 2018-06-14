import os
import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'nissand'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class CCITToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'Data',
                                 'KPI_Templates.xlsx')
    CCIT_MANU = 'HBC Italia'
    OCCUPANCY_SHEET = 'occupancy_target'


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
        self.occupancy_template = pd.read_excel(self.TEMPLATE_PATH, sheetname=self.OCCUPANCY_SHEET)

    def occupancy_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        numerator_filters = {
            'manufacturer_name': self.CCIT_MANU
        }
        result = SOS(self.data_provider,self.output).calculate_share_of_shelf(sos_filters=numerator_filters,
                                                                              include_empty=True)
        target = self.occupancy_template['target']
        return result
