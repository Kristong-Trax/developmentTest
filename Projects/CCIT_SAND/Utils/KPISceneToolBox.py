import os
import pandas as pd
import numpy as np
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log

# from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'nissand'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class CCITSceneToolBox:
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
        self.templates = self.data_provider[Data.TEMPLATES]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.occupancy_template = pd.read_excel(self.TEMPLATE_PATH, sheetname=self.OCCUPANCY_SHEET)

    def get_manufacturer_fk(self, manu):
        return self.all_products[self.all_products['manufacturer_fk'] == manu].drop_duplicates().values[0]

    def get_filtered_df(self, df, filters):
        for key, value in filters.items():
            try:
                df = df[df[key] == value]
            except KeyError:
                Log.warning('Field {} is not in the Data Frame'.format(key))
                continue
        return df

    def occupancy_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        numerator_filters = {
            'manufacturer_name': self.CCIT_MANU
        }
        filtered_products = self.get_filtered_df(self.products, numerator_filters)['product_fk']
        numerator_res = len(self.match_product_in_scene[self.match_product_in_scene['product_fk'].isin(filtered_products)])
        denominator_res = len(self.match_product_in_scene)
        result = np.divide(float(numerator_res), float(denominator_res))
        target = self.occupancy_template['target'].values[0]
        score = self.occupancy_template['points'] if result >= target else 0
        kpi_fk = self.common.get_kpi_fk_by_kpi_name('occupancy_score')
        manu_fk = self.get_manufacturer_fk(self.CCIT_MANU)
        template_fk = self.templates['template_fk'].drop_duplicates().values[0]
        identifier_parent = self.common.get_dictionary(kpi_fk=self.common.get_kpi_fk_by_kpi_name('store_score'))
        identifier_parent['session_fk'] = self.session_info['session_fk'].values[0]
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=manu_fk, numerator_result=numerator_res, result=result,
                                       denominator_id=template_fk, denominator_result=denominator_res, score=score,
                                       identifier_parent=identifier_parent)
        return
