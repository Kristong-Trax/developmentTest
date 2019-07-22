
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import numpy as np

import os

# from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common
# from Projects.MARSUAE_SAND.Utils.Fetcher import MARSUAE_SAND_Queries
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey
# from KPIUtils_v2.Calculations.AdjacencyCalculations import Adjancency
# from KPIUtils_v2.Calculations.AdjacencyCalculations import Block

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations
# from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox

__author__ = 'natalyak'


class MARSUAE_SANDSceneToolBox:

    PRICE = 'Price'
    
    def __init__(self, data_provider, output, common=None):
        self.output = output
        self.data_provider = data_provider
        # self.common = common
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
        self.store_id = self.data_provider[Data.STORE_FK] if self.data_provider[Data.STORE_FK] is not None \
                                                            else self.session_info['store_fk'].values[0]
        self.all_templates = self.data_provider[Data.ALL_TEMPLATES]
        self.store_type = self.data_provider.store_type
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.kpi_results = pd.DataFrame(columns=['kpi_fk', 'numerator', 'denominator', 'result', 'score'])

    def main_function(self):
        self.calculate_price()

    def calculate_price(self):
        prices_df = self.match_product_in_scene[~(self.match_product_in_scene['price'].isnull())]
        promo_price_df = self.get_promo_price_df()
        if not prices_df.empty:
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.PRICE)
            prices_df = prices_df.groupby(['product_fk'], as_index=False).agg({'price': np.max})
            if not promo_price_df.empty:
                prices_df = prices_df.merge(promo_price_df, on='product_fk', how='outer')
                prices_df['price'] = prices_df.apply(self.get_max_price, axis=1)
            for i, row in prices_df:
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=row['product_fk'], denominator_id=self.store_id,
                                               result=row['price'])
                self.add_kpi_result_to_kpi_results_df([kpi_fk, row['product_fk'], self.store_id, row['price'], None])

    def add_kpi_result_to_kpi_results_df(self, result_list):
        self.kpi_results.loc[len(self.kpi_results)] = result_list

    @staticmethod
    def get_max_price(row):
        max_value = max(row['price'], row['promotion_price'])
        return max_value

    def get_promo_price_df(self):
        promo_price_df = pd.DataFrame()
        if 'promotion_price' in self.match_product_in_scene.columns.values.tolist():
            promo_price_df = self.match_product_in_scene[~self.match_product_in_scene['promotion_price'].isnull()]
            if not promo_price_df.empty:
                promo_price_df = promo_price_df.groupby(['product_fk'], as_index=False).agg({'promotion_price': np.max})
        return promo_price_df
