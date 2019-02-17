from Trax.Algo.Calculations.Core.DataProvider import Data
import pandas as pd
from KPIUtils_v2.DB.CommonV2 import Common
import os

__author__ = 'Eli_Sam_Shivi'


class SceneToolBox:

    def __init__(self, data_provider):
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = pd.merge(self.data_provider[Data.MATCHES], self.all_products, on="product_fk")
        self.planograms = self.data_provider[Data.PLANOGRAM_ITEM_FACTS]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.store_id = self.data_provider[Data.STORE_INFO]['store_fk'].iloc[0]
        self.common = Common(data_provider)

