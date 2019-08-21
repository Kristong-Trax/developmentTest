from Trax.Algo.Calculations.Core.Constants import Keys, Fields
from Projects.TNUVAILV2.DATA.LocalConsts import Consts
from KPIUtils_v2.Utils.Consts import DataProvider
import pandas as pd


class TnuvaMocks:
    def __init__(self):
        pass

    @staticmethod
    def lvl3_results():
        lvl3_assortment_results = pd.DataFrame([
            {Keys.PRODUCT_FK: 15915, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 1,
             DataProvider.ScifConsts.FACINGS: 1, Keys.CATEGORY_FK: 260},
            {Keys.PRODUCT_FK: 15628, 'kpi_fk_lvl2': 3002, Consts.IN_STORE: 1,
             DataProvider.ScifConsts.FACINGS: 1, Keys.CATEGORY_FK: 254},
            {Keys.PRODUCT_FK: 15884, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 1,
             DataProvider.ScifConsts.FACINGS: 1, Keys.CATEGORY_FK: 260},
            {Keys.PRODUCT_FK: 15854, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 1,
             DataProvider.ScifConsts.FACINGS: 1, Keys.CATEGORY_FK: 260},
            {Keys.PRODUCT_FK: 15881, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 1,
             DataProvider.ScifConsts.FACINGS: 1, Keys.CATEGORY_FK: 260},
            {Keys.PRODUCT_FK: 15603, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 1,
             DataProvider.ScifConsts.FACINGS: 1, Keys.CATEGORY_FK: 272},
            {Keys.PRODUCT_FK: 15613, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 1,
             DataProvider.ScifConsts.FACINGS: 1, Keys.CATEGORY_FK: 264},
            {Keys.PRODUCT_FK: 15829, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 1,
             DataProvider.ScifConsts.FACINGS: 1, Keys.CATEGORY_FK: 262},
            {Keys.PRODUCT_FK: 15639, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 1,
             DataProvider.ScifConsts.FACINGS: 1, Keys.CATEGORY_FK: 252},
            {Keys.PRODUCT_FK: 15631, 'kpi_fk_lvl2': 3002, Consts.IN_STORE: 1,
             DataProvider.ScifConsts.FACINGS: 1, Keys.CATEGORY_FK: 262},
            {Keys.PRODUCT_FK: 15874, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 1,
             DataProvider.ScifConsts.FACINGS: 1, Keys.CATEGORY_FK: 260},
            {Keys.PRODUCT_FK: 15852, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 0,
             DataProvider.ScifConsts.FACINGS: 0, Keys.CATEGORY_FK: 260},
            {Keys.PRODUCT_FK: 15879, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 0,
             DataProvider.ScifConsts.FACINGS: 0, Keys.CATEGORY_FK: 260},
            {Keys.PRODUCT_FK: 15567, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 0,
             DataProvider.ScifConsts.FACINGS: 0, Keys.CATEGORY_FK: 260},
            {Keys.PRODUCT_FK: 16080, 'kpi_fk_lvl2': 3002, Consts.IN_STORE: 0,
             DataProvider.ScifConsts.FACINGS: 0, Keys.CATEGORY_FK: 260},
            {Keys.PRODUCT_FK: 15602, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 0,
             DataProvider.ScifConsts.FACINGS: 0, Keys.CATEGORY_FK: 260},
            {Keys.PRODUCT_FK: 15588, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 0,
             DataProvider.ScifConsts.FACINGS: 0, Keys.CATEGORY_FK: 264},
        ])
        return lvl3_assortment_results

    @staticmethod
    def get_mock_scif_1(own_manu):
        """ Notice! We are not considering policies here!"""
        mock_scif = pd.DataFrame([
            {Keys.SCENE_FK: 1, Keys.PRODUCT_FK: 100, DataProvider.ScifConsts.CATEGORY_FK: 1, Fields.FACINGS: 3,
             Fields.FACINGS_IGN_STACK: 2, Fields.MANUFACTURER_FK: own_manu, Fields.RLV_SOS_SC: 1},
            {Keys.SCENE_FK: 1, Keys.PRODUCT_FK: 101, DataProvider.ScifConsts.CATEGORY_FK: 1, Fields.FACINGS: 4,
             Fields.FACINGS_IGN_STACK: 2, Fields.MANUFACTURER_FK: 999, Fields.RLV_SOS_SC: 1},
            {Keys.SCENE_FK: 1, Keys.PRODUCT_FK: 102, DataProvider.ScifConsts.CATEGORY_FK: 1, Fields.FACINGS: 5,
             Fields.FACINGS_IGN_STACK: 2, Fields.MANUFACTURER_FK: 999, Fields.RLV_SOS_SC: 1},
            {Keys.SCENE_FK: 1, Keys.PRODUCT_FK: 103, DataProvider.ScifConsts.CATEGORY_FK: 2, Fields.FACINGS: 4,
             Fields.FACINGS_IGN_STACK: 2, Fields.MANUFACTURER_FK: 999, Fields.RLV_SOS_SC: 1},
            {Keys.SCENE_FK: 2, Keys.PRODUCT_FK: 104, DataProvider.ScifConsts.CATEGORY_FK: 2, Fields.FACINGS: 2,
             Fields.FACINGS_IGN_STACK: 2, Fields.MANUFACTURER_FK: own_manu, Fields.RLV_SOS_SC: 1},
            {Keys.SCENE_FK: 2, Keys.PRODUCT_FK: 105, DataProvider.ScifConsts.CATEGORY_FK: 3, Fields.FACINGS: 5,
             Fields.FACINGS_IGN_STACK: 5, Fields.MANUFACTURER_FK: own_manu, Fields.RLV_SOS_SC: 1},
            {Keys.SCENE_FK: 2, Keys.PRODUCT_FK: 106, DataProvider.ScifConsts.CATEGORY_FK: 3, Fields.FACINGS: 4,
             Fields.FACINGS_IGN_STACK: 0, Fields.MANUFACTURER_FK: own_manu, Fields.RLV_SOS_SC: 1},
            {Keys.SCENE_FK: 2, Keys.PRODUCT_FK: 107, DataProvider.ScifConsts.CATEGORY_FK: 3, Fields.FACINGS: 3,
             Fields.FACINGS_IGN_STACK: 1, Fields.MANUFACTURER_FK: 121, Fields.RLV_SOS_SC: 1},
            {Keys.SCENE_FK: 2, Keys.PRODUCT_FK: 108, DataProvider.ScifConsts.CATEGORY_FK: 4, Fields.FACINGS: 2,
             Fields.FACINGS_IGN_STACK: 1, Fields.MANUFACTURER_FK: 919, Fields.RLV_SOS_SC: 1}
        ])
        return mock_scif

    @staticmethod
    def get_result_value():
        res_val = pd.DataFrame([{Fields.PK: 1, 'value': 'OOS'}, {Fields.PK: 2, 'value': 'DISTRIBUTED'}])
        return res_val
