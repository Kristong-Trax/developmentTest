from Trax.Algo.Calculations.Core.Constants import Keys, Fields, PlanogramTagCompliance
from Projects.TNUVAILV2.Utils.Consts import Consts
import pandas as pd


class TnuvaMocks:
    def __init__(self):
        pass

    @staticmethod
    def lvl3_results():
        lvl3_assortment_results = pd.DataFrame([
            {Keys.PRODUCT_FK: 15915, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 1, Consts.FACINGS: 1, Keys.CATEGORY_FK: 260},
            {Keys.PRODUCT_FK: 15628, 'kpi_fk_lvl2': 3002, Consts.IN_STORE: 1, Consts.FACINGS: 1, Keys.CATEGORY_FK: 254},
            {Keys.PRODUCT_FK: 15884, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 1, Consts.FACINGS: 1, Keys.CATEGORY_FK: 260},
            {Keys.PRODUCT_FK: 15854, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 1, Consts.FACINGS: 1, Keys.CATEGORY_FK: 260},
            {Keys.PRODUCT_FK: 15881, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 1, Consts.FACINGS: 1, Keys.CATEGORY_FK: 260},
            {Keys.PRODUCT_FK: 15603, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 1, Consts.FACINGS: 1, Keys.CATEGORY_FK: 272},
            {Keys.PRODUCT_FK: 15613, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 1, Consts.FACINGS: 1, Keys.CATEGORY_FK: 264},
            {Keys.PRODUCT_FK: 15829, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 1, Consts.FACINGS: 1, Keys.CATEGORY_FK: 262},
            {Keys.PRODUCT_FK: 15639, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 1, Consts.FACINGS: 1, Keys.CATEGORY_FK: 252},
            {Keys.PRODUCT_FK: 15631, 'kpi_fk_lvl2': 3002, Consts.IN_STORE: 1, Consts.FACINGS: 1, Keys.CATEGORY_FK: 262},
            {Keys.PRODUCT_FK: 15874, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 1, Consts.FACINGS: 1, Keys.CATEGORY_FK: 260},
            {Keys.PRODUCT_FK: 15852, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 0, Consts.FACINGS: 0, Keys.CATEGORY_FK: 260},
            {Keys.PRODUCT_FK: 15879, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 0, Consts.FACINGS: 0, Keys.CATEGORY_FK: 260},
            {Keys.PRODUCT_FK: 15567, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 0, Consts.FACINGS: 0, Keys.CATEGORY_FK: 260},
            {Keys.PRODUCT_FK: 16080, 'kpi_fk_lvl2': 3002, Consts.IN_STORE: 0, Consts.FACINGS: 0, Keys.CATEGORY_FK: 260},
            {Keys.PRODUCT_FK: 15602, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 0, Consts.FACINGS: 0, Keys.CATEGORY_FK: 260},
            {Keys.PRODUCT_FK: 15588, 'kpi_fk_lvl2': 3000, Consts.IN_STORE: 0, Consts.FACINGS: 0, Keys.CATEGORY_FK: 264},
        ])
        return lvl3_assortment_results

    def empty_scif(self):
        empty_scene_item_facts = pd.DataFrame(
            columns=[Fields.PRODUCT_FK, Fields.SCENE_FK, Fields.FACINGS, Fields.TAGGED, Fields.OOS_SC, Fields.DIST_SC,
                     Fields.FACINGS_IGN_STACK, Fields.RLV_OOS_SC, Fields.RLV_SOS_SC])
        return empty_scene_item_facts

    def scif_without_own_manufacturer(self):
        pass