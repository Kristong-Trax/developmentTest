from datetime import datetime
import os
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.GOOGLEKR_SAND.Utils.Const import Const
from KPIUtils.GlobalDataProvider.PsDataProvider import PsDataProvider

__author__ = 'Eli_Sam_Shivi'


class SceneGOOGLEToolBox:

    def __init__(self, data_provider, common_v2):
        self.common_v2 = common_v2
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.templates = self.data_provider.all_templates
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.planograms = self.data_provider[Data.PLANOGRAM_ITEM_FACTS]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        # self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        # self.all_products = self.ps_data_provider.get_sub_category(self.all_products)
        # self.store_assortment = self.ps_data_provider.get_store_assortment()
        # self.store_sos_policies = self.ps_data_provider.get_store_policies()
        # self.labels = self.ps_data_provider.get_labels()
        self.store_info = self.data_provider[Data.STORE_INFO]
        # self.store_info = self.ps_data_provider.get_ps_store_info(self.store_info)
        self.fixture_template = {}
        self.current_date = datetime.now()

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        score = 0
        return score

    def google_global_SOS(self):
        scif = self.filter_df(self.scif.copy(), Const.EXCLUDE_FILTERS, exclude=1)
        brands_scif = scif[~scif[Const.BRAND].isin(Const.NOTABRAND)]
        if brands_scif.empty:
            return

        brand_totals = brands_scif.set_index(['brand_fk', Const.BRAND])\
                                  .groupby([Const.BRAND, 'brand_fk'])[Const.FACINGS]\
                                  .sum()

        total_facings = scif[Const.FACINGS].sum()
        google_fk, google_facings = brand_totals.reset_index(drop=False, level=1).loc[Const.GOOGLE_BRAND]

        for (brand_name, brand_fk), brand_facings in brand_totals.iteritems():
            scene_ratio = self.division(brand_facings, total_facings)
            kpi_fk = self.common_v2.get_kpi_fk_by_kpi_name(Const.SOS_SCENE)
            self.common_v2.write_to_db_result(
                fk=kpi_fk, numerator_id=brand_fk, numerator_result=brand_facings, result=scene_ratio, by_scene=True,
                denominator_result=total_facings)

            google_ratio = self.division(google_facings, brand_facings)
            kpi_fk = self.common_v2.get_kpi_fk_by_kpi_name(Const.SOS_RELATIVE)
            self.common_v2.write_to_db_result(
                fk=kpi_fk, numerator_id=google_fk, numerator_result=google_facings, result=google_ratio, by_scene=True,
                denominator_result=brand_facings, denominator_id=brand_fk)

    def get_planogram_fixture_details(self):
        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_name(Const.FIXTURE_POG)
        denominator = self.planograms['facings'].sum()
        numerator = len(self.match_product_in_scene[self.match_product_in_scene['compliance_status_fk'] == 3])
        ratio = self.division(numerator, denominator)
        planogram_id = self.planograms['planogram_id'].iloc[0]
        self.common_v2.write_to_db_result(
            fk=kpi_fk, numerator_id=planogram_id, numerator_result=numerator, denominator_result=denominator,
            score=ratio, by_scene=True)

    def get_fixture_osa(self):
        if self.planograms.empty:
            return False
        assortment_products = set(self.planograms['item_id'])
        fixture_products = set(self.scif[self.scif['manufacturer_name'] == "Google"]['product_fk'])
        common_products = fixture_products & assortment_products
        osa_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_name(Const.FIXTURE_OSA)
        numerator_result = len(common_products)
        denominator_result = len(assortment_products)
        missings_products = assortment_products - fixture_products
        score = self.division(numerator_result, denominator_result)
        self.common_v2.write_to_db_result(
            fk=osa_kpi_fk, numerator_result=numerator_result, denominator_result=denominator_result, score=score,
            result=len(missings_products), by_scene=True)
        missings_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_name(Const.MISSING_DENOMINATIONS)
        for product_fk in missings_products:
            planogram_facings = self.planograms[self.planograms['item_id'] == product_fk]['facings'].iloc[0]
            self.common_v2.write_to_db_result(fk=missings_kpi_fk, numerator_id=product_fk, result=planogram_facings,
                                              by_scene=True)
        return True

    @staticmethod
    def filter_df(df, filters, exclude=0):
        for exclude_filter in filters:
            if exclude:
                df = df[~df['product_type'].isin(Const.EXCLUDE_FILTERS[exclude_filter])]
            else:
                df = df[df['product_type'].isin(Const.EXCLUDE_FILTERS[exclude_filter])]
        return df

    @staticmethod
    def division(num, den):
        if den:
            ratio = round(num * 100.0 / den, 2)
        else:
            ratio = 0
        return ratio
