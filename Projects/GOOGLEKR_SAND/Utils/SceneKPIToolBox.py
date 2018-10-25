from datetime import datetime
import os
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.GOOGLEKR_SAND.Utils.Const import Const
from KPIUtils.GlobalDataProvider.PsDataProvider import PsDataProvider

__author__ = 'Eli_Sam_Shivi'


class SceneGOOGLEToolBox:

    def __init__(self, data_provider, common):
        self.common = common
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.planograms = self.data_provider[Data.PLANOGRAM_ITEM_FACTS]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]

    def google_global_SOS(self):
        scif = self.filter_df(self.scif.copy(), Const.EXCLUDE_FILTERS, exclude=1)
        brands_scif = scif[~scif[Const.BRAND].isin(Const.NOTABRAND)]
        if brands_scif.empty:
            return
        denominators = {
            Const.SOS_OUT_OF_SCENE: scif[Const.FACINGS].sum(),
            Const.SOS_IN_SCENE: brands_scif[Const.FACINGS].sum()
        }
        brand_totals = brands_scif.set_index(['brand_fk', Const.BRAND])\
                                  .groupby([Const.BRAND, 'brand_fk'])[Const.FACINGS]\
                                  .sum()

        for (brand_name, brand_fk), numerator in brand_totals.iteritems():
            for kpi in Const.SOS_KPIs:
                ratio = self.division(numerator, denominators[kpi])
                kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi)
                self.common.write_to_db_result(
                    fk=kpi_fk, numerator_id=brand_fk, numerator_result=numerator, result=ratio, by_scene=True,
                    denominator_id=self.common.scene_id, denominator_result=denominators[kpi])

    def get_planogram_fixture_details(self):
        fixture_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.FIXTURE_POG)
        compliance_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.FIXTURE_COMPLIANCE)
        all_facings = self.planograms['facings'].sum()
        planogram_id = self.planograms['planogram_id'].iloc[0]
        for i in range(1, 5):
            identifier_result = self.common.get_dictionary(compliance_fk=i)
            status_products = self.get_compliance_products(i, identifier_result)
            ratio = self.division(status_products, all_facings)
            self.common.write_to_db_result(
                fk=compliance_kpi_fk, numerator_id=i, numerator_result=status_products, denominator_id=planogram_id,
                denominator_result=all_facings, should_enter=True, identifier_result=identifier_result,
                score=ratio, by_scene=True, identifier_parent=Const.FIXTURE_POG)
            if i == 3:
                self.common.write_to_db_result(
                    fk=fixture_kpi_fk, numerator_id=planogram_id, numerator_result=status_products,
                    denominator_result=all_facings,
                    score=ratio, by_scene=True, identifier_result=Const.FIXTURE_POG)

    def get_compliance_products(self, compliance_status_fk, identifier_parent):
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.POG_PRODUCT)
        compliance_products = self.match_product_in_scene[
            self.match_product_in_scene['compliance_status_fk'] == compliance_status_fk]
        for product_fk in compliance_products['product_fk'].unique().tolist():
            product_facings = len(compliance_products[compliance_products['product_fk'] == product_fk])
            self.common.write_to_db_result(
                fk=kpi_fk, numerator_id=product_fk, result=product_facings, denominator_id=compliance_status_fk,
                by_scene=True, identifier_parent=identifier_parent, should_enter=True)
        return len(compliance_products)

    def get_fixture_osa(self):
        if self.planograms is None or self.planograms.empty:
            return False
        assortment_products = set(self.planograms['item_id'])
        fixture_products = set(self.scif[self.scif['manufacturer_name'] == "Google"]['product_fk'])
        common_products = fixture_products & assortment_products
        osa_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.FIXTURE_OSA)
        numerator_result = len(common_products)
        denominator_result = len(assortment_products)
        missings_products = assortment_products - fixture_products
        score = self.division(numerator_result, denominator_result)
        self.common.write_to_db_result(
            fk=osa_kpi_fk, numerator_result=numerator_result, denominator_result=denominator_result, score=score,
            result=len(missings_products), by_scene=True)
        missings_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.MISSING_DENOMINATIONS)
        for product_fk in missings_products:
            planogram_facings = self.planograms[self.planograms['item_id'] == product_fk]['facings'].iloc[0]
            self.common.write_to_db_result(fk=missings_kpi_fk, numerator_id=product_fk, result=planogram_facings,
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
