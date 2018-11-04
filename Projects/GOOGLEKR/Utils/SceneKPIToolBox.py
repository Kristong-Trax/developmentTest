from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.GOOGLEKR.Utils.Const import Const
import pandas as pd

__author__ = 'Eli_Sam_Shivi'


class SceneToolBox:

    def __init__(self, data_provider, common):
        self.common = common
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = pd.merge(self.data_provider[Data.MATCHES], self.all_products, on="product_fk")
        self.planograms = self.data_provider[Data.PLANOGRAM_ITEM_FACTS]
        if self.planograms is None:
            self.planograms = pd.DataFrame(columns=['item_id'])  # for the self.planograms will be an empty DF
        else:
            self.planograms = self.planograms[self.planograms['manufacturer_name'] == Const.GOOGLE]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]

    def google_global_SOS(self):
        scif = self.filter_df(self.scif.copy(), Const.EXCLUDE_FILTERS, exclude=1)
        brands_scif = scif[~scif[Const.BRAND].isin(Const.NOTABRAND)]
        if brands_scif.empty:
            return

        brand_totals = brands_scif.set_index([Const.BRAND, 'brand_fk']) \
                                  .groupby(level=[0, 1])[Const.FACINGS].sum()

        total_facings = scif[Const.FACINGS].sum()
        google_fk, google_facings = brand_totals.reset_index(drop=False, level=1).loc[Const.GOOGLE_BRAND]

        for (brand_name, brand_fk), brand_facings in brand_totals.iteritems():
            scene_ratio = self.division(brand_facings, total_facings)
            kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.SOS_SCENE)
            self.common.write_to_db_result(
                fk=kpi_fk, numerator_id=brand_fk, numerator_result=brand_facings, result=scene_ratio, by_scene=True,
                denominator_result=total_facings)

            google_ratio = self.division(google_facings, brand_facings)
            kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.SOS_RELATIVE)
            self.common.write_to_db_result(
                fk=kpi_fk, numerator_id=google_fk, numerator_result=google_facings, result=google_ratio, by_scene=True,
                denominator_result=brand_facings, denominator_id=brand_fk)

    def get_planogram_fixture_details(self):
        """
        Calculates the fixture level of POG (compliance_status "in place" out of all the other google products)
        and the status level (for all the 4 statuses)
        """
        fixture_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.FIXTURE_POG)
        all_facings, numerator_result, result = 0, 0, None
        score, planogram_id = 0, None
        if not self.planograms.empty:
            result = 1
            compliance_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.POG_STATUS)
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
                    numerator_result = status_products
                    score = ratio
        self.common.write_to_db_result(
            fk=fixture_kpi_fk, numerator_id=planogram_id, numerator_result=numerator_result,
            denominator_result=all_facings, result=result,
            score=score, by_scene=True, identifier_result=Const.FIXTURE_POG)

    def get_compliance_products(self, compliance_status_fk, identifier_parent):
        """
        Calculates for every compliance_status the amount of google products in this status,
        and writes every one of them in the product_level
        :param compliance_status_fk: 1, 2, 3 or 4
        :param identifier_parent: for the hierarchy
        :return: the number of google products with this status
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.POG_PRODUCT)
        compliance_products = self.match_product_in_scene[
                              (self.match_product_in_scene['compliance_status_fk'] == compliance_status_fk) &
                              (self.match_product_in_scene['manufacturer_name'] == Const.GOOGLE)]
        for product_fk in compliance_products['product_fk'].unique().tolist():
            product_facings = len(compliance_products[compliance_products['product_fk'] == product_fk])
            self.common.write_to_db_result(
                fk=kpi_fk, numerator_id=product_fk, result=product_facings, denominator_id=compliance_status_fk,
                by_scene=True, identifier_parent=identifier_parent, should_enter=True)
        return len(compliance_products)

    def get_fixture_osa(self):
        """
        Calculates the assortment based on the POG list, and the level of missing products
        """
        if self.planograms.empty:
            numerator_result, denominator_result = 0, 0
            score, result = 0, None
        else:
            assortment_products = set(self.planograms['item_id'])
            fixture_products = set(self.scif[self.scif['manufacturer_name'] == Const.GOOGLE]['product_fk'])
            common_products = fixture_products & assortment_products
            numerator_result = len(common_products)
            denominator_result = len(assortment_products)
            missings_products = assortment_products - fixture_products
            score = self.division(numerator_result, denominator_result)
            missings_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.MISSING_DENOMINATIONS)
            result = len(missings_products)
            for product_fk in missings_products:
                planogram_facings = self.planograms[self.planograms['item_id'] == product_fk]['facings'].iloc[0]
                self.common.write_to_db_result(fk=missings_kpi_fk, numerator_id=product_fk, result=planogram_facings,
                                               by_scene=True)
        osa_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.FIXTURE_OSA)
        self.common.write_to_db_result(
            fk=osa_kpi_fk, numerator_result=numerator_result, denominator_result=denominator_result, score=score,
            result=result, by_scene=True)

    @staticmethod
    def filter_df(df, filters, exclude=0):
        for key, val in filters.items():
            if not isinstance(val, list):
                val = [val]
            if exclude:
                df = df[~df[key].isin(val)]
            else:
                df = df[df[key].isin(val)]
        return df

    @staticmethod
    def division(num, den):
        if den:
            ratio = round(num * 100.0 / den, 2)
        else:
            ratio = 0
        return ratio
