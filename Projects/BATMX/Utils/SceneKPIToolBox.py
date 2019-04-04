from Trax.Algo.Calculations.Core.DataProvider import Data
import pandas as pd
from Projects.BATMX.Common import Common
from Projects.BATMX.Utils.Const import Const
from Trax.Utils.Logging.Logger import Log
from Projects.BATMX.Utils.PlanogramCompliance import PlanogramCompliance

__author__ = 'Eli_Sam_Shivi'


class SceneToolBox:

    def __init__(self, data_provider):
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.template_fk = self.data_provider[Data.TEMPLATES].iloc[0]['template_fk']
        self.scene_matches = pd.merge(self.data_provider[Data.MATCHES], self.all_products, on="product_fk")
        self.planogram_matches = self.data_provider._planogram_matches
        if self.planogram_matches is None:
            self.planogram_matches = pd.DataFrame(columns=["product_fk", "shelf_number", "manufacturer_fk"])
        else:
            self.scene_matches['match_fk'] = self.scene_matches['scene_match_fk']
            pog = PlanogramCompliance(data_provider=None)
            compliances = pog.get_compliance(manual_planogram_data=self.planogram_matches.copy(),
                                             manual_scene_data=self.scene_matches.copy())
            self.scene_matches.drop("compliance_status_fk", axis=1, inplace=True)
            self.scene_matches = self.scene_matches.merge(compliances, how="left", on="match_fk")
        self.planogram_matches = pd.merge(self.planogram_matches, self.all_products, on="product_fk")
        self.pog_matches, self.rog_matches = {}, {}
        self.pog_matches[Const.TOBACCO_CENTER] = self.planogram_matches[self.planogram_matches['shelf_number'] > 2]
        self.pog_matches[Const.PROMOTIONAL_TRAY] = self.planogram_matches[self.planogram_matches['shelf_number'] == 2]
        self.rog_matches[Const.TOBACCO_CENTER] = self.scene_matches[self.scene_matches['shelf_number'] > 2]
        self.rog_matches[Const.PROMOTIONAL_TRAY] = self.scene_matches[self.scene_matches['shelf_number'] == 2]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.store_id = self.data_provider[Data.STORE_INFO]['store_fk'].iloc[0]
        self.manufacturer_fk = int(self.data_provider[Data.OWN_MANUFACTURER]['param_value'].iloc[0])
        self.planogram_id = None if self.planogram_matches.empty else self.planogram_matches['planogram_fk'].iloc[0]
        self.common = Common(data_provider)

    def main_calculation(self):
        try:
            self.calculate_planogram_compliance(Const.PROMOTIONAL_TRAY)
        except Exception as e:
            Log.error('{}'.format(e))
        try:
            self.calculate_planogram_compliance(Const.TOBACCO_CENTER)
        except Exception as e:
            Log.error('{}'.format(e))
        try:
            self.calculate_oos()
        except Exception as e:
            Log.error('{}'.format(e))
        try:
            self.calculate_sos()
        except Exception as e:
            Log.error('{}'.format(e))

    def calculate_planogram_compliance(self, area):
        kpi_names = Const.POG_KPI_NAMES[area]
        pog_matches, rog_matches = self.pog_matches[area], self.rog_matches[area]
        pog_matches = pog_matches[pog_matches['manufacturer_fk'] == self.manufacturer_fk]
        rog_matches = rog_matches[rog_matches['manufacturer_fk'] == self.manufacturer_fk]
        fixture_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_names[Const.FIXTURE_LEVEL])
        all_facings, numerator_result, result = 0, 0, 0
        score = 0
        if not pog_matches.empty:
            result = 1
            numerator_result = self.get_compliance_per_status(pog_matches, rog_matches, kpi_names)
            all_facings = len(pog_matches)
            score = self.get_percentage_score(numerator_result, all_facings)
        self.common.write_to_db_result(
            fk=fixture_kpi_fk, numerator_id=self.planogram_id, numerator_result=numerator_result,
            denominator_result=all_facings, result=result, should_enter=True, denominator_id=self.template_fk,
            score=score, by_scene=True, identifier_result=self.common.get_dictionary(kpi_fk=fixture_kpi_fk))

    def get_compliance_per_status(self, pog_matches, rog_matches, kpi_names):
        """
        Calculates every status and writes it on db.
        :return: the Correctly Positioned products.
        """
        numerator_result, score = 0, 0
        status_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_names[Const.STATUS_LEVEL])
        all_facings = len(pog_matches)
        fixture_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_names[Const.FIXTURE_LEVEL])
        for compliance_status_fk in [3, 1, 2, 4]:  # the order is important!!! 3 has to be before 1!
            identifier_result = self.common.get_dictionary(compliance_fk=compliance_status_fk, kpi_fk=status_kpi_fk)
            status_products, rog_matches = self.get_status_products(
                compliance_status_fk, rog_matches, pog_matches, kpi_names)
            ratio = self.get_percentage_score(status_products, all_facings)
            self.common.write_to_db_result(
                fk=status_kpi_fk, numerator_id=compliance_status_fk, numerator_result=status_products,
                denominator_result=all_facings, should_enter=True, identifier_result=identifier_result,
                score=ratio, by_scene=True, identifier_parent=self.common.get_dictionary(kpi_fk=fixture_kpi_fk),
                denominator_id=self.planogram_id)
            if compliance_status_fk == 3:
                numerator_result = status_products
        return numerator_result

    def get_status_products(self, compliance_status_fk, rog_matches, pog_matches, kpi_names):
        """
        Calculates for every compliance_status the amount of google products in this status,
        and writes every one of them in the product_level
        :return: the number of google products with this status
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_names[Const.SKU_LEVEL])
        status_products = rog_matches[rog_matches['compliance_status_fk'] == compliance_status_fk]
        status_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_names[Const.STATUS_LEVEL])
        identifier_parent = self.common.get_dictionary(compliance_fk=compliance_status_fk, kpi_fk=status_kpi_fk)
        for product_fk in status_products['product_fk'].unique().tolist():
            planogram_facings = len(pog_matches[pog_matches['product_fk'] == product_fk])
            match_products = status_products[status_products['product_fk'] == product_fk]
            match_product_facings = len(match_products)
            if compliance_status_fk == 3:
                if match_product_facings > planogram_facings:
                    difference = match_product_facings - planogram_facings
                    remain_pks = match_products[-int(difference):]['scene_match_fk'].tolist()
                    rog_matches.loc[rog_matches['scene_match_fk'].isin(remain_pks), 'compliance_status_fk'] = 1
                    status_products = rog_matches[rog_matches['compliance_status_fk'] == compliance_status_fk]
                    match_product_facings = len(rog_matches[
                                                    (rog_matches['compliance_status_fk'] == compliance_status_fk) &
                                                    (rog_matches['product_fk'] == product_fk)])
            self.common.write_to_db_result(
                fk=kpi_fk, numerator_id=product_fk, result=match_product_facings, denominator_id=compliance_status_fk,
                by_scene=True, identifier_parent=identifier_parent, should_enter=True, context_id=self.planogram_id)
        return len(status_products), rog_matches

    def calculate_sos(self):
        fixture_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.SOS_LEVELS[Const.FIXTURE_LEVEL])
        matches = self.rog_matches[Const.TOBACCO_CENTER]
        matches = matches[(matches["product_type"].isin(["SKU", "Other", "Empty"])) &
                          (matches["category"] == "Cigarettes")]
        bat_matches = len(matches[matches["manufacturer_fk"] == self.manufacturer_fk])
        all_matches = len(matches)
        score = self.get_percentage_score(bat_matches, all_matches)
        self.common.write_to_db_result(
            fk=fixture_kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=bat_matches,
            denominator_result=all_matches, denominator_id=self.store_id, score=score, by_scene=True)

    def calculate_oos(self):
        fixture_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.OOS_FIXTURE)
        sku_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.OOS_SKU)
        identifier = self.common.get_dictionary(kpi_fk=fixture_kpi_fk)
        matches = self.pog_matches[Const.TOBACCO_CENTER]
        assortment_list = set(matches[matches["manufacturer_fk"] == self.manufacturer_fk]['product_fk'])
        scif_products = set(self.scif[self.scif['manufacturer_fk'] == self.manufacturer_fk]['product_fk'])
        oos_list = assortment_list - scif_products
        for product_fk in assortment_list:
            result = 0 if product_fk in oos_list else 1
            self.common.write_to_db_result(
                fk=sku_kpi_fk, numerator_id=product_fk, denominator_id=self.store_id, result=result, by_scene=True,
                identifier_parent=identifier, should_enter=True)
        oos_amount, all_amount = len(oos_list), len(assortment_list)
        score = self.get_percentage_score(oos_amount, all_amount)
        self.common.write_to_db_result(
            fk=fixture_kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=oos_amount,
            denominator_result=all_amount, denominator_id=self.store_id, score=score, by_scene=True,
            identifier_result=identifier)

    @staticmethod
    def get_percentage_score(num, den):
        ratio = num * 100.0 / den if den else 0
        return round(ratio, 2)
