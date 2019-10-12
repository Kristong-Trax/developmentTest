from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSceneToolBox
from KPIUtils_v2.Utils.Consts.GlobalConsts import ProductTypeConsts
from KPIUtils_v2.Utils.Consts.DataProvider import MatchesConsts, ProductsConsts, PogFactsConsts, ScifConsts
from Projects.PS3_SAND.Data.LocalConsts import Consts
# from Trax.Algo.Calculations.Core.DataProvider import Data
import pandas as pd
import os

__author__ = 'Eli_Sam_Shivi'

FIXTURE_POSITION_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'eye_level_algorithm.xlsx')


class SceneToolBox(GlobalSceneToolBox):

    def __init__(self, data_provider, common):
        GlobalSceneToolBox.__init__(self, data_provider, None, common)
        self.matches = pd.merge(self.matches, self.all_products, on=ProductsConsts.PRODUCT_FK)
        if self.planograms is None:
            self.planograms = pd.DataFrame(columns=[ProductsConsts.MANUFACTURER_NAME, PogFactsConsts.ITEM_ID])
            self.planogram_matches = pd.DataFrame(columns=[
                ProductsConsts.MANUFACTURER_NAME, ProductsConsts.PRODUCT_FK, MatchesConsts.SHELF_NUMBER_FROM_BOTTOM,
                MatchesConsts.SHELF_NUMBER])
            self.planogram_id = None
        else:
            self.planogram_id = self.planograms[PogFactsConsts.PLANOGRAM_ID].iloc[0]
        self.planogram_matches = pd.merge(self.planogram_matches, self.all_products, on=ProductsConsts.PRODUCT_FK)
        if not self.manufacturer_fk:
            self.manufacturer_fk = self.all_products[self.all_products[ProductsConsts.MANUFACTURER_NAME] ==
                                                     Consts.GOOGLE][ProductsConsts.MANUFACTURER_FK].iloc[0]
        self.manufacturer_fk = float(self.manufacturer_fk)
        self.fixture_position_template = pd.read_excel(FIXTURE_POSITION_PATH)
        self.missings = 0

# check if the scene passed the adding rule:

    def did_pass_rule(self):
        fk = self.get_kpi_fk_by_kpi_name(Consts.PASSED_RULE)
        if fk is None:
            return
        if self.planogram_matches.empty:
            score = 2
        elif self.matches.empty or self.manufacturer_fk not in \
                self.planogram_matches[ProductsConsts.MANUFACTURER_FK].tolist():
            return
        else:
            pog_bays, pog_shelves, pog_sequences = self.get_numbers_from_matches(self.planogram_matches.copy())
            rog_bays, rog_shelves, rog_sequences = self.get_numbers_from_matches(self.matches.copy())
            score = 1 if rog_sequences == pog_sequences and rog_shelves == pog_shelves else 0
        self.write_to_db(fk=fk, score=score)

    @staticmethod
    def get_numbers_from_matches(matches):
        matches = matches[~(matches[ProductsConsts.PRODUCT_TYPE].isin(
            [ProductTypeConsts.POS, ProductTypeConsts.IRRELEVANT]))]
        bays = len(matches[MatchesConsts.BAY_NUMBER].unique())
        shelves = len(matches[MatchesConsts.SHELF_NUMBER].unique())
        max_sequences = 0
        for shelf in matches[MatchesConsts.SHELF_NUMBER].unique().tolist():
            shelf_sequences = len(matches[matches[MatchesConsts.SHELF_NUMBER] == shelf])
            max_sequences = max(shelf_sequences, max_sequences)
        return bays, shelves, max_sequences

# SOS:

    def google_global_sos(self):
        """
        Calculates the SOS of google, and the relative SOS of google against any other brand.
        """
        total_kpi_fk = self.get_kpi_fk_by_kpi_name(Consts.SOS_SCENE)
        relative_kpi_fk = self.get_kpi_fk_by_kpi_name(
            Consts.KPIS_DICT[Consts.SHARE_OF_COMPETITOR][Consts.FIXTURE_LEVEL])
        scif = self.scif[self.scif[ProductsConsts.PRODUCT_TYPE] != ProductTypeConsts.IRRELEVANT]
        brands_scif = scif[~scif[ProductsConsts.BRAND_NAME].isin(Consts.NOT_BRAND)]
        if brands_scif.empty:
            return
        brand_totals = brands_scif.set_index([ProductsConsts.BRAND_NAME,
                                              ProductsConsts.BRAND_FK]).groupby(level=[0, 1])[ScifConsts.FACINGS].sum()
        total_facings = scif[ScifConsts.FACINGS].sum()
        if Consts.GOOGLE_BRAND in brands_scif[ProductsConsts.BRAND_NAME].tolist():
            google_fk, google_facings = brand_totals.reset_index(drop=False, level=1).loc[Consts.GOOGLE_BRAND]
        else:
            google_fk, google_facings = self.all_products[self.all_products[ProductsConsts.BRAND_NAME] ==
                                                          Consts.GOOGLE_BRAND][ProductsConsts.BRAND_FK].iloc[0], 0
        for (brand_name, brand_fk), brand_facings in brand_totals.iteritems():
            scene_ratio = self.get_percentage(brand_facings, total_facings, allow_0_in_den=True)
            self.write_to_db(
                fk=total_kpi_fk, numerator_id=brand_fk, numerator_result=brand_facings, result=scene_ratio,
                denominator_result=total_facings, score=scene_ratio)
            google_ratio = self.get_percentage(google_facings, brand_facings, allow_0_in_den=True)
            self.write_to_db(
                fk=relative_kpi_fk, numerator_id=google_fk, numerator_result=google_facings, result=google_ratio,
                denominator_result=brand_facings, denominator_id=brand_fk, score=google_ratio)

    def get_sos_target(self):
        """
        Comparing the proportion between the google products and all products in the POG
        to the proportion in the scene.
        """
        if self.planograms.empty:
            scene_proportion, pog_proportion = 0, 0
            scene_google_products, scene_products = 0, 0
            pog_google_products, pog_products = 0, 0
            score = 0
        else:
            pog_google_products = self.planograms[
                self.planograms[ProductsConsts.MANUFACTURER_FK] == self.manufacturer_fk][ScifConsts.FACINGS].sum()
            pog_products = self.planograms[ScifConsts.FACINGS].sum()
            pog_proportion = self.get_percentage(pog_google_products, pog_products, allow_0_in_den=True)
            scene_google_products = self.scif[self.scif[ProductsConsts.MANUFACTURER_FK] == self.manufacturer_fk][
                ScifConsts.FACINGS].sum()
            scene_products = self.scif[
                self.scif[ProductsConsts.PRODUCT_TYPE] != ProductTypeConsts.IRRELEVANT][ScifConsts.FACINGS].sum()
            scene_proportion = self.get_percentage(
                scene_google_products, scene_products, allow_0_in_den=True)
            score = self.get_percentage(scene_proportion, pog_proportion, allow_0_in_den=True)
        sos_rog_kpi_fk = self.get_kpi_fk_by_kpi_name(Consts.SOS_TARGET_ROG)
        sos_pog_kpi_fk = self.get_kpi_fk_by_kpi_name(Consts.SOS_TARGET_POG)
        self.write_to_db(fk=sos_pog_kpi_fk, numerator_result=pog_google_products, denominator_result=pog_products,
                         result=pog_proportion, score=pog_proportion)
        self.write_to_db(fk=sos_rog_kpi_fk, numerator_result=scene_google_products, denominator_result=scene_products,
                         target=pog_proportion, result=scene_proportion, score=score)

# POGs:

    def get_planogram_fixture_level(self):
        """
        Calculates the fixture level of POG (compliance_status "in place" out of all the other POG google products)
        and the status level (for all the 3 statuses, there are no "empty google")
        """
        fixture_kpi_fk = self.get_kpi_fk_by_kpi_name(
            Consts.KPIS_DICT[Consts.POG_COMPLIANCE][Consts.FIXTURE_LEVEL])
        all_facings, numerator_result, result = 0, 0, 0
        score = 0
        if not self.planograms.empty:
            result = 1
            numerator_result = self.get_compliance_status_level()
            all_facings = self.planograms[
                self.planograms[ProductsConsts.MANUFACTURER_FK] == self.manufacturer_fk][PogFactsConsts.FACINGS].sum()
            score = self.get_percentage(numerator_result, all_facings, allow_0_in_den=True)
        self.write_to_db(
            fk=fixture_kpi_fk, numerator_id=self.planogram_id, numerator_result=numerator_result,
            denominator_result=all_facings, result=result, should_enter=True,
            score=score, identifier_result=Consts.KPIS_DICT[Consts.POG_COMPLIANCE][Consts.FIXTURE_LEVEL])

    def get_compliance_status_level(self):
        """
        Calculates every status and writes it on db.
        :return: the amount of Correctly Positioned products.
        """
        numerator_result, score = 0, 0
        compliance_kpi_fk = self.get_kpi_fk_by_kpi_name(Consts.POG_STATUS)
        all_facings = self.matches[
            (self.matches[ProductsConsts.MANUFACTURER_FK] == self.manufacturer_fk) &
            (~self.matches[MatchesConsts.COMPLIANCE_STATUS_FK].isnull())][
            ProductsConsts.PRODUCT_FK].count()
        for compliance_status_fk in [3, 1, 2]:  # the order is important!!! 3 has to be before 1!
            identifier_result = self.get_dictionary(compliance_fk=compliance_status_fk)
            status_products = self.get_planogram_product_level(compliance_status_fk, identifier_result)
            ratio = self.get_percentage(status_products, all_facings, allow_0_in_den=True)
            self.write_to_db(
                fk=compliance_kpi_fk, numerator_id=compliance_status_fk, numerator_result=status_products,
                denominator_result=all_facings, should_enter=True, identifier_result=identifier_result, result=ratio,
                score=ratio, denominator_id=self.planogram_id,
                identifier_parent=Consts.KPIS_DICT[Consts.POG_COMPLIANCE][Consts.FIXTURE_LEVEL])
            if compliance_status_fk == 3:
                numerator_result = status_products
        return numerator_result

    def get_compliance_status_level2(self):
        """
        Calculates every status and writes it on db.
        :return: the amount of Correctly Positioned products.
        """
        numerator_result, score = 0, 0
        all_facings = self.matches[
            (self.matches[ProductsConsts.MANUFACTURER_FK] == self.manufacturer_fk) &
            (~self.matches[MatchesConsts.COMPLIANCE_STATUS_FK].isnull())][
            ProductsConsts.PRODUCT_FK].count()
        all_facings += self.missings
        for compliance_status_fk in [3, 1, 2, 4]:  # the order is important!!! 3 has to be before 1!
            status_kpi_fk = self.get_kpi_fk_by_kpi_name(Consts.MAPPING_STATUS_DICT[compliance_status_fk])
            identifier_result = self.get_dictionary(compliance_fk=compliance_status_fk)
            if compliance_status_fk == 4:
                status_products = self.missings
            else:
                status_products = self.get_planogram_product_level(compliance_status_fk, identifier_result)
            ratio = self.get_percentage(status_products, all_facings, allow_0_in_den=True)
            self.write_to_db(
                fk=status_kpi_fk, numerator_result=status_products,
                denominator_result=all_facings, should_enter=True, identifier_result=identifier_result, result=ratio,
                score=ratio, denominator_id=self.planogram_id,
                identifier_parent=Consts.KPIS_DICT[Consts.POG_COMPLIANCE][Consts.FIXTURE_LEVEL])
            if compliance_status_fk == 3:
                numerator_result = status_products
        return numerator_result

    def get_planogram_product_level(self, compliance_status_fk, identifier_parent):
        """
        Calculates for every compliance_status the amount of google products in this status,
        and writes every one of them in the product_level
        :param compliance_status_fk: 1, 2 or 3
        :param identifier_parent: for the hierarchy
        :return: the number of google products with this status
        """
        kpi_fk = self.get_kpi_fk_by_kpi_name(Consts.POG_PRODUCT)
        compliance_products = self.matches[
            (self.matches[MatchesConsts.COMPLIANCE_STATUS_FK] == compliance_status_fk) &
            (self.matches[ProductsConsts.MANUFACTURER_FK] == self.manufacturer_fk)]
        for product_fk in compliance_products[ProductsConsts.PRODUCT_FK].unique().tolist():
            planogram_facings = self.planograms[
                self.planograms[ProductsConsts.PRODUCT_FK] == product_fk][PogFactsConsts.FACINGS].sum()
            match_products = compliance_products[compliance_products[ProductsConsts.PRODUCT_FK] == product_fk]
            match_product_facings = len(match_products)
            if compliance_status_fk == 3:
                if match_product_facings > planogram_facings:
                    difference = match_product_facings - planogram_facings
                    remain_pks = match_products[-int(difference):][MatchesConsts.SCENE_MATCH_FK].tolist()
                    self.matches.loc[self.matches[MatchesConsts.SCENE_MATCH_FK].isin(remain_pks),
                                     MatchesConsts.COMPLIANCE_STATUS_FK] = 1
                    products_facings = self.matches[
                        (self.matches[MatchesConsts.COMPLIANCE_STATUS_FK] == compliance_status_fk) &
                        (self.matches[ProductsConsts.MANUFACTURER_FK] == self.manufacturer_fk) &
                        (self.matches[ProductsConsts.PRODUCT_FK] == product_fk)]
                    match_product_facings = len(products_facings)
            self.write_to_db(
                fk=kpi_fk, numerator_id=product_fk, result=match_product_facings, denominator_id=compliance_status_fk,
                identifier_parent=identifier_parent, should_enter=True, context_id=self.planogram_id,
                score=match_product_facings)
        compliance_products = self.matches[
            (self.matches[MatchesConsts.COMPLIANCE_STATUS_FK] == compliance_status_fk) &
            (self.matches[ProductsConsts.MANUFACTURER_FK] == self.manufacturer_fk)]
        return len(compliance_products)

# Denom Availability:

    def get_fixture_da(self):
        """
        Calculates the assortment based on the POG list
        """
        osa_kpi_fk = self.get_kpi_fk_by_kpi_name(
            Consts.KPIS_DICT[Consts.DENOM_AVAILABILITY][Consts.FIXTURE_LEVEL])
        numerator_result, denominator_result = 0, 0
        if self.planograms.empty:
            score, result = 0, None
        else:
            result = 0
            assortment_products = set(self.planograms[self.planograms[ProductsConsts.MANUFACTURER_FK] ==
                                                      self.manufacturer_fk][PogFactsConsts.ITEM_ID])
            for product_fk in assortment_products:
                result += self.write_missing_denominations(product_fk)
            denominator_result = len(assortment_products)
            numerator_result = denominator_result - result
            score = self.get_percentage(numerator_result, denominator_result, allow_0_in_den=True)
        self.write_to_db(
            fk=osa_kpi_fk, numerator_result=numerator_result, denominator_result=denominator_result, score=score,
            result=result)

    def write_missing_denominations(self, product_fk):
        """
        Writes the missing products
        :param product_fk:
        :return: 1 if missing otherwise 0
        """
        missings_kpi_fk = self.get_kpi_fk_by_kpi_name(Consts.MISSING_DENOMINATIONS)
        product_df = self.planograms[self.planograms[ProductsConsts.PRODUCT_FK] == product_fk].iloc[0]
        target = product_df[PogFactsConsts.FACINGS]
        actual = self.scif[self.scif[ProductsConsts.PRODUCT_FK] == product_fk][ScifConsts.FACINGS].sum()
        missings = target - actual
        if missings > 0:
            compliance = self.get_percentage(missings, target, allow_0_in_den=True)
            self.write_to_db(
                fk=missings_kpi_fk, numerator_id=product_fk, result=missings, target=target, score=compliance)
        result = 1 if actual == 0 else 0
        return result

    def write_missing_denominations2(self, product_fk):
        """
        Writes the missing products
        :param product_fk:
        :return: 1 if missing otherwise 0
        """
        identifier_parent = self.get_dictionary(compliance_fk=4)
        missings_kpi_fk = self.get_kpi_fk_by_kpi_name(Consts.MISSING_DENOMINATIONS)
        product_df = self.planograms[self.planograms[ProductsConsts.PRODUCT_FK] == product_fk].iloc[0]
        target = product_df[PogFactsConsts.FACINGS]
        actual = self.scif[self.scif[ProductsConsts.PRODUCT_FK] == product_fk][ScifConsts.FACINGS].sum()
        missings = target - actual
        if missings > 0:
            self.missings += missings
            compliance = self.get_percentage(missings, target, allow_0_in_den=True)
            self.write_to_db(
                fk=missings_kpi_fk, numerator_id=product_fk, result=missings, target=target,
                score=compliance, context_id=self.planogram_id, identifier_parent=identifier_parent)
        result = 1 if actual == 0 else 0
        return result

# Above the Fold:

    def get_above_the_fold(self):
        """
        Comparing the proportion between the google products and all products in the POG
        to the proportion in the scene.
        """
        self.calculate_above_the_fold_from_matches(
            self.matches[self.matches[ProductsConsts.PRODUCT_TYPE] !=
                         ProductTypeConsts.IRRELEVANT], Consts.FIXTURE_POSITION_ROG)
        if not self.planograms.empty:
            self.calculate_above_the_fold_from_matches(self.planogram_matches, Consts.FIXTURE_POSITION_POG)

    def calculate_above_the_fold_from_matches(self, matches, kpi_name):
        """
        Gets a matches DataFrame and calculates its shelves number, products amount, google products amount,
        position google products amount and the compliance of them
        :param matches: DataFrame (match_product_in_scene/planogram)
        :param kpi_name:
        """
        kpi_fk = self.get_kpi_fk_by_kpi_name(kpi_name)
        position_google_products, all_google_products, shelves_number, all_products = 0, 0, 0, 0
        if not matches.empty:
            shelves_number = max(matches[MatchesConsts.SHELF_NUMBER_FROM_BOTTOM].max(),
                                 matches[MatchesConsts.SHELF_NUMBER].max())
            ignored_shelves = self.fixture_position_template[
                (self.fixture_position_template[Consts.MIN_SHELVES] <= shelves_number) &
                (self.fixture_position_template[Consts.MAX_SHELVES] >= shelves_number)]
            if ignored_shelves.empty:
                eye_shelves = range(1, int(shelves_number + 1))
            else:
                minimum_shelf = int(ignored_shelves.iloc[0][Consts.IGNORE_FROM_BOTTOM])
                maximum_shelf = int(ignored_shelves.iloc[0][Consts.IGNORE_FROM_TOP])
                eye_shelves = range(minimum_shelf + 1, shelves_number + 1 - maximum_shelf)
            all_products = len(matches)
            all_google_products = matches[matches[ProductsConsts.MANUFACTURER_FK] == self.manufacturer_fk]
            position_google_products = len(
                all_google_products[all_google_products[MatchesConsts.SHELF_NUMBER_FROM_BOTTOM].isin(eye_shelves)])
            all_google_products = len(all_google_products)
        result = self.get_percentage(position_google_products, all_google_products, allow_0_in_den=True)
        self.write_to_db(fk=kpi_fk, numerator_result=position_google_products, denominator_result=all_google_products,
                         score=shelves_number, target=all_products, result=result)

# Facings Compliance:

    def get_facings_compliance(self):
        """
        Comparing the amount of google products in the scene and google products in the POG.
        """
        if self.planograms.empty:
            scene_google_products, pog_google_products, score = 0, 0, 0
        else:
            pog_google_products = self.planograms[
                self.planograms[ProductsConsts.MANUFACTURER_FK] == self.manufacturer_fk][PogFactsConsts.FACINGS].sum()
            scene_google_products = self.scif[
                (self.scif[ProductsConsts.MANUFACTURER_FK] == self.manufacturer_fk) &
                (self.scif[ProductsConsts.PRODUCT_TYPE] != ProductTypeConsts.IRRELEVANT)][ScifConsts.FACINGS].sum()
            score = self.get_percentage(scene_google_products, pog_google_products, allow_0_in_den=True)
        kpi_fk = self.get_kpi_fk_by_kpi_name(Consts.KPIS_DICT[Consts.FACINGS_COMPLIANCE][Consts.FIXTURE_LEVEL])
        self.write_to_db(fk=kpi_fk, numerator_result=scene_google_products, denominator_result=pog_google_products,
                         result=score, score=score)
