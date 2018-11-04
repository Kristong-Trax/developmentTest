from datetime import datetime
import os
import pandas as pd
import numpy as np
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.GOOGLEKR.Utils.Const import Const
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

__author__ = 'Eli_Sam_Shivi'

FIXTURE_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../Data',
                                     'KR - Google Fixture Targets.xlsx')


class ToolBox:

    def __init__(self, data_provider, output, common):
        self.common = common
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.templates = self.data_provider.all_templates
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.manufacturer_fk = self.all_products[self.all_products["manufacturer_name"] == Const.GOOGLE][
            "manufacturer_fk"].iloc[0]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.scenes = self.scene_info['scene_fk'].tolist()
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.all_products = self.ps_data_provider.get_sub_category(self.all_products)
        self.store_assortment = self.ps_data_provider.get_store_assortment()
        self.store_sos_policies = self.ps_data_provider.get_store_policies()
        self.labels = self.ps_data_provider.get_labels()
        self.scene_results = self.ps_data_provider.get_scene_results(self.scenes)
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_info = self.ps_data_provider.get_ps_store_info(self.store_info)
        self.fixture_template = {}
        for sheet in Const.SHEETS:
            self.fixture_template[sheet] = pd.read_excel(FIXTURE_TEMPLATE_PATH, sheet)
        self.current_date = datetime.now()
        self.required_fixtures = []
        self.get_required_fixtures()

    def get_required_fixtures(self):
        """
        This function creates a list (self.required_fixtures) the fixture_fk in the template with the required amount
        of it, the names of its Entry and Exit template_name, and the match scenes of this session
        """
        store_num = self.store_info['store_number_1'][0]
        fixture_template = self.fixture_template[Const.FIXTURE_TARGETS]
        fixture_template = fixture_template.join(self.fixture_template[Const.PK]
                                                 .set_index('New Task Name (unique)')[Const.PK],
                                                 on='New Task Name (unique)', how='left')
        relevant_fixtures = fixture_template[fixture_template['Store Number'] == store_num]
        relevant_fixtures = relevant_fixtures.set_index(Const.PK).groupby(Const.PK) \
            ['Number of Fixtures(Task)'].sum()
        entry_exit = self.fixture_template[Const.ENTRY_EXIT]
        for fixture_pk, denominator in relevant_fixtures.iteritems():
            fixture_entry_exit = entry_exit[entry_exit[Const.EXIT_PK] == fixture_pk]
            if fixture_entry_exit.empty:
                fixture_entry_exit = entry_exit[entry_exit[Const.ENTRY_PK] == fixture_pk]
            entry_name, exit_name = None, None
            if not fixture_entry_exit.empty:
                fixture_entry_exit = fixture_entry_exit.iloc[0]
                entry_name, exit_name = fixture_entry_exit[Const.ENTRY_NAME], fixture_entry_exit[Const.EXIT_NAME]
            self.required_fixtures.append({Const.FIXTURE_FK: fixture_pk,
                                           Const.REQUIRED_AMOUNT: denominator,
                                           Const.ENTRY_NAME: entry_name,
                                           Const.EXIT_NAME: exit_name})
        for fixture_type in self.required_fixtures:
            fixture_type[Const.ENTRY_SCENES] = self.scif[
                self.scif['template_name'] == fixture_type[Const.ENTRY_NAME]]['scene_fk'].unique().tolist()
            fixture_type[Const.EXIT_SCENES] = self.scif[
                self.scif['template_name'] == fixture_type[Const.EXIT_NAME]]['scene_fk'].unique().tolist()

    def google_global_fixture_compliance(self):
        """
        Calculates for every fixture (from the self.required_fixtures) the amount of exist scenes out of the required
        """
        num_of_fixtures, all_scores = 0.0, 0.0
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.FIXTURE_COMPLIANCE)
        for fixture in self.required_fixtures:
            fixture_pk = fixture[Const.FIXTURE_FK]
            denominator = fixture[Const.REQUIRED_AMOUNT]
            numerator = self.scene_info[self.scene_info['template_fk'] == fixture_pk].shape[0]
            ratio = self.division(numerator, denominator)
            score = 0
            if ratio >= 100:
                ratio = 100
                score = 1
            num_of_fixtures += 1
            all_scores += ratio
            fixture[Const.ACTUAL_AMOUNT] = numerator
            self.common.write_to_db_result(
                fk=kpi_fk, numerator_id=fixture_pk, numerator_result=numerator, denominator_id=fixture_pk,
                denominator_result=denominator, score=score, result=ratio,
                identifier_parent=Const.FIXTURE_HIGH_LEVEL, should_enter=True)
        set_average = all_scores / num_of_fixtures if num_of_fixtures > 0 else 0
        set_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.FIXTURE_HIGH_LEVEL)
        self.common.write_to_db_result(fk=set_kpi_fk, result=set_average, identifier_result=Const.FIXTURE_HIGH_LEVEL,
                                       numerator_id=self.manufacturer_fk)

    def get_osa_and_pog(self):
        """
        Calculates for every fixture the OSA (%), OOS (#) and the POG (%)
        """
        num_of_fixtures, all_scores = 0.0, 0.0
        fixture_pog_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.FIXTURE_POG)
        pog_scene_results = self.scene_results[self.scene_results['kpi_level_2_fk'] == fixture_pog_kpi_fk]
        fixture_osa_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.FIXTURE_OSA)
        osa_scene_results = self.scene_results[self.scene_results['kpi_level_2_fk'] == fixture_osa_kpi_fk]
        for fixture in self.required_fixtures:
            avg_pog_exit = self.get_fixture_osa_oos_pog(fixture, osa_scene_results, pog_scene_results)
            num_of_fixtures += 1
            all_scores += avg_pog_exit
        set_average = all_scores / num_of_fixtures if num_of_fixtures > 0 else 0
        set_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.POG_HIGH_LEVEL)
        self.common.write_to_db_result(fk=set_kpi_fk, result=set_average, identifier_result=Const.POG_HIGH_LEVEL,
                                       numerator_id=self.manufacturer_fk)

    def get_fixture_osa_oos_pog(self, fixture, osa_scene_results, pog_scene_results):
        """
        Gets a fixture, and writes in the DB its POG+OOS+OSA score
        :param fixture: dict with entry + exit names, match scene_fks Etc (from self.required_fixtures)
        :param osa_scene_results: filtered scene_results
        :param pog_scene_results: filtered scene_results
        :return: the POG average (for the set score)
        """
        pog_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.VISIT_POG)
        osa_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.VISIT_OSA)
        denominator, fixture_fk = fixture[Const.REQUIRED_AMOUNT], fixture[Const.FIXTURE_FK]
        entry_scenes, exit_scenes = fixture[Const.ENTRY_SCENES], fixture[Const.EXIT_SCENES]
        entry_osa_results = osa_scene_results[osa_scene_results['scene_fk'].isin(entry_scenes)]
        exit_osa_results = osa_scene_results[osa_scene_results['scene_fk'].isin(exit_scenes)]
        avg_osa_entry, avg_oos_entry = self.get_scores_and_results(entry_osa_results, denominator)
        avg_osa_exit, avg_oos_exit = self.get_scores_and_results(exit_osa_results, denominator)
        osa_delta = avg_osa_exit - avg_osa_entry
        oos_delta = avg_oos_exit if np.isnan(avg_oos_entry) else avg_oos_exit - avg_oos_entry
        self.common.write_to_db_result(
            fk=osa_kpi_fk, numerator_id=fixture_fk,
            numerator_result=osa_delta, denominator_result=oos_delta, result=avg_oos_exit, score=avg_osa_exit)
        entry_pog_results = pog_scene_results[pog_scene_results['scene_fk'].isin(entry_scenes)]
        exit_pog_results = pog_scene_results[pog_scene_results['scene_fk'].isin(exit_scenes)]
        avg_pog_entry, temporary = self.get_scores_and_results(entry_pog_results, denominator)
        avg_pog_exit, temporary = self.get_scores_and_results(exit_pog_results, denominator)
        delta = avg_pog_exit - avg_pog_entry
        identifier_result = self.common.get_dictionary(fixture=fixture_fk, kpi_fk=pog_kpi_fk)
        self.common.write_to_db_result(
            fk=pog_kpi_fk, numerator_id=fixture_fk, identifier_parent=Const.POG_HIGH_LEVEL,
            numerator_result=delta, score=avg_pog_exit, should_enter=True, identifier_result=identifier_result)
        for scene_result_fk in exit_pog_results['pk'].values:
            self.common.write_to_db_result(should_enter=True, scene_result_fk=scene_result_fk,
                                           identifier_parent=identifier_result, only_hierarchy=True)
        return avg_pog_exit

    @staticmethod
    def get_scores_and_results(scene_results, required_amount):
        """
        :param scene_results: results of specific scenes (one result for every scene)
        :param required_amount: the required amount of scenes in the scene_results
        :return: the avg of the scores and results. If there are too many scenes - take the betters, if less than
                 the required - calculate them as 0
        """
        if None in scene_results['result'].tolist():
            return 0, None
        scores, results = scene_results.sort_values(
            by='score', ascending=False)[['score', 'result']][:required_amount].sum() / required_amount
        return scores, results

    @staticmethod
    def division(num, den):
        ratio = num * 100.0 / den if den else 0
        return ratio
