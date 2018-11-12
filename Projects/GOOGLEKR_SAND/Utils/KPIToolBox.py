from datetime import datetime
import os
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.GOOGLEKR_SAND.Utils.Const import Const
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

__author__ = 'Eli_Sam_Shivi'

FIXTURE_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../Data',
                                     'KR - Google Fixture Targets v.2.xlsx')


class GOOGLEToolBox:

    def __init__(self, data_provider, output, common_v2):
        self.common = common_v2
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.templates = self.data_provider.all_templates
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
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
        store_num = self.store_info['store_number_1'][0]
        fixture_template = self.fixture_template[Const.FIXTURE_TARGETS]
        fixture_template = fixture_template.join(self.fixture_template[Const.PK]
                                                 .set_index('New Task Name (unique)')[Const.PK],
                                                 on='New Task Name (unique)', how='left')
        relevant_fixtures = fixture_template[fixture_template['Store Number'] == store_num]
        relevant_fixtures.drop_duplicates(inplace=True)
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
        self.choose_scenes()

    def google_global_fixture_compliance(self):
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
        self.common.write_to_db_result(fk=set_kpi_fk, score=set_average, identifier_result=Const.FIXTURE_HIGH_LEVEL)

    def get_osa_and_pog(self):
        num_of_fixtures, all_scores = 0.0, 0.0
        pog_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.VISIT_POG)
        fixture_pog_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.FIXTURE_POG)
        pog_scene_results = self.scene_results[self.scene_results['kpi_level_2_fk'] == fixture_pog_kpi_fk]
        osa_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.VISIT_OSA)
        fixture_osa_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.FIXTURE_OSA)
        osa_scene_results = self.scene_results[self.scene_results['kpi_level_2_fk'] == fixture_osa_kpi_fk]
        for fixture in self.required_fixtures:
            entry_osa_results = osa_scene_results[osa_scene_results['scene_fk'].isin(fixture[Const.ENTRY_SCENES])]
            exit_osa_results = osa_scene_results[osa_scene_results['scene_fk'].isin(fixture[Const.EXIT_SCENES])]
            avg_osa_entry, avg_oos_entry = self.get_scores_and_results(entry_osa_results, fixture[Const.REQUIRED_AMOUNT])
            avg_osa_exit, avg_oos_exit = self.get_scores_and_results(exit_osa_results, fixture[Const.REQUIRED_AMOUNT])
            osa_delta = avg_osa_exit - avg_osa_entry
            oos_delta = avg_oos_exit - avg_oos_entry
            self.common.write_to_db_result(
                fk=osa_kpi_fk, numerator_id=fixture[Const.FIXTURE_FK],
                numerator_result=osa_delta, denominator_result=oos_delta, result=avg_oos_exit, score=avg_osa_exit)
            entry_pog_results = pog_scene_results[pog_scene_results['scene_fk'].isin(fixture[Const.ENTRY_SCENES])]
            exit_pog_results = pog_scene_results[pog_scene_results['scene_fk'].isin(fixture[Const.EXIT_SCENES])]
            avg_pog_entry, temporary = self.get_scores_and_results(entry_pog_results, fixture[Const.REQUIRED_AMOUNT])
            avg_pog_exit, temporary = self.get_scores_and_results(exit_pog_results, fixture[Const.REQUIRED_AMOUNT])
            delta = avg_pog_exit - avg_pog_entry
            identifier_result = self.common.get_dictionary(fixture=fixture, kpi_fk=pog_kpi_fk)
            self.common.write_to_db_result(
                fk=pog_kpi_fk, numerator_id=fixture[Const.FIXTURE_FK], identifier_parent=Const.POG_HIGH_LEVEL,
                numerator_result=delta, score=avg_pog_exit, should_enter=True, identifier_result=identifier_result)
            for scene_result_fk in exit_pog_results['pk'].values:
                self.common.write_to_db_result(fk=Const.NON_KPI, should_enter=True, scene_result_fk=scene_result_fk,
                                               identifier_parent=identifier_result)
            num_of_fixtures += 1
            all_scores += avg_pog_exit
        set_average = all_scores / num_of_fixtures if num_of_fixtures > 0 else 0
        set_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.POG_HIGH_LEVEL)
        self.common.write_to_db_result(fk=set_kpi_fk, score=set_average, identifier_result=Const.POG_HIGH_LEVEL)

    @staticmethod
    def get_scores_and_results(scene_results, reuired_amount):
        scores, results = scene_results.sort_values(
            by='score', ascending=False)[['score', 'result']][:reuired_amount].sum() / reuired_amount
        return scores, results

    def choose_scenes(self):
        for fixture_type in self.required_fixtures:
            fixture_type[Const.ENTRY_SCENES] = self.scif[self.scif['template_name'] ==
                                                         fixture_type[Const.ENTRY_NAME]]['template_fk'].unique().tolist()
            fixture_type[Const.EXIT_SCENES] = self.scif[self.scif['template_name'] ==
                                                        fixture_type[Const.EXIT_NAME]]['template_fk'].unique().tolist()

    @staticmethod
    def division(num, den):
        ratio = num * 100.0 / den if den else 0
        return ratio
