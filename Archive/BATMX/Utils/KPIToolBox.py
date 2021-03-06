
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
import pandas as pd
from Projects.BATMX.Utils.Const import Const
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from Projects.BATMX.Common import Common

__author__ = 'elyashiv'


class BATMXToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.scenes = self.scene_info['scene_fk'].tolist()
        self.manufacturer_fk = int(self.data_provider[Data.OWN_MANUFACTURER]['param_value'].iloc[0])
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        scenes_with_templates = self.scif[['template_name', 'scene_fk']].drop_duplicates()
        exits = scenes_with_templates[scenes_with_templates[
            'template_name'].str.contains(Const.EXIT_TEMPLATE)]['scene_fk'].tolist()
        entries = scenes_with_templates[scenes_with_templates[
            'template_name'].str.contains(Const.ENTRY_TEMPLATE)]['scene_fk'].tolist()
        if self.scenes:
            self.scene_results = self.ps_data_provider.get_scene_results(self.scenes)
            self.exit_results = self.scene_results[self.scene_results["scene_fk"].isin(exits)]
            self.entry_results = self.scene_results[self.scene_results["scene_fk"].isin(entries)]
        else:
            self.scene_results = pd.DataFrame()

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        try:
            self.calculate_oos()
        except Exception as e:
            Log.error('{}'.format(e))
        try:
            self.calculate_pogs_and_sos_kpis()
        except Exception as e:
            Log.error('{}'.format(e))

    def calculate_pogs_and_sos_kpis(self):
        self.write_score_and_delta_sos(Const.SOS_LEVELS)
        self.write_score_and_delta_pog(Const.POG_KPI_NAMES[Const.TOBACCO_CENTER])
        self.write_score_and_delta_pog(Const.POG_KPI_NAMES[Const.PROMOTIONAL_TRAY])

    def write_score_and_delta_pog(self, kpi_names):
        visit_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_names[Const.VISIT_LEVEL])
        fixture_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_names[Const.FIXTURE_LEVEL])
        identifier_visit = self.common.get_dictionary(kpi_fk=visit_kpi_fk)
        exit_results = self.exit_results[self.exit_results['kpi_level_2_fk'] == fixture_kpi_fk]
        entry_results = self.entry_results[self.entry_results['kpi_level_2_fk'] == fixture_kpi_fk]
        self.write_hierarchy_pog_to_scene(exit_results, entry_results, identifier_visit, kpi_names)
        avg_exit = self.get_averages(exit_results)
        avg_entry = self.get_averages(entry_results)
        delta = avg_exit - avg_entry if avg_entry else avg_exit
        self.common.write_to_db_result(fk=visit_kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                                       score=avg_exit, result=delta, identifier_result=identifier_visit)

    def write_hierarchy_pog_to_scene(self, exit_results, entry_results, identifier_visit, kpi_names):
        pog_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_names[Const.PLANOGRAM_LEVEL])
        for planogram_fk in exit_results['numerator_id'].unique().tolist():
            pog_exit_results = exit_results[exit_results['numerator_id'] == planogram_fk]
            pog_exit_score = self.get_average(pog_exit_results, 'score')
            pog_exit_identifier = self.common.get_dictionary(
                planogram_fk=planogram_fk, entry_exit=Const.EXIT, kpi_fk=pog_kpi_fk)
            self.common.write_to_db_result(
                fk=pog_kpi_fk, numerator_id=planogram_fk, denominator_id=self.store_id,
                score=pog_exit_score, result=pog_exit_score, identifier_result=pog_exit_identifier,
                identifier_parent=identifier_visit, should_enter=True)
            pog_exit_fks = pog_exit_results['pk'].tolist()
            pog_entry_results = entry_results[entry_results['numerator_id'] == planogram_fk]
            pog_result_fks = pog_entry_results['pk'].tolist() + pog_exit_fks
            for scene_result_fk in pog_result_fks:
                self.common.write_to_db_result(
                    should_enter=True, scene_result_fk=scene_result_fk, numerator_id=self.manufacturer_fk,
                    denominator_id=self.store_id, identifier_parent=pog_exit_identifier, only_hierarchy=True)

    def write_score_and_delta_sos(self, kpi_names):
        visit_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_names[Const.VISIT_LEVEL])
        fixture_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_names[Const.FIXTURE_LEVEL])
        identifier = self.common.get_dictionary(kpi_fk=visit_kpi_fk)
        exit_results = self.exit_results[self.exit_results['kpi_level_2_fk'] == fixture_kpi_fk]
        entry_results = self.entry_results[self.entry_results['kpi_level_2_fk'] == fixture_kpi_fk]
        avg_exit = self.get_averages(exit_results)
        avg_entry = self.get_averages(entry_results)
        delta = avg_exit - avg_entry if avg_entry else avg_exit
        # May 23, 2019: changed per Rifka's request to include 'score' as 'result' - may need to be revised
        # self.common.write_to_db_result(fk=visit_kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
        #                                score=avg_exit, result=delta, identifier_result=identifier)
        self.common.write_to_db_result(fk=visit_kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                                       score=avg_exit, result=avg_exit, identifier_result=identifier)
        fixture_results_pk = exit_results['pk'].tolist() + entry_results['pk'].tolist()
        for scene_result_fk in fixture_results_pk:
            self.common.write_to_db_result(
                should_enter=True, scene_result_fk=scene_result_fk, numerator_id=self.manufacturer_fk,
                denominator_id=self.store_id, identifier_parent=identifier, only_hierarchy=True, only_hierarchy2=True)

    def calculate_oos(self):
        visit_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.OOS_VISIT)
        fixture_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.OOS_FIXTURE)
        identifier = self.common.get_dictionary(kpi_fk=visit_kpi_fk)
        entry_results = self.entry_results[self.entry_results['kpi_level_2_fk'] == fixture_kpi_fk]
        if entry_results.empty:
            return
        else:
            score = self.get_average(entry_results, 'score')
            self.common.write_to_db_result(
                fk=visit_kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                score=score, result=score, identifier_result=identifier,
                numerator_result=self.get_average(entry_results, 'numerator_result'),
                denominator_result=self.get_average(entry_results, 'denominator_result'))
            fixture_results_pk = entry_results['pk'].tolist() + self.exit_results[
                self.exit_results['kpi_level_2_fk'] == fixture_kpi_fk]['pk'].tolist()
            for scene_result_fk in fixture_results_pk:
                self.common.write_to_db_result(
                    should_enter=True, scene_result_fk=scene_result_fk, numerator_id=self.manufacturer_fk,
                    denominator_id=self.store_id, identifier_parent=identifier, only_hierarchy=True,
                    only_hierarchy2=True)

    @staticmethod
    def get_average(df, column):
        return df[column].sum() / len(df)

    @staticmethod
    def get_averages(fixture_results):
        if fixture_results.empty:
            return None
        else:
            fixtures_amount = fixture_results['score'].count()
            scores_sum = fixture_results['score'].sum()
            return scores_sum / fixtures_amount
