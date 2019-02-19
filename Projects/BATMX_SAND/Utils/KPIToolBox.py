
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
import pandas as pd
from Projects.BATMX_SAND.Utils.Const import Const
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from Projects.BATMX_SAND.Common import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

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
        self.manufacturer_fk = self.all_products[self.all_products["manufacturer_name"].isin(Const.BAT_MANUFACTURERS)][
            'manufacturer_fk'].iloc[0]
        exits, entries = [Const.EXIT], [Const.ENTRY] ################
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
        self.write_score_and_delta(Const.SOS_LEVELS)
        self.write_score_and_delta(Const.POG_KPI_NAMES[Const.TOBACCO_CENTER])
        self.write_score_and_delta(Const.POG_KPI_NAMES[Const.PROMOTIONAL_TRAY])

    def write_score_and_delta(self, kpi_names):
        visit_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_names[Const.VISIT_LEVEL])
        fixture_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_names[Const.FIXTURE_LEVEL])
        identifier = self.common.get_dictionary(kpi_fk=visit_kpi_fk)
        exit_results = self.exit_results[self.exit_results['kpi_level_2_fk'] == fixture_kpi_fk]
        entry_results = self.entry_results[self.entry_results['kpi_level_2_fk'] == fixture_kpi_fk]
        avg_exit = self.get_averages(exit_results)
        avg_entry = self.get_averages(entry_results)
        delta = avg_exit - avg_entry
        self.common.write_to_db_result(fk=visit_kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                                       score=avg_exit, result=delta, identifier_result=identifier)
        fixture_results_pk = exit_results['pk'].tolist() + entry_results['pk'].tolist()
        for scene_result_fk in fixture_results_pk:
            self.common.write_to_db_result(
                should_enter=True, scene_result_fk=scene_result_fk, numerator_id=self.manufacturer_fk,
                denominator_id=self.store_id, identifier_parent=identifier, only_hierarchy=True)

    def calculate_oos(self):
        visit_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.OOS_VISIT)
        fixture_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.OOS)
        identifier = self.common.get_dictionary(kpi_fk=visit_kpi_fk)
        results = self.entry_results[self.entry_results['kpi_level_2_fk'] == fixture_kpi_fk]
        if results.empty:
            return
        else:
            results = results.iloc[0]
            self.common.write_to_db_result(
                fk=visit_kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                score=results['score'], numerator_result=results['numerator_result'],
                denominator_result=results['denominator_result'], identifier_result=identifier)
            fixture_results_pk = results['pk'].tolist() + self.entry_results[
                self.entry_results['kpi_level_2_fk'] == fixture_kpi_fk]['pk'].tolist()
            for scene_result_fk in fixture_results_pk:
                self.common.write_to_db_result(
                    should_enter=True, scene_result_fk=scene_result_fk, numerator_id=self.manufacturer_fk,
                    denominator_id=self.store_id, identifier_parent=identifier, only_hierarchy=True)

    @staticmethod
    def get_averages(fixture_results):
        if fixture_results.empty:
            return None
        else:
            fixtures_amount = fixture_results['score'].count()
            scores_sum = fixture_results['score'].sum()
            return scores_sum / fixtures_amount
