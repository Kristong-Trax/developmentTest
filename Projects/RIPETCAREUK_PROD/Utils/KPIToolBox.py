import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log

from Projects.RIPETCAREUK_PROD.Utils.AtomicKpisCalculator import SosCalculator, ShelfLevelCalculator, ClipStripCalculator, \
    NumOfFacingsCalculator
from Projects.RIPETCAREUK_PROD.Utils.Const import SEPARATOR, RELEVANT_SCENE_TYPES, PERFECT_STORE
from Projects.RIPETCAREUK_PROD.Utils.GeneralToolBox import MarsUkGENERALToolBox
from Projects.RIPETCAREUK_PROD.Utils.ParseTemplates import ParseMarsUkTemplates, KPIConsts
from Projects.RIPETCAREUK_PROD.Utils.Scoring import get_proportional_scoring_by_groups, get_proportional_scoring, \
    get_binary_add_scoring, get_accumulative_scoring_by_score_groups
from Projects.RIPETCAREUK_PROD.Utils.Writer import KpiResultsWriter, KpiResultsWriterExcel
from Projects.RIPETCAREUK_PROD.Utils.Fetcher import MarsUkQueries


__author__ = 'Dudi S'


class MarsUkPerfectScore(object):

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.set_name = PERFECT_STORE
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.get_store_att10(self.store_id)
        # self.store_type = self.store_info['store_type'].iloc[0]
        self.tools = MarsUkGENERALToolBox(self.data_provider, self.output)
        self.template = ParseMarsUkTemplates(self.visit_date)
        self.templates_data = self.template.parse_templates()
        self._writer = self._get_writer()

    def _get_writer(self):
        return KpiResultsWriter(session_uid=self.session_uid,
                                rds_conn=self.rds_conn,
                                store_id=self.store_id,
                                visit_date=self.visit_date)

    @property
    def _kpi_type_calculator_mapping(self):
        return {
            NumOfFacingsCalculator.kpi_type: NumOfFacingsCalculator,
            SosCalculator.kpi_type: SosCalculator,
            ClipStripCalculator.kpi_type: ClipStripCalculator,
            ShelfLevelCalculator.kpi_type: ShelfLevelCalculator,
        }

    @property
    def _score_type_calculation_mapping(self):
        return {
            'ACCUMULATIVE': get_accumulative_scoring_by_score_groups,
            'BINARY': get_binary_add_scoring,
            'PROPORTIONAL': get_proportional_scoring,
            'PROPORTIONAL BY GROUPS': get_proportional_scoring_by_groups
        }

    def get_store_att10(self, store_fk):
        query = MarsUkQueries.get_store_attribute_10(store_fk)
        att10 = pd.read_sql_query(query, self.rds_conn.db)
        return att10.values[0][0]

    def calculate(self):
        """
        This function calculates the KPI results.
        """
        kpi_template = self.templates_data[KPIConsts.SHEET_NAME]
        perfect_score_record = kpi_template[kpi_template[KPIConsts.KPI_NAME] == self.set_name]
        perfect_score_tested_kpi_group = perfect_score_record.iloc[0][KPIConsts.TESTED_KPI_GROUP]
        perfect_score_pillars = kpi_template[kpi_template[KPIConsts.KPI_GROUP] == perfect_score_tested_kpi_group]
        pillar_results = pd.DataFrame(columns=['score', 'name'])
        for pillar_ind, pillar_data in perfect_score_pillars.iterrows():
            pillar_result = self._calculate_pillar_score(kpi_template, pillar_data)
            pillar_results = pillar_results.append(pillar_result)
        if not pillar_results.empty:
            perfect_store_score = pillar_results['score'].sum()
            self._writer.write_to_db_level_1_result(set_name=self.set_name, score=perfect_store_score)
        else:
            Log.warning('There are no results for set:{}'.format(self.set_name))
        self._writer.commit_results_data()

    def _calculate_pillar_score(self, kpi_template, pillar_data):
        pillar_tested_kpi_group = pillar_data[KPIConsts.TESTED_KPI_GROUP]
        pillar_name = pillar_data[KPIConsts.KPI_NAME]
        pillar_kpis = kpi_template[((kpi_template[KPIConsts.KPI_GROUP] == pillar_tested_kpi_group) &
                                    (kpi_template[KPIConsts.WEIGHT] != ''))]
        kpi_results = pd.DataFrame(columns=['score', 'kpi_weight', 'kpi_name'])
        total_weight_delta = 0
        potential_score = pillar_kpis['WEIGHT'].sum()
        for kpi_ind, kpi_data in pillar_kpis.iterrows():
            kpi_result, weight_delta = self._calculate_kpi_level_2_score(kpi_data, pillar_name)
            kpi_results = kpi_results.append(kpi_result)
            total_weight_delta += weight_delta
        if not kpi_results.empty:
            pillar_score = kpi_results['score'].sum()
            final_score = round((pillar_score / float(potential_score-total_weight_delta))*potential_score, 2)
            self._writer.write_to_db_level_1_result(set_name=pillar_name, score=final_score)
            # self._writer.write_to_db_level_1_result(set_name=pillar_name, score=pillar_score)

            # pillar_result = {
            #     'score': pillar_score,
            #     'name': pillar_name
            # }
            pillar_result = {
                'score': final_score,
                'name': pillar_name
            }
            return pd.DataFrame.from_records([pillar_result])
        else:
            return pd.DataFrame(columns=['score', 'name'])

    def _calculate_kpi_level_2_score(self, kpi_data, set_name):
        target = kpi_data[KPIConsts.TARGET]
        target_sheet_name = target.replace(KPIConsts.TARGET_PREFIX, '')
        target_sheet_data = self.templates_data[target_sheet_name]
        kpi_type = kpi_data[KPIConsts.KPI_TYPE]
        kpi_name = kpi_data[KPIConsts.KPI_NAME]
        kpi_weight = kpi_data[KPIConsts.WEIGHT] / 100
        kpi_score_type = kpi_data[KPIConsts.SCORE]
        relevant_scene_types = self._get_relevant_scene_types(kpi_data)
        calculator = self._kpi_type_calculator_mapping[kpi_type](self.store_type,
                                                                 self.tools,
                                                                 self.data_provider,
                                                                 self._writer)
        atomic_kpi_results, weight_delta = calculator.calculate(kpi_name=kpi_name, kpi_set_name=set_name,
                                                  target_template_data=target_sheet_data,
                                                  relevant_scene_types=relevant_scene_types,
                                                                kpi_weight=kpi_data[KPIConsts.WEIGHT])
        if not atomic_kpi_results.empty:
            score_calculator = self._score_type_calculation_mapping[kpi_score_type]
            kpi_score = score_calculator(atomic_kpi_results)
            kpi_weighted_score = kpi_score * kpi_weight
            kpi_result = {
                'score': kpi_weighted_score,
                'kpi_weight': kpi_weight,
                'kpi_name': kpi_name
            }

            self._writer.write_to_db_level_2_result(kpi_name=kpi_name, set_name=set_name, score=kpi_score)
            return pd.DataFrame.from_records([kpi_result]), weight_delta
        else:
            return pd.DataFrame(columns=['score', 'kpi_weight', 'kpi_name']), weight_delta

    @staticmethod
    def _get_relevant_scene_types(atomic_kpi_definition):
        return str(atomic_kpi_definition[RELEVANT_SCENE_TYPES]).split(
            SEPARATOR)
