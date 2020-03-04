import pandas as pd

from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

# from Projects.CCRU.Utils.Consts import CCRUConsts
from Projects.CCRU.Utils.JSON import CCRUJsonGenerator
from Projects.CCRU.Utils.ToolBox import CCRUKPIToolBox


__author__ = 'sergey'


SOURCE = 'SOURCE'
SET = 'SET'
FILE = 'FILE'
SHEET = 'SHEET'
POS = 'POS'
GAPS = 'GAPS'
TARGET = 'TARGET'
MARKETING = 'MARKETING'
SPIRITS = 'SPIRITS'
CONTRACT = 'CONTRACT'
EQUIPMENT = 'EQUIPMENT'
INTEGRATION = 'INTEGRATION'
TOPSKU = 'TOPSKU'
KPI_CONVERSION = 'KPI_CONVERSION'
BENCHMARK = 'BENCHMARK'
MR_TARGET = 'MR TARGET' 


class CCRUCalculations(BaseCalculationsScript):

    @log_runtime('Total Calculations', log_start=True)
    def run_project_calculations(self):

        self.timer.start()
        CCRUProjectCalculations(self.data_provider, self.output).main_function()
        self.timer.stop('CCRUCalculations.run_project_calculations')


class CCRUProjectCalculations:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = self.data_provider.project_name
        self.rds_conn = self.rds_connection()

        self.tool_box = CCRUKPIToolBox(self.data_provider, self.output, update_kpi_set=True)
        self.session_uid = self.tool_box.session_uid
        self.visit_date = self.tool_box.visit_date
        self.store_id = self.tool_box.store_id
        self.test_store = self.tool_box.test_store
        self.pos_kpi_set_name = self.tool_box.pos_kpi_set_name

        self.session_info = SessionInfo(data_provider)
        self.json = CCRUJsonGenerator()

        self.results = {}

    def main_function(self):

        if str(self.visit_date) < self.tool_box.MIN_CALC_DATE:
            Log.warning('Warning. Session cannot be calculated. '
                        'Visit date is less than {2} - {0}. '
                        'Store ID {1}.'
                        .format(self.visit_date, self.store_id, self.tool_box.MIN_CALC_DATE))

        elif self.tool_box.visit_type in [self.tool_box.SEGMENTATION_VISIT]:
            Log.warning('Warning. Session with Segmentation visit type has no KPI calculations.')

        elif self.tool_box.visit_type in [self.tool_box.PROMO_VISIT]:
            # Log.warning('Warning. Session with Promo visit type has no KPI calculations.')
            # self.calculate_promo_compliance()
            pass

        else:
            if self.pos_kpi_set_name not in self.tool_box.ALLOWED_POS_SETS:
                Log.warning('Warning. Session cannot be calculated. '
                            'POS KPI Set name in store attribute is invalid - {0}. '
                            'Store ID {1}.'
                            .format(self.pos_kpi_set_name, self.store_id))
            else:
                self.calculate_red_score()

        Log.debug('KPI calculation is completed')

    def calculate_red_score(self):
        kpi_source_json = self.json.create_kpi_source('KPI_Source.xlsx', self.pos_kpi_set_name)
        kpi_source = {}
        for row in kpi_source_json:
            # Log.info('SOURCE: {}'.format(row.get(SOURCE)))
            kpi_source[row.pop(SOURCE)] = row
        if kpi_source:
            pass

        # elif self.test_store == "Y":
        #     Log.warning('Warning. Session cannot be calculated: '
        #                 'Store is a test store. '
        #                 'Store ID {1}.'
        #                 .format(self.pos_kpi_set_name, self.store_id))
        #     return

        else:
            Log.warning('Warning. Session cannot be calculated. '
                        'POS KPI Set name in store attribute is invalid - {0}. '
                        'Store ID {1}.'
                        .format(self.pos_kpi_set_name, self.store_id))
            return

        mr_targets = {}
        for kpi_set, params in kpi_source.items():
            if params.get(MR_TARGET) is not None:
                mr_targets.update({kpi_set: params[MR_TARGET]})
                mr_targets.update({params[SET]: params[MR_TARGET]})
        self.tool_box.mr_targets = mr_targets

        kpi_sets_types_to_calculate = [POS, SPIRITS]
        for kpi_set_type in kpi_sets_types_to_calculate:
            if kpi_source[kpi_set_type][SET]:
                Log.debug('KPI calculation stage: {}'.format(kpi_source[kpi_set_type][SET]))
                self.tool_box.set_kpi_set(kpi_source[kpi_set_type][SET], kpi_set_type)
                self.json.project_kpi_dict['kpi_data'] = []
                self.json.create_kpi_data_json('kpi_data', kpi_source[kpi_set_type][FILE],
                                               sheet_name=kpi_source[kpi_set_type][SHEET],
                                               pos_kpi_set_name=self.pos_kpi_set_name)
                self.calculate_red_score_kpi_set(self.json.project_kpi_dict.get('kpi_data')[0],
                                                 kpi_source[kpi_set_type][SET],
                                                 mr_targets.get(kpi_set_type))

        if kpi_source[GAPS][SET]:
            Log.debug('KPI calculation stage: {}'.format(kpi_source[GAPS][SET]))
            self.tool_box.set_kpi_set(kpi_source[GAPS][SET], GAPS)
            self.json.create_kpi_data_json(
                'gaps', kpi_source[GAPS][FILE], sheet_name=kpi_source[GAPS][SHEET])
            self.tool_box.calculate_gaps_old(self.json.project_kpi_dict.get('gaps'))
            self.tool_box.calculate_gaps_new(self.json.project_kpi_dict.get('gaps'),
                                             kpi_source[GAPS][SET])

        if kpi_source[BENCHMARK][SET]:
            Log.debug('KPI calculation stage: {}'.format(kpi_source[BENCHMARK][SET]))
            self.tool_box.set_kpi_set(kpi_source[BENCHMARK][SET], BENCHMARK)
            self.json.create_kpi_data_json(
                'benchmark', kpi_source[BENCHMARK][FILE], sheet_name=kpi_source[BENCHMARK][SHEET])
            self.tool_box.calculate_benchmark(self.json.project_kpi_dict.get('benchmark'),
                                              kpi_source[BENCHMARK][SET])

        if kpi_source[CONTRACT][FILE]:
            Log.debug('Importing Contract Execution template')
            self.json.create_kpi_data_json(
                'contract', kpi_source[CONTRACT][FILE], sheet_name=kpi_source[CONTRACT][SHEET])

        if kpi_source[TOPSKU][SET]:
            Log.debug('KPI calculation stage: {}'.format(kpi_source[TOPSKU][SET]))
            include_to_contract = True if self.json.project_kpi_dict.get('contract') else False
            self.tool_box.set_kpi_set(kpi_source[TOPSKU][SET], TOPSKU)
            self.tool_box.calculate_top_sku(include_to_contract,
                                            kpi_source[TOPSKU][SET])

        if self.json.project_kpi_dict.get('contract'):
            if kpi_source[EQUIPMENT][SET]:
                equipment_target_data = self.tool_box.get_equipment_target_data()
                if equipment_target_data:
                    if kpi_source[TARGET][SET]:
                            Log.debug('KPI calculation stage: {}'.format(kpi_source[TARGET][SET]))
                            self.tool_box.set_kpi_set(kpi_source[TARGET][SET], TARGET)
                            self.json.project_kpi_dict['kpi_data'] = []
                            self.json.create_kpi_data_json('kpi_data', kpi_source[TARGET][FILE],
                                                           sheet_name=kpi_source[TARGET][SHEET])
                            self.calculate_red_score_kpi_set(self.json.project_kpi_dict.get('kpi_data')[0],
                                                             kpi_source[TARGET][SET])

                            Log.debug('KPI calculation stage: {}'.format(kpi_source[EQUIPMENT][SET]))
                            self.tool_box.set_kpi_set(kpi_source[EQUIPMENT][SET], EQUIPMENT)
                            self.tool_box.calculate_equipment_execution(self.json.project_kpi_dict.get('contract'),
                                                                        kpi_source[EQUIPMENT][SET],
                                                                        kpi_source[KPI_CONVERSION][FILE],
                                                                        equipment_target_data)

            if kpi_source[CONTRACT][SET]:
                Log.debug('KPI calculation stage: {}'.format(kpi_source[CONTRACT][SET]))
                self.tool_box.set_kpi_set(kpi_source[CONTRACT][SET], CONTRACT)
                self.tool_box.calculate_contract_execution(self.json.project_kpi_dict.get('contract'),
                                                           kpi_source[CONTRACT][SET])

        Log.debug('KPI calculation stage: {}'.format('Committing results old'))
        self.tool_box.commit_results_data_old()

        Log.debug('KPI calculation stage: {}'.format('Committing results new'))
        self.tool_box.commit_results_data_new()

    def calculate_red_score_kpi_set(self, kpi_data, kpi_set_name, set_target=None):

        score = 0
        score += self.tool_box.check_availability(kpi_data)
        score += self.tool_box.check_facings_sos(kpi_data)
        score += self.tool_box.check_share_of_cch(kpi_data)
        score += self.tool_box.check_number_of_skus_per_door_range(kpi_data)
        score += self.tool_box.check_number_of_doors(kpi_data)
        score += self.tool_box.check_number_of_scenes(kpi_data)
        score += self.tool_box.check_number_of_scenes_no_tagging(kpi_data)
        score += self.tool_box.check_customer_cooler_doors(kpi_data)
        score += self.tool_box.check_atomic_passed(kpi_data)
        score += self.tool_box.check_atomic_passed_on_the_same_scene(kpi_data)
        score += self.tool_box.check_sum_atomics(kpi_data)
        score += self.tool_box.check_dummies(kpi_data)
        score += self.tool_box.check_weighted_average(kpi_data)
        score += self.tool_box.check_kpi_scores(kpi_data)

        self.tool_box.create_kpi_groups(kpi_data.values()[0])

        self.tool_box.write_to_kpi_results_old(
            pd.DataFrame([(kpi_set_name,
                           self.session_uid,
                           self.store_id,
                           self.visit_date.isoformat(),
                           format(score, '.2f'), None)],
                         columns=['kps_name',
                                  'session_uid',
                                  'store_fk',
                                  'visit_date',
                                  'score_1',
                                  'kpi_set_fk']), 'level1')

        set_target = set_target if set_target is not None else 100

        self.tool_box.update_kpi_scores_and_results(
            {'KPI ID': 0,
             'KPI name Eng': kpi_set_name,
             'KPI name Rus': kpi_set_name,
             'Parent': 'root'},
            {'target': set_target,
             'weight': None,
             'result': score,
             'score': score,
             'weighted_score': score,
             'level': 0})

        # INTEGRATION is excluded from calculation starting 2020
        #
        # if kpi_set_type == POS:
        #     Log.debug('KPI calculation stage: {}'.format(kpi_source[INTEGRATION][SET]))
        #     self.tool_box.prepare_hidden_set(kpi_data, kpi_source[INTEGRATION][SET])

    def calculate_promo_compliance(self):
        self.json.create_kpi_data_json('promo', 'KPI_Promo_Tracking.xlsx', sheet_name='2019')
        kpi_data = self.json.project_kpi_dict.get('promo')

        Log.debug('KPI calculation stage: {}'.format('Promo Compliance'))
        self.tool_box.calculate_promo_compliance_store(kpi_data)

        Log.debug('KPI calculation stage: {}'.format('Committing results new'))
        self.tool_box.common.commit_results_data()

    def rds_connection(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self._rds_conn.db)
        except:
            self._rds_conn.disconnect_rds()
            self._rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        return self._rds_conn

