import pandas as pd

from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

from Projects.CCRU_SAND.Utils.JSON import CCRU_SANDJsonGenerator
from Projects.CCRU_SAND.Utils.ToolBox import CCRU_SANDKPIToolBox


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


class CCRU_SANDCalculations(BaseCalculationsScript):

    @log_runtime('Total Calculations', log_start=True)
    def run_project_calculations(self):

        self.timer.start()
        ProjectCalculations(self.data_provider, self.output).main_function()
        self.timer.stop('CCRU_SANDCalculations.run_project_calculations')


class ProjectCalculations:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = self.data_provider.project_name
        self.rds_conn = self.rds_connection()

        self.tool_box = CCRU_SANDKPIToolBox(self.data_provider, self.output)
        self.session_uid = self.tool_box.session_uid
        self.visit_date = self.tool_box.visit_date
        self.store_id = self.tool_box.store_id
        self.test_store = self.tool_box.test_store
        self.pos_kpi_set_name = self.tool_box.pos_kpi_set_name

        self.session_info = SessionInfo(data_provider)
        self.json = CCRU_SANDJsonGenerator()

        self.results = {}

    def main_function(self):

        if not self.tool_box.external_session_id.find('EasyMerch-P') < 0:
            Log.info('Promo session, no Custom KPI calculation implied')
            return

        self.json.create_kpi_data_json('kpi_source', 'KPI_Source.xlsx', sheet_name=self.pos_kpi_set_name)
        kpi_source_json = self.json.project_kpi_dict.get('kpi_data').get(self.pos_kpi_set_name)
        kpi_source = {}
        for row in kpi_source_json:
            kpi_source[row.pop(SOURCE)] = row

        if kpi_source:
            pass

        elif self.test_store == "Y":
            Log.warning('POS KPI Set name in store attribute is invalid: {0}. '
                        'Session Store ID {1} cannot be calculated. '
                        'Store ID {1} is a test store'
                        .format(self.pos_kpi_set_name, self.store_id))
            return

        else:
            Log.error('POS KPI Set name in store attribute is invalid: {0}. '
                      'Session Store ID {1} cannot be calculated. '
                      .format(self.pos_kpi_set_name, self.store_id))
            return

        kpi_sets_to_calculate = [POS, TARGET, MARKETING, SPIRITS]
        for kpi_set in kpi_sets_to_calculate:
            Log.info('KPI calculation stage: {}'.format(kpi_source[kpi_set][SET]))
            self.tool_box.set_kpi_set(kpi_source[kpi_set][SET])
            self.json.project_kpi_dict['kpi_data'] = []
            self.json.create_kpi_data_json('kpi_data', kpi_source[kpi_set][FILE], sheet_name=kpi_source[kpi_set][SHEET])
            kpi_data = self.json.project_kpi_dict.get('kpi_data')[0]
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
            score += self.tool_box.check_weighted_average(kpi_data)
            score += self.tool_box.check_kpi_scores(kpi_data)
            attributes_for_table1 = pd.DataFrame([(kpi_source[kpi_set][SET],
                                                   self.session_uid,
                                                   self.store_id,
                                                   self.visit_date.isoformat(),
                                                   format(score, '.2f'), None)],
                                                 columns=['kps_name',
                                                          'session_uid',
                                                          'store_fk',
                                                          'visit_date',
                                                          'score_1',
                                                          'kpi_set_fk'])
            self.tool_box.write_to_db_result(attributes_for_table1, 'level1')

            if kpi_set == POS:
                Log.info('KPI calculation stage: {}'.format(kpi_source[INTEGRATION][SET]))
                self.tool_box.prepare_hidden_set(kpi_data, kpi_source[INTEGRATION][SET])

        Log.info('KPI calculation stage: {}'.format(kpi_source[GAPS][SET]))
        self.json.create_kpi_data_json('gaps', kpi_source[GAPS][FILE], sheet_name=kpi_source[GAPS][SHEET])
        self.tool_box.calculate_gaps(self.json.project_kpi_dict.get('gaps'))
        self.tool_box.write_gaps()

        Log.info('KPI calculation stage: {}'.format(kpi_source[TOPSKU][SET]))
        self.tool_box.calculate_top_sku()

        Log.info('KPI calculation stage: {}'.format(kpi_source[CONTRACT][SET]))
        self.json.create_kpi_data_json('contract', kpi_source[CONTRACT][FILE], sheet_name=kpi_source[CONTRACT][SHEET])
        if self.json.project_kpi_dict.get('contract'):
            self.tool_box.calculate_equipment_execution(self.json.project_kpi_dict.get('contract'))
            self.tool_box.calculate_contract_execution(self.json.project_kpi_dict.get('contract'))

        Log.info('KPI calculation stage: {}'.format('Committing results'))
        self.tool_box.commit_results_data()

    def rds_connection(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self._rds_conn.db)
        except:
            self._rds_conn.disconnect_rds()
            self._rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        return self._rds_conn

