import pandas as pd
import datetime as dt

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector

from Projects.CCRU.Utils.JSON import CCRUJsonGenerator
from Projects.CCRU.Utils.ToolBox import CCRUKPIToolBox

__author__ = 'urid'

SUPERETTE = 'PoS 2017 - MT - Superette'
TARGET_EXECUTION = 'Target Execution 2017'
MARKETING = 'Marketing 2017'


class CCRUSuperetteCalculations:
    def __init__(self, data_provider, output, ps_data_provider):  #All relevant session data with KPI static info will trigger the KPI calculation
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.data_provider = data_provider
        self.project_name = data_provider.project_name
        self.output = output
        self.session_uid = self.data_provider.session_uid
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        # self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.rds_conn = self.rds_connection()
        self.session_info = SessionInfo(data_provider)
        self.store_id = self.data_provider[Data.STORE_FK]
        self.tool_box = CCRUKPIToolBox(self.data_provider, self.output, ps_data_provider, SUPERETTE)

        self.results = {}

    def rds_connection(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self._rds_conn.db)
        except:
            self._rds_conn.disconnect_rds()
            self._rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        return self._rds_conn

    def main_function(self):
        jg = CCRUJsonGenerator('ccru')
        jg.create_json('Superette.xlsx', SUPERETTE)

        calc_start_time = dt.datetime.utcnow()
        Log.debug('Calculation Started at {}'.format(calc_start_time))
        score = 0
        score += self.tool_box.check_availability(jg.project_kpi_dict.get('kpi_data')[0])
        score += self.tool_box.check_number_of_scenes(jg.project_kpi_dict.get('kpi_data')[0])
        score += self.tool_box.check_number_of_doors(jg.project_kpi_dict.get('kpi_data')[0])
        score += self.tool_box.check_number_of_doors_given_number_of_sku(jg.project_kpi_dict.get('kpi_data')[0])
        score += self.tool_box.check_number_of_doors_given_sos(jg.project_kpi_dict.get('kpi_data')[0])
        attributes_for_table1 = pd.DataFrame([(SUPERETTE, self.session_uid,
                                               self.store_id, self.visit_date.isoformat()
                                               , format(score, '.2f'), None)], columns=['kps_name',
                                                                                        'session_uid',
                                                                                        'store_fk',
                                                                                        'visit_date',
                                                                                        'score_1',
                                                                                        'kpi_set_fk'])
        self.tool_box.write_to_db_result(attributes_for_table1, 'level1')
        jg.create_gaps_json('gaps_guide.xlsx', sheet_name=SUPERETTE)
        self.tool_box.calculate_gaps(jg.project_kpi_dict.get('gaps'))
        self.tool_box.write_gaps()

        extra_sets_to_calculate = [(TARGET_EXECUTION, 'Target Execution'), (MARKETING, 'Marketing')]
        for extra_set_name, template_name in extra_sets_to_calculate:
            self.tool_box.change_set(extra_set_name)
            jg.project_kpi_dict['kpi_data'] = []
            jg.create_json('{}.xlsx'.format(template_name), extra_set_name)
            calc_start_time = dt.datetime.utcnow()
            Log.debug('Calculation Started at {}'.format(calc_start_time))
            score = 0
            score += self.tool_box.check_availability(jg.project_kpi_dict.get('kpi_data')[0])
            score += self.tool_box.facings_sos(jg.project_kpi_dict.get('kpi_data')[0])
            score += self.tool_box.check_number_of_scenes(jg.project_kpi_dict.get('kpi_data')[0])
            score += self.tool_box.check_number_of_doors(jg.project_kpi_dict.get('kpi_data')[0])
            attributes_for_table1 = pd.DataFrame([(extra_set_name, self.session_uid,
                                                   self.store_id, self.visit_date.isoformat()
                                                   , format(score, '.2f'), None)], columns=['kps_name',
                                                                                            'session_uid',
                                                                                            'store_fk',
                                                                                            'visit_date',
                                                                                            'score_1',
                                                                                            'kpi_set_fk'])
            self.tool_box.write_to_db_result(attributes_for_table1, 'level1')

        self.tool_box.calculate_contract_execution()
        self.tool_box.calculate_top_sku()
        self.tool_box.commit_results_data()
        calc_finish_time = dt.datetime.utcnow()
        Log.debug('Calculation time took {}'.format(calc_finish_time - calc_start_time))

