import datetime

import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup

from Projects.CCRU.Utils.JSON import CCRUJsonGenerator
from Projects.CCRU.Utils.ToolBox import CCRUKPIToolBox
from KPIUtils.Utils.Helpers.LogHandler import log_handler
from Trax.Data.Projects.Connector import ProjectConnector

from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Utils.Logging.Logger import Log


__author__ = 'urid'


HRC_BAR_2018 = 'Pos 2018 - HoReCa - Bar Tavern Night Clubs'
HRC_BAR_2018_GAPS = 'Pos 2018 - HoReCa Bar'
TARGET_EXECUTION = 'Target Execution 2018'
MARKETING = 'Marketing 2017'
SPIRITS = 'Spirits 2018 - HoReCa'


class CCRUHRCBar2018Calculations:
    def __init__(self, data_provider, output, ps_data_provider):  # All relevant session data with KPI static info will trigger the KPI calculation
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.data_provider = data_provider
        self.project_name = data_provider.project_name
        self.output = output
        self.session_uid = self.data_provider.session_uid
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        # self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.rds_conn = self.rds_connection()
        self.session_info = SessionInfo(data_provider)
        self.store_id = self.data_provider[Data.STORE_FK]
        self.tool_box = CCRUKPIToolBox(self.data_provider, self.output, ps_data_provider, HRC_BAR_2018)
        self.results = {}

    def rds_connection(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self._rds_conn.db)
        except:
            self._rds_conn.disconnect_rds()
            self._rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        return self._rds_conn

    # @log_handler.log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        jg = CCRUJsonGenerator('ccru')
        jg.create_json('HoReCa Bar Tavern_Night Club PoS 2018.xlsx', HRC_BAR_2018)

        calc_start_time = datetime.datetime.utcnow()
        Log.info('Calculation Started at {}'.format(calc_start_time))
        score = 0
        score += self.tool_box.check_weighted_average(jg.project_kpi_dict.get('kpi_data')[0])
        score += self.tool_box.check_availability(jg.project_kpi_dict.get('kpi_data')[0])
        score += self.tool_box.check_atomic_passed(jg.project_kpi_dict.get('kpi_data')[0])
        attributes_for_table1 = pd.DataFrame([(HRC_BAR_2018, self.session_uid,
                                               self.store_id, self.visit_date.isoformat()
                                               , format(score, '.2f'), None)], columns=['kps_name',
                                                                                        'session_uid',
                                                                                        'store_fk',
                                                                                        'visit_date',
                                                                                        'score_1',
                                                                                        'kpi_set_fk'])
        self.tool_box.write_to_db_result(attributes_for_table1, 'level1', HRC_BAR_2018)

        self.tool_box.prepare_hidden_set(jg.project_kpi_dict.get('kpi_data')[0])

        jg.create_gaps_json('gaps_guide_2018.xlsx', sheet_name=HRC_BAR_2018_GAPS)
        self.tool_box.calculate_gaps(jg.project_kpi_dict.get('gaps'))
        self.tool_box.write_gaps()

        extra_sets_to_calculate = [(TARGET_EXECUTION, 'Target Execution 2018', TARGET_EXECUTION),
                                   (MARKETING, 'Marketing', MARKETING),
                                   (SPIRITS, 'HoReCa Spirits 2018', 'Sheet1')]
        for extra_set_name, template_name, sheet_name in extra_sets_to_calculate:
            self.tool_box.change_set(extra_set_name)
            jg.project_kpi_dict['kpi_data'] = []
            jg.create_json('{}.xlsx'.format(template_name), sheet_name)
            calc_start_time = datetime.datetime.utcnow()
            Log.info('Calculation Started at {}'.format(calc_start_time))
            score = 0
            score += self.tool_box.check_availability(jg.project_kpi_dict.get('kpi_data')[0])
            score += self.tool_box.facings_sos(jg.project_kpi_dict.get('kpi_data')[0])
            score += self.tool_box.check_number_of_scenes(jg.project_kpi_dict.get('kpi_data')[0])
            score += self.tool_box.check_number_of_doors(jg.project_kpi_dict.get('kpi_data')[0])
            score += self.tool_box.customer_cooler_doors(jg.project_kpi_dict.get('kpi_data')[0])
            score += self.tool_box.check_kpi_scores(jg.project_kpi_dict.get('kpi_data')[0])
            score += self.tool_box.calculate_number_of_scenes_no_tagging(jg.project_kpi_dict.get('kpi_data')[0])
            attributes_for_table1 = pd.DataFrame([(extra_set_name, self.session_uid,
                                                   self.store_id, self.visit_date.isoformat()
                                                   , format(score, '.2f'), None)], columns=['kps_name',
                                                                                            'session_uid',
                                                                                            'store_fk',
                                                                                            'visit_date',
                                                                                            'score_1',
                                                                                            'kpi_set_fk'])
            self.tool_box.write_to_db_result(attributes_for_table1, 'level1')

        self.tool_box.calculate_top_sku()

        jg.create_equipment_json('Contract Execution 2018.xlsx', HRC_BAR_2018)
        if jg.project_kpi_dict.get('equipment'):
            self.tool_box.calculate_equipment_execution(jg.project_kpi_dict.get('equipment'))
            self.tool_box.calculate_contract_execution(jg.project_kpi_dict.get('equipment'))

        self.tool_box.commit_results_data()
        # calc_finish_time = datetime.datetime.utcnow()
        # Log.info('Calculation time took {}'.format(calc_finish_time - calc_start_time))
