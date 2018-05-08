import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup
import datetime

from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Utils.Logging.Logger import Log
from Projects.CCRUFIFA.Utils.ToolBox import CCRUFIFAKPIToolBox
from Projects.CCRUFIFA.Utils.JSON import JsonGenerator

__author__ = 'urid'

FIFAKPIS = 'FIFA'

class CCRUFIFAGenerator:
    def __init__(self, data_provider, output):  #All relevant session data with KPI static info will trigger the KPI calculation
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.data_provider = data_provider
        self.project_name = data_provider.project_name
        self.output = output
        self.session_uid = self.data_provider.session_uid
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.session_info = SessionInfo(data_provider)
        self.store_id = self.data_provider[Data.STORE_FK]
        self.tool_box = CCRUFIFAKPIToolBox(self.data_provider, self.output, FIFAKPIS)

        self.results = {}

    def main_function(self):
        jg = JsonGenerator('ccru')
        jg.create_json('FIFA KPIs.xlsx', FIFAKPIS)

        calc_start_time = datetime.datetime.utcnow()
        Log.info('Calculation Started at {}'.format(calc_start_time))
        score = 0
        score += self.tool_box.check_availability(jg.project_kpi_dict.get('kpi_data')[0])
        score += self.tool_box.check_survey_answer(jg.project_kpi_dict.get('kpi_data')[0])
        score += self.tool_box.facings_sos(jg.project_kpi_dict.get('kpi_data')[0])
        attributes_for_table1 = pd.DataFrame([(FIFAKPIS, self.session_uid,
                                               self.store_id, self.visit_date.isoformat()
                                               , format(score, '.2f'), None)], columns=['kps_name',
                                                                                                  'session_uid',
                                                                                                  'store_fk',
                                                                                                  'visit_date',
                                                                                                  'score_1',
                                                                                                  'kpi_set_fk'])
        self.tool_box.write_to_db_result(attributes_for_table1, 'level1')
        self.tool_box.commit_results_data()
        calc_finish_time = datetime.datetime.utcnow()
        Log.info('Calculation time took {}'.format(calc_finish_time-calc_start_time))
