import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup
import datetime

from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Projects.CCRUFIFA2018.Utils.ToolBox import CCRUFIFAKPIToolBox
from Projects.CCRUFIFA2018.Utils.JSON import JsonGenerator

__author__ = 'shani'

# FIFAKPIS = 'FIFA'

class CCRUFIFA2018Generator:
    def __init__(self, data_provider, output):  #All relevant session data with KPI static info will trigger the KPI calculation
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.data_provider = data_provider
        self.project_name = data_provider.project_name
        self.output = output
        self.session_uid = self.data_provider.session_uid
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.session_info = SessionInfo(data_provider)
        self.store_id = self.data_provider[Data.STORE_FK]
        self.tool_box = CCRUFIFAKPIToolBox(self.data_provider, self.output)
        self.store_type = self.data_provider.store_type
        self.results = {}

    def main_function(self):
        jg = JsonGenerator('ccru')
        kpi_set_name = self.tool_box.set_name
        try:
            jg.create_json('FIFA KPIs.xlsx', kpi_set_name, sheetname=kpi_set_name)
        except:
            Log.error('Session store "{}" is not set to calculation'.format(
                self.tool_box.session_info.store_type))

        calc_start_time = datetime.datetime.utcnow()
        Log.info('Calculation Started at {}'.format(calc_start_time))
        score = 0
        score += self.tool_box.check_weighted_average(jg.project_kpi_dict.get('kpi_data')[0])
        score += self.tool_box.calculate_share_of_cch_collers(jg.project_kpi_dict.get('kpi_data')[0])
        score += self.tool_box.check_survey_answer(jg.project_kpi_dict.get('kpi_data')[0])
        score += self.tool_box.weighted_cooler_standard(jg.project_kpi_dict.get('kpi_data')[0])
        attributes_for_table1 = pd.DataFrame([(kpi_set_name, self.session_uid,
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
