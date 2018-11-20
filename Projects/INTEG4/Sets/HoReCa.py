import datetime

import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup

from Projects.INTEG4.Utils.JSON import INTEG4JsonGenerator
from Projects.INTEG4.Utils.ToolBox import INTEG4KPIToolBox

from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log

__author__ = 'urid'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
HORECA = 'PoS 2017 - HoReCa'


class INTEG4HoReCaCalculations:
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
        self.tool_box = INTEG4KPIToolBox(self.data_provider, self.output, HORECA)

        self.results = {}

    def main_function(self):
        jg = INTEG4JsonGenerator('integ4')
        jg.create_json('HoReCa.xlsx', HORECA)

        calc_start_time = datetime.datetime.utcnow()
        Log.info('Calculation Started at {}'.format(calc_start_time))
        if not self.tool_box.scif.empty:
            score = 0
            score += self.tool_box.check_availability(jg.project_kpi_dict.get('kpi_data')[0])
            score += self.tool_box.facings_sos(jg.project_kpi_dict.get('kpi_data')[0])
            score += self.tool_box.check_survey_answer(jg.project_kpi_dict.get('kpi_data')[0])
            attributes_for_table1 = pd.DataFrame([(HORECA, self.session_uid,
                                                   self.store_id, self.visit_date.isoformat()
                                                   , format(score, '.2f'), None)], columns=['kps_name',
                                                                                            'session_uid',
                                                                                            'store_fk',
                                                                                            'visit_date',
                                                                                            'score_1',
                                                                                            'kpi_set_fk'])
            self.tool_box.write_to_db_result(attributes_for_table1, 'level1')
        else:
            Log.warning('Scene item facts is empty for this session')
        self.tool_box.commit_results_data()
        calc_finish_time = datetime.datetime.utcnow()
        Log.info('Calculation time took {}'.format(calc_finish_time - calc_start_time))
