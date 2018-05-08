# coding=utf-8

import pandas as pd
from Trax.Utils.Logging.Logger import Log
import datetime

from Projects.PNGCN_PROD_OLD.KPIToolBox import PNGToolBox

__author__ = 'ortalk'


class EmptySpaceKpiGenerator:
    def __init__(self, data_provider, output, project_name):
        # All relevant session data with KPI static info will trigger the KPI calculation
        self.KPIToolBox = PNGToolBox(data_provider, output, project_name)
        self.place_count = 0
        self.empty_count = 0
        self.categories = [4, 5, 6, 7, 8, 9, 10, 11, 13]

    def main_function(self):
        calc_start_time = datetime.datetime.utcnow()
        Log.info('Calculation Started at {}'.format(calc_start_time))
        if self.KPIToolBox.check_validation_of_session():
            for cat in self.categories:
                if self.KPIToolBox.check_validation_of_category(cat):
                    self.KPIToolBox.check_empty_spaces(cat)
                    kps_name = self.KPIToolBox.all_products[self.KPIToolBox.all_products['category_fk'] ==
                                                            cat]['category'].values[0]
                    attributes_for_table1 = pd.DataFrame([(kps_name, self.KPIToolBox.session_uid,
                                                           self.KPIToolBox.store_id, self.KPIToolBox.visit_date.isoformat()
                                                           , 100, None)], columns=['kps_name', 'session_uid', 'store_fk',
                                                                                   'visit_date', 'score_1',
                                                                                   'kpi_set_fk'])

                    kps = self.KPIToolBox.write_to_db_result(attributes_for_table1, 'level1', kps_name)
                    self.KPIToolBox.kpi_results_queries.append(kps)
                    kpis = self.KPIToolBox.scores.columns.tolist()
                    for kpi in kpis:
                        attributes_for_table2 = pd.DataFrame([(self.KPIToolBox.session_uid, self.KPIToolBox.store_id,
                                                               self.KPIToolBox.visit_date.isoformat(), None, kpi,
                                                               self.KPIToolBox.scores[kpi][0])],
                                                             columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk',
                                                                      'kpk_name', 'score'])
                        kpk = self.KPIToolBox.write_to_db_result(attributes_for_table2, 'level2', kps_name)
                        self.KPIToolBox.kpi_results_queries.append(kpk)
                        attributes_for_table3 = pd.DataFrame([(kpi, self.KPIToolBox.session_uid, kps_name,
                                                               self.KPIToolBox.store_id,
                                                               self.KPIToolBox.visit_date.isoformat(),
                                                             datetime.datetime.utcnow().isoformat(),
                                                               self.KPIToolBox.scores[kpi][0],
                                                             attributes_for_table2['kpi_fk'][0], None)],
                                                             columns=['display_text', 'session_uid', 'kps_name',
                                                                      'store_fk', 'visit_date',
                                                                      'calculation_time', 'score', 'kpi_fk',
                                                                      'atomic_kpi_fk'])

                        kpi = self.KPIToolBox.write_to_db_result(attributes_for_table3, 'level3', kps_name)
                        self.KPIToolBox.kpi_results_queries.append(kpi)
            self.KPIToolBox.commit_results_data()
            calc_finish_time = datetime.datetime.utcnow()
            Log.info('Calculation time took {}'.format(calc_finish_time - calc_start_time))
        else:
            Log.info('Session has no relevant scenes')




