from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import numpy as np


class SosVsTargetParentKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SosVsTargetParentKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        # in DB the config parameters should have 'child_kpi' and kpi_type
        # check what I will get as a dependency (all results or only relevant results?)
        child_kpi_results = self.dependencies_data

        if not child_kpi_results.empty:
            child_kpi_results['count'] = 1
            child_kpi_results['KPI Parent'] = 1234
            kpi_results = child_kpi_results.groupby(['KPI Parent'], as_index=False).agg({'count': np.sum})
            kpi_results['identifier_parent'] = kpi_results['KPI Parent'].apply(lambda x:
                                                                                     self.util.common.get_dictionary(
                                                                                         kpi_fk=int(float(x))))
            for i, row in kpi_results.iterrows():
                # self.write_to_db_result(fk=row['KPI Parent'], score=row['count'], should_enter=True,
                #                         numerator_id=self.util.own_manuf_fk, denominator_id=self.util.store_id,
                #                         identifier_result=row['identifier_parent'])
                self.write_to_db_result(fk=row['KPI Parent'], score=row['count'],
                                        numerator_id=self.util.own_manuf_fk, denominator_id=self.util.store_id)
                # self.util.add_kpi_result_to_kpi_results_df([row['KPI Parent'], self.util.own_manuf_fk, self.util.store_id, None,
                #                                             row['count']])