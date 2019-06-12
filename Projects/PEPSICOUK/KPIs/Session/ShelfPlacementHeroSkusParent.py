from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
import numpy as np


class ShelfPlacementHeroSkusParentKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(ShelfPlacementHeroSkusParentKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        child_kpi_results = self.dependencies_data
        if not child_kpi_results.empty:
            child_kpi_results['KPI Parent'] = self.util.common.get_kpi_fk_by_kpi_type(self.util.HERO_SKU_PLACEMENT_BY_SHELF_NUMBERS)
            kpi_results = child_kpi_results.groupby(['numerator_id', 'KPI Parent'], as_index=False).agg(
                {'result': np.sum})
            for i, row in kpi_results.iterrows():
                self.write_to_db_result(fk=row['KPI Parent'], numerator_id=row['numerator_id'], result=row['result'], score=row['result'],
                                        denominator_id=self.util.store_id)
                self.util.add_kpi_result_to_kpi_results_df([row['KPI Parent'], row.numerator_id, self.util.store_id,
                                                            row['result'], row['result']])

    @staticmethod
    def construct_hero_identifier_dict(row):
        id_dict = {'kpi_fk': int(float(row['KPI Parent'])), 'sku': row['numerator_id']}
        return id_dict