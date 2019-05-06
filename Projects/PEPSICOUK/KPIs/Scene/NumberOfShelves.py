from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript


class NumberOfShelvesKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None):
        super(NumberOfShelvesKpi, self).__init__(data_provider, config_params=config_params)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        if not self.util.filtered_matches.empty:
            kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.NUMBER_OF_SHELVES)
            matches = self.util.match_product_in_scene[~(self.util.match_product_in_scene['bay_number'] == -1)]
            # bay_shelf = matches.drop_duplicates(subset=['bay_number', 'shelf_number'])
            # shelf_num = len(bay_shelf)
            shelf_num = matches['shelf_number'].max()
            self.util.common.write_to_db_result(fk=kpi_fk, numerator_id=self.util.own_manuf_fk, result=shelf_num,
                                                denominator_id=self.util.store_id, by_scene=True)
            self.util.add_kpi_result_to_kpi_results_df(
                [kpi_fk, self.util.own_manuf_fk, self.util.store_id, shelf_num, None])