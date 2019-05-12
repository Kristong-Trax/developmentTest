from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript


class NumberOfFacingsKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(NumberOfFacingsKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        if not self.util.filtered_matches.empty:
            kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.NUMBER_OF_FACINGS)
            filtered_scif = self.util.filtered_scif.copy()
            filtered_scif['facings'] = filtered_scif.apply(self.update_facings_for_cardboard_boxes, axis=1)
            for i, row in self.util.filtered_scif.iterrows():
                self.write_to_db_result(fk=kpi_fk, numerator_id=row['product_fk'], result=row['facings'],
                                                    denominator_id=self.util.store_id, by_scene=True)
                self.util.add_kpi_result_to_kpi_results_df(
                    [kpi_fk, row['product_fk'], self.util.store_id, row['facings'], None])

    @staticmethod
    def update_facings_for_cardboard_boxes(row):
        facings = row['facings'] * 3 if row['att1'] == 'display cardboard box' else row['facings']
        return facings