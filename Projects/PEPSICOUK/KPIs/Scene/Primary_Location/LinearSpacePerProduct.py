from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils_v2.Utils.Consts.DataProvider import MatchesConsts
import numpy as np


class LinearSpacePerProductKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(LinearSpacePerProductKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        self.util.filtered_scif, self.util.filtered_matches = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif,
                                                                                 self.util.filtered_matches,
                                                                                 self.util.LINEAR_SPACE_PER_PRODUCT)
        if not self.util.filtered_matches.empty:
            kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.LINEAR_SPACE_PER_PRODUCT)
            filtered_matches = self.util.filtered_matches.copy()
            filtered_matches = filtered_matches.merge(self.util.all_products, on=MatchesConsts.PRODUCT_FK,
                                                      how='left')

            result_df = filtered_matches.groupby([MatchesConsts.PRODUCT_FK, MatchesConsts.SHELF_NUMBER,
                                                  MatchesConsts.BAY_NUMBER], as_index=False).agg({MatchesConsts.WIDTH_MM_ADVANCE: np.sum})

            shelves_cust_entity = self.util.custom_entities[self.util.custom_entities['entity_type'] == 'shelf_number']
            shelves_cust_entity['name'] = shelves_cust_entity['name'].astype(int)
            bays_cust_entity = self.util.custom_entities[self.util.custom_entities['entity_type'] == 'bay_number']
            bays_cust_entity['name'] = bays_cust_entity['name'].astype(int)

            result_df = result_df.merge(shelves_cust_entity, left_on=MatchesConsts.SHELF_NUMBER, right_on='name',
                                        how='left')

            result_df.rename(columns={'pk': 'shelf_fk'}, inplace=True)
            result_df = result_df.merge(bays_cust_entity, left_on=MatchesConsts.BAY_NUMBER, right_on='name',
                                        how='left')
            result_df.rename(columns={'pk': 'bay_fk'}, inplace=True)

            for i, row in result_df.iterrows():
                self.write_to_db_result(fk=kpi_fk, numerator_result=row[MatchesConsts.SHELF_NUMBER],
                                        result=row[MatchesConsts.WIDTH_MM_ADVANCE],
                                        numerator_id=row[MatchesConsts.PRODUCT_FK], denominator_id=row['bay_fk'],
                                        denominator_result=row[MatchesConsts.BAY_NUMBER], context_id=row['shelf_fk'],
                                        by_scene=True)
                self.util.add_kpi_result_to_kpi_results_df(
                    [kpi_fk, row[MatchesConsts.PRODUCT_FK], row['bay_fk'], row[MatchesConsts.WIDTH_MM_ADVANCE], None,
                     row['shelf_fk']])

        self.util.reset_filtered_scif_and_matches_to_exclusion_all_state()