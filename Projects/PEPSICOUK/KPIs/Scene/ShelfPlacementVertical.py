from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript

import pandas as pd
import numpy as np


class ShelfPlacementVerticalKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(ShelfPlacementVerticalKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        if not self.util.filtered_matches.empty:
            self.calculate_shelf_placement_vertical_mm()

    def calculate_shelf_placement_vertical_mm(self):
        probe_groups_list = self.util.probe_groups['probe_group_id'].unique().tolist()
        resulting_matches = pd.DataFrame()

        for probe_group in probe_groups_list:
            matches = self.util.match_product_in_scene[self.util.match_product_in_scene['probe_group_id'] == probe_group]
            filtered_matches = self.util.filtered_matches[self.util.filtered_matches['probe_group_id'] == probe_group]
            left_edge = self.get_left_edge_mm(matches)
            right_edge = self.get_right_edge_mm(matches)
            shelf_length = float(right_edge - left_edge)
            matches = self.define_product_position_mm(matches, shelf_length, left_edge, right_edge)
            matches_position = matches[['probe_match_fk', 'position']]
            filtered_matches = filtered_matches.merge(matches_position, on='probe_match_fk', how='left')
            if resulting_matches.empty:
                resulting_matches = filtered_matches
            else:
                resulting_matches = resulting_matches.append(filtered_matches)

        result_df = self.get_vertical_placement_kpi_result_df(resulting_matches)
        result_df = result_df[result_df['position'] == self._config_params['kpi_type']]
        for i, row in result_df.iterrows():
            self.util.common.write_to_db_result(fk=row['kpi_fk'], numerator_id=row['product_fk'],
                                                denominator_id=row['product_fk'],
                                                numerator_result=row['count'], denominator_result=row['total_facings'],
                                                result=row['ratio'], score=row['ratio'], by_scene=True)
            self.util.add_kpi_result_to_kpi_results_df([row['kpi_fk'], row['product_fk'], row['product_fk'], row['ratio'],
                                                        row['ratio']])

    @staticmethod
    def get_left_edge_mm(matches):
        matches['left_edge_mm'] = matches['x_mm'] - matches['width_mm_advance'] / 2
        left_edge = matches['left_edge_mm'].min()
        return left_edge

    @staticmethod
    def get_right_edge_mm(matches):
        matches['right_edge_mm'] = matches['x_mm'] + matches['width_mm_advance'] / 2
        right_edge = matches['right_edge_mm'].max()
        return right_edge

    def define_product_position_mm(self, matches, shelf_length, left_edge, right_edge):
        matches['position'] = ''
        matches.loc[(matches['x_mm'] >= left_edge) &
                    (matches['x_mm'] <= (
                            left_edge + shelf_length / 3)), 'position'] = self.util.SHELF_PLACEMENT_VERTICAL_LEFT
        matches.loc[(matches['x_mm'] > (left_edge + shelf_length / 3)) &
                    (matches['x_mm'] <= (left_edge +
                                         shelf_length * 2 / 3)), 'position'] = self.util.SHELF_PLACEMENT_VERTICAL_CENTER
        matches.loc[(matches['x_mm'] > (left_edge + shelf_length * 2 / 3)) &
                    (matches['x_mm'] <= right_edge), 'position'] = self.util.SHELF_PLACEMENT_VERTICAL_RIGHT
        return matches

    def get_vertical_placement_kpi_result_df(self, filtered_matches):
        all_products_df = filtered_matches.groupby(['product_fk'], as_index=False).agg({'count': np.sum})
        all_products_df.rename(columns={'count': 'total_facings'}, inplace=True)
        result_df = filtered_matches.groupby(['product_fk', 'position'], as_index=False).agg({'count': np.sum})
        result_df = result_df.merge(all_products_df, on='product_fk', how='left')
        result_df['ratio'] = result_df['count'] / result_df['total_facings'] * 100
        result_df['kpi_fk'] = result_df['position'].apply(lambda x: self.util.common.get_kpi_fk_by_kpi_type(x))
        return result_df