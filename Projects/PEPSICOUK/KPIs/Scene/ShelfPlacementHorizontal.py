from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript

import numpy as np


class ShelfPlacementHorizontalKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None):
        super(ShelfPlacementHorizontalKpi, self).__init__(data_provider, config_params=config_params)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        if not self.util.filtered_matches.empty:
            self.calculate_shelf_placement_horizontal()

    def calculate_shelf_placement_horizontal(self):
        external_targets = self.util.all_targets_unpacked
        shelf_placmnt_targets = external_targets[external_targets['operation_type'] == self.util.SHELF_PLACEMENT]
        if not shelf_placmnt_targets.empty:
            bay_max_shelves = self.get_scene_bay_max_shelves(shelf_placmnt_targets)
            bay_all_shelves = bay_max_shelves.drop_duplicates(subset=['bay_number', 'shelves_all_placements'],
                                                              keep='first')
            relevant_matches = self.filter_out_irrelevant_matches(bay_all_shelves)
            if not relevant_matches.empty:
                for i, row in bay_max_shelves.iterrows():
                    shelf_list = map(lambda x: float(x), row['Shelves From Bottom To Include (data)'].split(','))
                    relevant_matches.loc[(relevant_matches['bay_number'] == row['bay_number']) &
                                         (relevant_matches['shelf_number_from_bottom'].isin(shelf_list)), 'position'] = row['type']
                kpi_results = self.get_kpi_results_df(relevant_matches, bay_max_shelves)
                kpi_results = kpi_results[kpi_results['type'] == self._config_params['kpi_type']]
                for i, result in kpi_results.iterrows():
                    self.util.common.write_to_db_result(fk=result['kpi_level_2_fk'], numerator_id=result['product_fk'],
                                                        denominator_id=result['product_fk'],
                                                        denominator_result=result['total_facings'],
                                                        numerator_result=result['count'], result=result['ratio'],
                                                        score=result['ratio'], by_scene=True)
                    self.util.add_kpi_result_to_kpi_results_df([result['kpi_level_2_fk'], result['product_fk'],
                                                           result['product_fk'], result['ratio'], result['ratio']])

    def get_scene_bay_max_shelves(self, shelf_placement_targets):
        scene_bay_max_shelves = self.util.match_product_in_scene.groupby(['bay_number'],
                                                                    as_index=False).agg({'shelf_number_from_bottom':np.max})
        scene_bay_max_shelves.rename(columns={'shelf_number_from_bottom': 'shelves_in_bay'}, inplace=True)
        min_shelf_in_template = shelf_placement_targets[self.util.NUMBER_OF_SHELVES_TEMPL_COLUMN].min()
        scene_bay_max_shelves = scene_bay_max_shelves[scene_bay_max_shelves['shelves_in_bay'] >= min_shelf_in_template]

        max_shelf_in_template = shelf_placement_targets[self.util.NUMBER_OF_SHELVES_TEMPL_COLUMN].max()
        shelf_placement_targets = self.complete_missing_target_shelves(scene_bay_max_shelves, max_shelf_in_template,
                                                                     shelf_placement_targets)

        scene_bay_max_shelves = scene_bay_max_shelves.merge(shelf_placement_targets, left_on='shelves_in_bay',
                                                            right_on=self.util.NUMBER_OF_SHELVES_TEMPL_COLUMN)
        scene_bay_max_shelves.rename(columns=self.util.SHELF_PLC_TARGET_COL_RENAME, inplace=True)
        scene_bay_max_shelves = scene_bay_max_shelves[self.util.SHELF_PLC_TARGETS_COLUMNS + ['bay_number', 'shelves_in_bay']]
        scene_bay_max_shelves = scene_bay_max_shelves.drop_duplicates()

        bay_all_relevant_shelves = self.get_bay_relevant_shelves_df(scene_bay_max_shelves)
        scene_bay_max_shelves = scene_bay_max_shelves.merge(bay_all_relevant_shelves, on='bay_number', how='left')

        scene_bay_max_shelves = scene_bay_max_shelves[~(scene_bay_max_shelves['bay_number'] == -1)]

        relevant_bays = self.util.filtered_matches['bay_number'].unique().tolist()
        final_df = scene_bay_max_shelves[scene_bay_max_shelves['bay_number'].isin(relevant_bays)]
        return final_df

    def filter_out_irrelevant_matches(self, target_kpis_df):
        relevant_matches = self.util.scene_bay_shelf_product[~(self.util.scene_bay_shelf_product['bay_number'] == -1)]
        relevant_matches = relevant_matches[relevant_matches['bay_number'].isin(target_kpis_df['bay_number'].unique().tolist())]
        for i, row in target_kpis_df.iterrows():
            all_shelves = map(lambda x: float(x), row['shelves_all_placements'].split(','))
            rows_to_remove = relevant_matches[(relevant_matches['bay_number'] == row['bay_number']) &
                                              (~(relevant_matches['shelf_number_from_bottom'].isin(all_shelves)))].index
            relevant_matches.drop(rows_to_remove, inplace=True)
        relevant_matches['position'] = ''
        return relevant_matches

    def complete_missing_target_shelves(self, scene_bay_df, max_shelf_template, shelf_placement_targets):
        shelf_placement_targets = shelf_placement_targets[self.util.SHELF_PLC_TARGETS_COLUMNS]
        shelf_placement_targets = shelf_placement_targets.reset_index(drop=True)
        for i, row in scene_bay_df.iterrows():
            if row['shelves_in_bay'] > max_shelf_template:
                if row['shelves_in_bay'] not in shelf_placement_targets[self.util.NUMBER_OF_SHELVES_TEMPL_COLUMN].values.tolist():
                    rows_to_add = shelf_placement_targets[shelf_placement_targets[self.util.NUMBER_OF_SHELVES_TEMPL_COLUMN] \
                                                                == max_shelf_template]
                    rows_to_add[self.util.NUMBER_OF_SHELVES_TEMPL_COLUMN] = row['shelves_in_bay']
                    top_shelf_range = ','.join(map(lambda x: str(x),
                                                        range(int(float(max_shelf_template)), int(float(row['shelves_in_bay']+1)))))
                    rows_to_add.loc[rows_to_add['type'] == self.util.PLACEMENT_BY_SHELF_NUMBERS_TOP,
                                    self.util.RELEVANT_SHELVES_TEMPL_COLUMN] = top_shelf_range
                    shelf_placement_targets = shelf_placement_targets.append(rows_to_add, ignore_index=True)
        return shelf_placement_targets

    def get_bay_relevant_shelves_df(self, scene_bay_max_shelves):
        scene_bay_max_shelves[self.util.RELEVANT_SHELVES_TEMPL_COLUMN] = scene_bay_max_shelves[
            self.util.RELEVANT_SHELVES_TEMPL_COLUMN].astype(str)
        bay_all_relevant_shelves = scene_bay_max_shelves[
            ['bay_number', self.util.RELEVANT_SHELVES_TEMPL_COLUMN]].drop_duplicates()
        bay_all_relevant_shelves['shelves_all_placements'] = bay_all_relevant_shelves.groupby('bay_number') \
            [self.util.RELEVANT_SHELVES_TEMPL_COLUMN].apply(lambda x: (x + ',').cumsum().str.strip())
        if 'bay_number' in bay_all_relevant_shelves.index.names:
            bay_all_relevant_shelves.index.names = ['custom_ind']
        bay_all_relevant_shelves = bay_all_relevant_shelves.drop_duplicates(subset=['bay_number'], keep='last') \
            [['bay_number', 'shelves_all_placements']]
        bay_all_relevant_shelves['shelves_all_placements'] = bay_all_relevant_shelves['shelves_all_placements']. \
            apply(lambda x: x[0:-1])
        return bay_all_relevant_shelves

    def get_kpi_results_df(self, relevant_matches, kpi_targets_df):
        total_products_facings = relevant_matches.groupby(['product_fk'], as_index=False).agg({'count': np.sum})
        total_products_facings.rename(columns={'count': 'total_facings'}, inplace=True)
        result_df = relevant_matches.groupby(['product_fk', 'position'], as_index=False).agg({'count':np.sum})
        result_df = result_df.merge(total_products_facings, on='product_fk', how='left')

        kpis_df = kpi_targets_df.drop_duplicates(subset=['kpi_level_2_fk', 'type', 'KPI Parent'])
        result_df = result_df.merge(kpis_df, left_on='position', right_on='type', how='left')
        result_df['ratio'] = result_df.apply(self.get_sku_ratio, axis=1)
        return result_df

    @staticmethod
    def get_sku_ratio(row):
        ratio = float(row['count']) / row['total_facings'] * 100
        return ratio
