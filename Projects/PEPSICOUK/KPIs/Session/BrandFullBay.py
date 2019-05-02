from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Utils.Logging.Logger import Log
import numpy as np


class BrandFullBayKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None):
        super(BrandFullBayKpi, self).__init__(data_provider, config_params=config_params)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        brand_full_bay_kpi_fks = [self.util.common.get_kpi_fk_by_kpi_type(kpi) for kpi in self.util.BRAND_FULL_BAY_KPIS]
        external_kpi_targets = self.util.commontools.all_targets_unpacked[
            self.util.commontools.all_targets_unpacked['kpi_level_2_fk'].isin(brand_full_bay_kpi_fks)]
        external_kpi_targets = external_kpi_targets.reset_index(drop=True)
        if not external_kpi_targets.empty:
            external_kpi_targets['group_fk'] = external_kpi_targets['Group Name'].apply(lambda x:
                                                                                        self.util.custom_entities[
                                                                                            self.util.custom_entities[
                                                                                                'name'] == x][
                                                                                            'pk'].values[0])
            filtered_matches = self.util.filtered_matches[~(self.util.filtered_matches['bay_number'] == -1)]
            if not filtered_matches.empty:
                scene_bay_product = filtered_matches.groupby(['scene_fk', 'bay_number', 'product_fk'],
                                                             as_index=False).agg({'count': np.sum})
                scene_bay_product = scene_bay_product.merge(self.util.all_products, on='product_fk', how='left')
                scene_bay = scene_bay_product.groupby(['scene_fk', 'bay_number'], as_index=False).agg({'count': np.sum})
                scene_bay.rename(columns={'count': 'total_facings'}, inplace=True)
                for i, row in external_kpi_targets.iterrows():
                    filters = self.util.get_full_bay_and_positional_filters(row)
                    brand_relevant_df = scene_bay_product[
                        self.util.toolbox.get_filter_condition(scene_bay_product, **filters)]
                    result_df = brand_relevant_df.groupby(['scene_fk', 'bay_number'], as_index=False).agg(
                        {'count': np.sum})
                    result_df = result_df.merge(scene_bay, on=['scene_fk', 'bay_number'], how='left')
                    result_df['ratio'] = result_df['count'] / result_df['total_facings']
                    target_ratio = int(float(self._config_params['ratio'])*100)
                    if target_ratio == 100:
                        result = len(result_df[result_df['ratio'] >= 1])
                        self.write_to_db_result(fk=row['kpi_level_2_fk'], numerator_id=row['group_fk'],
                                                score=result)
                        self.util.add_kpi_result_to_kpi_results_df(
                            [row['kpi_level_2_fk'], row['group_fk'], None, None, result])
                    else:
                        result = len(result_df[result_df['ratio'] >= (target_ratio/100.00)])
                        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type('Brand Full Bay_{}'.format(target_ratio))
                        self.write_to_db_result(fk=kpi_fk, numerator_id=row['group_fk'], score=result)
                        self.util.add_kpi_result_to_kpi_results_df([kpi_fk, row['group_fk'], None, None, result])