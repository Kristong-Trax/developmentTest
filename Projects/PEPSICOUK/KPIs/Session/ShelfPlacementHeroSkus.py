from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Utils.Logging.Logger import Log
import numpy as np


class ShelfPlacementHeroSkusKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(ShelfPlacementHeroSkusKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        # if not self.util.lvl3_ass_result.empty:
        if not self.dependencies_data[self.dependencies_data['kpi_type'] == self.util.HERO_SKU_AVAILABILITY_SKU].empty:
            external_targets = self.util.commontools.all_targets_unpacked
            shelf_placmnt_targets = external_targets[external_targets['operation_type'] == self.util.SHELF_PLACEMENT]
            kpi_fks = shelf_placmnt_targets['kpi_level_2_fk'].unique().tolist()
            scene_placement_results = self.util.scene_kpi_results[self.util.scene_kpi_results['kpi_level_2_fk'].isin(kpi_fks)]
            if not scene_placement_results.empty:
                hero_results = self.get_hero_results_df(scene_placement_results)
                if not hero_results.empty:
                    kpis_df = self.util.kpi_static_data[['pk', 'type']]
                    kpis_df.rename(columns={'pk': 'kpi_level_2_fk'}, inplace=True)
                    hero_results = hero_results.merge(kpis_df, on='kpi_level_2_fk', how='left')
                    # hero_results['parent_type'] = hero_results['KPI Parent'].apply(self.get_kpi_type_by_pk)
                    hero_results['parent_type'] = hero_results['KPI Parent']
                    hero_results = hero_results[hero_results['type'] == self._config_params['level']]
                    if not hero_results.empty:
                        hero_results['type'] = hero_results['type'].apply(lambda x: '{} {}'.format(self.util.HERO_PREFIX, x))
                        hero_results['parent_type'] = hero_results['parent_type'].apply(lambda x: '{} {}'.format(self.util.HERO_PREFIX, x))
                        hero_results['kpi_level_2_fk'] = hero_results['type'].apply(self.util.common.get_kpi_fk_by_kpi_type)
                        hero_results['KPI Parent'] = hero_results['parent_type'].apply(self.util.common.get_kpi_fk_by_kpi_type)
                        hero_results['identifier_parent'] = hero_results.apply(self.construct_hero_identifier_dict, axis=1)

                        for i, row in hero_results.iterrows():
                            # self.write_to_db_result(fk=row['kpi_level_2_fk'], numerator_id=row['numerator_id'],
                            #                                denominator_id=row['numerator_id'], denominator_result=row['denominator_result'],
                            #                                numerator_result=row['numerator_result'], result=row['ratio'],
                            #                                score=row['ratio'], identifier_parent=row['identifier_parent'],
                            #                                should_enter=True)
                            self.write_to_db_result(fk=row['kpi_level_2_fk'], numerator_id=row['numerator_id'],
                                                    denominator_id=row['numerator_id'],
                                                    denominator_result=row['denominator_result'],
                                                    numerator_result=row['numerator_result'], result=row['ratio'],
                                                    score=row['ratio'])
                            self.util.add_kpi_result_to_kpi_results_df([row.kpi_level_2_fk, row.numerator_id, row['numerator_id'], row['ratio'],
                                                                   row['ratio']])
                        # writes to hierarchy
                        # hero_parent_results = hero_results.groupby(['numerator_id', 'KPI Parent'], as_index=False).agg({'ratio': np.sum})
                        # hero_parent_results['identifier_parent'] = hero_parent_results.apply(self.construct_hero_identifier_dict, axis=1)
                        #
                        # top_sku_parent = self.util.common.get_kpi_fk_by_kpi_type(self.util.HERO_PLACEMENT)
                        # top_parent_identifier_par = self.util.common.get_dictionary(kpi_fk=top_sku_parent)
                        # for i, row in hero_parent_results.iterrows():
                        #     self.write_to_db_result(fk=row['KPI Parent'], numerator_id=row['numerator_id'], result=row['ratio'],
                        #                                    score=row['ratio'], identifier_result=row['identifier_parent'],
                        #                                    identifier_parent=top_parent_identifier_par, denominator_id=self.util.store_id,
                        #                                    should_enter=True)
                        #     self.util.add_kpi_result_to_kpi_results_df([row['KPI Parent'], row.numerator_id, self.util.store_id, row['ratio'],
                        #                                             row['ratio']])
                        # self.write_to_db_result(fk=top_sku_parent, numerator_id=self.util.own_manuf_fk, denominator_id=self.util.store_id,
                        #                                result=len(hero_parent_results), score=len(hero_parent_results),
                        #                                identifier_result=top_parent_identifier_par, should_enter=True)
                        # self.util.add_kpi_result_to_kpi_results_df([top_sku_parent, self.util.own_manuf_fk, self.util.store_id, len(hero_parent_results),
                        #                                        len(hero_parent_results)])

    def get_hero_results_df(self, scene_placement_results):
        kpi_results = scene_placement_results.groupby(['kpi_level_2_fk', 'numerator_id'], as_index=False).agg(
            {'numerator_result': np.sum})
        products_df = scene_placement_results.groupby(['numerator_id'], as_index=False).agg(
            {'numerator_result': np.sum})
        products_df.rename(columns={'numerator_result': 'denominator_result'}, inplace=True)
        kpi_results = kpi_results.merge(products_df, on='numerator_id', how='left')
        hero_skus = self.util.lvl3_ass_result[self.util.lvl3_ass_result['in_store'] == 1]['product_fk'].values.tolist()
        hero_skus = self.util.get_available_hero_sku_list(self.dependencies_data)
        hero_results = kpi_results[kpi_results['numerator_id'].isin(hero_skus)]
        kpi_parent = self.util.commontools.all_targets_unpacked.drop_duplicates(subset=['kpi_level_2_fk', 'KPI Parent'])[['kpi_level_2_fk', 'KPI Parent']]
        hero_results = hero_results.merge(kpi_parent, on='kpi_level_2_fk')
        hero_results['ratio'] = hero_results['numerator_result'] / hero_results['denominator_result'] * 100
        return hero_results

    def get_kpi_type_by_pk(self, kpi_fk):
        try:
            kpi_fk = int(float(kpi_fk))
            return self.util.kpi_static_data[self.util.kpi_static_data['pk'] == kpi_fk]['type'].values[0]
        except IndexError:
            Log.info("Kpi name: {} is not equal to any kpi name in static table".format(kpi_fk))
            return None

    # @staticmethod
    # def get_sku_ratio(row):
    #     ratio = row['count'] / row['total_facings']
    #     return ratio

    @staticmethod
    def construct_hero_identifier_dict(row):
        id_dict = {'kpi_fk': int(float(row['KPI Parent'])), 'sku': row['numerator_id']}
        return id_dict