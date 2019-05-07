from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import numpy as np


class SosVsTargetHeroSkuKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SosVsTargetHeroSkuKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        sos_targets = self.util.sos_vs_target_targets.copy()
        # sos_targets = sos_targets[sos_targets['type'] == self._config_params['kpi_type']]
        self.util.filtered_scif, self.util.filtered_matches = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif,
                                                                                 self.util.filtered_matches,
                                                                                 self.util.HERO_SKU_SPACE_TO_SALES_INDEX)
        self.calculate_hero_sku_sos_vs_target(sos_targets)
        self.util.reset_filtered_scif_and_matches_to_exclusion_all_state()

    def calculate_hero_sku_sos_vs_target(self, sos_targets):
        kpi_filtered_products = self.util.filtered_scif['product_fk'].unique().tolist()
        hero_list = self.util.lvl3_ass_result[self.util.lvl3_ass_result['in_store'] == 1]['product_fk'].unique().tolist()
        hero_list = filter(lambda x: x in kpi_filtered_products, hero_list)

        sos_targets = sos_targets[sos_targets['type'] == self.util.HERO_SKU_SPACE_TO_SALES_INDEX]
        sos_targets_hero_list = sos_targets['numerator_value'].values.tolist()
        additional_skus = list(set(hero_list) - set(sos_targets_hero_list))
        category_fk = self.util.all_products[self.util.all_products['category'] == 'CSN']['category_fk'].values[0]
        hero_kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.HERO_SKU_SPACE_TO_SALES_INDEX)
        kpi_hero_parent = self.util.common.get_kpi_fk_by_kpi_type(self.util.HERO_SKU_SOS_VS_TARGET)
        additional_rows = []
        for sku in additional_skus:
            values_to_append = {'numerator_id': sku, 'numerator_type': 'product_fk', 'numerator_value': sku, 'denominator_type': 'category_fk',
                                'denominator_value': category_fk, 'Target': None, 'denominator_id': category_fk,
                                'kpi_level_2_fk': hero_kpi_fk, 'KPI Parent': kpi_hero_parent,
                                'identifier_parent': self.util.common.get_dictionary(kpi_fk=int(float(kpi_hero_parent)))}
            additional_rows.append(values_to_append)
        df_to_append = pd.DataFrame.from_records(additional_rows)
        sos_targets = sos_targets.append(df_to_append)

        sos_targets = sos_targets[sos_targets['numerator_value'].isin(hero_list)]
        self.calculate_and_write_to_db_sos_vs_target_kpi_results(sos_targets)

    def calculate_and_write_to_db_sos_vs_target_kpi_results(self, sos_targets):
        for i, row in sos_targets.iterrows():
            general_filters = {row['denominator_type']: row['denominator_value']}
            sos_filters = {row['numerator_type']: row['numerator_value']}
            numerator_linear, denominator_linear = self.util.calculate_sos(sos_filters, **general_filters)

            result = numerator_linear / denominator_linear if denominator_linear != 0 else 0
            score = result / row['Target'] if row['Target'] else 0
            if row['Target']:
                # self.write_to_db_result(fk=row.kpi_level_2_fk, numerator_id=row.numerator_id,
                #                         numerator_result=numerator_linear, denominator_id=row.denominator_id,
                #                         denominator_result=denominator_linear, result=result * 100, score=score,
                #                         target=row['Target'] * 100, identifier_parent=row.identifier_parent,
                #                         should_enter=True)
                self.write_to_db_result(fk=row.kpi_level_2_fk, numerator_id=row.numerator_id,
                                        numerator_result=numerator_linear, denominator_id=row.denominator_id,
                                        denominator_result=denominator_linear, result=result * 100, score=score,
                                        target=row['Target'] * 100)
            else:
                # self.write_to_db_result(fk=row.kpi_level_2_fk, numerator_id=row.numerator_id,
                #                                numerator_result=numerator_linear, denominator_id=row.denominator_id,
                #                                denominator_result=denominator_linear, result=result * 100,
                #                                identifier_parent=row.identifier_parent, should_enter=True)
                self.write_to_db_result(fk=row.kpi_level_2_fk, numerator_id=row.numerator_id,
                                        numerator_result=numerator_linear, denominator_id=row.denominator_id,
                                        denominator_result=denominator_linear, result=result * 100)
            self.util.add_kpi_result_to_kpi_results_df(
                [row.kpi_level_2_fk, row.numerator_id, row.denominator_id, result * 100,
                 score])

        # sos_targets['count'] = 1
        # parent_kpis_df = sos_targets.groupby(['KPI Parent'], as_index=False).agg({'count': np.sum})
        # parent_kpis_df['identifier_parent'] = parent_kpis_df['KPI Parent'].apply(lambda x:
        #                                                                          self.util.common.get_dictionary(
        #                                                                              kpi_fk=int(float(x))))
        # for i, row in parent_kpis_df.iterrows():
        #     self.write_to_db_result(fk=row['KPI Parent'], score=row['count'], should_enter=True,
        #                                    numerator_id=self.util.own_manuf_fk, denominator_id=self.util.store_id,
        #                                    identifier_result=row['identifier_parent'])
        #     self.util.add_kpi_result_to_kpi_results_df([row['KPI Parent'], self.util.own_manuf_fk, self.util.store_id, None,
        #                                            row['count']])