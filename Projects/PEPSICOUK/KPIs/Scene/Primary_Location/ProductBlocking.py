from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
# from KPIUtils_v2.Calculations.BlockCalculations import Block
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
import pandas as pd


class ProductBlockingKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(ProductBlockingKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)
        self.block = Block(self.data_provider, custom_scif=self.util.filtered_scif,
                           custom_matches=self.util.filtered_matches)

    def kpi_type(self):
        pass

    def calculate(self):
        if not self.util.filtered_matches.empty:
            self.util.filtered_scif, self.util.filtered_matches = \
                self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif,
                                                                                     self.util.filtered_matches,
                                                                                     self.util.PRODUCT_BLOCKING)
            self.block = Block(self.data_provider, custom_scif=self.util.filtered_scif,
                               custom_matches=self.util.filtered_matches)
            if not self.util.filtered_matches.empty:
                self.calculate_product_blocking()
            self.util.reset_filtered_scif_and_matches_to_exclusion_all_state()

    def calculate_product_blocking(self):
        external_targets = self.util.all_targets_unpacked[self.util.all_targets_unpacked['type'] == self.util.PRODUCT_BLOCKING]
        additional_block_params = {'check_vertical_horizontal': True, 'minimum_facing_for_block': 3,
                                   'include_stacking': True,
                                   'allowed_products_filters': {'product_type': ['Empty']}}
        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.PRODUCT_BLOCKING)

        for i, row in external_targets.iterrows():
            group_fk = self.util.custom_entities[self.util.custom_entities['name'] == row['Group Name']]['pk'].values[0]
            # filters = self.util.get_block_and_adjacency_filters(row)
            filters = self.util.get_block_filters(row)
            target = row['Target']
            additional_block_params.update({'minimum_block_ratio': float(target)/100})

            result_df = self.block.network_x_block_together(filters, additional=additional_block_params)
            score = max_ratio = 0
            result = self.util.commontools.get_yes_no_result(0)
            if not result_df.empty:
                max_ratio = result_df['facing_percentage'].max()
                result_df = result_df[result_df['is_block']==True]
                if not result_df.empty:
                    max_ratio = result_df['facing_percentage'].max()
                    result_df = result_df[result_df['facing_percentage'] == max_ratio]
                    result = self.util.commontools.get_yes_no_result(1)
                    orientation = result_df['orientation'].values[0]
                    score = self.util.commontools.get_kpi_result_value_pk_by_value(orientation.upper())
            self.write_to_db_result(fk=kpi_fk, numerator_id=group_fk, denominator_id=self.util.store_id,
                                    numerator_result=max_ratio * 100,
                                    score=score, result=result, target=target, by_scene=True)
