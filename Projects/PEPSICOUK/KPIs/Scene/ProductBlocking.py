from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils_v2.Calculations.AdjacencyCalculations import Block

import pandas as pd

#TODO: MAKE SURE THAT THE DATA PROVIDER HAS ONLY RESPECTIVE SCENE DATA FOR ONE SCENE (NOT SESSION!)


class ProductBlockingKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None):
        super(ProductBlockingKpi, self).__init__(data_provider, config_params=config_params)
        self.util = PepsicoUtil(None, data_provider)
        self.block = Block(self.data_provider, custom_scif=self.util.filtered_scif, custom_matches=self.util.filtered_matches)

    def kpi_type(self):
        pass

    def calculate(self):
        if not self.util.filtered_matches.empty:
            self.calculate_product_blocking()

    def calculate_product_blocking(self):
        external_targets = self.util.all_targets_unpacked[self.util.all_targets_unpacked['type'] == self.util.PRODUCT_BLOCKING]
        additional_block_params = {'check_vertical_horizontal': True, 'minimum_facing_for_block': 3,
                                   'include_stacking': True,
                                   'allowed_products_filters': {'product_type': ['Empty']}}
        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.PRODUCT_BLOCKING)

        for i, row in external_targets.iterrows():
            group_fk = self.util.custom_entities[self.util.custom_entities['name'] == row['Group Name']]['pk'].values[0]
            filters = self.util.get_block_and_adjacency_filters(row)
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
            self.util.common.write_to_db_result(fk=kpi_fk, numerator_id=group_fk, denominator_id=self.util.store_id,
                                                numerator_result=max_ratio * 100,
                                                score=score, result=result, target=target, by_scene=True)

            # connection with adjacency kpi
            # TODO: setup the dependency
            self.util.block_results = self.util.block_results.append(pd.DataFrame([{'Group Name': row['Group Name'],
                                                                     'Score': result_df['is_block'].values[0] if not result_df.empty else False}]))