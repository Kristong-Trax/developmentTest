from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils_v2.Calculations.AdjacencyCalculations import Adjancency

#TODO: MAKE SURE THAT THE DATA PROVIDER HAS ONLY RESPECTIVE SCENE DATA FOR ONE SCENE (NOT SESSION!)


class BlocksAdjacencyKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(BlocksAdjacencyKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)
        self.adjacency = Adjancency(self.data_provider, custom_scif=self.util.filtered_scif,
                                    custom_matches=self.util.filtered_matches)

    def kpi_type(self):
        pass

    def calculate(self):
        if not self.util.filtered_matches.empty:
            self.calculate_adjacency()

    def calculate_adjacency(self):
        block_pairs = self.get_group_pairs()
        if block_pairs:
            external_targets = self.util.all_targets_unpacked[self.util.all_targets_unpacked['type']
                                                                     == self.util.PRODUCT_BLOCKING]
            additional_block_params = {'check_vertical_horizontal': True, 'minimum_facing_for_block': 3,
                                       'minimum_block_ratio': 0.9, 'include_stacking': True,
                                       'allowed_products_filters': {'product_type': ['Empty']}}
            kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.PRODUCT_BLOCKING_ADJACENCY)

            for pair in block_pairs:
                pair = list(pair)
                group_1_fk = self.util.custom_entities[self.util.custom_entities['name'] == pair[0]]['pk'].values[0]
                group_2_fk = self.util.custom_entities[self.util.custom_entities['name'] == pair[1]]['pk'].values[0]

                group_1_targets = external_targets[external_targets['Group Name'] == pair[0]].iloc[0]
                group_1_filters = self.util.get_block_and_adjacency_filters(group_1_targets)

                group_2_targets = external_targets[external_targets['Group Name'] == pair[1]].iloc[0]
                group_2_filters = self.util.get_block_and_adjacency_filters(group_2_targets)

                result_df = self.adjacency.network_x_adjacency_calculation({'anchor_products': group_1_filters,
                                                                            'tested_products': group_2_filters},
                                                                           location=None,
                                                                           additional=additional_block_params)
                score = 0
                result = self.util.commontools.get_yes_no_result(0)
                if not result_df.empty:
                    score = 1 if result_df['is_adj'].values[0] else 0
                    result = self.util.commontools.get_yes_no_result(score)
                self.write_to_db_result(fk=kpi_fk, numerator_id=group_1_fk, denominator_id=group_2_fk,
                                                    result=result, score=score, by_scene=True)

    def get_group_pairs(self):
        valid_groups = self.util.block_results[self.util.block_results['Score'] == 1]['Group Name'].values.tolist()
        result_set = set()
        for i, group in enumerate(valid_groups):
            [result_set.add(frozenset([group, valid_groups[j]])) for j in range(i+1, len(valid_groups))]
        return list(result_set)

    def construct_block_results(self):
        for i, result in self.dependencies_data.iterrows():
            block = self.util.custom_entities[self.util.custom_entities['pk'] == result['numerator_id']]['name'].values[0]
            score = 1 if result['result'] == 4 else 0
            self.util.block_results.append({'Group Name': block, 'Score': score})
