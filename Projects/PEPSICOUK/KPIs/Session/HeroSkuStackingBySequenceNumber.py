from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Utils.Logging.Logger import Log


class HeroSkuStackingBySequenceNumberKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider):
        super(HeroSkuStackingBySequenceNumberKpi, self).__init__(data_provider)
        self.util = PepsicoUtil()

    def kpi_type(self):
        pass

    def calculate(self):
        if not self.util.lvl3_ass_result.empty:
            hero_list = self.util.lvl3_ass_result[self.util.lvl3_ass_result['in_store'] == 1]['product_fk'].unique().tolist()
            # hero_list = self.filtered_scif['product_fk'].unique().tolist()
            if hero_list:
                relevant_matches = self.util.filtered_matches[self.util.filtered_matches['product_fk'].isin(hero_list)]
                relevant_matches = relevant_matches.reset_index(drop=True)
                relevant_matches['facing_sequence_number'] = relevant_matches['facing_sequence_number'].astype(str)
                relevant_matches['all_sequences'] = relevant_matches.groupby(['scene_fk', 'bay_number', 'shelf_number', 'product_fk']) \
                                                        ['facing_sequence_number'].apply(lambda x: (x + ',').cumsum().str.strip())
                grouped_matches = relevant_matches.drop_duplicates(subset=['scene_fk', 'bay_number', 'shelf_number', 'product_fk'],
                                                                   keep='last')
                grouped_matches['is_stack'] = grouped_matches.apply(self.util.get_stack_data, axis=1)
                stacking_kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.HERO_SKU_STACKING)
                for sku in hero_list:
                    stack_info = grouped_matches[grouped_matches['product_fk'] == sku]['is_stack'].values.tolist()
                    score = 0
                    if any(stack_info):
                        score = 1
                    result = self.util.commontools.get_yes_no_result(score)
                    self.write_to_db_result(fk=stacking_kpi_fk, numerator_id=sku, score=score, result=result)

                    self.util.add_kpi_result_to_kpi_results_df([stacking_kpi_fk, sku, None, result, score])