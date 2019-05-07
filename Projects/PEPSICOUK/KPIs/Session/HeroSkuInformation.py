from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript


class HeroSkuInformationKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(HeroSkuInformationKpi, self).__init__(data_provider, config_params=config_params ,**kwargs)
        self.util = PepsicoUtil(None,data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        if not self.util.lvl3_ass_result.empty:
            hero_sku_list = self.util.lvl3_ass_result[self.util.lvl3_ass_result['in_store'] == 1]['product_fk'].values.tolist()
            # hero_sku_list = self.filtered_scif['product_fk'].unique().tolist() # comment after
            # stacking_kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.HERO_SKU_STACKING)
            price_kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.HERO_SKU_PRICE)
            promo_price_kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.HERO_SKU_PROMO_PRICE)
            for sku in hero_sku_list:
                # self.calculate_hero_sku_stacking_width(sku, stacking_kpi_fk)
                # self.calculate_hero_sku_stacking_layer_more_than_one(sku, stacking_kpi_fk)
                self.calculate_hero_sku_price(sku, price_kpi_fk)
                self.calculate_hero_sku_promo_price(sku, promo_price_kpi_fk)

    def calculate_hero_sku_stacking_width(self, sku, kpi_fk):
        score = 0
        relevant_matches = self.util.filtered_matches[self.util.filtered_matches['product_fk'] == sku]
        # get only those locations where sku appears
        entities = relevant_matches.drop_duplicates(subset=['scene_fk', 'bay_number', 'shelf_number'])
        for i, entity in entities.iterrows():
            result_df = self.util.filtered_matches[(self.util.filtered_matches['product_fk'] == sku) &
                                              (self.util.filtered_matches['scene_fk'] == entity['scene_fk']) &
                                              (self.util.filtered_matches['bay_number'] == entity['bay_number']) &
                                              (self.util.filtered_matches['shelf_number'] == entity['shelf_number'])]
            stacking_layers = result_df['stacking_layer'].unique().tolist()
            if len(stacking_layers) > 1:
                result_df['x_mm_left'] = result_df['x_mm'] - result_df['width_mm_advance'] / 2
                result_df['x_mm_right'] = result_df['x_mm'] + result_df['width_mm_advance'] / 2
                for i, row in result_df.iterrows():
                    upper_stacking_df = result_df[result_df['stacking_layer'] > row['stacking_layer']]
                    upper_stacking_df = upper_stacking_df[~((upper_stacking_df['x_mm_right'] <= row['x_mm_left']) |
                                                            (upper_stacking_df['x_mm_left'] >= row['x_mm_right']))]
                    if not upper_stacking_df.empty:
                        score = 1
                        break

        self.write_to_db_result(fk=kpi_fk, numerator_id=sku, score=score, result=score)
        self.util.add_kpi_result_to_kpi_results_df([kpi_fk, sku, None, score, score])

    def calculate_hero_sku_promo_price(self, sku, kpi_fk):
        price = 0
        prices_df = self.util.filtered_matches[(~(self.util.filtered_matches['promotion_price'].isnull())) &
                                          (self.util.filtered_matches['product_fk'] == sku)]
        if not prices_df.empty:
            price = 1
        result = self.util.commontools.get_yes_no_result(price)
        self.write_to_db_result(fk=kpi_fk, numerator_id=sku, result=result)
        self.util.add_kpi_result_to_kpi_results_df([kpi_fk, sku, None, price, None])

    def calculate_hero_sku_price(self, sku, kpi_fk):
        # what should we write if there is no price at all?
        price = -1
        prices_df = self.util.filtered_matches[((~(self.util.filtered_matches['price'].isnull())) |
                                          (~(self.util.filtered_matches['promotion_price'].isnull())))&
                                          (self.util.filtered_matches['product_fk'] == sku)]
        if not prices_df.empty:
            prices_list = prices_df['price'].values.tolist()
            prices_list.extend(prices_df['promotion_price'].values.tolist())
            prices_list = filter(lambda v: v == v, prices_list)
            prices_list = filter(lambda v: v is not None, prices_list)
            if prices_list:
                price = max(prices_list)
        self.write_to_db_result(fk=kpi_fk, numerator_id=sku, result=price)
        self.util.add_kpi_result_to_kpi_results_df([kpi_fk, sku, None, price, None])

    def calculate_hero_sku_stacking_layer_more_than_one(self, sku, kpi_fk):
        score = 0
        product_df = self.util.filtered_matches[(self.util.filtered_matches['product_fk'] == sku) &
                                           (self.util.filtered_matches['stacking_layer'] > 1)]
        if not product_df.empty:
            score = 1
        self.write_to_db_result(fk=kpi_fk, numerator_id=sku, score=score, result=score)
        self.util.add_kpi_result_to_kpi_results_df([kpi_fk, sku, None, score, score])