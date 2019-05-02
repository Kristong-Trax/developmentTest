from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript


class HeroSkuPromoPriceKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None):
        super(HeroSkuPromoPriceKpi, self).__init__(data_provider, config_params=config_params)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        if not self.util.lvl3_ass_result.empty:
            hero_sku_list = self.util.lvl3_ass_result[self.util.lvl3_ass_result['in_store'] == 1]['product_fk'].values.tolist()
            promo_price_kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.HERO_SKU_PROMO_PRICE)
            for sku in hero_sku_list:
                self.calculate_hero_sku_promo_price(sku, promo_price_kpi_fk)

    def calculate_hero_sku_promo_price(self, sku, kpi_fk):
        price = 0
        prices_df = self.util.filtered_matches[(~(self.util.filtered_matches['promotion_price'].isnull())) &
                                          (self.util.filtered_matches['product_fk'] == sku)]
        if not prices_df.empty:
            price = 1
        result = self.util.commontools.get_yes_no_result(price)
        self.write_to_db_result(fk=kpi_fk, numerator_id=sku, result=result)
        self.util.add_kpi_result_to_kpi_results_df([kpi_fk, sku, None, price, None])
