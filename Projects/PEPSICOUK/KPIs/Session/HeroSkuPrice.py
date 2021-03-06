from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript


class HeroSkuPriceKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(HeroSkuPriceKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        if not self.dependencies_data[self.dependencies_data['kpi_type'] == self.util.HERO_SKU_AVAILABILITY_SKU].empty:
            hero_sku_list = self.util.get_available_hero_sku_list(self.dependencies_data)
            price_kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.HERO_SKU_PRICE)
            for sku in hero_sku_list:
                self.calculate_hero_sku_price(sku, price_kpi_fk)

    def calculate_hero_sku_price(self, sku, kpi_fk):
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
