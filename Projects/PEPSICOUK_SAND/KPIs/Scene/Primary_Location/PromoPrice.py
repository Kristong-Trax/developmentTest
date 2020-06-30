from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts, MatchesConsts


class PromoPriceKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(PromoPriceKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        self.util.filtered_scif, self.util.filtered_matches = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif,
                                                                                 self.util.filtered_matches,
                                                                                 self.util.PROMO_PRICE_SCENE)
        sku_list = self.util.filtered_scif[ScifConsts.PRODUCT_FK]
        promo_price_kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.PROMO_PRICE_SCENE)
        for sku in sku_list:
            self.calculate_hero_sku_promo_price(sku, promo_price_kpi_fk)
        self.util.reset_filtered_scif_and_matches_to_exclusion_all_state()

    def calculate_hero_sku_promo_price(self, sku, kpi_fk):
        price = 0
        prices_df = self.util.filtered_matches[(~(self.util.filtered_matches[MatchesConsts.PROMOTION_PRICE].isnull())) &
                                                (self.util.filtered_matches[ScifConsts.PRODUCT_FK] == sku)]
        if not prices_df.empty:
            price = 1
        result = self.util.commontools.get_yes_no_result(price)
        self.write_to_db_result(fk=kpi_fk, numerator_id=sku, denominator_id=sku, result=result)
        self.util.add_kpi_result_to_kpi_results_df([kpi_fk, sku, None, price, None, None])
