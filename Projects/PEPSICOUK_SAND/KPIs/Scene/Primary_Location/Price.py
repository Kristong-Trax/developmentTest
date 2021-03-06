from Projects.PEPSICOUK_SAND.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import ScifConsts, MatchesConsts


class PriceKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(PriceKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        self.util.filtered_scif, self.util.filtered_matches = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif,
                                                                                 self.util.filtered_matches,
                                                                                 self.util.PRICE_SCENE)

        sku_list = self.util.filtered_scif[ScifConsts.PRODUCT_FK]
        price_kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.PRICE_SCENE)
        for sku in sku_list:
            self.calculate_hero_sku_price(sku, price_kpi_fk)
        self.util.reset_filtered_scif_and_matches_to_exclusion_all_state()

    def calculate_hero_sku_price(self, sku, kpi_fk):
        price = -1
        prices_df = self.util.filtered_matches[((~(self.util.filtered_matches[MatchesConsts.PRICE].isnull())) |
                                          (~(self.util.filtered_matches[MatchesConsts.PROMOTION_PRICE].isnull()))) &
                                          (self.util.filtered_matches[ScifConsts.PRODUCT_FK] == sku)]
        if not prices_df.empty:
            prices_list = prices_df[MatchesConsts.PRICE].values.tolist()
            prices_list.extend(prices_df[MatchesConsts.PROMOTION_PRICE].values.tolist())
            prices_list = filter(lambda v: v == v, prices_list)
            prices_list = filter(lambda v: v is not None, prices_list)
            if prices_list:
                price = max(prices_list)
        self.write_to_db_result(fk=kpi_fk, numerator_id=sku, denominator_id=sku,result=price)
        self.util.add_kpi_result_to_kpi_results_df([kpi_fk, sku, None, price, None, None])
