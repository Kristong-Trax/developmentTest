from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import ScifConsts, MatchesConsts


class SecondaryPriceKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SecondaryPriceKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)
        self.kpi_name = self._config_params['kpi_type']

    def kpi_type(self):
        pass

    def calculate(self):
        if self.util.commontools.are_all_bins_tagged:
            self.util.filtered_scif_secondary, self.util.filtered_matches_secondary = \
                self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif_secondary,
                                                                                     self.util.filtered_matches_secondary,
                                                                                     self.kpi_name)

            filtered_matches = self.util.filtered_matches_secondary
            if not filtered_matches.empty:
                product_display = filtered_matches.drop_duplicates(subset=[MatchesConsts.PRODUCT_FK, 'display_id'])
                store_area = self.util.filtered_scif_secondary['store_area_fk'].values[0]
                kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.kpi_name)
                for i, row in product_display.iterrows():
                    # price = -1
                    prices_df = filtered_matches[((~(filtered_matches[MatchesConsts.PRICE].isnull())) |
                                              (~(filtered_matches[MatchesConsts.PROMOTION_PRICE].isnull()))) &
                                              (filtered_matches[ScifConsts.PRODUCT_FK] == row[MatchesConsts.PRODUCT_FK]) &
                                              (filtered_matches['display_id'] == row['display_id'])]
                    prices_list = prices_df[MatchesConsts.PRICE].values.tolist()
                    prices_list.extend(prices_df[MatchesConsts.PROMOTION_PRICE].values.tolist())
                    prices_list = filter(lambda v: v == v, prices_list)
                    prices_list = filter(lambda v: v is not None, prices_list)
                    if prices_list:
                        price = max(prices_list)
                        self.write_to_db_result(fk=kpi_fk, numerator_id=row[MatchesConsts.PRODUCT_FK],
                                                denominator_id=row[MatchesConsts.PRODUCT_FK],
                                                denominator_result=row['display_id'],
                                                context_id=store_area, result=price)
            self.util.reset_secondary_filtered_scif_and_matches_to_exclusion_all_state()
