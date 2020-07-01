from Projects.PEPSICOUK_SAND.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts, MatchesConsts


class SecondaryPriceMechanicKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SecondaryPriceMechanicKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)
        self.kpi_name = self._config_params['kpi_type']

    def kpi_type(self):
        pass

    def calculate(self):
        self.util.filtered_scif_secondary, self.util.filtered_matches_secondary = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif_secondary,
                                                                                 self.util.filtered_matches_secondary,
                                                                                 self.kpi_name)
        filtered_matches = self.util.filtered_matches_secondary.merge(self.util.all_products, on=[ScifConsts.PRODUCT_FK],
                                                                      how='left')
        filtered_matches = filtered_matches[filtered_matches[ScifConsts.PRODUCT_TYPE] \
                                                          == 'POS'].drop_duplicates(subset=[ScifConsts.PRODUCT_FK])

        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.kpi_name)
        for i, row in filtered_matches.iterrows():
            self.write_to_db_result(fk=kpi_fk, numerator_id=row[ScifConsts.PRODUCT_FK],
                                    denominator_id=row['display_id'], denominator_result=row['display_id'],
                                    context_id=row['store_area_fk'], result=1)
        self.util.reset_secondary_filtered_scif_and_matches_to_exclusion_all_state()
