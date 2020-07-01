from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts


class LinearBrandVsBrandIndexKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(LinearBrandVsBrandIndexKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        self.util.filtered_scif, self.util.filtered_matches = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif,
                                                                                 self.util.filtered_matches,
                                                                                 self._config_params['kpi_type'])

        index_targets = self.util.get_relevant_sos_vs_target_kpi_targets(brand_vs_brand=True)
        index_targets['numerator_id'] = index_targets.apply(self.util.retrieve_relevant_item_pks, axis=1,
                                                            args=('numerator_type', 'numerator_value'))
        index_targets['denominator_id'] = index_targets.apply(self.util.retrieve_relevant_item_pks, axis=1,
                                                              args=('denominator_type', 'denominator_value'))
        index_targets['identifier_parent'] = index_targets['KPI Parent'].apply(lambda x:
                                                                           self.util.common.get_dictionary(
                                                                               kpi_fk=int(float(x))))
        index_targets = index_targets[index_targets['type'] == self._config_params['kpi_type']]
        location_type_fk = self.util.all_templates[self.util.all_templates[ScifConsts.LOCATION_TYPE] == 'Primary Shelf'] \
            [ScifConsts.LOCATION_TYPE_FK].values[0]
        for i, row in index_targets.iterrows():
            general_filters = {row['additional_filter_type_1']: row['additional_filter_value_1']}
            numerator_sos_filters = {row['numerator_type']: row['numerator_value']}
            num_num_linear, num_denom_linear = self.util.calculate_sos(numerator_sos_filters, **general_filters)
            numerator_sos = num_num_linear/num_denom_linear if num_denom_linear else 0

            denominator_sos_filters = {row['denominator_type']: row['denominator_value']}
            denom_num_linear, denom_denom_linear = self.util.calculate_sos(denominator_sos_filters, **general_filters)
            denominator_sos = denom_num_linear/denom_denom_linear if denom_denom_linear else 0

            if denominator_sos == 0:
                index = 0 if numerator_sos == 0 else 1
            else:
                index = numerator_sos / denominator_sos

            self.write_to_db_result(fk=row.kpi_level_2_fk, numerator_id=row.numerator_id,
                                    numerator_result=num_num_linear, denominator_id=row.denominator_id,
                                    denominator_result=denom_num_linear, result=index, score=index,
                                    context_id=location_type_fk)
            self.util.add_kpi_result_to_kpi_results_df([row.kpi_level_2_fk, row.numerator_id, row.denominator_id, index,
                                                        index, None])

        self.util.reset_filtered_scif_and_matches_to_exclusion_all_state()
