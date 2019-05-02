from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Utils.Logging.Logger import Log


class LinearBrandVsBrandIndexKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider):
        super(LinearBrandVsBrandIndexKpi, self).__init__(data_provider)
        self.util = PepsicoUtil()

    def kpi_type(self):
        pass

    def calculate(self):
        # index_targets = self.get_relevant_sos_index_kpi_targets_data()
        index_targets = self.util.get_relevant_sos_vs_target_kpi_targets(brand_vs_brand=True)
        index_targets['numerator_id'] = index_targets.apply(self.util.retrieve_relevant_item_pks, axis=1,
                                                            args=('numerator_type', 'numerator_value'))
        index_targets['denominator_id'] = index_targets.apply(self.util.retrieve_relevant_item_pks, axis=1,
                                                              args=('denominator_type', 'denominator_value'))
        index_targets['identifier_parent'] = index_targets['KPI Parent'].apply(lambda x:
                                                                           self.util.common.get_dictionary(
                                                                               kpi_fk=int(float(x))))
        for i, row in index_targets.iterrows():
            general_filters = {row['additional_filter_type_1']: row['additional_filter_value_1']}
            numerator_sos_filters = {row['numerator_type']: row['numerator_value']}
            num_num_linear, num_denom_linear = self.util.calculate_sos(numerator_sos_filters, **general_filters)
            numerator_sos = num_num_linear/num_denom_linear if num_denom_linear else 0

            denominator_sos_filters = {row['denominator_type']: row['denominator_value']}
            denom_num_linear, denom_denom_linear = self.util.calculate_sos(denominator_sos_filters, **general_filters)
            denominator_sos = denom_num_linear/denom_denom_linear if denom_denom_linear else 0

            index = numerator_sos / denominator_sos if denominator_sos else 0
            self.write_to_db_result(fk=row.kpi_level_2_fk, numerator_id=row.numerator_id,
                                           numerator_result=num_num_linear, denominator_id=row.denominator_id,
                                           denominator_result=denom_num_linear, result=index, score=index,
                                           identifier_parent=row.identifier_parent, should_enter=True)
            self.util.add_kpi_result_to_kpi_results_df([row.kpi_level_2_fk, row.numerator_id, row.denominator_id, index,
                                                   index])

        parent_kpis_df = index_targets.drop_duplicates(subset=['KPI Parent'])
        parent_kpis_df.rename(columns={'identifier_parent': 'identifier_result'}, inplace=True)
        for i, row in parent_kpis_df.iterrows():
            self.write_to_db_result(fk=row['KPI Parent'], numerator_id=self.util.own_manuf_fk,
                                           denominator_id=self.util.store_id, score=1,
                                           identifier_result=row.identifier_result, should_enter=True)
            self.util.add_kpi_result_to_kpi_results_df([row['KPI Parent'], self.util.own_manuf_fk, self.util.store_id, None,
                                                   1])
