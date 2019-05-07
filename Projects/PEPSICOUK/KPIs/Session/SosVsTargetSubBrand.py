from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import numpy as np


class SosVsTargetSubBrandKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SosVsTargetSubBrandKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        sos_targets = self.util.sos_vs_target_targets.copy()
        # sos_targets = sos_targets[sos_targets['type'] == self._config_params['kpi_type']]
        self.calculate_sub_brand_sos_vs_target(sos_targets)

    def calculate_sub_brand_sos_vs_target(self, sos_targets):
        sos_targets = sos_targets[sos_targets['type'] == self.util.SUB_BRAND_SPACE_TO_SALES_INDEX]
        session_sub_brands = self.util.filtered_scif['sub_brand'].unique().tolist()
        session_sub_brands = filter(lambda v: v == v, session_sub_brands)
        session_sub_brands = filter(lambda v: v is not None, session_sub_brands)
        targets_sub_brands = sos_targets['numerator_value'].values.tolist()
        additional_sub_brands = list(set(session_sub_brands) - set(targets_sub_brands))
        category_fk = self.util.all_products[self.util.all_products['category'] == 'CSN']['category_fk'].values[0]
        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.SUB_BRAND_SPACE_TO_SALES_INDEX)
        kpi_parent = self.util.common.get_kpi_fk_by_kpi_type(self.util.SUB_BRAND_SPACE_SOS_VS_TARGET)

        additional_rows = []
        for sub_brand in additional_sub_brands:
            sub_brand_df = self.util.custom_entities[self.util.custom_entities['name'] == sub_brand]
            sub_brand_fk = sub_brand_df['pk'].values[0] if len(sub_brand_df) > 0 else None
            if sub_brand_df is None:
                Log.warning('Sub_brand: {} is not in custom_entity table. Please add'.format(sub_brand))
            else:
                values_to_append = {'numerator_id': sub_brand_fk, 'numerator_type': 'sub_brand',
                                    'numerator_value': sub_brand,
                                    'denominator_type': 'category_fk',
                                    'denominator_value': category_fk, 'Target': None, 'denominator_id': category_fk,
                                    'kpi_level_2_fk': kpi_fk, 'KPI Parent': kpi_parent,
                                    'identifier_parent': self.util.common.get_dictionary(kpi_fk=int(float(kpi_parent)))}
                additional_rows.append(values_to_append)

        df_to_append = pd.DataFrame.from_records(additional_rows)
        sos_targets = sos_targets.append(df_to_append)

        sos_targets = sos_targets[sos_targets['numerator_value'].isin(session_sub_brands)]
        self.calculate_and_write_to_db_sos_vs_target_kpi_results(sos_targets)

    def calculate_and_write_to_db_sos_vs_target_kpi_results(self, sos_targets):
        for i, row in sos_targets.iterrows():
            general_filters = {row['denominator_type']: row['denominator_value']}
            sos_filters = {row['numerator_type']: row['numerator_value']}
            numerator_linear, denominator_linear = self.util.calculate_sos(sos_filters, **general_filters)

            result = numerator_linear / denominator_linear if denominator_linear != 0 else 0
            score = result / row['Target'] if row['Target'] else 0
            if row['Target']:
                # self.write_to_db_result(fk=row.kpi_level_2_fk, numerator_id=row.numerator_id,
                #                         numerator_result=numerator_linear, denominator_id=row.denominator_id,
                #                         denominator_result=denominator_linear, result=result * 100, score=score,
                #                         target=row['Target'] * 100, identifier_parent=row.identifier_parent,
                #                         should_enter=True)
                self.write_to_db_result(fk=row.kpi_level_2_fk, numerator_id=row.numerator_id,
                                        numerator_result=numerator_linear, denominator_id=row.denominator_id,
                                        denominator_result=denominator_linear, result=result * 100, score=score,
                                        target=row['Target'] * 100)
            else:
                # self.write_to_db_result(fk=row.kpi_level_2_fk, numerator_id=row.numerator_id,
                #                                numerator_result=numerator_linear, denominator_id=row.denominator_id,
                #                                denominator_result=denominator_linear, result=result * 100,
                #                                identifier_parent=row.identifier_parent, should_enter=True)
                self.write_to_db_result(fk=row.kpi_level_2_fk, numerator_id=row.numerator_id,
                                        numerator_result=numerator_linear, denominator_id=row.denominator_id,
                                        denominator_result=denominator_linear, result=result * 100)
            self.util.add_kpi_result_to_kpi_results_df(
                [row.kpi_level_2_fk, row.numerator_id, row.denominator_id, result * 100,
                 score])

        # sos_targets['count'] = 1
        # parent_kpis_df = sos_targets.groupby(['KPI Parent'], as_index=False).agg({'count': np.sum})
        # parent_kpis_df['identifier_parent'] = parent_kpis_df['KPI Parent'].apply(lambda x:
        #                                                                          self.util.common.get_dictionary(
        #                                                                              kpi_fk=int(float(x))))
        # for i, row in parent_kpis_df.iterrows():
        #     self.write_to_db_result(fk=row['KPI Parent'], score=row['count'], should_enter=True,
        #                                    numerator_id=self.util.own_manuf_fk, denominator_id=self.util.store_id,
        #                                    identifier_result=row['identifier_parent'])
        #     self.util.add_kpi_result_to_kpi_results_df([row['KPI Parent'], self.util.own_manuf_fk, self.util.store_id, None,
        #                                            row['count']])