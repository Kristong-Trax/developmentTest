from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts
import pandas as pd
import numpy as np


class SosVsTargetSubBrandKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SosVsTargetSubBrandKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        # sos_targets = self.util.sos_vs_target_targets.copy()
        # sos_targets = sos_targets[sos_targets['type'] == self._config_params['kpi_type']]
        self.util.filtered_scif, self.util.filtered_matches = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif,
                                                                                 self.util.filtered_matches,
                                                                                 self.util.SUB_BRAND_SPACE_TO_SALES_INDEX)
        # self.calculate_sub_brand_sos_vs_target(sos_targets)
        self.calculate_sub_brand_out_of_category_sos()
        self.util.reset_filtered_scif_and_matches_to_exclusion_all_state()

    def calculate_sub_brand_out_of_category_sos(self):
        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.SUB_BRAND_SPACE_TO_SALES_INDEX)
        filtered_scif = self.util.filtered_scif
        category_df = filtered_scif.groupby([ScifConsts.CATEGORY_FK],
                                            as_index=False).agg({'updated_gross_length': np.sum})
        category_df.rename(columns={'updated_gross_length': 'cat_len'}, inplace=True)
        brand_cat_df = filtered_scif.groupby(["sub_brand", ScifConsts.CATEGORY_FK],
                                             as_index=False).agg({'updated_gross_length': np.sum})
        brand_cat_df.merge(category_df, on=ScifConsts.CATEGORY_FK, how='left')
        brand_cat_df['sos'] = brand_cat_df['updated_gross_length'] / brand_cat_df['cat_len']
        for i, row in brand_cat_df.iterrows():
            self.write_to_db_result(fk=kpi_fk, numerator_id=row["sub_brand"],
                                    numerator_result=row['updated_gross_length'],
                                    denominator_id=row[ScifConsts.CATEGORY_FK],
                                    denominator_result=row['cat_len'], result=row['sos'] * 100)
            self.util.add_kpi_result_to_kpi_results_df(
                [kpi_fk, row["sub_brand"], row[ScifConsts.CATEGORY_FK], row['sos'] * 100, None])

    # def calculate_sub_brand_sos_vs_target(self, sos_targets):
    #     sos_targets = sos_targets[sos_targets['type'] == self.util.SUB_BRAND_SPACE_TO_SALES_INDEX]
    #     session_sub_brands = self.util.filtered_scif['sub_brand'].unique().tolist()
    #     session_sub_brands = filter(lambda v: v == v, session_sub_brands)
    #     session_sub_brands = filter(lambda v: v is not None, session_sub_brands)
    #     targets_sub_brands = sos_targets['numerator_value'].values.tolist()
    #     additional_sub_brands = list(set(session_sub_brands) - set(targets_sub_brands))
    #     category_fk = self.util.all_products[self.util.all_products['category'] == 'CSN']['category_fk'].values[0]
    #     kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.SUB_BRAND_SPACE_TO_SALES_INDEX)
    #     kpi_parent = self.util.common.get_kpi_fk_by_kpi_type(self.util.SUB_BRAND_SPACE_SOS_VS_TARGET)
    #
    #     additional_rows = []
    #     for sub_brand in additional_sub_brands:
    #         sub_brand_df = self.util.custom_entities[self.util.custom_entities['name'] == sub_brand]
    #         sub_brand_fk = sub_brand_df['pk'].values[0] if len(sub_brand_df) > 0 else None
    #         if sub_brand_fk is None:
    #             Log.warning('Sub_brand: {} is not in custom_entity table. Please add'.format(sub_brand))
    #         else:
    #             values_to_append = {'numerator_id': sub_brand_fk, 'numerator_type': 'sub_brand',
    #                                 'numerator_value': sub_brand,
    #                                 'denominator_type': 'category_fk',
    #                                 'denominator_value': category_fk, 'Target': None, 'denominator_id': category_fk,
    #                                 'kpi_level_2_fk': kpi_fk, 'KPI Parent': kpi_parent,
    #                                 'identifier_parent': self.util.common.get_dictionary(kpi_fk=int(float(kpi_parent)))}
    #             additional_rows.append(values_to_append)
    #
    #     df_to_append = pd.DataFrame.from_records(additional_rows)
    #     sos_targets = sos_targets.append(df_to_append)
    #
    #     sos_targets = sos_targets[sos_targets['numerator_value'].isin(session_sub_brands)]
    #     self.calculate_and_write_to_db_sos_vs_target_kpi_results(sos_targets)
    #
    # def calculate_and_write_to_db_sos_vs_target_kpi_results(self, sos_targets):
    #     for i, row in sos_targets.iterrows():
    #         general_filters = {row['denominator_type']: row['denominator_value']}
    #         sos_filters = {row['numerator_type']: row['numerator_value']}
    #         numerator_linear, denominator_linear = self.util.calculate_sos(sos_filters, **general_filters)
    #
    #         result = numerator_linear / denominator_linear if denominator_linear != 0 else 0
    #         score = result / row['Target'] if row['Target'] else 0
    #         if row['Target']:
    #             self.write_to_db_result(fk=row.kpi_level_2_fk, numerator_id=row.numerator_id,
    #                                     numerator_result=numerator_linear, denominator_id=row.denominator_id,
    #                                     denominator_result=denominator_linear, result=result * 100, score=score,
    #                                     target=row['Target'] * 100)
    #         else:
    #             self.write_to_db_result(fk=row.kpi_level_2_fk, numerator_id=row.numerator_id,
    #                                     numerator_result=numerator_linear, denominator_id=row.denominator_id,
    #                                     denominator_result=denominator_linear, result=result * 100)
    #         self.util.add_kpi_result_to_kpi_results_df(
    #             [row.kpi_level_2_fk, row.numerator_id, row.denominator_id, result * 100,
    #              score])
