from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import numpy as np


class SosVsTargetSegmentKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SosVsTargetSegmentKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        sos_targets = self.util.sos_vs_target_targets.copy()
        # sos_targets = sos_targets[sos_targets['type'] == self._config_params['kpi_type']]
        self.util.filtered_scif, self.util.filtered_matches = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif,
                                                                                 self.util.filtered_matches,
                                                                                 self.util.PEPSICO_SEGMENT_SPACE_TO_SALES_INDEX)
        self.calculate_pepsico_segment_space_sos_vs_target(sos_targets)
        self.util.reset_filtered_scif_and_matches_to_exclusion_all_state()

    def calculate_pepsico_segment_space_sos_vs_target(self, sos_targets):
        sos_targets = sos_targets[sos_targets['type'] == self.util.PEPSICO_SEGMENT_SPACE_TO_SALES_INDEX]
        session_sub_category_list = self.util.filtered_scif['sub_category_fk'].unique().tolist()
        session_sub_category_list = filter(lambda v: v == v, session_sub_category_list)
        session_sub_category_list = filter(lambda v: v is not None, session_sub_category_list)
        targets_sub_category_list = sos_targets['denominator_value'].values.tolist()
        additional_sub_categories = list(set(session_sub_category_list) - set(targets_sub_category_list))
        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.PEPSICO_SEGMENT_SPACE_TO_SALES_INDEX)
        kpi_parent = self.util.common.get_kpi_fk_by_kpi_type(self.util.PEPSICO_SEGMENT_SOS_VS_TARGET)

        additional_rows = []
        for sub_category in additional_sub_categories:
            values_to_append = {'numerator_id': self.util.own_manuf_fk, 'numerator_type': 'manufacturer_fk',
                                'numerator_value': self.util.own_manuf_fk, 'denominator_type': 'sub_category_fk',
                                'denominator_value': sub_category, 'Target': None, 'denominator_id': sub_category,
                                'kpi_level_2_fk': kpi_fk, 'KPI Parent': kpi_parent,
                                'identifier_parent': self.util.common.get_dictionary(kpi_fk=int(float(kpi_parent)))}
            additional_rows.append(values_to_append)
        df_to_append = pd.DataFrame.from_records(additional_rows)
        sos_targets = sos_targets.append(df_to_append)

        sos_targets = sos_targets[sos_targets['denominator_value'].isin(session_sub_category_list)]
        self.calculate_and_write_to_db_sos_vs_target_kpi_results(sos_targets)

    def calculate_and_write_to_db_sos_vs_target_kpi_results(self, sos_targets):
        for i, row in sos_targets.iterrows():
            general_filters = {row['denominator_type']: row['denominator_value']}
            sos_filters = {row['numerator_type']: row['numerator_value']}
            numerator_linear, denominator_linear = self.util.calculate_sos(sos_filters, **general_filters)

            result = numerator_linear / denominator_linear if denominator_linear != 0 else 0
            score = result / row['Target'] if row['Target'] else 0
            if row['Target']:
                self.write_to_db_result(fk=row.kpi_level_2_fk, numerator_id=row.numerator_id,
                                        numerator_result=numerator_linear, denominator_id=row.denominator_id,
                                        denominator_result=denominator_linear, result=result * 100, score=score,
                                        target=row['Target'] * 100)
            else:
                self.write_to_db_result(fk=row.kpi_level_2_fk, numerator_id=row.numerator_id,
                                        numerator_result=numerator_linear, denominator_id=row.denominator_id,
                                        denominator_result=denominator_linear, result=result * 100)
            self.util.add_kpi_result_to_kpi_results_df(
                [row.kpi_level_2_fk, row.numerator_id, row.denominator_id, result * 100,
                 score])
