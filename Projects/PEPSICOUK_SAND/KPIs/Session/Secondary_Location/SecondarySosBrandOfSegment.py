from Projects.PEPSICOUK_SAND.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import ScifConsts
import pandas as pd
import numpy as np


class SecondarySosBrandOfSegmentKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SecondarySosBrandOfSegmentKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)
        self.kpi_name = self._config_params['kpi_type']

    def kpi_type(self):
        pass

    def calculate(self):
        self.util.filtered_scif_secondary, self.util.filtered_matches_secondary = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif_secondary,
                                                                                 self.util.filtered_matches_secondary,
                                                                                 self.kpi_name)
        self.calculate_brand_out_of_sub_category_sos()
        self.util.reset_secondary_filtered_scif_and_matches_to_exclusion_all_state()

    def calculate_brand_out_of_sub_category_sos(self):
        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.kpi_name)
        filtered_scif = self.util.filtered_scif_secondary
        sub_cat_df = filtered_scif.groupby([ScifConsts.SUB_CATEGORY_FK],
                                           as_index=False).agg({'updated_gross_length': np.sum})
        sub_cat_df.rename(columns={'updated_gross_length': 'sub_cat_len'}, inplace=True)
        brand_sub_cat_df = filtered_scif.groupby([ScifConsts.BRAND_FK, ScifConsts.SUB_CATEGORY_FK],
                                                 as_index=False).agg({'updated_gross_length': np.sum})
        brand_sub_cat_df = brand_sub_cat_df.merge(sub_cat_df, on=ScifConsts.SUB_CATEGORY_FK, how='left')
        brand_sub_cat_df['sos'] = brand_sub_cat_df['updated_gross_length'] / brand_sub_cat_df['sub_cat_len']
        for i, row in brand_sub_cat_df.iterrows():
            self.write_to_db_result(fk=kpi_fk, numerator_id=row[ScifConsts.BRAND_FK],
                                    numerator_result=row['updated_gross_length'],
                                    denominator_id=row[ScifConsts.SUB_CATEGORY_FK],
                                    denominator_result=row['sub_cat_len'], result=row['sos'] * 100)