from datetime import datetime
import pandas as pd
import os
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log
from KPIUtils.GlobalDataProvider.PsDataProvider import PsDataProvider
from Projects.CCUS.Pillars.Utils.Const import Const
from KPIUtils_v2.DB.CommonV2 import Common


__author__ = 'Ortal'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                         'Data', 'Template.xlsx')


def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result

        return wrapper

    return decorator


class PillarsPROGRAMSToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.k_engine = BaseCalculationsScript(data_provider, output)
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_results_queries = []
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.store_info['store_type'].iloc[0]
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.store_areas = self.ps_data_provider.get_store_area_df()
        self.all_brand = self.all_products[['brand_name', 'brand_fk']].set_index(u'brand_fk').to_dict()
        self.scenes_result = self.data_provider.scene_kpi_results
        self.scenes_result = self.scenes_result.loc[self.scenes_result['type'] == Const.SCENE_KPI_NAME]

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        if self.scenes_result.empty:
            return  # if no scenes with result- there are no scenes or programs. should do nothing.
        programs = self.scenes_result['numerator_id'].unique()
        for current_program_id in programs:
            scene_count = self.count_specific_program_scenes(current_program_id)

            kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name=Const.SESSION_KPI_NAME)
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=current_program_id,
                                           result=scene_count, by_scene=False, denominator_id=self.store_id)

    def count_specific_program_scenes(self, program_id):
        current_program_result = self.scenes_result.loc[self.scenes_result['numerator_id'] == program_id]
        program_scene_count = current_program_result['score'].sum()
        return program_scene_count

    def get_brand_name_from_fk(self, brand_fk):
        return self.all_brand[brand_fk]



