# coding=utf-8

import pandas as pd
import json

from Trax.Algo.Calculations.Core.KPI.UnifiedKpiSingleton import UnifiedKPISingleton
from Projects.PNGJP_SAND2.Utils.KPIToolBox import PNGJP_SAND2ToolBox
from Projects.PNGJP_SAND2.Utils.KpiQualitative import PNGJP_SAND2KpiQualitative_ToolBox
from Projects.PNGJP_SAND2.Utils.KPIToolBox import log_runtime
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Projects.PNGJP_SAND2.Utils.Fetcher import PNGJP_SAND2Queries


class PNGJP_SAND2Util(UnifiedKPISingleton):

    def __init__(self, output, data_provider):
        super(PNGJP_SAND2Util, self).__init__(data_provider)
        self.output = output
        self.data_provider = data_provider
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = PNGJP_SAND2ToolBox(self.data_provider, self.output)
        self.KpiQualitative_tool_box = PNGJP_SAND2KpiQualitative_ToolBox(self.data_provider, self.output)
        self.main_function()

        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.match_display_in_scene = self.tool_box.match_display_in_scene
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.common = Common(self.data_provider)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.external_targets = self.get_all_kpi_external_targets()
        self.all_targets_unpacked = self.unpack_all_external_targets()

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        self.tool_box.main_calculation()
        self.tool_box.hadle_update_custom_scif()
        self.tool_box.commit_results_data()
        self.KpiQualitative_tool_box.main_calculation()
        self.KpiQualitative_tool_box.commit_results_data()

    def get_all_kpi_external_targets(self):
        query = PNGJP_SAND2Queries.get_kpi_external_targets(self.visit_date)
        external_targets = pd.read_sql_query(query, self.rds_conn.db)
        return external_targets

    def unpack_all_external_targets(self):
        targets_df = self.external_targets.drop_duplicates(subset=['operation_type', 'kpi_level_2_fk', 'key_json',
                                                                   'data_json'])
        output_targets = pd.DataFrame(columns=targets_df.columns.values.tolist())
        if not targets_df.empty:
            keys_df = self.unpack_external_targets_json_fields_to_df(targets_df, field_name='key_json')
            data_df = self.unpack_external_targets_json_fields_to_df(targets_df, field_name='data_json')
            targets_df = targets_df.merge(keys_df, on='pk', how='left')
            targets_df = targets_df.merge(data_df, on='pk', how='left')
            kpi_data = self.kpi_static_data[['pk', 'type']]
            kpi_data.rename(columns={'pk': 'kpi_level_2_fk'}, inplace=True)
            output_targets = targets_df.merge(kpi_data, on='kpi_level_2_fk', how='left')
        if output_targets.empty:
            Log.warning('KPI External Targets Results are empty')
        return output_targets

    @staticmethod
    def unpack_external_targets_json_fields_to_df(input_df, field_name):
        data_list = []
        for i, row in input_df.iterrows():
            data_item = json.loads(row[field_name]) if row[field_name] else {}
            data_item.update({'pk': row.pk})
            data_list.append(data_item)
        output_df = pd.DataFrame(data_list)
        return output_df

    def filter_scif_for_scene_kpis(self, params):
        scif = self.scif.copy()
        return scif

    def get_target_by_kpi_type(self, kpi_type):
        ext_target = self.all_targets_unpacked[self.all_targets_unpacked['type'] == kpi_type]
        return ext_target