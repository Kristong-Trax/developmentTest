# coding=utf-8

from Trax.Algo.Calculations.Core.KPI.UnifiedKpiSingleton import UnifiedKPISingleton
from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
from Trax.Utils.Logging.Logger import Log
from Projects.PNGJP_SAND2.Utils.TemplateParser import PNGJPTemplateParser

__author__ = 'prasanna'

KPI_TYPE_COL = 'type'


class PNGJP_SAND2BlockGoldenUtil(UnifiedKPISingleton):

    def __init__(self, output, data_provider):
        super(PNGJP_SAND2BlockGoldenUtil, self).__init__(data_provider)
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)

        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.templates = self.data_provider[Data.TEMPLATES]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.scif = self.data_provider.scene_item_facts
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.store_info.iloc[0].store_fk
        self.store_type = self.data_provider.store_type
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.template_parser = PNGJPTemplateParser(self.data_provider, self.rds_conn)

        self.targets_from_template = self.template_parser.get_targets()
        self.custom_entity_data = self.template_parser.get_custom_entity()
        self.external_targets = self.template_parser.get_external_targets()

        self.match_display_in_scene = self.data_provider.match_display_in_scene
        self.current_scene_fk = self.scene_info.iloc[0].scene_fk
        self.template_name = self.templates.iloc[0].template_name
        self.match_product_data = self.match_product_in_scene.merge(self.products, on='product_fk', how='left')
        self.block = Block(self.data_provider, self.output)

    @staticmethod
    def ensure_as_list(template_fks):
        if isinstance(template_fks, list):
            ext_target_template_fks = template_fks
        else:
            ext_target_template_fks = list([template_fks])
        return ext_target_template_fks

    def check_if_the_kpi_is_available(self, kpi_name):
        status = True
        res_df = self.kpi_static_data[self.kpi_static_data[KPI_TYPE_COL] == kpi_name]
        if res_df.empty:
            status = False
            Log.warning("Error: KPI {} not found in static.kpi_level_2 table.".format(kpi_name))
        return status
