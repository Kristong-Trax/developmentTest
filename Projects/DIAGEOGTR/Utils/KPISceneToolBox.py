import numpy as np
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from Trax.Utils.Logging.Logger import Log
from collections import defaultdict
import os

__author__ = 'prasanna'

GTR_SKU_FACINGS_BY_SCENE = 'GTR_SKU_FACINGS_BY_SCENE'
KPI_TYPE_COL = 'type'


class DIAGEOGTRSceneToolBox:

    def __init__(self, data_provider, output, common):
        self.output = output
        self.data_provider = data_provider
        self.common = common
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
        self.match_display_in_scene = self.data_provider.match_display_in_scene
        self.current_scene_fk = self.scene_info.iloc[0].scene_fk
        self.template_name = self.templates.iloc[0].template_name
        self.match_product_data = self.match_product_in_scene.merge(self.products, on='product_fk', how='left')
        self.set_up_template = pd.read_excel(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                                          'set_up.xlsx'), sheet_name='Functional KPIs',
                                             keep_default_na=False)

    def parse_config_from_setup(self, kpi_details):
        targets = {}
        relev_setup_template = self.set_up_template[
            self.set_up_template['KPI Type'] == kpi_details.iloc[0][KPI_TYPE_COL]
            ]
        if not relev_setup_template.empty:
            boolean_columns = ['Include Empty', "Include Others", "Include Stacking", "Include Irrelevant"]
            for column in boolean_columns:
                value = relev_setup_template[column].iloc[0]
                if not pd.isnull(value) and value.lower().strip() == "exclude":
                    is_include = False
                else:
                    is_include = True
                targets[column] = is_include

            string_columns = ['Scene type / Tasks']
            for column in string_columns:
                value = relev_setup_template[column].iloc[0]
                if not pd.isnull(value) and value != '':
                    value = [v.strip() for v in value.strip().split(",")]
                    targets[column] = value
                else:
                    targets[column] = []
            targets['template_names'] = targets['Scene type / Tasks']
        return targets

    def check_if_the_kpi_is_available(self, kpi_name):
        status = True
        res_df = self.kpi_static_data[self.kpi_static_data[KPI_TYPE_COL] == kpi_name]
        if res_df.empty:
            status = False
            Log.warning("Error: KPI {} not found in static.kpi_level_2 table.".format(kpi_name))
        return status

    def calculate_sku_facings_by_scene(self):

        if not self.check_if_the_kpi_is_available(GTR_SKU_FACINGS_BY_SCENE):
            Log.warning('Unable to calculate GTR_SKU_FACINGS_BY_SCENE: KPIs are not in kpi_level_2')
            return

        kpi_details = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GTR_SKU_FACINGS_BY_SCENE)
            & (self.kpi_static_data['delete_time'].isnull())]

        targets = self.parse_config_from_setup(kpi_details)
        current_scene_fk = self.scene_info.iloc[0].scene_fk
        current_template_fk = self.templates['template_fk'].iloc[0]
        current_template_name = self.templates['template_name'].iloc[0]

        if len(targets.get('template_names')) > 0 and current_template_name not in targets.get('template_names'):
            Log.info("""Session: {sess}; Scene:{scene}. Scene Type not matching [{k} not in {v}] 
            target for calculating {kpi}."""
                     .format(sess=self.session_uid,
                             scene=self.current_scene_fk,
                             kpi=kpi_details.iloc[0][KPI_TYPE_COL],
                             k=self.templates.iloc[0].template_fk,
                             v=targets.get('template_names')
                             ))
        else:
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(GTR_SKU_FACINGS_BY_SCENE)
            scif = self.scif.copy()
            scif = scif[~(scif["product_type"] == 'POS')]

            # Ignore Others
            if not targets.get("Include Others"):
                scif = scif[~(scif["product_type"] == 'Other')]
            # Ignore Empty
            if not targets.get("Include Empty"):
                scif = scif[~(scif["product_type"] == 'Empty')]
            # Ignore Irrelevant
            if not targets.get("Include Irrelevant"):
                scif = scif[~(scif["product_type"] == 'Irrelevant')]

            facings_field = 'facings' if targets.get("Include Stacking") else 'facings_ign_stack'
            total_sku_facings_in_scene = int(scif[facings_field].sum())
            for i, row in scif.iterrows():
                product_fk = row['product_fk']
                if pd.isnull(row[facings_field]):
                    sku_facings = 0
                    print(row)
                    continue
                else:
                    sku_facings = int(row[facings_field])
                self.common.write_to_db_result(
                    fk=kpi_fk,
                    numerator_id=product_fk,
                    numerator_result=sku_facings,
                    denominator_id=current_template_fk,
                    denominator_result=total_sku_facings_in_scene,
                    context_id=self.store_id,
                    result=sku_facings,
                    score=sku_facings,
                    by_scene=True)
