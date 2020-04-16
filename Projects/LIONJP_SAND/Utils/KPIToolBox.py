import os
import pandas as pd

from KPIUtils_v2.Calculations.SequenceCalculationsV2 import Sequence
from KPIUtils_v2.Calculations.CalculationsUtils.Constants import AdditionalAttr
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.DB.CommonV2 import Common

from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log

from Projects.LIONJP_SAND.Data.Consts import Consts


__author__ = 'nidhin'


class LIONJP_SANDToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
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
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.setup_file = "setup.xlsx"
        self.kpi_sheet = self.get_setup(Consts.KPI_SHEET_NAME)

    def get_setup(self, sheet_name):
        setup_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        setup_path = os.path.join(setup_path, self.setup_file)
        setup = pd.read_excel(setup_path, sheet_name=sheet_name)
        return setup

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        try:
            if self.kpi_sheet.empty:
                Log.error("'kpi_list' sheet in setup file is empty.")
                return
            kpi_types = [x.strip() for x in self.kpi_sheet[Consts.KPI_TYPE].unique()]

            for kpi_type in kpi_types:
                kpis = self.kpi_sheet[self.kpi_sheet[Consts.KPI_TYPE] == kpi_type]
                if kpi_type == Consts.FSOS:
                    self.main_sos_calculations(kpis)
                elif kpi_type == Consts.ADJACENCY:
                    self.main_adjacency_calculations(kpis)
                else:
                    Log.warning("KPI_TYPE:{kt} not found in setup=>kpi_list sheet.".format(kt=kpi_type))
                    continue

                self.common.commit_results_data()
            return
        except Exception as err:
            Log.error("LionJP KPI calculation failed due to the following error: {}".format(err))

    def main_sos_calculations(self, kpis):
        for row_number, row_data in kpis.iterrows():
            if row_data[Consts.KPI_NAME] == Consts.FACINGS_IN_CELL_PER_PRODUCT:
                self.calculate_facings_in_cell_per_product()

    def calculate_facings_in_cell_per_product(self):
        kpi_db = self.kpi_static_data[
              (self.kpi_static_data[Consts.KPI_FAMILY] == Consts.PS_KPI_FAMILY)
              & (self.kpi_static_data[Consts.KPI_NAME_DB] == Consts.FACINGS_IN_CELL_PER_PRODUCT)
              & (self.kpi_static_data['delete_time'].isnull())]

        if kpi_db.empty:
            print("KPI Name:{} not found in DB".format(Consts.FACINGS_IN_CELL_PER_PRODUCT))
        else:
            print("KPI Name:{} found in DB".format(Consts.FACINGS_IN_CELL_PER_PRODUCT))
            kpi_fk = kpi_db.pk.values[0]
            match_prod_scene_data = self.match_product_in_scene.merge(
                self.products, how='left', on='product_fk', suffixes=('', '_prod'))
            grouped_data = match_prod_scene_data.query(
                '(stacking_layer==1) or (product_type=="POS")'
            ).groupby(
                ['scene_fk', 'bay_number', 'shelf_number', 'product_fk']
            )
            for data_tup, scene_data_df in grouped_data:
                scene_fk, bay_number, shelf_number, product_fk = data_tup
                facings_count_in_cell = len(scene_data_df)
                cur_template_fk = int(self.scene_info[self.scene_info['scene_fk'] == scene_fk].get('template_fk'))
                self.common.write_to_db_result(fk=kpi_fk,
                                               numerator_id=product_fk,
                                               denominator_id=self.store_id,
                                               context_id=cur_template_fk,
                                               numerator_result=bay_number,
                                               denominator_result=shelf_number,
                                               result=facings_count_in_cell,
                                               score=scene_fk)

    @staticmethod
    def get_entity_query():
        query = """select 
                    pk product_fk,
                    "entity_1" entity,
                    ean_code
                    from 
                    static_new.product p 
                    where 1=1 
                    and ean_code in ("4902430916028")
                    union 
                    select
                    pk product_fk,
                    "entity_2" entity,
                    ean_code
                    from 
                    static_new.product p 
                    where 1=1 
                    and ean_code in ("4903301282945")
                    union 
                    select
                    pk product_fk,
                    "entity_3" entity,
                    ean_code
                    from 
                    static_new.product p 
                    where 1=1 
                    and ean_code in ("4582469502593")
                    """
        return query

    def main_adjacency_calculations(self, kpis):
        for row_number, row_data in kpis.iterrows():
            if row_data[Consts.KPI_NAME] == Consts.PRODUCT_GROUP_ADJACENCY_IN_WHOLE_STORE:
                self.calculate_sequence_per_scene()
            else:
                Log.warning("KPI_NAME:{kn} not found in setup=>kpi_list sheet.".format(kn=row_data[Consts.KPI_NAME]))

    def calculate_sequence_per_scene(self):
        scene_fks = self.scif['scene_fk'].unique()
        for scene_fk in scene_fks:
            if scene_fk != 964190:
                continue
            location = {"scene_fk": scene_fk}
            query = self.get_entity_query()
            df_entities = pd.read_sql_query(query, self.rds_conn.db)
            population = {'entity': ["entity_1", "entity_3"]}
            sequence_params = {AdditionalAttr.DIRECTION: 'DOWN',
                               AdditionalAttr.EXCLUDE_FILTER: {"product_type": ["Empty", "Other", "Irrelevant", "POS"]},
                               AdditionalAttr.CHECK_ALL_SEQUENCES: False,
                               AdditionalAttr.STRICT_MODE: False,
                               AdditionalAttr.REPEATING_OCCURRENCES: True,
                               AdditionalAttr.INCLUDE_STACKING: False,
                               AdditionalAttr.ALLOWED_PRODUCTS_FILTERS: {'entity': ["entity_2"]},
                               AdditionalAttr.MIN_TAGS_OF_ENTITY: 1}

            self.data_provider._set_all_products(self.data_provider.all_products.merge(df_entities, on="product_fk"))
            sequence_res = Sequence(self.data_provider).calculate_sequence(population, location, sequence_params)
            for row_num, row_data in sequence_res.iterrows():
                for column, data in row_data.items():
                    print ("{} {}".format(column, data))