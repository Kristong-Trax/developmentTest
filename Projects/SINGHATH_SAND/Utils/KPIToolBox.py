import os
import pandas as pd

from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector


__author__ = 'nidhin'
# excel template constants
TEMPLATE_PARENT_FOLDER = 'Data'
TEMPLATE_NAME = 'Template.xlsx'
# dataframe constants
DF_KPI_TYPE_COL = 'type'
# sheet name constants
KPI_SHEET = 'KPIs'
PRICE_SHEET = 'Price'
POS_PRESENCE_SHEET = 'POS Presence'
DUMP_DISPLAY_PRESENCE = 'Dump Display Presence'
# KPI Sheet columns constants
KPI_ACTIVE_COL = 'IS_ACTIVE'
KPI_NAME_COL = 'KPI_NAME'
KPI_SHEET_STORE_TYPES_COL = 'STORE_TYPES'
KPI_SHEET_NAME_COL = 'SHEET_NAME'
# Price sheet columns constants
PRICE_SHEET_EAN_CODE_1_COL = 'EAN CODE 1'
PRICE_SHEET_EAN_CODE_2_COL = 'EAN CODE 2'
PRICE_SHEET_PRICE_DIFFERENCE_COL = 'PRICE_DIFFERENCE'
# pos presence columns constants
POS_PRESENCE_EAN_COL = 'POS EAN CODE'
# dump display presence columns constants
DUMP_DISPLAY_CATEGORY_COL = 'CATEGORY'
DUMP_DISPLAY_PROD_TYPE_COL = 'PROD_TYPE'
DUMP_DISPLAY_EAN_CODE_COL = 'EAN_CODE'
DUMP_DISPLAY_COUNT_COL = 'COUNT'
DUMP_DISPLAY_SCENE_TYPE_COL = 'SCENE_TYPE'
DUMP_DISPLAY_LOGIC_COL = 'LOGIC'
DUMP_DISPLAY_SKU_TYPE = 'SKU'
DUMP_DISPLAY_POS_TYPE = 'POS'
DUMP_DISPLAY_PROD_TYPE_LIST = [DUMP_DISPLAY_SKU_TYPE, DUMP_DISPLAY_POS_TYPE]
# This should match the category name in DB.
DUMP_CATEGORY_MAP = {
    'water_dump': 'Water',
    'beer_dump': 'Beer',
    'soda_dump': 'Soda',
}


class SINGHATHToolBox:

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
        self.templates = self.data_provider[Data.TEMPLATES]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.templates_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                           '..', TEMPLATE_PARENT_FOLDER,
                                           TEMPLATE_NAME)
        self.kpi_template = pd.ExcelFile(self.templates_path)

    def get_products_price_for_ean_codes(self, ean_codes, session_fk):
        # https://jira.trax-cloud.com/browse/TOHA-2024 to have this in data provider
        self.rds_conn.connect_rds()
        query = """
                    select 
                    value as price, is_promotion,
                    product_fk, name, ean_code, category_fk, brand_fk, type as product_type,
                    sub_category_fk
                    from probedata.manual_collection_price mcp
                    join static_new.product prod on mcp.product_fk=prod.pk
                    where mcp.value is not null
                    and prod.is_active =1
                    and session_fk={session_fk}
                    and ean_code in {ean_codes};
                    """
        df = pd.read_sql_query(query.format(ean_codes=ean_codes,
                                            session_fk=session_fk,
                                            ), self.rds_conn.db)
        return df

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        self.filter_and_send_kpi_to_calc()
        self.common.commit_results_data()
        return 0

    def filter_and_send_kpi_to_calc(self):
        kpi_sheet = self.kpi_template.parse(KPI_SHEET)
        for index, kpi_sheet_row in kpi_sheet.iterrows():
            if not is_nan(kpi_sheet_row[KPI_ACTIVE_COL]):
                if str(kpi_sheet_row[KPI_ACTIVE_COL]).strip().lower() in ['0.0', 'n', 'no']:
                    print("KPI :{} deactivated in sheet.".format(kpi_sheet_row[KPI_NAME_COL]))
                    continue
            if not is_nan(kpi_sheet_row[KPI_SHEET_STORE_TYPES_COL]):
                if bool(kpi_sheet_row[KPI_SHEET_STORE_TYPES_COL].strip()) and \
                        kpi_sheet_row[KPI_SHEET_STORE_TYPES_COL].strip().lower() != 'all':
                    print "Check the store types in excel..."
                    permitted_store_types = [x.strip() for x in
                                             kpi_sheet_row[KPI_SHEET_STORE_TYPES_COL].split(',') if x.strip()]
                    if self.store_info.store_type.values[0] not in permitted_store_types:
                        print "Store type not permitted..."
                        continue
            kpi = self.kpi_static_data[(self.kpi_static_data[DF_KPI_TYPE_COL] == kpi_sheet_row[KPI_NAME_COL])
                                       & (self.kpi_static_data['delete_time'].isnull())]
            if kpi.empty:
                print("KPI Name:{} not found in DB".format(kpi_sheet_row[KPI_NAME_COL]))
                continue
            sheet_name = kpi_sheet_row[KPI_SHEET_NAME_COL]
            sheet_data_frame = self.kpi_template.parse(sheet_name).fillna(method='ffill')
            if sheet_name == PRICE_SHEET:
                self.write_price_difference(kpi, sheet_data_frame)
            elif sheet_name == POS_PRESENCE_SHEET:
                self.write_pos_presence(kpi, sheet_data_frame)
            elif sheet_name == DUMP_DISPLAY_PRESENCE:
                self.write_dump_display_presence(kpi, sheet_data_frame)

    def write_price_difference(self, kpi, price_sheet_data_frame):
        # drop the first column
        price_sheet_data_frame.columns = price_sheet_data_frame.iloc[0]
        price_sheet_data_frame = price_sheet_data_frame.reindex(price_sheet_data_frame.index.drop(0))

        all_ean_codes = tuple(map(str, price_sheet_data_frame[[PRICE_SHEET_EAN_CODE_1_COL,
                                                               PRICE_SHEET_EAN_CODE_2_COL]].values.ravel('F')))
        prod_price_data = self.get_products_price_for_ean_codes(ean_codes=all_ean_codes,
                                                                session_fk=self.session_info['pk'].iloc[0])
        for index, each_row in price_sheet_data_frame.iterrows():
            result = 1
            own_manufacturer_ean = each_row.get(PRICE_SHEET_EAN_CODE_1_COL, None)
            competitive_manufacturer_ean = each_row.get(PRICE_SHEET_EAN_CODE_2_COL, None)
            if not own_manufacturer_ean or not competitive_manufacturer_ean:
                continue
            own_manufacturer = prod_price_data.query("ean_code=='{code}'"
                                                     .format(code=own_manufacturer_ean))
            competitive_manufacturer = prod_price_data.query("ean_code=='{code}'"
                                                             .format(code=competitive_manufacturer_ean))
            if own_manufacturer.empty or competitive_manufacturer.empty:
                print "Own or Competitive Manufacturer not present in the session."
                continue
            own_manufacturer_price = own_manufacturer['price'].iloc[0]
            competitive_manufacturer_price = competitive_manufacturer['price'].iloc[0]
            # if the `calculated price difference` <= `given price difference` then only it is a pass
            if float(own_manufacturer_price - competitive_manufacturer_price) > \
                    float(each_row.get(PRICE_SHEET_PRICE_DIFFERENCE_COL, 0)):
                result = 0
            self.common.write_to_db_result(
                fk=kpi['pk'].iloc[0],
                numerator_id=int(own_manufacturer['product_fk'].iloc[0]),
                numerator_result=result,
                denominator_id=int(competitive_manufacturer['product_fk'].iloc[0]),
                denominator_result=result,
                context_id=self.store_id,
                result=own_manufacturer_price,
                score=competitive_manufacturer_price,
            )

    def write_pos_presence(self, kpi, pos_data_frame):
        for each_ean in pos_data_frame[POS_PRESENCE_EAN_COL]:
            presence = 1
            product_df = self.scif.query("product_ean_code=='{each_ean}' and product_type=='{type}'".format(
                each_ean=each_ean,
                type=DUMP_DISPLAY_POS_TYPE
            ))
            if product_df.empty:
                product_df = self.all_products.query("product_ean_code=='{each_ean}' and product_type=='{type}'".format(
                    each_ean=each_ean,
                    type=DUMP_DISPLAY_POS_TYPE
                ))
                presence = 0
                if product_df.empty:
                    # This should not happen
                    # This means the POS ean code is not in the product master data
                    continue
                    # raise Exception("KPI {kpi_name}: The product with EAN {ean} and type {type}"
                    #                 " in template is not in DB.".format(
                    #                     kpi_name=kpi[DF_KPI_TYPE_COL].iloc[0],
                    #                     ean=each_ean,
                    #                     type=DUMP_DISPLAY_POS_TYPE,
                    #                 ))
            self.common.write_to_db_result(
                fk=kpi['pk'].iloc[0],
                numerator_id=int(product_df['product_fk'].iloc[0]),
                denominator_id=self.store_id,
                context_id=self.store_id,
                result=presence,
                score=presence,
            )

    def write_dump_display_presence(self, kpi, dump_display_data_frame):
        dump_display_data_group = dump_display_data_frame.groupby(DUMP_DISPLAY_CATEGORY_COL)
        for category, dump_display_data in dump_display_data_group:
            presence = 0
            # iterate through rows for each category
            category_name = DUMP_CATEGORY_MAP[category]
            category_fk = self.all_products.query("category=='{category}'"
                                                  .format(category=category_name))['category_fk'].iloc[0]
            # get the applicable scene types -- start
            set_scene_types = set()
            scene_type_list = list(dump_display_data[DUMP_DISPLAY_SCENE_TYPE_COL].values.ravel('F'))
            for each_list in scene_type_list:
                set_scene_types.update(tuple(str(each.strip()) for each in each_list.split(',')))
            # get the applicable scene types -- end
            # make template name case-insensitive search -- start
            self.templates["template_name"] = self.templates["template_name"].str.lower()
            _scene_types = map(str.lower, list(set_scene_types))
            # make template name case-insensitive search -- start
            allowed_template_fks = self.templates.query("template_name in {allowed_templates}".format(
                allowed_templates=_scene_types
            ))['template_fk'].values.tolist()
            template_scif = self.scif.query('template_fk in {}'.format(allowed_template_fks))
            if template_scif.empty:
                print "kpi: {kpi}: Template/Scene Types: {templates} are not present in session {sess}" \
                    .format(kpi=kpi[DF_KPI_TYPE_COL].iloc[0],
                            templates=_scene_types,
                            sess=self.session_uid)
                continue
            template_scif_by_scene_id = template_scif.groupby('scene_id')
            row_truths = []  # to check if all items in the category is true
            for scene_id, scene_data in template_scif_by_scene_id:
                dump_display_product_group = dump_display_data.groupby(DUMP_DISPLAY_PROD_TYPE_COL)
                one_condition_fail = False
                for prod_type, product_items in dump_display_product_group:
                    if one_condition_fail:
                        continue
                    logic = product_items[DUMP_DISPLAY_LOGIC_COL].iloc[0].strip().lower()
                    for idx, each_prod_entry in product_items.iterrows():
                        _pos_codes = str(each_prod_entry[DUMP_DISPLAY_EAN_CODE_COL])
                        all_pos_ean_codes = tuple(map(str, [x.strip() for x in _pos_codes.split(',') if x]))
                        facings_count = 0
                        prod_scif_with_ean = scene_data.query(
                            'product_ean_code in {all_skus} and category_fk=="{category_fk}"'
                                .format(all_skus=all_pos_ean_codes,
                                        category_fk=category_fk))
                        if not prod_scif_with_ean.empty:
                            facings_count = int(prod_scif_with_ean['facings'].iloc[0])
                        if facings_count < int(each_prod_entry[DUMP_DISPLAY_COUNT_COL]):
                            if logic == 'and':
                                # one prod type didn't satisfy; try next scene.
                                one_condition_fail = True
                                break
                        else:
                            row_truths.append(prod_type)
                if row_truths and all([ech in row_truths for ech in DUMP_DISPLAY_PROD_TYPE_LIST]):
                    # dump display is found, break out and save presence for this category
                    presence = 1
                    break
                else:
                    row_truths = []

            # save for each category
            self.common.write_to_db_result(
                fk=int(kpi['pk'].iloc[0]),
                # only one category
                numerator_id=category_fk,
                denominator_id=self.store_id,
                context_id=self.store_id,
                result=presence,
                score=presence,
            )


def is_nan(value):
    if value != value:
        return True
    return False
