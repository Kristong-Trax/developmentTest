__author__ = 'prasanna'

import pandas as pd
from pathlib import Path
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from collections import defaultdict, OrderedDict
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.CommonV2 import Common
from datetime import datetime, timedelta
import json
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert


class PNGJPTemplateParser(object):

    FILTER_TYPES_AND_COLUMNS_MAPPING = OrderedDict([
        ("ean_code", "product_ean_code"),
        ("brand", "brand_local_name"),
        ("category", "category_local_name"),
        ("size", "size"),
        ("sub_packages_number", "number_of_sub_packages"),
        ("superbrand", "Super Brand"),
        ("subbrand", "sub_brand_name"),
        ("subcategory", "sub_category_local_name")
    ])

    def __init__(self, data_provider, rds_conn):
        self.data_provider = data_provider
        self.rds_conn = rds_conn
        self.filename = str(Path(__file__).parent.parent / "Data/TemplateBlockAndGoldenZoneKPI.xlsx")
        self.block_config = self.load_sheet("Block")
        self.golden_zone_config = self.load_sheet("Golden Zone")
        self.load_data_from_db()
        self.preprocess_all_sheets()
        self.sync = SyncTemplateWithExternalTargets(data_provider, rds_conn, self.get_targets())
        self.sync.execute()

    def load_sheet(self, sheet_name):
        df = pd.read_excel(self.filename, sheet_name=sheet_name)
        return df

    def load_data_from_db(self):
        self.load_templates_from_db()
        # self.load_category_from_db()
        self.load_custom_entity_from_db()

    def preprocess_all_sheets(self):
        self.preprocess_block_config()
        self.preprocess_golden_zone_config()

    @staticmethod
    def str_to_int_list(item):
        return [int(v) for v in str(item).strip().split(",")]

    @staticmethod
    def str_to_str_list(item):
        item = u"{}".format(item)
        return [u"{}".format(v) for v in item.strip().split(",")]

    @staticmethod
    def transform_percentage(item):
        return float(str(item).replace("%", ""))

    @staticmethod
    def map_namelist_to_fks(item, lookup_dict):
        values = u"{}".format(item).strip().split(",")
        fks = []
        for value in values:
            value = value.strip()
            if value:
                fks.append(lookup_dict.get(value))
        return fks

    @staticmethod
    def filter_population_fields(row, to_list=True, transform_columns_to_mapped_columns=True):
        population = defaultdict(list)

        key_columns = ['Filter Type1', 'Filter Type2', 'Filter Type3', 'Filter Type4', 'Filter Type5']
        value_columns = ['Filter Value1', 'Filter Value2', 'Filter Value3', 'Filter Value4', 'Filter Value5']

        for filter_type, filter_value in zip(key_columns, value_columns):
            key = row[filter_type]
            if not pd.isnull(key):
                value = row[filter_value]
                if pd.isnull(value):
                    continue
                if to_list:
                    if isinstance(value, (float,int)):
                        # To handle, if the filter values are numbers
                        value = u"{:.0f}".format(value)
                    else:
                        value = unicode(value).strip()
                    population[key].extend([unicode(v).strip() for v in value.split(",") if v])
                else:
                    population[key].append(value)

        population = dict(population)
        if transform_columns_to_mapped_columns:
            mappings = PNGJPTemplateParser.FILTER_TYPES_AND_COLUMNS_MAPPING
            population = { mappings.get(k): v for k, v in population.items() if mappings.get(k)}
        return population

    def load_templates_from_db(self):
        query = """select pk, name from static.template where delete_date is null;"""
        self.templatedf = pd.read_sql_query(query, self.rds_conn.db)
        self.template_dict = self.templatedf[['pk', 'name']].set_index('name').to_dict()['pk']

    def load_category_from_db(self):
        query = """select pk, name, local_name from static_new.category where delete_date is null;"""
        self.categorydf = pd.read_sql_query(query, self.rds_conn.db)
        self.category_dict = self.categorydf[['pk', 'local_name']].set_index('local_name').to_dict()['pk']

    def load_custom_entity_from_db(self):
        values_to_filter = list(self.block_config['Product Group Name'].unique())
        values_to_filter += list(self.golden_zone_config['Product Group Name'].unique())
        values_to_filter_as_str = u",".join([u"'{}'".format(v) for v in values_to_filter])
        query = u"""
        select pk, name from static.custom_entity 
        where name in ({})
        """.format(values_to_filter_as_str)
        self.custom_entity = pd.read_sql_query(query, self.rds_conn.db)
        self.custom_entity_dict = self.custom_entity[['pk', 'name']].set_index('name').to_dict()['pk']

    def preprocess_block_config(self):
        self.block_config["template_fks"] = self.block_config['SceneType'].apply(
            self.map_namelist_to_fks, args=(self.template_dict,)
        )
        # self.block_config["category_fks"] = self.block_config['Category'].apply(
        #     self.map_namelist_to_fks, args=(self.category_dict,)
        # )
        self.block_config["product_group_name_fks"] = self.block_config['Product Group Name'].apply(
            self.map_namelist_to_fks, args=(self.custom_entity_dict,)
        )
        self.block_config["block_threshold_perc"] = self.block_config["Block Threshold"].apply(
            self.transform_percentage) * 100

        self.block_config["population_filter"] = self.block_config.apply(
            self.filter_population_fields, axis=1, args=(True, True)
        )
        self.block_config["include_stacking"] = self.block_config[
            'Stacking ( Exclude = 0 / Include = 1)'].fillna(False).astype(bool)
        self.block_config["include_empty"] = self.block_config[
            'Include Empty ( Exclude = 0 / Include = 1)'].fillna(False).astype(bool)
        self.block_config["include_other"] = self.block_config[
            'Include Other ( Exclude = 0 / Include = 1)'].fillna(False).astype(bool)


    def preprocess_golden_zone_config(self):
        self.golden_zone_config["template_fks"] = self.golden_zone_config['SceneType'].apply(
            self.map_namelist_to_fks, args=(self.template_dict,)
        )
        # self.golden_zone_config["category_fks"] = self.golden_zone_config['Category'].apply(
        #     self.map_namelist_to_fks, args=(self.category_dict,)
        # )
        self.golden_zone_config["product_group_name_fks"] = self.golden_zone_config['Product Group Name'].apply(
            self.map_namelist_to_fks, args=(self.custom_entity_dict,)
        )
        self.golden_zone_config["target_perc"] = self.golden_zone_config["Target"].apply(
            self.transform_percentage
        ) * 100
        self.golden_zone_config["1_5_shelf"] = self.golden_zone_config["[1-5] Shelf"].apply(self.str_to_int_list)
        self.golden_zone_config["6_7_shelf"] = self.golden_zone_config["[6,7] Shelf"].apply(self.str_to_int_list)
        self.golden_zone_config["8_9_shelf"] = self.golden_zone_config["[8,9] Shelf"].apply(self.str_to_int_list)
        self.golden_zone_config["10_12_shelf"] = self.golden_zone_config["[10, 11, 12] Shelf"].apply(
            self.str_to_int_list
        )
        self.golden_zone_config["above_12_shelf"] = self.golden_zone_config["Above 12"].apply(self.str_to_int_list)

        self.golden_zone_config["population_filter"] = self.golden_zone_config.apply(
            self.filter_population_fields, axis=1, args=(True, True)
        )
        self.golden_zone_config["include_stacking"] = self.golden_zone_config[
            'Stacking ( Exclude = 0 / Include = 1)'].fillna(False).astype(bool)

    def get_targets(self):
        return {
            "Block": self.block_config,
            "Golden Zone": self.golden_zone_config
        }

    def get_custom_entity(self):
        return self.custom_entity

    def get_external_targets(self):
        df = pd.DataFrame()
        try:
            df = self.sync.get_all_kpi_external_targets()
            df = self.sync.post_process_ext_targets(df)
        except Exception as e:
            print("Failed {}".format(e))
        return df


class SyncTemplateWithExternalTargets(object):

    def __init__(self, data_provider, rds_conn, targets_from_template):
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.rds_conn = rds_conn
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.all_products = data_provider['all_products']
        self.targets_from_template = targets_from_template
        self.ext_targets = self.get_all_kpi_external_targets()
        self.ext_targets = self.post_process_ext_targets(self.ext_targets)
        self.session_uid = self.data_provider.session_uid
        self.is_old_visit = self.check_if_the_current_session_has_json()

    def check_if_the_current_session_has_json(self):
        query = "select * from probedata.analysis_results where session_uid = '{}'".format(self.session_uid)
        df = pd.read_sql_query(query, self.rds_conn.db)
        if len(df) > 0:
            return True
        else:
            return False

    def population_filter_to_ean_codes(self, targets):
        current_ean_codes = pd.DataFrame()
        for idx, each_target in targets.iterrows():
            boolean_masks = []
            population_filters = each_target.population_filter
            for column_name, values in population_filters.items():
                boolean_masks.append(
                    self.all_products[column_name].isin(values)
                    # self.match_product_data[column_name].isin(values)
                )
            population_filter_final_mask = reduce(lambda x, y: x & y, boolean_masks)
            ean_codes = self.all_products[population_filter_final_mask]['product_ean_code'].unique()
            record = {
                "KPI_NAME": each_target['KPI Name'],
                "Product_Group_Name": each_target['Product Group Name'],
                "Population_Filter": population_filters,
                "entities": ean_codes
            }
            current_ean_codes = current_ean_codes.append(record, ignore_index=True)
        return current_ean_codes

    @staticmethod
    def get_kpi_external_targets(visit_date):
        operation_type = 'config'
        return """SELECT ext.*, ot.operation_type, kpi.type as kpi_type
        FROM static.kpi_external_targets ext
        LEFT JOIN static.kpi_operation_type ot on ext.kpi_operation_type_fk=ot.pk
        LEFT JOIN static.kpi_level_2 kpi on ext.kpi_level_2_fk = kpi.pk
        WHERE ot.operation_type = '{}' 
        AND ( (ext.start_date<='{}' and ext.end_date is null) or  (ext.start_date<='{}' and ext.end_date>='{}'));
        """.format(operation_type, visit_date, visit_date, visit_date)

    def get_all_kpi_external_targets(self):
        query = self.get_kpi_external_targets(self.visit_date)
        external_targets = pd.read_sql_query(query, self.rds_conn.db)
        return external_targets

    def post_process_ext_targets(self, ext_targets):
        ext_targets['Config Name'] = ext_targets['key_json'].apply(
            lambda x: json.loads(x).get("Config Name", ""))
        ext_targets['Product_Group_Name'] = ext_targets['Config Name']
        ext_targets['entities'] = ext_targets['data_json'].apply(
            lambda x: json.loads(x).get("entities"))
        return ext_targets

    def execute(self):
        # self.is_old_visit = False
        self.sync_external_targets('Block', "PGJAPAN_BLOCK_COMPLIANCE_BY_SCENE")
        self.sync_external_targets('Golden Zone', "PGJAPAN_GOLDEN_ZONE_COMPLIANCE_BY_SCENE")

    def sync_external_targets(self, kpi_group_name, kpi_type):
        # print("Syncing => {} - {}".format(kpi_group_name, kpi_type))
        current_eans = self.population_filter_to_ean_codes(self.targets_from_template[kpi_group_name])
        # print(current_eans.shape)

        insert_queries = []
        insert_queries_after_patch = []
        for idx, row in current_eans.iterrows():
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type)
            product_group_name = row["Product_Group_Name"]
            rel_ext_targets = self.ext_targets[
                (self.ext_targets['Config Name'].str.encode('utf8') == product_group_name.encode('utf8'))
                &
                (self.ext_targets['kpi_type'] == kpi_type)]
            if rel_ext_targets.empty:
                if self.is_old_visit:
                    # print("Ignoring this line item, since its a recalc")
                    continue
                # print("Product group not found in ext_targets")
                # print("Inserting valid product_groups into external_targets")
                kpi_level_2_fk = kpi_fk
                kpi_operation_type_fk = 2
                start_date = str(datetime.now().date())
                key_json = {
                    "Config Name": row['Product_Group_Name'].replace("'", "\\'").encode('utf-8'),
                }
                data_json = {
                    "Population": row['Population_Filter'],
                    "Config Name": row['Product_Group_Name'].replace("'", "\\'").encode('utf-8'),
                    "entities": row['entities'].tolist()
                }
                # check if we need to re-insert / insert
                new_record = {
                    "kpi_operation_type_fk": {0: kpi_operation_type_fk},
                    "kpi_level_2_fk": {0: kpi_level_2_fk},
                    "start_date": {0: start_date},
                    # "key_json": {0: key_json},
                    # "data_json": {0: data_json}
                    "key_json": {0: json.dumps(key_json).encode('ascii').decode('unicode-escape')},
                    "data_json": {0: json.dumps(data_json).encode('ascii').decode('unicode-escape')}
                 }
                insert_queries.append(insert(new_record, "static.kpi_external_targets"))
            else:
                # check if the entities in the product_group changed recently.
                if rel_ext_targets.shape[0] > 1:
                    print("More than one records...")
                else:
                    if self.is_old_visit:
                        print("use the current eans stored in db")
                        # rel_ext_targets['entities'].iloc[0]
                        continue
                    # print("Only one record.")
                    # print("If the entities matching with curr_eancodes")
                    # relv_current_eans = block_current_eans[
                    #     (current_eans['Product_Group_Name'].str.encode('utf8') == product_group_name.encode('utf8'))
                    #     &
                    #     (current_eans['KPI_NAME'] == KPI_NAME)]['entities'].iloc[0]
                    relv_current_eans = row['entities']
                    relv_target_eans = rel_ext_targets['entities'].iloc[0]
                    if len(set(relv_target_eans) - set(relv_current_eans)) == 0:
                        pass
                        # print("Same")
                        # print("Use this pk")
                    else:
                        # if the visit is a new visit, then apply this
                        # self.new_session
                        # if not, use old
                        # print("There are diff in entities. So end the current pk and save the new one.")
                        ext_target_pk_to_end = rel_ext_targets.pk.iloc[0]
                        # print("PK to update {}".format(ext_target_pk_to_end))
                        # update the end date for this pk
                        end_date = str((datetime.now() - timedelta(days=1)).date())
                        to_update = {"end_date": {0: end_date}}
                        update_query = self.get_table_update_query(to_update,
                                                                   "static.kpi_external_targets",
                                                                   "pk = {}".format(ext_target_pk_to_end))
                        self.commit_to_db([update_query])
                        # insert the new record to external_target with relv_current_eans.
                        kpi_level_2_fk = kpi_fk
                        kpi_operation_type_fk = 2
                        start_date = str(datetime.now().date())
                        key_json = {
                            "Config Name": row['Product_Group_Name'].replace("'", "\\'").encode('utf-8'),
                        }
                        data_json = {
                            "Population": row['Population_Filter'],
                            "Config Name": row['Product_Group_Name'].replace("'", "\\'").encode('utf-8'),
                            "entities": row['entities'].tolist()
                        }
                        # check if we need to re-insert / insert
                        new_record = {
                            "kpi_operation_type_fk": {0: kpi_operation_type_fk},
                            "kpi_level_2_fk": {0: kpi_level_2_fk},
                            "start_date": {0: start_date},
                            # "key_json": {0: key_json},
                            # "data_json": {0: data_json}
                            "key_json": {0: json.dumps(key_json).encode('ascii').decode('unicode-escape')},
                            "data_json": {0: json.dumps(data_json).encode('ascii').decode('unicode-escape')}
                        }
                        insert_queries_after_patch.append(insert(new_record, "static.kpi_external_targets"))

        if len(insert_queries) > 0:
            # print("call insert_statement check")
            self.commit_to_db(insert_queries)

        if len(insert_queries_after_patch) > 0:
            # print("call insert_statement after updating old ones")
            self.commit_to_db(insert_queries_after_patch)

    @staticmethod
    def get_table_update_query(entries, table, condition):
        updated_values = []
        for key in entries.keys():
            if key == "pk":
                # print("Skipping pk in update stmt generation")
                continue
            updated_values.append("{} = '{}'".format(key, entries[key][0]))

        query = "UPDATE {} SET {} WHERE {}".format(table, ", ".join(updated_values), condition)

        return query

    def commit_to_db(self, queries):
        pass
        # self.rds_conn.connect_rds()
        # cur = self.rds_conn.db.cursor()
        # for query in queries:
        #     try:
        #         print(query)
        #         print("Check if any queries!")
        #         # cur.execute(query)
        #         # self.rds_conn.db.commit()
        #         print 'kpis were added/updated to the db'
        #     except Exception as e:
        #         print 'kpis were not inserted: {}'.format(repr(e))


#
# if __name__ == "__main__":
#     from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
#     print("Testing template parser")
#     LoggerInitializer.init('pngjp-sand2 Scene Calculations')
#     Config.init()
#     project_name = "pngjp-sand2"
#     session = 'E2C4A7B5-AF2B-40BA-9D35-932FC1826568'
#     data_provider = KEngineDataProvider(project_name)
#     data_provider.load_session_data(session_uid=session)
#     rds_conn = PSProjectConnector(project_name, DbUsers.CalculationEng)
#     p = PNGJPTemplateParser(data_provider=data_provider, rds_conn=rds_conn)
    # for k, v in p.get_targets().items():
    #     print(k)
    #     print(v)
    # targets = p.get_targets()
    # print()

