__author__ = 'prasanna'

import pandas as pd
from pathlib import Path
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from collections import defaultdict, OrderedDict


class PNGJPTemplateParser(object):

    # product_fk, product_name, product_ean_code, product_type
    # manufacturer_name, product_short_name
    # category_fk, sub_category_fk
    # category, sub_category, brand_name
    # category_local_name, manufacturer_local_name, brand_local_name, sub_category_local_name
    # product_english_name, brand_fk
    # sub_brand, Super Brand, sub_brand_name
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
        if rds_conn is None:
            # self.rds_conn = PSProjectConnector(self.data_provider.project_name, DbUsers.CalculationEng)
            self.rds_conn = PSProjectConnector("pngjp-sand2", DbUsers.CalculationEng)
        else:
            self.rds_conn = rds_conn
        self.filename = str(Path(__file__).parent.parent / "Data/TemplateBlockAndGoldenZoneKPI.xlsx")
        self.block_config = self.load_sheet("Block")
        self.golden_zone_config = self.load_sheet("Golden Zone")
        self.load_data_from_db()
        self.preprocess_all_sheets()

    def load_sheet(self, sheet_name):
        df = pd.read_excel(self.filename, sheet_name=sheet_name)
        return df

    def load_data_from_db(self):
        self.load_templates_from_db()
        self.load_category_from_db()
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
                    population[key].extend([unicode(v) for v in value.split(",") if v])
                else:
                    population[key].append(value)

        population = dict(population)
        print(population)
        if transform_columns_to_mapped_columns:
            mappings = PNGJPTemplateParser.FILTER_TYPES_AND_COLUMNS_MAPPING
            population = { mappings.get(k): v for k, v in population.items() if mappings.get(k)}
        print (population)
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
        print(query)
        self.custom_entity = pd.read_sql_query(query, self.rds_conn.db)
        self.custom_entity_dict = self.custom_entity[['pk', 'name']].set_index('name').to_dict()['pk']

    def preprocess_block_config(self):
        self.block_config["template_fks"] = self.block_config['SceneType'].apply(
            self.map_namelist_to_fks, args=(self.template_dict,)
        )
        self.block_config["category_fks"] = self.block_config['Category'].apply(
            self.map_namelist_to_fks, args=(self.category_dict,)
        )
        self.block_config["product_group_name_fks"] = self.block_config['Product Group Name'].apply(
            self.map_namelist_to_fks, args=(self.custom_entity_dict,)
        )
        self.block_config["block_threshold_perc"] = self.block_config["Block Threshold"].apply(
            self.transform_percentage) * 100

        self.block_config["population_filter"] = self.block_config.apply(self.filter_population_fields, axis=1, args=(True, True))
        self.block_config["stacking_include"] = self.block_config['Stacking ( Exclude = 0 / Include = 1)'].astype(bool)


    def preprocess_golden_zone_config(self):
        self.golden_zone_config["template_fks"] = self.golden_zone_config['SceneType'].apply(
            self.map_namelist_to_fks, args=(self.template_dict,)
        )
        self.golden_zone_config["category_fks"] = self.golden_zone_config['Category'].apply(
            self.map_namelist_to_fks, args=(self.category_dict,)
        )
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
        self.golden_zone_config["stacking_include"] = self.golden_zone_config[
            'Stacking ( Exclude = 0 / Include = 1)'].astype(bool)

    def get_targets(self):
        return {
            "Block": self.block_config,
            "Golden Zone": self.golden_zone_config
        }

    def get_custom_entity(self):
        return self.custom_entity

    def get_custom_entity(self):
        if hasattr(self, "custom_entity"):
            return self.custom_entity
        else:
            return pd.DataFrame()


if __name__ == "__main__":
    print("Testing template parser")
    LoggerInitializer.init('pngjp-sand2 Scene Calculations')
    Config.init()
    p = PNGJPTemplateParser(None, None)
    for k, v in p.get_targets().items():
        print(k)
        print(v)
    targets = p.get_targets()
    print()
