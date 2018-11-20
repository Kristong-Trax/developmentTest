from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Logging.Logger import Log

from Projects.RIPETCAREUK_PROD.Utils.Const import RELEVANT_SCENE_TYPES, SEPARATOR, PRODUCT_EAN_CODES
from Projects.RIPETCAREUK_PROD.Utils.ParseTemplates import ParseMarsUkTemplates, KPIConsts, SosConsts
import pandas as pd


class CheckValidity(object):
    def __init__(self, project_name):
        self.template = ParseMarsUkTemplates()
        self.templates_data = self.template.parse_templates()
        self._gaps = pd.DataFrame(columns=['sheet_name', 'field_name', 'row_number','problem'])
        self._db_objects = self.get_db_objects(project_name)

    def get_gaps(self):
        main_gaps = self.check_main_sheet_for_scene_types(self.templates_data[KPIConsts.SHEET_NAME], KPIConsts.SHEET_NAME)
        assortment_gaps = self.check_ean_codes(self.templates_data[KPIConsts.ASSORTMENT_SHEET], KPIConsts.ASSORTMENT_SHEET)

    @staticmethod
    def get_db_objects(project_name):
        rds_conn = PSProjectConnector(project_name, DbUsers.CalculationEng)
        brand_names = pd.read_sql_query('select name from static.brand', rds_conn.db)['name'].tolist()
        product_ean_codes = pd.read_sql_query('select product_ean_code from static.product', rds_conn.db)['product_ean_code'].tolist()
        scene_type_names = pd.read_sql_query('select display_name from static.template', rds_conn.db)['display_name'].tolist()
        sub_category = pd.read_sql_query('select distinct sub_category from static.product', rds_conn.db)['sub_category'].tolist()
        return {
            'brand_names': brand_names,
            'product_ean_codes': product_ean_codes,
            'scene_type_names': scene_type_names,
            'sub_category': sub_category
        }

    @property
    def _entities_sheet_name_sif_name_mapping(self):
        return {
            'Brand': 'brand_name',
            'Sub-Category': 'sub_category'
        }

    def check_main_sheet_for_scene_types(self, sheet_data, sheet_name):
        """

        :param pd.DataFrame sheet_data:
        :return:
        """
        gaps = pd.DataFrame(columns=['sheet_name', 'field_name', 'row_number', 'problem'])
        for index, row in sheet_data.iterrows():
            scene_types = str(row[RELEVANT_SCENE_TYPES])
            if not scene_types:
                continue
            scene_types = scene_types.split(SEPARATOR)
            for scene_type in scene_types:
                if scene_type not in self._db_objects['scene_type_names']:
                    gap = pd.DataFrame({
                        'sheet_name': sheet_name,
                        'row_number': index + 1,
                        'field_name': RELEVANT_SCENE_TYPES,
                        'problem': scene_types + 'not in project\'s scene types'
                    })
                    gaps = gaps.append(gap)
        return gaps

    def check_ean_codes(self, sheet_data, sheet_name):
        gaps = pd.DataFrame(columns=['sheet_name', 'field_name', 'row_number', 'problem'])
        for index, row in sheet_data.iterrows():
            product_ean_codes = str(row[PRODUCT_EAN_CODES])
            if not product_ean_codes:
                continue
            product_ean_codes = product_ean_codes.split(SEPARATOR)
            for product in product_ean_codes:
                if product_ean_codes not in self._db_objects['product_ean_codes']:
                    gap = pd.DataFrame.from_records([{
                        'sheet_name': sheet_name,
                        'row_number': index + 1,
                        'field_name': PRODUCT_EAN_CODES,
                        'problem': product + 'not in project\'s scene types'
                    }])
                    gaps = gaps.append(gap)
        return gaps

    def _get_scene_item_fact_key_for_entity(self, entity_type_sheet):
        if entity_type_sheet != '':
            return self._entities_sheet_name_sif_name_mapping[entity_type_sheet]
        else:
            return entity_type_sheet

    def check_numerator_denominator(self, sheet_data, sheet_name):
        gaps = pd.DataFrame(columns=['sheet_name', 'field_name', 'row_number', 'problem'])
        for index, row in sheet_data.iterrows():
            numerator_type = self._get_scene_item_fact_key_for_entity(sheet_data[SosConsts.SOS_NUM_ENTITY])
            denominator_type = self._get_scene_item_fact_key_for_entity(sheet_data[SosConsts.SOS_DENOM_ENTITY])
            numerators = str(sheet_data[SosConsts.SOS_NUMERATOR]).split(SEPARATOR)
            denominators = str(sheet_data[SosConsts.SOS_DENOMINATOR]).split(SEPARATOR)

            for numerator in numerators:
                if numerator not in self._db_objects[numerator_type]:
                    gap = pd.DataFrame({
                        'sheet_name': sheet_name,
                        'row_number': index + 1,
                        'field_name': SosConsts.SOS_NUMERATOR,
                        'problem': numerator + 'not in project\'s' + numerator_type
                    })
                    gaps = gaps.append(gap)

            for denominator in denominators:
                if denominators not in self._db_objects[denominator_type]:
                    gap = pd.DataFrame({
                        'sheet_name': sheet_name,
                        'row_number': index + 1,
                        'field_name': SosConsts.SOS_DENOMINATOR,
                        'problem': denominator + 'not in project\'s' + denominator_type
                    })
                    gaps = gaps.append(gap)
        return gaps

# if __name__ == '__main__':
#     Config.init()
#     LoggerInitializer.init('TREX')
#     CheckValidity('ripetcareuk-prod').get_gaps()
