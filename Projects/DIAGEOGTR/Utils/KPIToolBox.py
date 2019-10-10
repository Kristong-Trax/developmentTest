import pandas as pd
import os
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from KPIUtils.DIAGEO.ToolBox import DIAGEOToolBox, DIAGEOConsts
from KPIUtils.GlobalProjects.DIAGEO.Utils.Fetcher import DIAGEOQueries
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.GlobalProjects.DIAGEO.Utils.ParseTemplates import parse_template  # if needed
from KPIUtils.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from OutOfTheBox.Calculations.ManufacturerSOS import ManufacturerFacingsSOSInWholeStore, \
    ManufacturerFacingsSOSPerSubCategoryInStore
from OutOfTheBox.Calculations.SubCategorySOS import SubCategoryFacingsSOSPerCategory

__author__ = 'Yasmin'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

SOD = 'SHARE_OF_DISPLAY'
SOD_GROUPED_SCENES = 'SHARE_OF_DISPLAY_GROUPED_SCENES'
SOWB = 'SHARE_OF_WALL_BAY'
FSOS = "SHARE_OF_SHELF"

PRICE_PROMOTION = 'PRICE_PROMOTION'
PRICE_PROMOTION_BRAND_SKU = 'PRICE_PROMOTION_BRAND_SKU'
PRICE_PROMOTION_SCENE_BRAND_SKU = 'PRICE_PROMOTION_SCENE_BRAND_SKU'
PRICE_PROMOTION_SKU = 'PRICE_PROMOTION_SKU'

entities = {"category": "category_fk", "sub_category": "sub_category_fk", "manufacturer": "manufacturer_fk",
            "template": "template_fk", "brand": "brand_fk", "product": "product_fk", "scene": "scene_fk"}

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')


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


class DIAGEOGTRConsts:

    def __init__(self):
        pass

    SEPARATOR = ','
    STORE_TYPE = "store_type"
    KPI_TYPE = 'kpi_type'
    KPI_GROUP = 'kpi_group'
    KPI_FAMILY_NAME = 'kpi_family_name'
    KPI_NAME = 'kpi_name'
    KPI_LEVEL_2_FK = 'kpi_level_2_fk'
    ATOMIC_KPI_FK = "atomic_kpi_fk"
    KPI_ATOMIC_NAME = 'atomic_kpi_name'
    KPI_FK = 'kpi_fk'
    CATEGORY_FK = 'category_fk'
    SCENE_FK = "scene_fk"
    TEMPLATE_FK = "template_fk"
    BRAND_FK = "brand_fk"
    SUB_CATEGORY_FK = "sub_category_fk"
    MANUFACTURER_FK = "manufacturer_fk"
    KPI_SET_NAME = 'kpi_set_name'
    SCENE_ID = "scene_id"
    ITEM_ID = "item_id"
    PRODUCT_FK = 'product_fk'
    BAY_NUMBER = "bay_number"
    TYPE = "type"
    SCORE_AFTER_ACTIONS = 'score_after_actions'
    DENOMINATOR_RESULT_AFTER_ACTIONS = 'denominator_result_after_actions'
    UTF_8 = "utf-8"

    TEMPLATE_NAME = "template_name"
    DENOMINATOR_ID = "denominator_id"
    NUMERATOR_ID = "numerator_id"
    CONTEXT_ID = "context_id"
    FACINGS = "facings"

    # Template
    STORE_POLICY = "store_policy"
    SCENE_POLICY = "scene_policy"
    EXCLUDE_EMPTY = "exclude_empty"
    EXCLUDE_IRRELEVANT = "exclude_irrelevant"

    ENTITY_1 = "entity_1"
    ENTITY_2 = "entity_2"
    ENTITY_3 = "entity_3"

    EMPTY = 0
    IRRELEVANT = 145
    EMPTY_ENTITY = 65534

    NUM_OF_DISPLAYS = 'num_of_displays'
    TOTAL_NUM_OF_DISPLAYS = 'total_num_of_displays'


class DIAGEOGTRToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    ACTIVATION_STANDARD = 'Activation Standard'
    DEBUG = False

    def __init__(self, data_provider, output):
        self.output = output
        self.common = Common(data_provider)
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO].merge(
            self.data_provider[Data.ALL_TEMPLATES][['template_fk', 'template_name']], on='template_fk', how='left')
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.survey_questions_data = self.data_provider[Data.SURVEY_RESPONSES]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.template_data = pd.read_excel(TEMPLATE_PATH, 'KPIs').fillna('')
        self.template_data.columns = map(str.lower, self.template_data.columns)
        self.set_templates_data = {}
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.match_display_in_scene = self.get_match_display()
        self.store_type = self.store_info['additional_attribute_1'].values[0]
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_static_data_new = self.common.get_new_kpi_static_data()
        self.output = output

        self.product_attribute_price_data = self.get_product_attribute_price_data()
        self.product_atrribute_length_data = self.get_product_attribute_length_data()

        self.product_attribute_price_with_scene_data = self.get_product_attribute_price_with_scene_data()
        self.product_attribute_length_with_scene_data = self.get_product_attribute_length_with_scene_data()

        self.common = Common(self.data_provider)
        self.commonV2 = CommonV2(self.data_provider)
        self.tools = DIAGEOToolBox(self.data_provider, output,
                                   match_display_in_scene=self.match_display_in_scene)  # replace the old one
        self.diageo_generator = DIAGEOGenerator(self.data_provider, self.output, self.common)

        self.kpi_results_queries = []
        self.scores = {self.LEVEL1: {}, self.LEVEL2: {}, self.LEVEL3: {}}

    def get_product_attribute_length_data(self):
        ##replace(json_extract(p.labels, '$.sub_brand'), '"', "") sub_brand,
        query = """
        SELECT  		
            session_fk,
            p.pk product_fk,
            mcna.pk mcna_fk,
            mcna.name mcna_name,
            sub_category_fk,
            manufacturer_fk,
            p.brand_fk,
            sb.pk sub_brand_fk,
            value length_in_meters
        FROM probedata.manual_collection_number mcn
        INNER JOIN static.manual_collection_number_attributes mcna 
        ON mcn.manual_collection_number_attributes_fk= mcna.pk 
        INNER JOIN static_new.product p on p.pk = mcn.product_fk
        INNER JOIN static_new.brand b on p.brand_fk = b.pk
        INNER JOIN static.sub_brand sb on sb.name = JSON_UNQUOTE(JSON_Extract(labels, '$.sub_brand'))
        WHERE 1=1 
            AND session_fk = (SELECT pk FROM probedata.session ses WHERE ses.session_uid='{}') 
            AND deleted_time is null
            AND mcna.pk=1
        ORDER by 
            sub_category_fk,
            manufacturer_fk,
            brand_fk,
            length_in_meters
        """.format(self.session_uid)

        product_attribute_length_data = pd.read_sql_query(query, self.rds_conn.db)

        return product_attribute_length_data

    def get_product_attribute_length_with_scene_data(self):
        """
        This function extracts promotion price details from probedata.manual_collection_price table.
        """
        query = """
        SELECT 
            scif.session_id,
            bskulen.brand_fk,
            mcna_fk,
            item_id,
            scif.facings,
            bskulen.category_fk,
            bskulen.sub_category_fk,
            bskulen.manufacturer_fk,
            bskulen.per_sku_length_in_mm,
            (scif.facings*bskulen.per_sku_length_in_mm) length_in_mm
        FROM 
        reporting.scene_item_facts scif
        INNER JOIN static_new.product p ON scif.item_id = p.pk
        INNER JOIN 
        (
            SELECT  
                session_id,
                brand_fk,
                max(mcna.pk) mcna_fk,
                max(mcna.name) mcna_name ,
                category_fk,
                sub_category_fk,
                manufacturer_fk,
                max(value*1000) brand_length_in_mm,
                sum(facings) facings,
                max(value*1000)/sum(facings) per_sku_length_in_mm 
            FROM reporting.scene_item_facts scif
            LEFT JOIN probedata.manual_collection_number mcn on (mcn.product_fk = scif.item_id and mcn.session_fk = scif.session_id) 
            RIGHT JOIN static_new.product p on p.pk = scif.item_id 
            LEFT JOIN static.manual_collection_number_attributes mcna ON mcn.manual_collection_number_attributes_fk= mcna.pk 
            JOIN static_new.brand b on p.brand_fk = b.pk
            WHERE session_id = (SELECT pk FROM probedata.session ses WHERE ses.session_uid='{}')
            AND scif.facings>0
                GROUP BY 
                session_id,
                brand_fk
            HAVING 
            per_sku_length_in_mm IS NOT NULL
            and max(mcna.pk)=1
        ) as bskulen
        ON (scif.session_id = bskulen.session_id AND p.brand_fk = bskulen.brand_fk)
        """.format(self.session_uid)

        df_result = pd.read_sql_query(query, self.rds_conn.db)

        return df_result

    def get_product_attribute_price_data(self):
        query = """
        SELECT
        mcp.creation_time,
        mcp.session_fk,
        mcp.product_fk,
        c.pk country_fk,
        mcp.manual_collection_price_attributes_fk
        mcpa_fk,
        mcpa.name mcpa_name,
        mcp.value price,
        mcp.is_promotion
        FROM probedata.manual_collection_price mcp
        INNER JOIN probedata.session ses on mcp.session_fk = ses.pk
        INNER JOIN static.manual_collection_price_attributes mcpa ON mcp.manual_collection_price_attributes_fk = mcpa.pk
        INNER JOIN static.stores st on ses.store_fk = st.pk
        left  JOIN static.regions reg on st.region_fk = reg.pk
        left  JOIN static.countries c on reg.country_fk = c.pk
        WHERE 1=1
        AND mcp.deleted_time IS NULL
        AND ses.session_uid = '{}' 
        """.format(self.session_uid)

        product_attribute_price_data = pd.read_sql_query(query, self.rds_conn.db)

        return product_attribute_price_data

    def get_product_attribute_price_with_scene_data(self):
        """
        This function extracts promotion price details from probedata.manual_collection_price table.
        """
        query = """
        SELECT 
        mcp.creation_time, 
        scif.scene_id scene_fk, 
        mcp.session_fk, 
        p.brand_fk, 
        mcp.product_fk,
        p.name product_name, 
        mcp.manual_collection_price_attributes_fk mcpa_fk, 
        mcpa.name mcpa_name,
        mcp.value price, 
        mcp.is_promotion 
        FROM probedata.manual_collection_price mcp
        INNER JOIN reporting.scene_item_facts scif ON (scif.session_id=mcp.session_fk AND scif.item_id=mcp.product_fk)
        LEFT JOIN static.manual_collection_price_attributes mcpa ON mcp.manual_collection_price_attributes_fk=mcpa.pk
        LEFT JOIN probedata.session ses ON mcp.session_fk=ses.pk
        LEFT JOIN static_new.product p ON p.pk=mcp.product_fk
        WHERE mcp.deleted_time IS NULL 
        AND mcp.session_fk=(SELECT pk FROM probedata.session ses WHERE ses.session_uid='{}')""".format(self.session_uid)

        product_attribute_price_with_scene_data = pd.read_sql_query(query, self.rds_conn.db)

        return product_attribute_price_with_scene_data

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = DIAGEOQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = DIAGEOQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_kpi_result_dict(self, entity_key_pk, kpi_results):

        for kpi_result in kpi_results:
            if entity_key_pk == kpi_result['numerator_id']:
                return kpi_result
        return dict()

    def main_calculation(self, kpi_set_names):
        """
        This function calculates the KPI results.
        # """
        # SOS Out Of The Box kpis
        self.activate_ootb_kpis()

        assortment_res_dict = self.diageo_generator.diageo_global_assortment_function_v2()
        self.commonV2.save_json_to_new_tables(assortment_res_dict)

        # Global assortment kpis - v3 for NEW MOBILE REPORTS use
        assortment_res_dict_v3 = self.diageo_generator.diageo_global_assortment_function_v3()
        self.commonV2.save_json_to_new_tables(assortment_res_dict_v3)

        self.mpis = self.data_provider[Data.MATCHES].merge(
            self.data_provider[Data.SCENES_INFO][['scene_fk', 'template_fk']], on='scene_fk', how='left')
        self.mpis = self.mpis.merge(
            self.data_provider[Data.ALL_TEMPLATES][['template_fk', 'template_name']], on='template_fk', how='left')

        self.mpis['product_fk'] = self.mpis['product_fk'].astype(int)
        self.products['product_fk'] = self.products['product_fk'].astype(int)

        self.mpis = self.mpis.merge(self.products, on='product_fk', how='left')

        for kpi_set_name in kpi_set_names:
            if kpi_set_name == SOWB:
                self.calculate_share_of_wall_bay(kpi_set_name)
            elif kpi_set_name == PRICE_PROMOTION:
                self.calculate_number_of_price_promotion(kpi_set_name)
            # elif kpi_set_name == FSOS:
            #     self.calculate_facings_sos(kpi_set_name)
            elif kpi_set_name == SOD:
                self.calculate_share_of_display(kpi_set_name)
            elif kpi_set_name == SOD_GROUPED_SCENES:
                self.calculate_share_of_display_grouped_scenes(kpi_set_name)
            else:
                continue
        self.commonV2.commit_results_data()

    # def calculate_facings_sos(self, kpi_set_name):
    #     template_kpis = self.template_data[self.template_data[DIAGEOGTRConsts.KPI_SET_NAME] == kpi_set_name]
    #
    #     for row, template_kpi in template_kpis.iterrows():
    #         entity_1 = template_kpi[DIAGEOGTRConsts.ENTITY_1].strip().lower()
    #
    #         kpi_name = template_kpi[DIAGEOGTRConsts.KPI_NAME]
    #
    #         template = template_kpi[DIAGEOGTRConsts.SCENE_POLICY].split(",")
    #         template_names = [scene.strip().encode(DIAGEOGTRConsts.UTF_8) for scene in template]
    #
    #         entity_key_1 = entities[entity_1] if entity_1 in entities.keys() else None
    #
    #         self.calculate_facings_sos_entity(kpi_name, template_names, entity_key_1)
    #
    # def calculate_facings_sos_entity(self, kpi_name, template_names, entity_key_1):
    #     kpi_results = {}
    #     # new static kpi table check
    #     kpi_static_new = self.kpi_static_data_new[self.kpi_static_data_new[DIAGEOGTRConsts.TYPE] == kpi_name]
    #
    #     if kpi_static_new.empty:
    #         if DIAGEOGTRToolBox.DEBUG:
    #             print("kpi_name(type - new KPI)={} not found in DB".format(kpi_name))
    #         kpi_level_2_fk = 0
    #     else:
    #         kpi_level_2_fk = kpi_static_new['pk'].iloc[0]
    #
    #     for template_name in template_names:
    #         df_scene_info = self.scene_info[self.scene_info[DIAGEOGTRConsts.TEMPLATE_NAME] == template_name]
    #
    #         if df_scene_info.empty:
    #             if DIAGEOGTRToolBox.DEBUG:
    #                 print (" scene_policy:{} is not matching with template_name(in DB) ".format(template_name))
    #             continue
    #
    #         for row, scene_info in df_scene_info.iterrows():
    #             scene_fk = scene_info[DIAGEOGTRConsts.SCENE_FK]
    #             template_fk = scene_info[DIAGEOGTRConsts.TEMPLATE_FK]
    #
    #             df_scene = self.scif[self.scif[DIAGEOGTRConsts.SCENE_FK] == scene_fk]
    #
    #             if df_scene.empty:
    #                 if DIAGEOGTRToolBox.DEBUG:
    #                     print("scene_fk:{} not matching".format(scene_fk))
    #                 continue
    #
    #             df_fsos = df_scene.groupby([entity_key_1], as_index=False)[DIAGEOGTRConsts.FACINGS].sum()
    #
    #             if df_fsos.empty:
    #                 if DIAGEOGTRToolBox.DEBUG:
    #                     print "No SKUs"
    #                 continue
    #
    #             for row_num, row_data in df_fsos.iterrows():
    #                 entity_key_value = str(int(template_fk)) + "-" + str(int(row_data[entity_key_1]))
    #                 kpi_result = kpi_results.get(entity_key_value, dict())
    #                 kpi_result[DIAGEOGTRConsts.NUMERATOR_ID] = row_data[entity_key_1]
    #                 kpi_result[DIAGEOGTRConsts.DENOMINATOR_ID] = template_fk
    #                 kpi_result[DIAGEOGTRConsts.CONTEXT_ID] = self.store_id
    #
    #                 if DIAGEOGTRConsts.FACINGS in kpi_result.keys():
    #                     kpi_result[DIAGEOGTRConsts.FACINGS] = kpi_result[DIAGEOGTRConsts.FACINGS] + \
    #                                                           row_data[DIAGEOGTRConsts.FACINGS]
    #                 else:
    #                     kpi_result[DIAGEOGTRConsts.FACINGS] = row_data[DIAGEOGTRConsts.FACINGS]
    #
    #                 kpi_results[entity_key_value] = kpi_result
    #
    #     for key, kpi_result in kpi_results.items():
    #         kpi_result[DIAGEOGTRConsts.KPI_LEVEL_2_FK] = kpi_level_2_fk
    #
    #         if kpi_level_2_fk != 0:
    #             self.commonV2.write_to_db_result(fk=kpi_result[DIAGEOGTRConsts.KPI_LEVEL_2_FK],
    #                                              numerator_id=kpi_result[DIAGEOGTRConsts.NUMERATOR_ID],
    #                                              denominator_id=kpi_result[DIAGEOGTRConsts.DENOMINATOR_ID],
    #                                              context_id=kpi_result[DIAGEOGTRConsts.CONTEXT_ID],
    #                                              numerator_result=kpi_result[DIAGEOGTRConsts.FACINGS],
    #                                              result=kpi_result[DIAGEOGTRConsts.FACINGS],
    #                                              score=0)
    #     kpi_results = {}

    def calculate_assortment_sets(self, set_name):
        """
        This function calculates every Assortment-typed KPI from the relevant sets, and returns the set final score.
        """
        scores = []
        for params in self.set_templates_data[set_name]:
            target = str(params.get(self.store_type, ''))
            if target.isdigit() or target.capitalize() in (self.tools.RELEVANT_FOR_STORE, self.tools.OR_OTHER_PRODUCTS):
                products = str(params.get(self.tools.PRODUCT_EAN_CODE,
                                          params.get(self.tools.PRODUCT_EAN_CODE2, ''))).replace(',', ' ').split()
                target = 1 if not target.isdigit() else int(target)
                kpi_name = params.get(self.tools.GROUP_NAME, params.get(self.tools.PRODUCT_NAME))
                kpi_static_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                                       (self.kpi_static_data['kpi_name'] == kpi_name)]
                if len(products) > 1:
                    result = 0
                    for product in products:
                        product_score = self.tools.calculate_assortment(product_ean_code=product)
                        result += product_score
                        try:
                            product_name = \
                                self.all_products[self.all_products['product_ean_code'] == product][
                                    'product_name'].values[
                                    0]
                        except Exception as e:
                            Log.warning('Product {} is not defined in the DB'.format(product))
                            continue
                        try:
                            atomic_fk = \
                                kpi_static_data[kpi_static_data['atomic_kpi_name'] == product_name][
                                    'atomic_kpi_fk'].values[
                                    0]
                        except Exception as e:
                            Log.warning('Product {} is not defined in the DB'.format(product_name))
                            continue
                        self.write_to_db_result(atomic_fk, product_score, level=self.LEVEL3)
                    score = 1 if result >= target else 0
                else:
                    result = self.tools.calculate_assortment(product_ean_code=products)
                    atomic_fk = kpi_static_data['atomic_kpi_fk'].values[0]
                    score = 1 if result >= target else 0
                    self.write_to_db_result(atomic_fk, score, level=self.LEVEL3)

                scores.append(score)
                kpi_fk = kpi_static_data['kpi_fk'].values[0]
                self.write_to_db_result(kpi_fk, score, level=self.LEVEL2)

        if not scores:
            return False
        set_score = (sum(scores) / float(len(scores))) * 100
        return set_score

    def calculate_activation_standard(self):
        """
        This function calculates the Activation Standard KPI, and saves the result to the DB (for all 3 levels).
        """
        final_score = 0
        for params in self.tools.download_template(self.ACTIVATION_STANDARD):
            set_name = params.get(self.tools.ACTIVATION_SET_NAME)
            kpi_name = params.get(self.tools.ACTIVATION_KPI_NAME)
            target = float(params.get(self.tools.ACTIVATION_TARGET))
            target = target * 100 if target < 1 else target
            score_type = params.get(self.tools.ACTIVATION_SCORE)
            weight = float(params.get(self.tools.ACTIVATION_WEIGHT))
            if kpi_name:
                kpi_fk = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                              (self.kpi_static_data['kpi_name'] == kpi_name)]['kpi_fk'].values[0]
                score = self.scores[self.LEVEL2].get(kpi_fk, 0)
            else:
                set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_name]['kpi_set_fk'].values[0]
                score = self.scores[self.LEVEL1].get(set_fk, 0)
            if score >= target:
                score = 100
            else:
                if score_type == 'PROPORTIONAL':
                    score = (score / float(target)) * 100
                else:
                    score = 0
            final_score += score * weight
            self.save_level2_and_level3(self.ACTIVATION_STANDARD, set_name, score)
        set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] ==
                                      self.ACTIVATION_STANDARD]['kpi_set_fk'].values[0]
        self.write_to_db_result(set_fk, final_score, self.LEVEL1)

    def calculate_share_of_display_grouped_scenes(self, kpi_set_name):
        """
        This function will handle multiple scene types captured in one scene. This requirement is from DIAGEOGTR.
        sales reps/auditors will capture same scene types scenes under one scene. Recognition team will each scene as bay
        in one scene type.
        """

        template_kpis = self.template_data[self.template_data[DIAGEOGTRConsts.KPI_SET_NAME] == kpi_set_name]

        for row, template_kpi in template_kpis.iterrows():
            entity_1 = template_kpi[DIAGEOGTRConsts.ENTITY_1].strip().lower()
            entity_2 = template_kpi[DIAGEOGTRConsts.ENTITY_2].strip().lower()
            entity_3 = template_kpi[DIAGEOGTRConsts.ENTITY_3].strip().lower()
            exclude_empty = template_kpi[DIAGEOGTRConsts.EXCLUDE_EMPTY].strip().lower()
            exclude_irrelevant = template_kpi[DIAGEOGTRConsts.EXCLUDE_IRRELEVANT].strip().lower()

            kpi_name = template_kpi[DIAGEOGTRConsts.KPI_NAME]
            template = template_kpi[DIAGEOGTRConsts.SCENE_POLICY].split(",")
            template_names = [scene.strip().encode(DIAGEOGTRConsts.UTF_8) for scene in template]

            entity_key_1 = entities[entity_1] if entity_1 in entities.keys() else None
            entity_key_2 = entities[entity_2] if entity_2 in entities.keys() else None
            entity_key_3 = entities[entity_3] if entity_3 in entities.keys() else None

            if entity_key_2 or entity_key_3:
                self.calculate_share_of_display_grouped_scenes_entity(kpi_name, template_names,
                                                                      entity_key_1, entity_key_2, entity_key_3,
                                                                      exclude_empty,
                                                                      exclude_irrelevant)
            else:
                self.calculate_share_of_display_grouped_scenes_single_entity(kpi_set_name, kpi_name, template_names,
                                                                             entity_key_1, exclude_empty,
                                                                             exclude_irrelevant)

    def calculate_share_of_display_grouped_scenes_entity(self, kpi_name, template_names, entity_key_1,
                                                         entity_key_2, entity_key_3, exclude_empty,
                                                         exclude_irrelevant):
        entity_key_0 = DIAGEOGTRConsts.BAY_NUMBER
        entity_key_4 = DIAGEOGTRConsts.PRODUCT_FK

        kpi_results = {}

        # new static kpi table check
        kpi_static_new = self.kpi_static_data_new[self.kpi_static_data_new[DIAGEOGTRConsts.TYPE] == kpi_name]

        if kpi_static_new.empty:
            if DIAGEOGTRToolBox.DEBUG:
                print("kpi_name(type - new KPI)={} not found in DB".format(kpi_name))
            kpi_level_2_fk = 0
        else:
            kpi_level_2_fk = kpi_static_new['pk'].iloc[0]

        for template_name in template_names:
            df_scene_info = self.scene_info[self.scene_info[DIAGEOGTRConsts.TEMPLATE_NAME] == template_name]

            if df_scene_info.empty:
                if DIAGEOGTRToolBox.DEBUG:
                    print (" scene_policy:{} is not matching with template_name(in DB) ".format(template_name))
                continue

            denominator_count = 0
            for row, scene_info in df_scene_info.iterrows():
                scene_fk = scene_info[DIAGEOGTRConsts.SCENE_FK]
                template_fk = scene_info[DIAGEOGTRConsts.TEMPLATE_FK]

                df_scene = self.mpis[self.mpis[DIAGEOGTRConsts.SCENE_FK] == scene_fk]

                if df_scene.empty:
                    if DIAGEOGTRToolBox.DEBUG:
                        print("scene_fk:{} not matching".format(scene_fk))
                    continue

                df_max_bay = df_scene.groupby([DIAGEOGTRConsts.SCENE_FK], as_index=False)[entity_key_0].max()

                if df_max_bay.empty:
                    if DIAGEOGTRToolBox.DEBUG:
                        print "Number of bays is zero"
                    continue

                denominator_count += df_max_bay[DIAGEOGTRConsts.BAY_NUMBER].iloc[0]
                total_num_of_displays = df_max_bay[DIAGEOGTRConsts.BAY_NUMBER].iloc[0]
                total_num_of_displays_2 = df_max_bay[DIAGEOGTRConsts.BAY_NUMBER].iloc[0]

                for bay_number in range(1, total_num_of_displays + 1):
                    df_bay = df_scene[df_scene[DIAGEOGTRConsts.BAY_NUMBER] == bay_number]
                    if df_bay.empty:
                        continue

                    df_bay = df_bay[[entity_key_0, entity_key_1, entity_key_2, entity_key_3, entity_key_4]]

                    if exclude_empty == 'y':
                        df_bay = df_bay[df_bay[DIAGEOGTRConsts.PRODUCT_FK] != DIAGEOGTRConsts.EMPTY]

                    if exclude_irrelevant == 'y':
                        df_bay = df_bay[df_bay[DIAGEOGTRConsts.PRODUCT_FK] != DIAGEOGTRConsts.IRRELEVANT]

                    df_bay = df_bay.fillna(DIAGEOGTRConsts.EMPTY_ENTITY)
                    df_bay_gb_columns = [entity_key_0, entity_key_1, entity_key_2, entity_key_3]
                    df_bay_group = pd.DataFrame(df_bay.groupby(df_bay_gb_columns).size().reset_index(name="count"))

                    if df_bay_group.empty:
                        if DIAGEOGTRToolBox.DEBUG:
                            print("GroupBy Empty:{},{},{},{}".format(entity_key_0, entity_key_1,
                                                                     entity_key_2, entity_key_3))
                        continue

                    if len(df_bay_group) == 1:
                        entity_key_value = str(int(template_fk))
                        entity_key_value += "-" + str(int(df_bay_group.iloc[0][entity_key_1]))  # sub_category
                        entity_key_value += "-" + str(int(df_bay_group.iloc[0][entity_key_2]))  # manufacturer
                        entity_key_value += "-" + str(int(df_bay_group.iloc[0][entity_key_3]))  # brand
                        kpi_result = kpi_results.get(entity_key_value, dict())
                        kpi_result[DIAGEOGTRConsts.NUMERATOR_ID] = df_bay_group.iloc[0][entity_key_3]  # brand
                        kpi_result[DIAGEOGTRConsts.DENOMINATOR_ID] = template_fk
                        kpi_result[DIAGEOGTRConsts.CONTEXT_ID] = df_bay_group.iloc[0][entity_key_1]  # sub_category
                        kpi_result[DIAGEOGTRConsts.SCORE_AFTER_ACTIONS] = bay_number
                        kpi_result[DIAGEOGTRConsts.DENOMINATOR_RESULT_AFTER_ACTIONS] = df_bay_group.iloc[0][
                            entity_key_2]  # Manufacturer
                    else:
                        entity_key_value = str(int(template_fk))
                        kpi_result = kpi_results.get(entity_key_value, dict())
                        kpi_result[DIAGEOGTRConsts.NUMERATOR_ID] = 0
                        kpi_result[DIAGEOGTRConsts.DENOMINATOR_ID] = template_fk
                        kpi_result[DIAGEOGTRConsts.CONTEXT_ID] = 0
                        kpi_result[DIAGEOGTRConsts.SCORE_AFTER_ACTIONS] = bay_number
                        kpi_result[DIAGEOGTRConsts.DENOMINATOR_RESULT_AFTER_ACTIONS] = 0

                    if DIAGEOGTRConsts.NUM_OF_DISPLAYS in kpi_result.keys():
                        kpi_result[DIAGEOGTRConsts.NUM_OF_DISPLAYS] = kpi_result[DIAGEOGTRConsts.NUM_OF_DISPLAYS] + 1
                    else:
                        kpi_result[DIAGEOGTRConsts.NUM_OF_DISPLAYS] = 1

                    kpi_result[DIAGEOGTRConsts.TOTAL_NUM_OF_DISPLAYS] = total_num_of_displays_2
                    kpi_results[entity_key_value] = kpi_result

            for key, kpi_result in kpi_results.items():
                if denominator_count != 0:
                    numerator = kpi_result[DIAGEOGTRConsts.NUM_OF_DISPLAYS]
                    denominator = float(denominator_count)
                    score_pure_displays = round(numerator / denominator, 6)
                else:
                    score_pure_displays = 0

                kpi_result['score_per_displays'] = score_pure_displays
                kpi_result[DIAGEOGTRConsts.KPI_LEVEL_2_FK] = kpi_level_2_fk

                if kpi_level_2_fk != 0:
                    self.commonV2.write_to_db_result(fk=kpi_result[DIAGEOGTRConsts.KPI_LEVEL_2_FK],
                                                     numerator_id=kpi_result[DIAGEOGTRConsts.NUMERATOR_ID],
                                                     denominator_id=kpi_result[DIAGEOGTRConsts.DENOMINATOR_ID],
                                                     context_id=kpi_result[DIAGEOGTRConsts.CONTEXT_ID],
                                                     numerator_result=kpi_result[DIAGEOGTRConsts.NUM_OF_DISPLAYS],
                                                     denominator_result=denominator_count,
                                                     denominator_result_after_actions=kpi_result[
                                                         DIAGEOGTRConsts.DENOMINATOR_RESULT_AFTER_ACTIONS],
                                                     score_after_actions=kpi_result[
                                                         DIAGEOGTRConsts.SCORE_AFTER_ACTIONS],
                                                     result=kpi_result['score_per_displays'],
                                                     score=kpi_result['score_per_displays'])

            kpi_results = {}

    def calculate_share_of_display_grouped_scenes_single_entity(self, kpi_set_name, kpi_name, template_names,
                                                                entity_key_1, exclude_empty, exclude_irrelevant):

        entity_key_0 = DIAGEOGTRConsts.BAY_NUMBER

        kpi_results = {}

        # old static kpi table check
        kpi_static = self.kpi_static_data[
            (self.kpi_static_data[DIAGEOGTRConsts.KPI_SET_NAME] == kpi_set_name) &
            (self.kpi_static_data[DIAGEOGTRConsts.KPI_ATOMIC_NAME] == kpi_name)]

        if kpi_static.empty:
            if DIAGEOGTRToolBox.DEBUG:
                atomic_kpi_fk = 0
                print("kpi_atomic_name={}, kpi_name={} not found in DB".format(kpi_name, kpi_name))
        else:
            atomic_kpi_fk = kpi_static.iloc[0][DIAGEOGTRConsts.ATOMIC_KPI_FK]

        # new static kpi table check
        kpi_static_new = self.kpi_static_data_new[self.kpi_static_data_new[DIAGEOGTRConsts.TYPE] == kpi_name]
        if kpi_static_new.empty:
            if DIAGEOGTRToolBox.DEBUG:
                print("kpi_name(type - new KPI)={} not found in DB".format(kpi_name))
            kpi_level_2_fk = 0
        else:
            kpi_level_2_fk = kpi_static_new['pk'].iloc[0]

        for template_name in template_names:
            df_scene_info = self.scene_info[self.scene_info[DIAGEOGTRConsts.TEMPLATE_NAME] == template_name]

            if df_scene_info.empty:
                if DIAGEOGTRToolBox.DEBUG:
                    print (" scene_policy:{} is not matching with template_name(in DB) ".format(template_name))
                continue

            denominator_count = 0

            for row, scene_info in df_scene_info.iterrows():
                scene_fk = scene_info[DIAGEOGTRConsts.SCENE_FK]
                template_fk = scene_info[DIAGEOGTRConsts.TEMPLATE_FK]

                df_scene = self.mpis[self.mpis[DIAGEOGTRConsts.SCENE_FK] == scene_fk]

                if df_scene.empty:
                    if DIAGEOGTRToolBox.DEBUG:
                        print("scene_fk:{} not matching".format(scene_fk))
                    continue

                df_max_bay = df_scene.groupby([DIAGEOGTRConsts.SCENE_FK], as_index=False)[entity_key_0].max()

                if df_max_bay.empty:
                    if DIAGEOGTRToolBox.DEBUG:
                        print "Number of bays is zero"
                    continue

                denominator_count += df_max_bay[DIAGEOGTRConsts.BAY_NUMBER].iloc[0]
                total_num_of_displays = df_max_bay[DIAGEOGTRConsts.BAY_NUMBER].iloc[0]
                total_num_of_displays_2 = df_max_bay[DIAGEOGTRConsts.BAY_NUMBER].iloc[0]

                for bay_number in range(1, total_num_of_displays + 1):
                    df_bay = df_scene[df_scene[DIAGEOGTRConsts.BAY_NUMBER] == bay_number]
                    if df_bay.empty:
                        continue

                    df_bay = df_bay[[entity_key_0, entity_key_1, DIAGEOGTRConsts.PRODUCT_FK]]

                    if exclude_empty == 'y':
                        df_bay = df_bay[df_bay[DIAGEOGTRConsts.PRODUCT_FK] != DIAGEOGTRConsts.EMPTY]  # ex-empty

                    if exclude_irrelevant == 'y':
                        df_bay = df_bay[df_bay[DIAGEOGTRConsts.PRODUCT_FK] != DIAGEOGTRConsts.IRRELEVANT]  # ex-empty

                    df_bay_group = pd.DataFrame(df_bay.fillna(-1).groupby(
                        [entity_key_0, entity_key_1]).size().reset_index(name="count"))

                    if df_bay_group.empty:
                        if DIAGEOGTRToolBox.DEBUG:
                            print("Group by did not return any results:{},{}".format(entity_key_0, entity_key_1))
                        continue

                    if len(df_bay_group) == 1:
                        entity_key_value = str(int(template_fk))
                        entity_key_value += "-" + str(int(df_bay_group.iloc[0][entity_key_1]))  # sub_category
                        kpi_result = kpi_results.get(entity_key_value, dict())
                        kpi_result[DIAGEOGTRConsts.NUMERATOR_ID] = df_bay_group.iloc[0][entity_key_1]
                        kpi_result[DIAGEOGTRConsts.DENOMINATOR_ID] = template_fk
                        kpi_result[DIAGEOGTRConsts.CONTEXT_ID] = df_bay_group.iloc[0][entity_key_0]
                    else:
                        entity_key_value = "0-0"
                        kpi_result = kpi_results.get(entity_key_value, dict())
                        kpi_result[DIAGEOGTRConsts.NUMERATOR_ID] = 0
                        kpi_result[DIAGEOGTRConsts.DENOMINATOR_ID] = template_fk
                        kpi_result[DIAGEOGTRConsts.CONTEXT_ID] = 0

                    if DIAGEOGTRConsts.NUM_OF_DISPLAYS in kpi_result.keys():
                        kpi_result[DIAGEOGTRConsts.NUM_OF_DISPLAYS] = kpi_result[DIAGEOGTRConsts.NUM_OF_DISPLAYS] + 1
                    else:
                        kpi_result[DIAGEOGTRConsts.NUM_OF_DISPLAYS] = 1

                    kpi_result[DIAGEOGTRConsts.TOTAL_NUM_OF_DISPLAYS] = total_num_of_displays_2
                    kpi_results[entity_key_value] = kpi_result

            for key, kpi_result in kpi_results.items():
                if kpi_result[DIAGEOGTRConsts.TOTAL_NUM_OF_DISPLAYS] != 0:
                    score_pure_displays = round(
                        kpi_result[DIAGEOGTRConsts.NUM_OF_DISPLAYS] / float(
                            kpi_result[DIAGEOGTRConsts.TOTAL_NUM_OF_DISPLAYS]), 6)
                else:
                    score_pure_displays = 0

                kpi_result['score_per_displays'] = score_pure_displays
                kpi_result[DIAGEOGTRConsts.KPI_LEVEL_2_FK] = kpi_level_2_fk

                if kpi_level_2_fk != 0:
                    self.commonV2.write_to_db_result(fk=kpi_result[DIAGEOGTRConsts.KPI_LEVEL_2_FK],
                                                     numerator_id=kpi_result[DIAGEOGTRConsts.NUMERATOR_ID],
                                                     denominator_id=kpi_result[DIAGEOGTRConsts.DENOMINATOR_ID],
                                                     context_id=kpi_result[DIAGEOGTRConsts.CONTEXT_ID],
                                                     numerator_result=kpi_result[DIAGEOGTRConsts.NUM_OF_DISPLAYS],
                                                     denominator_result=denominator_count,
                                                     result=kpi_result['score_per_displays'],
                                                     score=kpi_result['score_per_displays'])
            kpi_results = {}

    def calculate_share_of_display(self, kpi_set_name):
        template_kpis = self.template_data[self.template_data[DIAGEOGTRConsts.KPI_SET_NAME] == kpi_set_name]

        for row, template_kpi in template_kpis.iterrows():
            entity_1 = template_kpi[DIAGEOGTRConsts.ENTITY_1].strip().lower()
            entity_2 = template_kpi[DIAGEOGTRConsts.ENTITY_2].strip().lower()
            entity_3 = template_kpi[DIAGEOGTRConsts.ENTITY_3].strip().lower()

            kpi_name = template_kpi[DIAGEOGTRConsts.KPI_NAME]
            template = template_kpi[DIAGEOGTRConsts.SCENE_POLICY].split(",")
            template_names = [scene.strip().encode(DIAGEOGTRConsts.UTF_8) for scene in template]

            entity_key_1 = entities[entity_1] if entity_1 in entities.keys() else None
            entity_key_2 = entities[entity_2] if entity_2 in entities.keys() else None
            entity_key_3 = entities[entity_3] if entity_3 in entities.keys() else None

            self.calculate_share_of_display_entity(kpi_set_name, kpi_name, template_names,
                                                   entity_key_1, entity_key_2, entity_key_3)

    def calculate_share_of_display_entity(self, kpi_set_name, kpi_name, template_names, entity_key_1, entity_key_2,
                                          entity_key_3):
        kpi_results = {}

        kpi_static = self.kpi_static_data[
            (self.kpi_static_data[DIAGEOGTRConsts.KPI_SET_NAME] == kpi_set_name) &
            (self.kpi_static_data[DIAGEOGTRConsts.KPI_ATOMIC_NAME] == kpi_name)]

        atomic_kpi_fk = 0
        if kpi_static.empty:
            if DIAGEOGTRToolBox.DEBUG:
                print("kpi_set_name={}, kpi_atomic_name={} not found in DB".format(kpi_set_name, kpi_name))
        else:
            atomic_kpi_fk = kpi_static.iloc[0][DIAGEOGTRConsts.ATOMIC_KPI_FK]

        kpi_static_new = self.kpi_static_data_new[self.kpi_static_data_new['type'] == kpi_name]

        if kpi_static_new.empty:
            if DIAGEOGTRToolBox.DEBUG:
                print("kpi_name(type - new KPI)={} not found in DB".format(kpi_name))
            return None

        kpi_level_2_fk = kpi_static_new['pk'].iloc[0]

        if DIAGEOGTRToolBox.DEBUG:
            print "atomic_kpi_fk={}, kpi_level_2_fk={}".format(atomic_kpi_fk, kpi_level_2_fk)

        total_num_of_displays = 0
        for template_name in template_names:
            df_scif = self.scif[self.scif[DIAGEOGTRConsts.TEMPLATE_NAME] == template_name]

            if df_scif.empty:
                if DIAGEOGTRToolBox.DEBUG:
                    print ("scene_policy:{} is not matching with template_name(in DB) ".format(template_name))
                continue

            total_num_of_displays += len(df_scif)

            scene_fk = 0
            for row, scene_info in df_scif.iterrows():
                scene_fk = scene_info[DIAGEOGTRConsts.SCENE_FK]
                df_scif = self.scif[self.scif[DIAGEOGTRConsts.SCENE_FK] == scene_fk]

                if df_scif.empty:
                    if DIAGEOGTRToolBox.DEBUG:
                        print("scene_fk:{} not matching".format(scene_fk))
                    continue

                try:
                    df_entity_key = pd.DataFrame(
                        df_scif.groupby([entity_key_1, entity_key_2, entity_key_3]).size().reset_index(name="count"))
                except Exception as ex:
                    print (ex)
                    continue

                if len(df_entity_key) == 1:
                    entity_key_pk = df_entity_key.iloc[0][entity_key_3]
                    dict_key = str(entity_key_1) + "-" + str(entity_key_2) + "-" + str(entity_key_3)
                else:
                    entity_key_pk = 0
                    dict_key = "0-0-0"

                kpi_result = kpi_results.get(dict_key, dict())
                kpi_result[DIAGEOGTRConsts.NUMERATOR_ID] = entity_key_pk
                kpi_result[DIAGEOGTRConsts.DENOMINATOR_ID] = scene_info[DIAGEOGTRConsts.TEMPLATE_FK]

                if DIAGEOGTRConsts.NUM_OF_DISPLAYS in kpi_result.keys():
                    kpi_result[DIAGEOGTRConsts.NUM_OF_DISPLAYS] = kpi_result[DIAGEOGTRConsts.NUM_OF_DISPLAYS] + 1
                else:
                    kpi_result[DIAGEOGTRConsts.NUM_OF_DISPLAYS] = 1

                kpi_result[DIAGEOGTRConsts.TOTAL_NUM_OF_DISPLAYS] = total_num_of_displays
                kpi_results[entity_key_pk] = kpi_result

        score_pure_displays = 0
        for key, kpi_result in kpi_results.items():
            if total_num_of_displays != 0:
                score_pure_displays = round(kpi_result[DIAGEOGTRConsts.NUM_OF_DISPLAYS] /
                                            float(total_num_of_displays), 2)

            kpi_result['score_per_displays'] = score_pure_displays
            kpi_result[DIAGEOGTRConsts.KPI_LEVEL_2_FK] = kpi_level_2_fk

            self.commonV2.write_to_db_result(fk=kpi_result[DIAGEOGTRConsts.KPI_LEVEL_2_FK],
                                             numerator_id=kpi_result[DIAGEOGTRConsts.NUMERATOR_ID],
                                             denominator_id=kpi_result[DIAGEOGTRConsts.DENOMINATOR_ID],
                                             numerator_result=kpi_result[DIAGEOGTRConsts.NUM_OF_DISPLAYS],
                                             denominator_result=total_num_of_displays,
                                             result=kpi_result['score_per_displays'])

    def calculate_number_of_price_promotion(self, kpi_set_name):
        template_kpis = self.template_data[self.template_data[DIAGEOGTRConsts.KPI_SET_NAME] == kpi_set_name]

        if template_kpis.empty:
            if DIAGEOGTRToolBox.DEBUG:
                print ("No KPIs matching {}".format(kpi_set_name))
            return None

        for row, template_kpi in template_kpis.iterrows():
            entity_1 = template_kpi[DIAGEOGTRConsts.ENTITY_1].strip().lower()
            entity_2 = template_kpi[DIAGEOGTRConsts.ENTITY_2].strip().lower()
            entity_3 = template_kpi[DIAGEOGTRConsts.ENTITY_3].strip().lower()

            entity_1 = entities[entity_1] if len(entity_1) != 0 else ""
            entity_2 = entities[entity_2] if len(entity_2) != 0 else ""
            entity_3 = entities[entity_3] if len(entity_3) != 0 else ""

            kpi_name = template_kpi[DIAGEOGTRConsts.KPI_NAME]

            if kpi_name == PRICE_PROMOTION_SKU:
                self.calculate_number_of_price_promotion_entity(kpi_set_name, kpi_name)
            else:
                self.calculate_number_of_price_promotion_entity_with_scene(kpi_set_name, kpi_name, entity_1, entity_2,
                                                                           entity_3)

    def calculate_number_of_price_promotion_entity_with_scene(self, kpi_set_name, kpi_name,
                                                              entity_key_1, entity_key_2=None, entity_key_3=None):

        kpi_static = self.kpi_static_data[
            (self.kpi_static_data[DIAGEOGTRConsts.KPI_SET_NAME] == kpi_set_name) &
            (self.kpi_static_data[DIAGEOGTRConsts.KPI_ATOMIC_NAME] == kpi_name)]

        if kpi_static.empty:
            if DIAGEOGTRToolBox.DEBUG:
                print("kpi_set_name={}, kpi_atomic_name={} not found in DB".format(kpi_set_name, kpi_name))
            atomic_kpi_fk = 0
        else:
            atomic_kpi_fk = kpi_static.iloc[0][DIAGEOGTRConsts.ATOMIC_KPI_FK]

        kpi_static_new = self.kpi_static_data_new[self.kpi_static_data_new['type'] == kpi_name]

        if kpi_static_new.empty:
            if DIAGEOGTRToolBox.DEBUG:
                print("kpi_set_name={}, kpi_atomic_name={} not found in DB".format(kpi_set_name, kpi_name))
            kpi_level_2_fk = 0
        else:
            kpi_level_2_fk = kpi_static_new['pk'].iloc[0]

        if DIAGEOGTRToolBox.DEBUG:
            print "atomic_kpi_fk={}, kpi_level_2_fk={}".format(atomic_kpi_fk, kpi_level_2_fk)

        if kpi_static_new.empty and kpi_static.empty:
            return None

        df_count = pd.DataFrame()

        if kpi_name == PRICE_PROMOTION_BRAND_SKU:
            df_count = pd.DataFrame(
                self.product_attribute_price_with_scene_data.groupby(
                    [entity_key_1, entity_key_2]).size().reset_index(name="count"))
        elif kpi_name == PRICE_PROMOTION_SCENE_BRAND_SKU:
            df_count = pd.DataFrame(
                self.product_attribute_price_with_scene_data.groupby(
                    [entity_key_1, entity_key_2, entity_key_3]).size().reset_index(name="count"))
        else:
            if DIAGEOGTRToolBox.DEBUG:
                print ("kpi_name is not found in DB{}".format(kpi_name))

        if df_count.empty:
            if kpi_name == PRICE_PROMOTION_BRAND_SKU:
                if DIAGEOGTRToolBox.DEBUG:
                    print ("No promotion price for entities:{},{}".format(entity_key_1, entity_key_2))
            elif kpi_name == PRICE_PROMOTION_SCENE_BRAND_SKU:
                if DIAGEOGTRToolBox.DEBUG:
                    print ("No promotion price for entities:{},{},{}".format(entity_key_1, entity_key_2, entity_key_3))
        else:
            for row_count, row_data in df_count.iterrows():
                price_promotion_count = row_data['count']

                if kpi_name == PRICE_PROMOTION_BRAND_SKU:
                    numerator_id = row_data[entity_key_2]
                    denominator_id = row_data[entity_key_1]
                    context_id = self.store_id
                elif kpi_name == PRICE_PROMOTION_SCENE_BRAND_SKU:
                    numerator_id = row_data[entity_key_3]
                    denominator_id = row_data[entity_key_2]
                    context_id = row_data[entity_key_1]
                else:
                    numerator_id = 0
                    denominator_id = 0
                    context_id = 0

                self.commonV2.write_to_db_result(fk=kpi_level_2_fk,
                                                 numerator_id=numerator_id,
                                                 numerator_result=price_promotion_count,
                                                 denominator_id=denominator_id,
                                                 denominator_result=0,
                                                 context_id=context_id,
                                                 result=0,
                                                 score=0)

    def calculate_number_of_price_promotion_entity(self, kpi_set_name, kpi_name):
        kpi_static = self.kpi_static_data[
            (self.kpi_static_data[DIAGEOGTRConsts.KPI_SET_NAME] == kpi_set_name) &
            (self.kpi_static_data[DIAGEOGTRConsts.KPI_ATOMIC_NAME] == kpi_name)]

        if kpi_static.empty:
            if DIAGEOGTRToolBox.DEBUG:
                print("kpi_set_name={}, kpi_atomic_name={} not found in DB".format(kpi_set_name, kpi_name))
            atomic_kpi_fk = 0
        else:
            atomic_kpi_fk = kpi_static.iloc[0][DIAGEOGTRConsts.ATOMIC_KPI_FK]

        kpi_static_new = self.kpi_static_data_new[self.kpi_static_data_new['type'] == kpi_name]

        if kpi_static_new.empty:
            if DIAGEOGTRToolBox.DEBUG:
                print("kpi_set_name={}, kpi_atomic_name={} not found in DB".format(kpi_set_name, kpi_name))
            kpi_level_2_fk = 0
        else:
            kpi_level_2_fk = kpi_static_new['pk'].iloc[0]

        if DIAGEOGTRToolBox.DEBUG:
            print "atomic_kpi_fk={}, kpi_level_2_fk={}".format(atomic_kpi_fk, kpi_level_2_fk)

        if kpi_static_new.empty and kpi_static.empty:
            return None

        if kpi_name == PRICE_PROMOTION_SKU:
            df_price = self.product_attribute_price_data
        else:
            if DIAGEOGTRToolBox.DEBUG:
                print ("kpi_name:{} not found in DB".format(kpi_name))
            return

        if df_price.empty:
            if DIAGEOGTRToolBox.DEBUG:
                print ("No price details captured manually")
            return

        for row_count, row_data in df_price.iterrows():
            numerator_id = row_data['product_fk']  # product_fk
            denominator_id = row_data['country_fk']  # country_fk
            numerator_result = row_data['is_promotion']  # is an integer field
            result = row_data['price']  # Price can be float so storing it in result
            score = row_data['price']  # Price can be float so storing it in score

            self.commonV2.write_to_db_result(fk=kpi_level_2_fk,
                                             numerator_id=numerator_id,
                                             numerator_result=numerator_result,
                                             denominator_id=denominator_id,
                                             result=result,
                                             score=score)

    def validate_kpi(self, kpi_data):
        validation = True
        store_types = kpi_data[DIAGEOGTRConsts.STORE_TYPE]

        if store_types and self.store_type not in store_types.split(DIAGEOGTRConsts.SEPARATOR):
            validation = False
        return validation

    def write_to_db_result(self, fk, score, level):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(fk, score, level)
        if level == self.LEVEL1:
            table = KPS_RESULT
        elif level == self.LEVEL2:
            table = KPK_RESULT
        elif level == self.LEVEL3:
            table = KPI_RESULT
        else:
            return
        query = insert(attributes, table)
        self.kpi_results_queries.append(query)

    def create_attributes_dict(self, fk, score, level):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score, '.2f'), fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0].replace("'",
                                                                                                                "\\'")
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0].replace("'", "\\'")
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, kpi_fk, fk)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'kpi_fk', 'atomic_kpi_fk'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    def save_level2_and_level3(self, set_name, kpi_name, score):
        """
        Given KPI data and a score, this functions writes the score for both KPI level 2 and 3 in the DB.
        """
        kpi_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                        (self.kpi_static_data['kpi_name'] == kpi_name)]
        kpi_fk = kpi_data['kpi_fk'].values[0]
        atomic_kpi_fk = kpi_data['atomic_kpi_fk'].values[0]
        self.write_to_db_result(kpi_fk, score, self.LEVEL2)
        self.write_to_db_result(atomic_kpi_fk, score, self.LEVEL3)

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        insert_queries = self.merge_insert_queries(self.kpi_results_queries)
        cur = self.rds_conn.db.cursor()
        delete_queries = DIAGEOQueries.get_delete_session_results_query_old_tables(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in insert_queries:
            cur.execute(query)
        self.rds_conn.db.commit()

    @staticmethod
    def merge_insert_queries(insert_queries):
        query_groups = {}
        for query in insert_queries:
            static_data, inserted_data = query.split('VALUES ')
            if static_data not in query_groups:
                query_groups[static_data] = []
            query_groups[static_data].append(inserted_data)
        merged_queries = []
        for group in query_groups:
            merged_queries.append('{0} VALUES {1}'.format(group, ',\n'.join(query_groups[group])))
        return merged_queries

    def calculate_share_of_wall_bay(self, kpi_set_name):
        template_kpis = self.template_data[self.template_data[DIAGEOGTRConsts.KPI_SET_NAME] == kpi_set_name]

        for row, template_kpi in template_kpis.iterrows():
            entity_1 = template_kpi[DIAGEOGTRConsts.ENTITY_1].strip().lower()
            kpi_name = template_kpi[DIAGEOGTRConsts.KPI_NAME]
            entity_key_1 = entities[entity_1] if entity_1 in entities.keys() else None

            if kpi_name == 'SHARE_OF_WALL_BAY_LENGTH':
                self.calculate_share_of_wall_bay_entity(kpi_set_name, kpi_name)
            else:
                self.calculate_share_of_wall_bay_entity_with_scene(kpi_set_name, kpi_name, entity_key_1)

    def calculate_share_of_wall_bay_entity(self, kpi_set_name, kpi_name):
        kpi_static = self.kpi_static_data[
            (self.kpi_static_data[DIAGEOGTRConsts.KPI_SET_NAME] == kpi_set_name) &
            (self.kpi_static_data[DIAGEOGTRConsts.KPI_ATOMIC_NAME] == kpi_name)]

        if kpi_static.empty:
            if DIAGEOGTRToolBox.DEBUG:
                print("kpi_set_name={}, kpi_atomic_name={} not found in DB".format(kpi_set_name, kpi_name))
            atomic_kpi_fk = 0
        else:
            atomic_kpi_fk = kpi_static.iloc[0][DIAGEOGTRConsts.ATOMIC_KPI_FK]

        kpi_static_new = self.kpi_static_data_new[self.kpi_static_data_new['type'] == kpi_name]

        if kpi_static_new.empty:
            if DIAGEOGTRToolBox.DEBUG:
                print("kpi_name(type - new KPI)={} not found in DB".format(kpi_name))
            kpi_level_2_fk = 0
        else:
            kpi_level_2_fk = kpi_static_new['pk'].iloc[0]

        if DIAGEOGTRToolBox.DEBUG:
            print "atomic_kpi_fk={}, kpi_level_2_fk={}".format(atomic_kpi_fk, kpi_level_2_fk)

        df_sowb = self.product_atrribute_length_data

        if df_sowb.empty:
            return

        for row_num, kpi_result in df_sowb.iterrows():
            self.commonV2.write_to_db_result(fk=kpi_level_2_fk,
                                             numerator_id=kpi_result['sub_brand_fk'],
                                             denominator_id=kpi_result['manufacturer_fk'],
                                             context_id=kpi_result['sub_category_fk'],
                                             numerator_result=0,
                                             denominator_result=0,
                                             result=kpi_result['length_in_meters'],
                                             score=kpi_result['length_in_meters'])

    def calculate_share_of_wall_bay_entity_with_scene(self, kpi_set_name, kpi_name, entity_key_1):
        kpi_results = {}

        kpi_static = self.kpi_static_data[
            (self.kpi_static_data[DIAGEOGTRConsts.KPI_SET_NAME] == kpi_set_name) &
            (self.kpi_static_data[DIAGEOGTRConsts.KPI_ATOMIC_NAME] == kpi_name)]

        if kpi_static.empty:
            if DIAGEOGTRToolBox.DEBUG:
                print("kpi_set_name={}, kpi_atomic_name={} not found in DB".format(kpi_set_name, kpi_name))
            atomic_kpi_fk = 0
        else:
            atomic_kpi_fk = kpi_static.iloc[0][DIAGEOGTRConsts.ATOMIC_KPI_FK]

        kpi_static_new = self.kpi_static_data_new[self.kpi_static_data_new['type'] == kpi_name]

        if kpi_static_new.empty:
            if DIAGEOGTRToolBox.DEBUG:
                print("kpi_name(type - new KPI)={} not found in DB".format(kpi_name))
            kpi_level_2_fk = 0
        else:
            kpi_level_2_fk = kpi_static_new['pk'].iloc[0]

        if DIAGEOGTRToolBox.DEBUG:
            print "atomic_kpi_fk={}, kpi_level_2_fk={}".format(atomic_kpi_fk, kpi_level_2_fk)

        df_total_length = pd.DataFrame(
            self.product_attribute_length_with_scene_data.groupby('session_id')['length_in_mm'].sum().reset_index())

        if df_total_length.empty:
            if DIAGEOGTRToolBox.DEBUG:
                print("total length is empty")
            return None

        entity_total_length = df_total_length.iloc[0].length_in_mm

        df_sowb = pd.DataFrame(
            self.product_attribute_length_with_scene_data.groupby(entity_key_1)['length_in_mm'].sum().reset_index())

        if df_sowb.empty:
            if DIAGEOGTRToolBox.DEBUG:
                print "entity={} mismatch".format(entity_key_1)
            return None

        entity_key_pk = 0
        if len(df_sowb) == 1:
            entity_key_pk = df_sowb.iloc[0][entity_key_1]

        for row_num, row_data in df_sowb.iterrows():
            entity_key_pk = row_data[entity_key_1]
            kpi_result = kpi_results.get(entity_key_pk, dict())
            entity_length = row_data['length_in_mm']
            kpi_result[DIAGEOGTRConsts.NUMERATOR_ID] = entity_key_pk
            kpi_result[DIAGEOGTRConsts.DENOMINATOR_ID] = self.store_id

            if 'entity_length' in kpi_result.keys():
                kpi_result['entity_length'] += entity_length
            else:
                kpi_result['entity_length'] = entity_length

            kpi_result['entity_total_length'] = entity_total_length
            kpi_results[entity_key_pk] = kpi_result

        score = 0
        for key, kpi_result in kpi_results.items():
            if kpi_result['entity_total_length'] != 0:
                score = round(kpi_result['entity_length'] / float(kpi_result['entity_total_length']), 2)

            kpi_result['score'] = score
            kpi_result['kpi_level_2_fk'] = kpi_level_2_fk

            self.commonV2.write_to_db_result(fk=kpi_result['kpi_level_2_fk'],
                                             numerator_id=kpi_result[DIAGEOGTRConsts.NUMERATOR_ID],
                                             denominator_id=kpi_result[DIAGEOGTRConsts.DENOMINATOR_ID],
                                             numerator_result=kpi_result['entity_length'],
                                             denominator_result=kpi_result['entity_total_length'],
                                             result=kpi_result['score'])

    def activate_ootb_kpis(self):
        # FACINGS_SOS_MANUFACTURER_IN_WHOLE_STORE - level 1
        sos_store_fk = self.commonV2.get_kpi_fk_by_kpi_name('SOS OUT OF STORE')
        sos_store = ManufacturerFacingsSOSInWholeStore(data_provider=self.data_provider,
                                                       kpi_definition_fk=sos_store_fk).calculate()
        # FACINGS_SOS_CATEGORY_IN_WHOLE_STORE - level 2
        sos_cat_out_of_store_fk = self.commonV2.get_kpi_fk_by_kpi_name('SOS CATEGORY OUT OF STORE')
        sos_cat_out_of_store = self.calculate_sos_of_cat_of_out_of_store_new(sos_cat_out_of_store_fk)

        # FACINGS_SOS_SUB_CATEGORY_OUT_OF_CATEGORY - level 3
        sos_sub_cat_out_of_cat_fk = self.commonV2.get_kpi_fk_by_kpi_name('SOS SUB CATEGORY OUT OF CATEGORY')
        sos_sub_cat_out_of_cat = SubCategoryFacingsSOSPerCategory(data_provider=self.data_provider,
                                                                  kpi_definition_fk=sos_sub_cat_out_of_cat_fk).calculate()

        # FACINGS_SOS_MANUFACTURER_OUT_OF_SUB_CATEGORY - level 4
        sos_man_out_of_sub_cat_fk = self.commonV2.get_kpi_fk_by_kpi_name('SOS MANUFACTURER OUT OF SUB CATEGORY')
        sos_man_out_of_sub_cat = ManufacturerFacingsSOSPerSubCategoryInStore(
            data_provider=self.data_provider, kpi_definition_fk=sos_man_out_of_sub_cat_fk).calculate()

        # FACINGS_SOS_BRAND_OUT_OF_SUB_CATEGORY_IN_WHOLE_STORE - level 5
        sos_brand_out_of_sub_cat_fk = self.commonV2.get_kpi_fk_by_kpi_name('SOS BRAND OUT OF MANUFACTURER')
        sos_brand_out_of_sub_cat = self.calculate_sos_of_brand_out_of_manufacturer_in_sub_cat(
            sos_brand_out_of_sub_cat_fk)

        # Savings results in Hierarchy
        self.save_hierarchy(sos_store, sos_cat_out_of_store, sos_sub_cat_out_of_cat, sos_man_out_of_sub_cat,
                            sos_brand_out_of_sub_cat)

    def calculate_sos_of_brand_out_of_manufacturer_in_sub_cat(self, kpi_fk):
        res_list = []
        res_dict = dict()
        # Get rid of Irrelevant and Empty types and keep only facings > 1
        filtered_scif = self.scif[
            ~self.scif['product_type'].isin(['Irrelevant', 'Empty']) & self.scif['facings_ign_stack'] > 0]

        # Filter by each Sub Category and Manufacturer
        sub_cat_fk_list = filtered_scif['sub_category_fk'].unique().tolist()
        for sub_cat in sub_cat_fk_list:
            filtered_scif_by_sub_cat = filtered_scif[filtered_scif['sub_category_fk'] == sub_cat]
            list_of_relevant_manufacturers = filtered_scif_by_sub_cat['manufacturer_fk'].unique().tolist()
            for manu_fk in list_of_relevant_manufacturers:
                filtered_scif_by_sub_cat_and_manufacturer = filtered_scif_by_sub_cat[
                    filtered_scif_by_sub_cat['manufacturer_fk'] == manu_fk]
                denominator_result = filtered_scif_by_sub_cat_and_manufacturer['facings_ign_stack'].sum()

                # Calculate results per Brand
                list_of_relevant_brands = filtered_scif_by_sub_cat_and_manufacturer['brand_fk'].unique().tolist()
                for brand_fk in list_of_relevant_brands:
                    filtered_scif_by_brand = filtered_scif_by_sub_cat_and_manufacturer[
                        filtered_scif_by_sub_cat_and_manufacturer['brand_fk'] == brand_fk]
                    facings_brand_results = filtered_scif_by_brand['facings_ign_stack'].sum()
                    result_for_brand = facings_brand_results / denominator_result

                    # Preparing the results' dictionary
                    res_dict['kpi_definition_fk'] = kpi_fk
                    res_dict['numerator_id'] = brand_fk
                    res_dict['numerator_result'] = facings_brand_results
                    res_dict['denominator_id'] = int(sub_cat)
                    res_dict['denominator_result'] = denominator_result
                    res_dict['identifier_result'] = (int(brand_fk), int(sub_cat), int(manu_fk))
                    res_dict['identifier_parent'] = int(manu_fk), (int(sub_cat))
                    res_dict['result'] = result_for_brand
                    res_dict['score'] = result_for_brand
                    res_list.append(res_dict.copy())
        return res_list

    def calculate_sos_of_cat_of_out_of_store_new(self, kpi_fk):
        res_list = []
        res_dict = dict()
        # Get rid of Irrelevant and Empty types and keep only facings ignore stacking > 1
        filtered_scif = self.scif[
            ~self.scif['product_type'].isin(['Irrelevant', 'Empty']) & self.scif['facings_ign_stack'] > 0]
        denominator_result = filtered_scif['facings_ign_stack'].sum()
        categories_fk_list = filtered_scif['category_fk'].unique().tolist()

        # Calculate result per category (using facings_ign_stack!)
        for category_fk in categories_fk_list:
            filtered_scif_by_category = filtered_scif[filtered_scif['category_fk'] == category_fk]
            facings_category_result = filtered_scif_by_category['facings_ign_stack'].sum()
            result_for_category = facings_category_result / denominator_result

            # Preparing the results' dictionary
            res_dict['kpi_definition_fk'] = kpi_fk
            res_dict['numerator_id'] = category_fk
            res_dict['numerator_result'] = facings_category_result
            res_dict['denominator_id'] = self.store_id
            res_dict['denominator_result'] = denominator_result
            res_dict['result'] = result_for_category
            res_dict['score'] = result_for_category
            res_list.append(res_dict.copy())
        return res_list

    def save_hierarchy(self, level_1, level_2, level_3, level_4, level_5):
        for i in level_1:
            res = i.to_dict
            kpi_identifier = "level_1"
            self.commonV2.write_to_db_result(fk=res['kpi_definition_fk'], numerator_id=res['numerator_id'],
                                             denominator_id=res['denominator_id'],
                                             numerator_result=res['numerator_result'],
                                             denominator_result=res['denominator_result'], result=res['result'],
                                             score=res['result'],
                                             identifier_result=kpi_identifier, should_enter=False)

        for res in level_2:
            kpi_identifier = "level_2_" + str(int(res['numerator_id']))
            parent_identifier = "level_1"
            self.commonV2.write_to_db_result(fk=res['kpi_definition_fk'], numerator_id=res['numerator_id'],
                                             denominator_id=res['denominator_id'],
                                             numerator_result=res['numerator_result'],
                                             denominator_result=res['denominator_result'], result=res['result'],
                                             score=res['result'],
                                             identifier_result=kpi_identifier,
                                             identifier_parent=parent_identifier, should_enter=True)

        for i in level_3:
            res = i.to_dict
            kpi_identifier = str(int(res['numerator_id']))
            parent_identifier = "level_2_" + str(int(res['denominator_id']))
            self.commonV2.write_to_db_result(fk=res['kpi_definition_fk'], numerator_id=res['numerator_id'],
                                             denominator_id=res['denominator_id'],
                                             numerator_result=res['numerator_result'],
                                             denominator_result=res['denominator_result'], result=res['result'],
                                             score=res['result'],
                                             identifier_result=kpi_identifier,
                                             identifier_parent=parent_identifier, should_enter=True)

        for i in level_4:
            res = i.to_dict
            kpi_identifier = "level_4_" + str((int(res['numerator_id']), int(res['denominator_id'])))
            parent_identifier = str(int(res['denominator_id']))
            self.commonV2.write_to_db_result(fk=res['kpi_definition_fk'], numerator_id=res['numerator_id'],
                                             denominator_id=res['denominator_id'],
                                             numerator_result=res['numerator_result'],
                                             denominator_result=res['denominator_result'], result=res['result'],
                                             score=res['result'],
                                             identifier_result=kpi_identifier,
                                             identifier_parent=parent_identifier, should_enter=True)

        for res in level_5:
            kpi_identifier = "level_5_" + str(res['identifier_result'])
            parent_identifier = "level_4_" + str(res['identifier_parent'])
            self.commonV2.write_to_db_result(fk=res['kpi_definition_fk'], numerator_id=res['numerator_id'],
                                             denominator_id=res['denominator_id'],
                                             numerator_result=res['numerator_result'],
                                             denominator_result=res['denominator_result'], result=res['result'],
                                             score=res['result'],
                                             identifier_result=kpi_identifier, identifier_parent=parent_identifier,
                                             should_enter=True)
