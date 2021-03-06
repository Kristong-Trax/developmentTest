import os
import pandas as pd
from datetime import datetime
import json

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Storage.Factory import StorageFactory
from Trax.Utils.Logging.Logger import Log
from Trax.Algo.Calculations.Core.Shortcuts import BaseCalculationsGroup
from Projects.INBEVBE_SAND.Utils.PositionGraph import INBEVBE_SANDPositionGraphs
from Projects.INBEVBE_SAND.Utils.GeneralToolBox import INBEVBE_SANDGENERALToolBox

__author__ = 'Nimrod'

BUCKET = 'traxuscalc'

EMPTY = 'Empty'
POURING_SURVEY_TEXT = 'Are the below brands pouring?'
KPI_NAME = 'Atomic'
PRODUCT_NAME = 'Product Name'
TEMP_TEMPLATES_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
CACHE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'CacheData')
UPDATED_DATE_FILE = 'LastUpdated'
UPDATED_DATE_FORMAT = '%Y-%m-%d'


class INBEVBE_SANDINBEVToolBox:
    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    EXCLUDE_EMPTY = 0
    INCLUDE_EMPTY = 1
    DIAGEO = 'Diageo'
    ASSORTMENT = 'assortment'
    AVAILABILITY = 'availability'

    RELEVANT_FOR_STORE = 'Y'
    IRRELEVANT_FOR_STORE = 'N'
    OR_OTHER_PRODUCTS = 'Or'

    UNLIMITED_DISTANCE = 'General'

    # Templates fields #

    # Availability KPIs
    PRODUCT_NAME = PRODUCT_NAME
    PRODUCT_EAN_CODE = 'Product EAN'
    PRODUCT_EAN_CODE2 = 'EAN Number'
    ADDITIONAL_SKUS = '1st Follower Product EAN'
    ENTITY_TYPE = 'Entity Type'
    TARGET = 'Target'

    # POSM KPIs
    DISPLAY_NAME = 'Product Name'

    # Relative Position
    CHANNEL = 'Channel'

    LOCATION = 'Primary "In store location"'
    TESTED = 'Tested SKU2'
    ANCHOR = 'Anchor SKU2'
    TOP_DISTANCE = 'Up to (above) distance (by shelves)'
    BOTTOM_DISTANCE = 'Up to (below) distance (by shelves)'
    LEFT_DISTANCE = 'Up to (Left) Distance (by SKU facings)'
    RIGHT_DISTANCE = 'Up to (right) distance (by SKU facings)'

    # Block Together
    BRAND_NAME = 'Brand Name'
    SUB_BRAND_NAME = 'Brand Variant'

    VISIBILITY_PRODUCTS_FIELD = 'additional_attribute_2'
    BRAND_POURING_FIELD = 'additional_attribute_1'

    ENTITY_TYPE_CONVERTER = {'SKUs': 'product_ean_code',
                             'Brand': 'brand_name',
                             'Sub brand': 'sub_brand_name',
                             'Category': 'category',
                             'display': 'display_name'}

    KPI_SETS = ['MPA', 'New Products', 'POSM', 'Secondary', 'Relative Position', 'Brand Blocking',
                'Brand Pouring', 'Visible to Customer']
    KPI_SETS_WITH_PRODUCT_AS_NAME = ['MPA', 'New Products', 'POSM']
    KPI_SETS_WITH_PERCENT_AS_SCORE = ['Share of Assortment', 'Linear Share of Shelf vs. Target',
                                      'Product Blocking', 'Shelf Level', 'OSA', 'Linear Share of Shelf', 'Share of Assortment']
    KPI_SETS_WITHOUT_A_TEMPLATE = ['OSA', 'Linear Share of Shelf', 'Shelf Impact Score', 'Share of Assortment', 'Pallet Presence']
    TEMPLATES_PATH = 'Inbev_templates/'
    KPI_NAME = KPI_NAME

    def __init__(self, data_provider, output, **data):
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.kpi_static_data = data.get('kpi_static_data')
        self.match_display_in_scene = data.get('match_display_in_scene')
        self.general_tools = INBEVBE_SANDGENERALToolBox(data_provider, output,
                                                               kpi_static_data=self.kpi_static_data)
        self.amz_conn = StorageFactory.get_connector(BUCKET)
        self.templates_path = self.TEMPLATES_PATH + self.project_name + '/'
        self.cloud_templates_path = '{}{}/{}'.format(self.TEMPLATES_PATH, self.project_name, {})
        self.local_templates_path = os.path.join(CACHE_PATH, 'templates')

    @property
    def position_graphs(self):
        if not hasattr(self, '_position_graphs'):
            self._position_graphs = INBEVBE_SANDPositionGraphs(self.data_provider)
        return self._position_graphs

    @property
    def match_product_in_scene(self):
        if not hasattr(self, '_match_product_in_scene'):
            self._match_product_in_scene = self.position_graphs.match_product_in_scene
        return self._match_product_in_scene

    def check_survey_answer(self, survey_text, target_answer):
        """
        :param survey_text: The name of the survey in the DB.
        :param target_answer: The required answer/s for the KPI to pass.
        :return: True if the answer matches the target; otherwise - False.
        """
        return self.general_tools.check_survey_answer(survey_text, target_answer)

    def calculate_visible_percentage(self, visible_filters, **filters):
        """
        :param visible_filters: These are the parameters which dictates whether a scene type is visible or not.
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The percentage of visible SKUs out of the total number of SKUs.
        """
        filters_including_visible = filters.copy()
        for key in visible_filters.keys():
            filters_including_visible[key] = visible_filters[key]

        visible_products = self.calculate_assortment(**filters_including_visible)
        all_products = self.calculate_assortment(**filters)
        if all_products > 0:
            percentage = (visible_products / float(all_products)) * 100
        else:
            percentage = 0
        return percentage

    def calculate_number_of_scenes(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The number of scenes matching the filtered Scene Item Facts data frame.
        """
        return self.general_tools.calculate_number_of_scenes(**filters)

    def calculate_availability(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Total number of SKUs facings appeared in the filtered Scene Item Facts data frame.
        """
        return self.general_tools.calculate_availability(**filters)

    def calculate_assortment(self, assortment_entity='product_ean_code', minimum_assortment_for_entity=1, **filters):
        """
        :param assortment_entity: This is the entity on which the assortment is calculated.
        :param minimum_assortment_for_entity: This is the number of assortment per each unique entity in order for it
                                              to be counted in the final assortment result (default is 1).
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Number of unique SKUs appeared in the filtered Scene Item Facts data frame.
        """
        if set(filters.keys()).difference(self.scif.keys()):
            filtered_df = self.match_product_in_scene[self.get_filter_condition(self.match_product_in_scene, **filters)]
        else:
            filtered_df = self.scif[self.get_filter_condition(self.scif, **filters)]
        if minimum_assortment_for_entity == 1:
            assortment = len(filtered_df[assortment_entity].unique())
        else:
            assortment = 0
            for entity_id in filtered_df[assortment_entity].unique():
                assortment_for_entity = filtered_df[filtered_df[assortment_entity] == entity_id]
                if 'facings' in filtered_df.columns:
                    assortment_for_entity = assortment_for_entity['facings'].sum()
                else:
                    assortment_for_entity = len(assortment_for_entity)
                if assortment_for_entity >= minimum_assortment_for_entity:
                    assortment += 1
        return assortment


    def calculate_share_of_shelf(self, manufacturer=DIAGEO, sos_filters=None, include_empty=EXCLUDE_EMPTY,
                                 **general_filters):
        """
        :param manufacturer: This is taken as a lone sos-filter in case sos_filters param is None.
        :param sos_filters: These are the parameters on which ths SOS is calculated (out of the general DF).
        :param include_empty: This dictates whether Empty-typed SKUs are included in the calculation.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: The ratio of the SOS.
        """
        if not sos_filters:
            sos_filters = {'manufacturer_name': manufacturer}
        return self.general_tools.calculate_share_of_shelf(sos_filters, include_empty, **general_filters)

    def calculate_brand_pouring_status(self, brand, **filters):
        """
        :param brand: The brand to run the KPI on.
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The brand pouring status.
        """
        brand_pouring_status = False

        sub_category = self.scif[(self.scif['brand_name'] == brand) &
                                 (self.scif['category'] == 'Spirit')]['sub_category'].values[0]
        filtered_df = self.scif[self.get_filter_condition(self.scif, sub_category=sub_category, **filters)]
        if filtered_df[filtered_df['brand_name'] == brand].empty:
            return False
        brands_in_category = filtered_df['brand_name'].unique().tolist()

        sos_by_brand = {}
        for brand_name in brands_in_category:
            sos_by_brand[brand_name] = self.calculate_share_of_shelf(sos_filters={'brand_name': brand_name},
                                                                     include_empty=self.EXCLUDE_EMPTY,
                                                                     sub_category=sub_category, **filters)
        pouring_sos = sos_by_brand[brand]

        pouring_survey = self.check_survey_answer(survey_text=POURING_SURVEY_TEXT, target_answer=brand)

        if pouring_survey and pouring_sos == max(sos_by_brand.values()):
            brand_pouring_status = True

        return brand_pouring_status

    def calculate_relative_position(self, tested_filters, anchor_filters, direction_data, min_required_to_pass=1,
                                    **general_filters):
        """
        :param tested_filters: The tested SKUs' filters
        :param anchor_filters: The anchor SKUs' filters.
        :param direction_data: The allowed distance between the tested and anchor SKUs.
                               In form: {'top': 4, 'bottom: 0, 'left': 100, 'right': 0}
                               Alternative form: {'top': (0, 1), 'bottom': (1, 1000), ...} - As range.
        :param min_required_to_pass: Number of appearances needed to be True for relative position in order for KPI
                                     to pass. If all appearances are required: ==a string or a big number.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: True if (at least) one pair of relevant SKUs fits the distance requirements; otherwise - returns False.
        """
        return self.general_tools.calculate_relative_position(tested_filters, anchor_filters, direction_data,
                                                              min_required_to_pass, **general_filters)

    def calculate_block_together(self, allowed_products_filters=None, include_empty=EXCLUDE_EMPTY, **filters):
        """
        :param allowed_products_filters: These are the parameters which are allowed to corrupt the block without failing it.
        :param include_empty: This parameter dictates whether or not to discard Empty-typed products.
        :param filters: These are the parameters which the blocks are checked for.
        :return: True - if in (at least) one of the scenes all the relevant SKUs are grouped together in one block;
                 otherwise - returns False.
        """
        return self.general_tools.calculate_block_together(allowed_products_filters, include_empty, **filters)

    def get_filter_condition(self, df, **filters):
        """
        :param df: The data frame to be filters.
        :param filters: These are the parameters which the data frame is filtered by.
                       Every parameter would be a tuple of the value and an include/exclude flag.
                       INPUT EXAMPLE (1):   manufacturer_name = (DIAGEOAUDIAGEOToolBox.DIAGEO,
                                                                 DIAGEOAUDIAGEOToolBox.INCLUDE_FILTER)
                       INPUT EXAMPLE (2):   manufacturer_name = DIAGEOAUDIAGEOToolBox.DIAGEO
        :return: a filtered Scene Item Facts data frame.
        """
        return self.general_tools.get_filter_condition(df, **filters)

    def download_template(self, set_name):
        """
        This function receives a KPI set name and return its relevant template as a JSON.
        """
        with open(os.path.join(self.local_templates_path, set_name), 'rb') as f:
            json_data = json.load(f)
        return json_data

    def update_templates(self):
        """
        This function checks whether the recent templates are updated.
        If they're not, it downloads them from the Cloud and saves them in a local path.
        """
        if not os.path.exists(self.local_templates_path):
            os.makedirs(self.local_templates_path)
            self.save_latest_templates()
        else:
            files_list = os.listdir(self.local_templates_path)
            if files_list and UPDATED_DATE_FILE in files_list:
                with open(os.path.join(self.local_templates_path, UPDATED_DATE_FILE), 'rb') as f:
                    date = datetime.strptime(f.read(), UPDATED_DATE_FORMAT)
                if date.date() == datetime.utcnow().date():
                    return
                else:
                    self.save_latest_templates()
            else:
                self.save_latest_templates()

    def save_latest_templates(self):
        """
        This function reads the latest templates from the Cloud, and saves them in a local path.
        """
        if not os.path.exists(self.local_templates_path):
            os.makedirs(self.local_templates_path)
        dir_name = self.get_latest_directory_date_from_cloud(self.cloud_templates_path.format(''), self.amz_conn)
        files = [f.key for f in self.amz_conn.bucket.list(self.cloud_templates_path.format(dir_name))]
        for file_path in files:
            file_name = file_path.split('/')[-1]
            with open(os.path.join(self.local_templates_path, file_name), 'wb') as f:
                self.amz_conn.download_file(file_path, f)
        with open(os.path.join(self.local_templates_path, UPDATED_DATE_FILE), 'wb') as f:
            f.write(datetime.utcnow().strftime(UPDATED_DATE_FORMAT))
        Log.info('Latest version of templates has been saved to cache')

    @staticmethod
    def get_latest_directory_date_from_cloud(cloud_path, amz_conn):
        """
        This function reads all files from a given path (in the Cloud), and extracts the dates of their mother dirs
        by their name. Later it returns the latest date (up to today).
        """
        files = amz_conn.bucket.list(cloud_path)
        files = [f.key.replace(cloud_path, '') for f in files]
        files = [f for f in files if len(f.split('/')) > 1]
        files = [f.split('/')[0] for f in files]
        files = [f for f in files if f.isdigit()]
        if not files:
            return
        dates = [datetime.strptime(f, '%y%m%d') for f in files]
        for date in sorted(dates, reverse=True):
            if date.date() <= datetime.utcnow().date():
                return date.strftime("%y%m%d")
        return

    @staticmethod
    def get_json_data(file_path):
        """
        This function gets a file's path and extract its content into a JSON.
        """
        output = pd.read_excel(file_path)
        if KPI_NAME not in output.keys() and PRODUCT_NAME not in output.keys():
            for index in xrange(len(output)):
                row = output.iloc[index].tolist()
                if KPI_NAME in row or PRODUCT_NAME in row:
                    output = output[index + 1:]
                    output.columns = row
                    break
        output = output[[key for key in output.keys() if isinstance(key, (str, unicode))]]
        output = output.to_json(orient='records')
        json_data = json.loads(output)
        # Removing None values + Converting all values to unicode-typed + Removing spaces from headers
        for i in xrange(len(json_data)):
            for key in json_data[i].keys():
                if json_data[i][key] is None:
                    json_data[i].pop(key)
                else:
                    json_data[i][key] = unicode(json_data[i][key]).strip()
                try:
                    json_data[i][key.strip()] = json_data[i].pop(key)
                except KeyError:
                    continue
        return json_data
