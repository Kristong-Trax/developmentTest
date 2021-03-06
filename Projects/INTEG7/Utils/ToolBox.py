
import os
import pandas as pd
import json

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Storage.Factory import StorageFactory
from Trax.Algo.Calculations.Core.Shortcuts import BaseCalculationsGroup

from Projects.INTEG7.Utils.GeneralToolBox import GENERALToolBox

__author__ = 'Nimrod'

BUCKET = 'traxuscalc'

EMPTY = 'Empty'
POURING_SURVEY_TEXT = 'Are the below brands pouring?'
KPI_NAME = 'Atomic'
PRODUCT_NAME = 'Product Name'
TEMP_TEMPLATES_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')

class DIAGEOToolBox:

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
    PRODUCT_EAN_CODE = 'Leading Product EAN'
    PRODUCT_EAN_CODE2 = 'Product EAN'
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
    KPI_SETS_WITH_PERCENT_AS_SCORE = KPI_SETS_WITH_PRODUCT_AS_NAME + ['Visible to Customer', 'Relative Position',
                                                                      'Brand Blocking']
    KPI_SETS_WITHOUT_A_TEMPLATE = ['Secondary', 'Visible to Customer']
    TEMPLATES_PATH = 'Diageo_templates/'
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
        self.general_tools = GENERALToolBox(data_provider, output, self.kpi_static_data, geometric_kpi_flag=True)
        self.amz_conn = StorageFactory.get_connector(BUCKET)
        self.templates_path = self.TEMPLATES_PATH + self.project_name + '/'

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

    def calculate_assortment(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Number of unique SKUs appeared in the filtered Scene Item Facts data frame.
        """
        return self.general_tools.calculate_assortment(**filters)

    def calculate_posm(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Number of unique POSMs appeared in the filtered Display data frame.
        """
        filtered_display = self.match_display_in_scene[self.get_filter_condition(self.match_display_in_scene, **filters)]
        assortment = len(filtered_display['display_name'].unique())
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
                       INPUT EXAMPLE (1):   manufacturer_name = (DIAGEOToolBox.DIAGEO, DIAGEOToolBox.INCLUDE_FILTER)
                       INPUT EXAMPLE (2):   manufacturer_name = DIAGEOToolBox.DIAGEO
        :return: a filtered Scene Item Facts data frame.
        """
        return self.general_tools.get_filter_condition(df, **filters)

    def download_template(self, set_name):
        """
        This function receives a KPI set name and return its relevant template as a JSON.
        """
        # temp_file_path = '{}/{}_temp.xlsx'.format(os.getcwd(), set_name)
        # f = open(temp_file_path, 'wb')
        # self.amz_conn.download_file('{}{}.xlsx'.format(self.templates_path, set_name), f)
        # f.close()
        temp_file_path = '{}/{}.xlsx'.format(TEMP_TEMPLATES_PATH, set_name)
        json_data = self.get_json_data(temp_file_path)
        # os.remove(temp_file_path)
        return json_data

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
                    output = output[index+1:]
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
