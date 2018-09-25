from Trax.Algo.Calculations.Core.DataProvider import Data
#from Trax.Cloud.Services.Connector.Keys import DbUsers
#from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'hunter'


class SOVIToolBox:

    def __init__(self, data_provider, output, common_db2):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.common_db2 = common_db2
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
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.sos = SOS(self.data_provider, self.output)


    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        self.calculate_entire_store_sos()


    def calculate_entire_store_sos(self):
        general_filters = {}  # entire session/store visit
        sos_filters = {'United Deliver': 'Y'}
        sos_value = self.sos.calculate_share_of_shelf(sos_filters, **general_filters)
        sos_value *= 100
        sos_value = round(sos_value, 2)
        print('Entire store: {}%'.format(sos_value))

        # this assumes that template groups where United does NOT have products should NOT be shown
        template_group_list = self.scif[self.scif['United Deliver'] == 'Y'].template_group.unique()

        for template_group in template_group_list:
            self.calculate_template_group_sos(template_group)


    def calculate_template_group_sos(self, template_group):
        general_filters = {}  # entire session/store visit
        sos_filters = {'United Deliver': 'Y',
                       'template_group': template_group}
        sos_value = self.sos.calculate_share_of_shelf(sos_filters, **general_filters)
        sos_value *= 100
        sos_value = round(sos_value, 2)
        print('{}: {}%'.format(template_group, sos_value))

        att4_list = self.scif[(self.scif['United Deliver'] == 'Y') &
                              (self.scif['template_group'] == template_group)].att4.unique()

        for att4 in att4_list:
            self.calculate_att4_sos(template_group, att4)

    def calculate_att4_sos(self, template_group, att4):
        general_filters = {}  # entire session/store visit
        sos_filters = {'United Deliver': 'Y',
                       'template_group': template_group,
                       'att4': att4
                       }
        sos_value = self.sos.calculate_share_of_shelf(sos_filters, **general_filters)
        sos_value *= 100
        sos_value = round(sos_value, 2)
        print('{} - {}: {}%'.format(template_group, att4, sos_value))

        category_list = self.scif[(self.scif['United Deliver'] == 'Y') &
                                  (self.scif['template_group'] == template_group) &
                                  (self.scif['att4'] == att4)].category.unique()

        for category in category_list:
            self.calculate_category_sos(template_group, att4, category)

    def calculate_category_sos(self, template_group, att4, category):
        general_filters = {}
        sos_filters = {'United Deliver': 'Y',
                       'template_group': template_group,
                       'att4': att4,
                       'category': category
                       }
        sos_value = self.sos.calculate_share_of_shelf(sos_filters, **general_filters)
        sos_value *= 100
        sos_value = round(sos_value, 2)
        print('{} - {} - {}: {}%'.format(template_group, att4, category, sos_value))

        manufacturer_list = self.scif[(self.scif['United Deliver'] == 'Y') &
                                  (self.scif['template_group'] == template_group) &
                                  (self.scif['att4'] == att4) &
                                  (self.scif['category'] == category)].manufacturer_name.unique()

        for manufacturer_name in manufacturer_list:
            self.calculate_manufacturer_sos(template_group, att4, category, manufacturer_name)

    def calculate_manufacturer_sos(self, template_group, att4, category, manufacturer_name):
        general_filters = {
                       'template_group': template_group,
                       'att4': att4,
                       'category': category
                       }
        sos_filters = {'manufacturer_name': manufacturer_name}
        sos_value = self.sos.calculate_share_of_shelf(sos_filters, **general_filters)
        sos_value *= 100
        sos_value = round(sos_value, 2)
        print('{} - {} - {} - {}: {}%'.format(template_group, att4, category, manufacturer_name, sos_value))

        brand_name_list = self.scif[(self.scif['template_group'] == template_group) &
                                    (self.scif['att4'] == att4) &
                                    (self.scif['category'] == category) &
                                    (self.scif['manufacturer_name'] == manufacturer_name)].brand_name.unique()

        for brand_name in brand_name_list:
            self.calculate_brand_sos(template_group, att4, category, manufacturer_name, brand_name)

    def calculate_brand_sos(self, template_group, att4, category, manufacturer_name, brand_name):
        general_filters = {
            'template_group': template_group,
            'att4': att4,
            'category': category
        }
        sos_filters = {'manufacturer_name': manufacturer_name,
                       'brand_name': brand_name
                       }
        sos_value = self.sos.calculate_share_of_shelf(sos_filters, **general_filters)
        sos_value *= 100
        sos_value = round(sos_value, 2)
        print('{} - {} - {} - {} - {}: {}%'.format(template_group, att4, category, manufacturer_name,
                                                   brand_name, sos_value))

        product_name_list = self.scif[(self.scif['template_group'] == template_group) &
                                      (self.scif['att4'] == att4) &
                                      (self.scif['category'] == category) &
                                      (self.scif['manufacturer_name'] == manufacturer_name) &
                                      (self.scif['brand_name'] == brand_name)].product_name.unique()

        for product_name in product_name_list:
            self.calculate_product_name_sos(template_group, att4, category, manufacturer_name, brand_name, product_name)

    def calculate_product_name_sos(self, template_group, att4, category, manufacturer_name, brand_name, product_name):
        general_filters = {
            'template_group': template_group,
            'att4': att4,
            'category': category
        }
        sos_filters = {'manufacturer_name': manufacturer_name,
                       'brand_name': brand_name,
                       'product_name': product_name
                       }
        sos_value = self.sos.calculate_share_of_shelf(sos_filters, **general_filters)
        sos_value *= 100
        sos_value = round(sos_value, 2)
        print('{} - {} - {} - {} - {} - {}: {}%'.format(template_group, att4, category, manufacturer_name,
                                                        brand_name, product_name, sos_value))