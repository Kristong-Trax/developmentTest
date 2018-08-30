from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os
from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'limorc'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class CCAAUToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1

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
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.template = self.data_provider.all_templates  # templates

        self.toolbox = GENERALToolBox(data_provider)
        kpi_path = os.path.dirname(os.path.realpath(__file__))
        base_file = os.path.basename(kpi_path)
        self.exclude_filters = pd.read_excel(os.path.join(kpi_path[:- len(base_file)], 'Data', 'template.xlsx'),
                                             sheetname="Exclude")
        self.Include_filters = pd.read_excel(os.path.join(kpi_path[:- len(base_file)], 'Data', 'template.xlsx'),
                                             sheetname="Include")

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        score = 0
        return score

    def calculate_sos(self, sos_filters, include_empty=Const.EXCLUDE_EMPTY, **general_filters):
        """
        :param sos_filters:  numerator type =
        :param include_empty: This dictates whether Empty-typed SKUs are included in the calculation.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: The numerator facings, denominator facings, numerator linear and denominator linear.
        """
        facing_exclude_template = self.exclude_filters[self.exclude_filters['KPI'] == 'Share of Shelf by Facing']
        linear_exclude_template = self.exclude_filters[self.exclude_filters['KPI'] == 'Share of Shelf by Linear']
        facing_include_template = self.exclude_filters[self.Include_filters['KPI'] == 'Share of Shelf by Facing']
        linear_include_template = self.exclude_filters[self.Include_filters['KPI'] == 'Share of Shelf by Linear']

        scene_templates = self.scif['template_fk'].unique().tolist()
        scene_manufactures = self.scif['manfacutre_fk'].unique().tolist()

        for template in scene_templates:

            for manufacture in scene_manufactures:
                sos_filters = {"template_fk": (template, self.EXCLUDE_FILTER),
                               "manfacutre_fk": (manufacture, self.EXCLUDE_FILTER)}
                # exclude filters
                general_facing_filters = self.create_dict_filters(facing_exclude_template, self.EXCLUDE_FILTER)
                general_linear_filters = self.create_dict_filters(linear_exclude_template, self.EXCLUDE_FILTER)

                # include_filters
                facing_include_template = self.create_dict_filters(facing_exclude_template, self.INCLUDE_FILTER)
                linear_include_template = self.create_dict_filters(linear_exclude_template, self.INCLUDE_FILTER)

        # sos facing
        ### {"limor": " "}
                numerator_facings = self.calculate_share_space(**dict(sos_filters,facing_include_template, general_facing_filters))
                numerator_linear = self.calculate_share_space(**dict(sos_filters,linear_include_template, general_linear_filters))

                denominator_linear = self.calculate_share_space(**dict(linear_include_template, general_linear_filters))
                denominator_facings = self.calculate_share_space(**dict(facing_include_template, general_facing_filters))

        # denominator_facings, denominator_linear = self.calculate_share_space(**general_filters)
        return numerator_facings, denominator_facings, numerator_linear, denominator_linear

    def create_dict_filters(self, template, parametr):

        filters_dict = {}
        template_without_second = template[template['Param 2'].isnull()]

        for row in template_without_second:
            filters_dict[row['Param 1']] = (row['Value 1'].tolist(), parametr)

        return filters_dict

    def filter_2_cond(self,df,template):

        filters_dict = {}
        template_without_second = template[not template['Param 2'].isnull()]

        for row in template_without_second:
            filters_dict[row['Param 1']] = (row['Value 1'].tolist(), parametr)

        return filters_dict

    def calculate_share_space(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total number of facings and the shelf width (in mm) according to the filters.
        """
        filtered_scif = self.scif[self.toolbox.get_filter_condition(self.scif, **filters)]
        sum_of_facings = filtered_scif['facings'].sum()
        space_length = filtered_scif['net_len_ign_stack'].sum()
        return sum_of_facings, space_length
