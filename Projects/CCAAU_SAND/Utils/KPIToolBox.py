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
        self.kpi_static_data = self.common.get_new_kpi_static_data()
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
        self.calculate_sos()
        self.common.commit_results_data_to_new_tables()

    def calculate_sos(self):
        """
        :param sos_filters:  numerator type =
        :param include_empty: This dictates whether Empty-typed SKUs are included in the calculation.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: The numerator facings, denominator facings, numerator linear and denominator linear.
        """

        facing_kpi_fk = self.kpi_static_data[self.kpi_static_data['client_name'] == 'FACINGS_SOS_SCENE_TYPE_BY_MANUFACTURER']['pk'].iloc[0]
        linear_kpi_fk = self.kpi_static_data[self.kpi_static_data['client_name'] == 'LINEAR_SOS_SCENE_TYPE_BY_MANUFACTURER']['pk'].iloc[0]
        den_facing_exclude_template = self.exclude_filters[(self.exclude_filters['KPI'] == 'Share of Shelf by Facing') & (self.exclude_filters['apply on'] == 'Denominator')]
        den_linear_exclude_template = self.exclude_filters[(self.exclude_filters['KPI'] == 'Share of Shelf by Linear')& (self.exclude_filters['apply on'] == 'Denominator')]
        num_facing_exclude_template = self.exclude_filters[(self.exclude_filters['KPI'] == 'Share of Shelf by Facing') & (self.exclude_filters['apply on'] == 'Numerator')]
        num_linear_exclude_template = self.exclude_filters[(self.exclude_filters['KPI'] == 'Share of Shelf by Linear') & (self.exclude_filters['apply on'] == 'Numerator')]

        scene_templates = self.scif['template_fk'].unique().tolist()
        scene_manufactures = self.scif['manufacturer_fk'].unique().tolist()

        df = self.scif

        for template in scene_templates:

            for manufacture in scene_manufactures:
                sos_filters = {"template_fk": (template, self.INCLUDE_FILTER),
                               "manufacturer_fk": (manufacture, self.INCLUDE_FILTER)}
                tem_filters={"template_fk": (template, self.INCLUDE_FILTER)}

                # exclude filters denominator
                den_general_facing_filters = self.create_dict_filters(den_facing_exclude_template, self.EXCLUDE_FILTER)
                den_general_linear_filters = self.create_dict_filters(den_linear_exclude_template, self.EXCLUDE_FILTER)


                # exclude filters denominator
                num_general_facing_filters = self.create_dict_filters(num_facing_exclude_template, self.EXCLUDE_FILTER)
                num_general_linear_filters = self.create_dict_filters(num_linear_exclude_template, self.EXCLUDE_FILTER)

                # include_filters

        # sos facing
        ### {"limor": " "}

                df = self.filter_2_cond(df, num_facing_exclude_template)

                dict_num_facing = dict(
                    (k, v) for d in [sos_filters, num_general_facing_filters] for k, v in
                    d.items())
                numerator_facings = self.calculate_share_space(df, dict_num_facing)[0]

                df = self.filter_2_cond(df, num_linear_exclude_template)
                dict_num_linear = dict(
                    (k, v) for d in [sos_filters, num_general_linear_filters] for k, v in d.items())
                numerator_linear = self.calculate_share_space(df, dict_num_linear)[0]

                df = self.filter_2_cond(df, den_facing_exclude_template)
                dict_den_facing = dict((k, v) for d in [tem_filters, den_general_facing_filters] for k, v in d.items())
                denominator_facings = self.calculate_share_space(df, dict_den_facing)[0]


                dict_den_linear = dict(
                    (k, v) for d in [tem_filters, den_general_linear_filters] for k, v in d.items())
                denominator_linear = self.calculate_share_space(df, dict_den_linear )[0]

                score_facing = 0 if denominator_facings == 0 else (numerator_facings/denominator_facings)*100
                score_linear = 0 if denominator_linear == 0 else (numerator_linear / denominator_linear)*100

                self.common.write_to_db_result_new_tables(facing_kpi_fk,manufacture ,numerator_facings,score_facing,template,
                                                          denominator_facings ,score_facing)
                self.common.write_to_db_result_new_tables(linear_kpi_fk, manufacture, numerator_linear,score_linear, template,
                                                          denominator_linear,
                                                          score_linear)


    def create_dict_filters(self, template, parametr):

        filters_dict = {}
        template_without_second = template[template['Param 2'].isnull()]

        for row in template_without_second.iterrows():
            filters_dict[row[1]['Param 1']] = (row[1]['Value 1'].split(','), parametr)

        return filters_dict

    def filter_2_cond(self,df,template):

        filters_dict = {}
        template_without_second = template[template['Param 2'].notnull()]

        if template_without_second is not None:
            for row in template_without_second.iterrows():
                df = df.loc[(~df[row[1]['Param 1']].isin(row[1]['Value 1'].split(',')))| (~df[row[1]['Param 2']].isin(row[1]['Value 2'].split(',')))]

        return df

    def calculate_share_space(self,df, filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total number of facings and the shelf width (in mm) according to the filters.
        """
        filtered_scif = df[self.toolbox.get_filter_condition(df,**filters)]
        sum_of_facings = filtered_scif['facings'].sum()
        space_length = filtered_scif['gross_len_add_stack'].sum()
        return sum_of_facings, space_length