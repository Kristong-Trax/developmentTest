
import os
import json
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.DB.Common import Common as CommonV1
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment

from Projects.MOLSONCOORSHR.Utils.ParseTemplates import parse_template
from Projects.MOLSONCOORSHR.Utils.Fetcher import Queries


from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
from KPIUtils_v2.Calculations.SurveyCalculations import Survey

from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'sergey'

KPIS_TEMPLATE_NAME = 'MOLSONCOORS_CE_KPIs.xlsx'
KPIS_TEMPLATE_SHEET = 'KPIs'

NUMERATOR = 'numerator'
DENOMINATOR = 'denominator'

LINEAR_SOS_VS_TARGET = 'linear sos vs target'
FACINGS_SOS_VS_TARGET = 'facings sos vs target'
FACINGS_VS_TARGET = 'facings vs target'
DISTRIBUTION = 'distribution'
GROUP = 'group'
SUM_OF_SCORES = 'sum of scores'
CONDITIONAL_PROPORTIONAL = 'conditional proportional'
WEIGHTED_AVERAGE = 'weighted average'
WEIGHTED_SCORE = 'weighted score'

SOS_MANUFACTURER_CATEGORY = 'SOS_manufacturer_category'

MANUFACTURER_NAME = 'manufacturer_name'
MANUFACTURER = 'manufacturer'
MANUFACTURER_FK = 'manufacturer_fk'
CATEGORY = 'category'
CATEGORY_FK = 'category_fk'
LOCATION_TYPE = 'location_type'


class MOLSONCOORSHRToolBox:

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
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.toolbox = GENERALToolBox(data_provider)

        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []

        self.template_path = self.get_template_path()
        self.template_data = self.get_template_data()
        self.sos_store_policies = self.get_sos_store_policies(self.visit_date.strftime('%Y-%m-%d'))

        self.scores = pd.DataFrame()

    def get_sos_store_policies(self, visit_date):
        query = Queries.get_sos_store_policies(visit_date)
        store_policies = pd.read_sql_query(query, self.rds_conn.db)
        return store_policies

    def main_calculation(self):
        """
        This function starts the KPI results calculation.
        """
        if not self.template_data or KPIS_TEMPLATE_SHEET not in self.template_data:
            Log.error('KPIs template sheet is empty or not found')
            return

        self.kpis_calculation()

    def kpis_calculation(self, kpi_group=''):
        """
        This is a recursive function.
        The function calculates each level KPI cascading from the highest level to the lowest.
        """
        total_score = 0
        kpis = self.template_data['KPIs'][self.template_data['KPIs']['KPI Group'] == kpi_group]
        for index, kpi in kpis.iterrows():

            child_kpi_group = kpi['Child KPI Group']
            kpi_type = kpi['KPI Type'].lower()
            weight = float(kpi['Weight']) if kpi['Weight'] else 1
            score_function = kpi['Score Function'].lower()
            l_threshold = float(kpi['Lower Threshold']) if kpi['Lower Threshold'] else 0
            h_threshold = float(kpi['Higher Threshold']) if kpi['Higher Threshold'] else 1

            if not child_kpi_group:
                if kpi_type in [LINEAR_SOS_VS_TARGET, FACINGS_SOS_VS_TARGET]:
                    score = self.calculate_sos_vs_target(kpi)
                elif kpi_type in [FACINGS_VS_TARGET, DISTRIBUTION]:
                    score = self.calculate_assortment_vs_target(kpi)
                else:
                    Log.error("KPI of type '{}' is not supported".format(kpi_type))
                    score = 0
            else:
                score = self.kpis_calculation(child_kpi_group)

            if score < l_threshold:
                score = 0
            elif score >= h_threshold:
                score = 100

            if score_function in [WEIGHTED_SCORE]:
                total_score += score * weight
            elif score_function in [SUM_OF_SCORES]:
                total_score += score
            else:
                total_score += 0

            self.scores = self.scores.append({'KPI': kpi['KPI name Eng'], 'Score': score, 'Weight': weight}, ignore_index=True)

        return total_score

    def calculate_assortment_vs_target(self, kpi):


        return 0

    def calculate_sos_vs_target(self, kpi):
        """
        The function filters only the relevant scenes by Location Type and calculates the linear SOS and
        the facing SOS according to Manufacturer and Category set in the target.
         :return:
        """

        location_type = kpi['Location Type']
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(SOS_MANUFACTURER_CATEGORY + ('_' + location_type if location_type else ''))

        sos_store_policies = self.sos_store_policies[self.sos_store_policies['kpi_fk'] == str(kpi_fk)]

        sos_store_policy = None
        store_policy_passed = 0
        for index, policy in sos_store_policies.iterrows():
            sos_store_policy = policy
            store_policy = json.loads(policy['store_policy'])
            store_policy_passed = 1
            for key in store_policy.keys():
                if key in self.store_info.columns.tolist():
                    if self.store_info[key][0] in store_policy[key]:
                        continue
                    else:
                        store_policy_passed = 0
                        break
                else:
                    Log.error("Store Policy attribute is not found: '{}'").format(key)
                    store_policy_passed = 0
                    break
            if store_policy_passed:
                break

        if store_policy_passed:

            general_filters = {LOCATION_TYPE: location_type}
            sos_policy = json.loads(sos_store_policy['sos_policy'])
            numerator_sos_filters = {MANUFACTURER_NAME: sos_policy[NUMERATOR][MANUFACTURER], CATEGORY: sos_policy[DENOMINATOR][CATEGORY]}
            denominator_sos_filters = {CATEGORY: sos_policy[DENOMINATOR][CATEGORY]}

            numer_facings, numer_linear = self.calculate_share_space(**dict(numerator_sos_filters, **general_filters))
            denom_facings, denom_linear = self.calculate_share_space(**dict(denominator_sos_filters, **general_filters))

            if kpi['KPI Type'].lower() == LINEAR_SOS_VS_TARGET:
                result = numer_linear / float(denom_linear) if denom_linear else 0
            elif kpi['KPI Type'].lower() == FACINGS_SOS_VS_TARGET:
                result = numer_facings / float(denom_facings) if denom_facings else 0
            else:
                Log.error("KPI Type is invalid: '{}'").format(kpi['KPI Type'])
                result = 0

            if sos_store_policy['target']:
                target = float(sos_store_policy['target'])
            else:
                Log.error("SOS target is not set for Store ID {}").format(self.store_id)
                target = 0

            score = result / target * 100 if target else 0

        else:
            Log.error("Store Policy is not found for Store ID {}").format(self.store_id)
            score = 0

        return score

    def calculate_share_space(self, ignore_stacking=1, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :param ignore_stacking: 1 is to ignore stacking.
        :return: The total number of facings and the shelf width (in mm) according to the filters.
        """
        filtered_scif = self.scif[self.toolbox.get_filter_condition(self.scif, **filters)]
        if ignore_stacking:
            sum_of_facings = filtered_scif['facings_ign_stack'].sum()
            space_length = filtered_scif['net_len_ign_stack'].sum()
        else:
            sum_of_facings = filtered_scif['facings'].sum()
            space_length = filtered_scif['net_len_ign_stack'].sum()

        return sum_of_facings, space_length


    def get_template_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', KPIS_TEMPLATE_NAME)

    def get_template_data(self):
        template_data = {}
        try:
            sheet_names = pd.ExcelFile(self.template_path).sheet_names
            for sheet in sheet_names:
                template_data[sheet] = parse_template(self.template_path, sheet, lower_headers_row_index=0)
        except IOError as e:
            Log.error('Template {} does not exist. {}'.format(KPIS_TEMPLATE_NAME, repr(e)))
        return template_data

