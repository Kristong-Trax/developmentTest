import pandas as pd
import os
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.CommonV2 import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'sathiyanarayanan'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

#KPI DB
PS_KPI_FAMILY = 19
KPI_FAMILY='kpi_family_fk'
TYPE = "type"

SUB_CATEGORY= 'sub_category'
CATEGORY = 'category'

# BATAU - pk in table static_new.manufacturer
MANUFACTURER_FK = 10

# Category - pks in table static_new.category
RYO_FK = 4
TMC_FK = 3

# SubCategory - pks in table static_new.sub_category
LOW_FK = 5
VFM_FK = 6
PRE_FK = 7
ASP_PRE_FK = 8
LOW = 'Low'
VFM = 'VFM'
PRE ='Premium'
ASP_PRE  = 'AspirationalPremium'

TMC = 'Tailor Made Cigarettes'
RYO = 'Roll Your Own Cigarettes'

#GENERAL
ROUNDING_DIGITS = 4
MAPPINGS = {'manufacturer_name':'manufacturer_fk',
            'brand_name':'brand_fk',
            'category':'category_fk',
            'sub_category':'sub_category_fk',
            'product_name':'product_fk',
            'store':'store_id'}

# Template
KPI_SHEET  = 'KPI'
CATEGORY_SHEET = 'Category'
KPI_TYPE = 'kpi_type'
KPI_NAME = 'kpi_name'
KPI_PARENT = 'kpi_parent_name'
KPI_TEMPLATE_TYPE = 'template_type'

NUMERATOR_FILTER = 'numerator_filter'
DENOMINATOR_FILTER = 'denominator_filter'

NUMERATOR_ENTITIES = 'numerator_entities'
DENOMINATOR_ENTITIES = 'denominator_entities'

NUMERATOR_FK = 'numerator_key'
DENOMINATOR_FK = 'denominator_key'

class BATAUToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.templates_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        self.excel_file_path = os.path.join(self.templates_path, 'Template.xlsx')
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

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        self.scif = self.scif[self.scif['facings'] > 0]

        self.calculate_share_of_range_SOR_Type_1()
        self.calculate_share_of_range_SOR_Type_2()
        self.calculate_share_of_range_SOR_Type_3()
        self.calculate_share_of_range_SOR_Category()
        self.calculate_share_of_range_SOR_Sub_Category()

        self.common.commit_results_data()
        score = 0
        return score

    def calculate_share_of_range_SOR_Type_1(self):

        df_tp_ps_kpis = self.get_template_details(KPI_SHEET)
        df_tp_ps_kpis = df_tp_ps_kpis[df_tp_ps_kpis[KPI_TYPE]=='SOR_Type_1']

        kpi_output =  "kpi_parent_name:{}, kpi_name:{}, numerator_result:{},"
        kpi_output += "denominator_result:{}, result:{}"

        for index, row in df_tp_ps_kpis.iterrows():
            kpi = self.kpi_static_data[(self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
                                 & (self.kpi_static_data[TYPE] == row[KPI_NAME]) & (self.kpi_static_data['delete_time'].isnull())]

            if kpi.empty:
                print("KPI Name:{} not found in DB".format(row[KPI_NAME]))
            else:
                    kpi_fk = int(kpi['pk'])
                    kpi_parent_name = str(row[KPI_PARENT])
                    kpi_name = str(row[KPI_NAME])

                    numerator_filter = str(row[NUMERATOR_FILTER])
                    denominator_filter = str(row[DENOMINATOR_FILTER])

                    numerator_entities = [MAPPINGS[x.strip()] for x in str(row[NUMERATOR_ENTITIES]).split(',')]
                    denominator_entities = [MAPPINGS[x.strip()] for x in str(row[DENOMINATOR_ENTITIES]).split(',')]

                    denominator = int(len((self.scif.query(denominator_filter)['product_fk']).drop_duplicates()))

                    numerator_fk = MAPPINGS[str(row[NUMERATOR_FK]).strip()]
                    denominator_fk = MAPPINGS[str(row[DENOMINATOR_FK]).strip()]

                    list_numerator_columns = [MAPPINGS[x.strip()] for x in str(row[NUMERATOR_ENTITIES]).split(',')]
                    list_numerator_columns.append('product_fk')

                    list_denominator_columns = [MAPPINGS[x.strip()] for x in str(row[DENOMINATOR_ENTITIES]).split(',')]
                    list_denominator_columns.append('product_fk')

                    df_denominator = pd.DataFrame(
                        self.scif.query(denominator_filter)[list_denominator_columns]).drop_duplicates()
                    df_denominator = pd.DataFrame(
                        df_denominator.groupby(denominator_entities).size().reset_index(name='count'))

                    if df_denominator.empty:
                        denominator_id = self.store_id

                        print("Denominator: No records for kpi_fk:{} & filter:{}".format(kpi_fk, denominator_filter))

                        if len(kpi_parent_name.strip()) == 0:
                            self.common.write_to_db_result(fk=kpi_fk,
                                                           numerator_id=10,
                                                           numerator_result=0,
                                                           denominator_id=denominator_id,
                                                           denominator_result=0,
                                                           result=0,
                                                           score=0,
                                                           identifier_result=kpi_name,
                                                           should_enter=False)
                        else:
                            self.common.write_to_db_result(fk=kpi_fk,
                                                           numerator_id=10,
                                                           numerator_result=0,
                                                           denominator_id=denominator_id,
                                                           denominator_result=0,
                                                           result=0,
                                                           score=0,
                                                           identifier_parent=kpi_parent_name,
                                                           identifier_result=kpi_name,
                                                           should_enter=True)
                    else:
                        for denominator_index, denominator_row in df_denominator.iterrows():
                            denominator_id = int(denominator_row[denominator_fk])

                            df_numerator = pd.DataFrame(self.scif.query(numerator_filter)[list_numerator_columns]).drop_duplicates()
                            df_numerator = pd.DataFrame(df_numerator.groupby(numerator_entities).size().reset_index(name='count'))

                            if df_numerator.empty:
                                print("Numerator: No records for kpi_fk:{} & filter:{}".format(kpi_fk, numerator_filter))
                                if len(kpi_parent_name.strip()) == 0:
                                    self.common.write_to_db_result(fk=kpi_fk,
                                                                   numerator_id=10,
                                                                   numerator_result=0,
                                                                   denominator_id=denominator_id,
                                                                   denominator_result=0,
                                                                   result=0,
                                                                   score=0,
                                                                   identifier_result=kpi_name,
                                                                   should_enter=False)
                                else:
                                    self.common.write_to_db_result(fk=kpi_fk,
                                                                   numerator_id=10,
                                                                   numerator_result=0,
                                                                   denominator_id=denominator_id,
                                                                   denominator_result=0,
                                                                   result=0,
                                                                   score=0,
                                                                   identifier_parent=kpi_parent_name,
                                                                   identifier_result=kpi_name,
                                                                   should_enter=True)
                            else:
                                for numerator_index, numerator_row in df_numerator.iterrows():
                                    numerator_id = int(numerator_row[numerator_fk])
                                    numerator = int(numerator_row['count'])

                                    try:
                                        score = round(float(numerator) / float(denominator), ROUNDING_DIGITS)
                                    except:
                                        score = 0.0

                                    result = score

                                    if len(kpi_parent_name.strip()) ==0:
                                        self.common.write_to_db_result(fk=kpi_fk,
                                                                       numerator_id=numerator_id,
                                                                       numerator_result=numerator,
                                                                       denominator_id=denominator_id,
                                                                       denominator_result=denominator,
                                                                       result=result,
                                                                       score=score,
                                                                       identifier_result=kpi_name,
                                                                       should_enter=False)
                                    else:
                                        self.common.write_to_db_result(fk=kpi_fk,
                                                                       numerator_id=numerator_id,
                                                                       numerator_result=numerator,
                                                                       denominator_id=denominator_id,
                                                                       denominator_result=denominator,
                                                                       result=result,
                                                                       score=score,
                                                                       identifier_parent=kpi_parent_name,
                                                                       identifier_result=kpi_name,
                                                                       should_enter=True)
                                        print("fk:{}".format(kpi_fk))
                                        print("numerator_id:{}".format(numerator_id))
                                        print("numerator_result:{}".format(numerator))
                                        print("denominator_id:{}".format(denominator_id))
                                        print("denominator_result:{}".format(denominator))
                                        print("result:{}".format(result))
                                        print("score:{}".format(score))
                                        print("identifier_parent:{}".format(kpi_parent_name))
                                        print("identifier_result:{}".format(kpi_name))
                                        print("\n")

    def calculate_share_of_range_SOR_Type_2(self):
        df_tp_ps_kpis = self.get_template_details(KPI_SHEET)
        df_tp_ps_kpis = df_tp_ps_kpis[df_tp_ps_kpis[KPI_TYPE]=='SOR_Type_2']

        kpi_output =  "kpi_parent_name:{}, kpi_name:{}, numerator_result:{},"
        kpi_output += "denominator_result:{}, result:{}"

        for index, row in df_tp_ps_kpis.iterrows():
            # kpi = self.kpi_static_data[(self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
            #                      & (self.kpi_static_data[TYPE] == row[KPI_NAME])]
            kpi = self.kpi_static_data[(self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
                                       & (self.kpi_static_data[TYPE] == row[KPI_NAME]) & (
                                       self.kpi_static_data['delete_time'].isnull())]
            if kpi.empty:
                print("KPI Name:{} not found in DB".format(row[KPI_NAME]))
            else:
                print("KPI Name:{}".format(row[KPI_NAME]))
                kpi_fk = int(kpi['pk'])
                kpi_parent_name = str(row[KPI_PARENT])
                kpi_name = str(row[KPI_NAME])

                numerator_filter = str(row[NUMERATOR_FILTER])
                denominator_filter = str(row[DENOMINATOR_FILTER])

                numerator_entities = [MAPPINGS[x.strip()] for x in str(row[NUMERATOR_ENTITIES]).split(',')]
                denominator_entities = [MAPPINGS[x.strip()] for x in str(row[DENOMINATOR_ENTITIES]).split(',')]

                denominator = int(len((self.scif.query(denominator_filter)['product_fk']).drop_duplicates()))

                numerator_fk = MAPPINGS[str(row[NUMERATOR_FK]).strip()]
                denominator_fk = MAPPINGS[str(row[DENOMINATOR_FK]).strip()]

                list_numerator_columns = [MAPPINGS[x.strip()] for x in str(row[NUMERATOR_ENTITIES]).split(',')]
                list_numerator_columns.append('product_fk')

                list_denominator_columns = [MAPPINGS[x.strip()] for x in str(row[DENOMINATOR_ENTITIES]).split(',')]
                list_denominator_columns.append('product_fk')

                df_denominator = pd.DataFrame(
                    self.scif.query(denominator_filter)[list_denominator_columns]).drop_duplicates()
                df_denominator = pd.DataFrame(
                    df_denominator.groupby(denominator_entities).size().reset_index(name='count'))

                df_numerator = pd.DataFrame(self.scif.query(numerator_filter)[list_numerator_columns]).drop_duplicates()
                df_numerator = pd.DataFrame(df_numerator.groupby(numerator_entities).size().reset_index(name='count'))

                df_result = df_numerator.merge(df_denominator, how='inner', on=denominator_fk)

                if df_result.empty:
                    print("No records for kpi_fk:{} & filter:{}".format(kpi_fk, numerator_filter))
                    if len(kpi_parent_name.strip()) == 0:
                        self.common.write_to_db_result(fk=kpi_fk,
                                                       numerator_id=0,
                                                       numerator_result=0,
                                                       denominator_id=0,
                                                       denominator_result=0,
                                                       result=0,
                                                       score=0,
                                                       identifier_result=kpi_name,
                                                       should_enter=False)
                    else:
                        self.common.write_to_db_result(fk=kpi_fk,
                                                       numerator_id=0,
                                                       numerator_result=0,
                                                       denominator_id=0,
                                                       denominator_result=0,
                                                       result=0,
                                                       score=0,
                                                       identifier_parent=kpi_parent_name,
                                                       identifier_result=kpi_name,
                                                       should_enter=True)
                else:
                    for numerator_index, numerator_row in df_result.iterrows():
                        numerator_id =   int(numerator_row[numerator_fk])
                        denominator_id = int(numerator_row[denominator_fk])

                        numerator = int(numerator_row['count_x'])
                        denominator = int(numerator_row['count_y'])

                        try:
                            score = round(float(numerator) / float(denominator), ROUNDING_DIGITS)
                        except:
                            score = 0.0

                        result = score

                        if len(kpi_parent_name.strip())==0:
                            self.common.write_to_db_result(fk=kpi_fk,
                                                           numerator_id=numerator_id,
                                                           numerator_result=numerator,
                                                           denominator_id=denominator_id,
                                                           denominator_result=denominator,
                                                           result=result,
                                                           score=score,
                                                           identifier_result=kpi_name,
                                                           should_enter=False)
                        else:
                            self.common.write_to_db_result(fk=kpi_fk,
                                                           numerator_id=numerator_id,
                                                           numerator_result=numerator,
                                                           denominator_id=denominator_id,
                                                           denominator_result=denominator,
                                                           result=result,
                                                           score=score,
                                                           identifier_parent=kpi_parent_name,
                                                           identifier_result=kpi_name,
                                                           should_enter=True)
                        print("fk:{}".format(kpi_fk))
                        print("numerator_id:{}".format(numerator_id))
                        print("numerator_result:{}".format(numerator))
                        print("denominator_id:{}".format(denominator_id))
                        print("denominator_result:{}".format(denominator))
                        print("result:{}".format(result))
                        print("score:{}".format(score))
                        print("identifier_parent:{}".format(kpi_parent_name))
                        print("identifier_result:{}".format(kpi_name))
                        print("\n")

    def calculate_share_of_range_SOR_Type_3(self):

        df_tp_ps_kpis = self.get_template_details(KPI_SHEET)
        df_tp_ps_kpis = df_tp_ps_kpis[df_tp_ps_kpis[KPI_TYPE]=='SOR_Type_3']

        print(df_tp_ps_kpis.shape)

        kpi_output =  "kpi_parent_name:{}, kpi_name:{}, numerator_result:{},"
        kpi_output += "denominator_result:{}, result:{}"

        for index, row in df_tp_ps_kpis.iterrows():
            # kpi = self.kpi_static_data[(self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
            #                      & (self.kpi_static_data[TYPE] == row[KPI_NAME])]

            kpi = self.kpi_static_data[(self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
                                       & (self.kpi_static_data[TYPE] == row[KPI_NAME]) & (
                                       self.kpi_static_data['delete_time'].isnull())]
            if kpi.empty:
                print("KPI Name:{} not found in DB".format(row[KPI_NAME]))
                continue

            kpi_fk = int(kpi['pk'])
            kpi_parent_name = str(row[KPI_PARENT])
            kpi_name = str(row[KPI_NAME])

            numerator_filter = str(row[NUMERATOR_FILTER])
            denominator_filter = str(row[DENOMINATOR_FILTER])

            numerator_entities = [MAPPINGS[x.strip()] for x in str(row[NUMERATOR_ENTITIES]).split(',')]
            denominator_entities = [MAPPINGS[x.strip()] for x in str(row[DENOMINATOR_ENTITIES]).split(',')]

            denominator = int(len((self.scif.query(denominator_filter)['product_fk']).drop_duplicates()))

            numerator_fk = MAPPINGS[str(row[NUMERATOR_FK]).strip()]
            denominator_fk = MAPPINGS[str(row[DENOMINATOR_FK]).strip()]

            list_numerator_columns = [MAPPINGS[x.strip()] for x in str(row[NUMERATOR_ENTITIES]).split(',')]
            list_numerator_columns.append('product_fk')

            list_denominator_columns = [MAPPINGS[x.strip()] for x in str(row[DENOMINATOR_ENTITIES]).split(',')]
            list_denominator_columns.append('product_fk')

            df_denominator=pd.DataFrame(self.scif.query(denominator_filter)[list_denominator_columns]).drop_duplicates()
            df_denominator=pd.DataFrame(df_denominator.groupby(denominator_entities).size().reset_index(name='count'))

            if df_denominator.empty:
                denominator_id = self.store_id
                if len(kpi_parent_name.strip()) == 0:
                    self.common.write_to_db_result(fk=kpi_fk,
                                                   numerator_id=0,
                                                   numerator_result=0,
                                                   denominator_id=0,
                                                   denominator_result=0,
                                                   result=0,
                                                   score=0,
                                                   identifier_result=kpi_name,
                                                   should_enter=False)
                else:
                    self.common.write_to_db_result(fk=kpi_fk,
                                                   numerator_id=0,
                                                   numerator_result=0,
                                                   denominator_id=0,
                                                   denominator_result=0,
                                                   result=0,
                                                   score=0,
                                                   identifier_parent=kpi_parent_name,
                                                   identifier_result=kpi_name,
                                                   should_enter=True)
            else:
                df_numerator = pd.DataFrame(self.scif.query(numerator_filter)[list_numerator_columns]).drop_duplicates()
                df_numerator = pd.DataFrame(df_numerator.groupby(numerator_entities).size().reset_index(name='count'))

                if df_numerator.empty:
                    if len(kpi_parent_name.strip()) == 0:
                        self.common.write_to_db_result(fk=kpi_fk,
                                                       numerator_id=0,
                                                       numerator_result=0,
                                                       denominator_id=0,
                                                       denominator_result=0,
                                                       result=0,
                                                       score=0,
                                                       identifier_result=kpi_name,
                                                       should_enter=False)
                    else:
                        self.common.write_to_db_result(fk=kpi_fk,
                                                       numerator_id=0,
                                                       numerator_result=0,
                                                       denominator_id=0,
                                                       denominator_result=0,
                                                       result=0,
                                                       score=0,
                                                       identifier_parent=kpi_parent_name,
                                                       identifier_result=kpi_name,
                                                       should_enter=True)
                else:
                    for numerator_index, numerator_row in df_numerator.iterrows():
                        numerator_id = int(numerator_row[numerator_fk])
                        denominator_id = int(numerator_row[denominator_fk])
                        numerator = int(numerator_row['count'])

                        try:
                            score = round(float(numerator) / float(denominator), ROUNDING_DIGITS)
                        except:
                            score = 0.0

                        result = score

                        if len(kpi_parent_name.strip()) ==0:
                            self.common.write_to_db_result(fk=kpi_fk,
                                                           numerator_id=numerator_id,
                                                           numerator_result=numerator,
                                                           denominator_id=denominator_id,
                                                           denominator_result=denominator,
                                                           result=result,
                                                           score=score,
                                                           identifier_result=kpi_name,
                                                           should_enter=False)
                        else:
                            self.common.write_to_db_result(fk=kpi_fk,
                                                           numerator_id=numerator_id,
                                                           numerator_result=numerator,
                                                           denominator_id=denominator_id,
                                                           denominator_result=denominator,
                                                           result=result,
                                                           score=score,
                                                           identifier_parent=kpi_parent_name,
                                                           identifier_result=kpi_name,
                                                           should_enter=True)
                            print("fk:{}".format(kpi_fk))
                            print("numerator_id:{}".format(numerator_id))
                            print("numerator_result:{}".format(numerator))
                            print("denominator_id:{}".format(denominator_id))
                            print("denominator_result:{}".format(denominator))
                            print("result:{}".format(result))
                            print("score:{}".format(score))
                            print("identifier_parent:{}".format(kpi_parent_name))
                            print("identifier_result:{}".format(kpi_name))
                            print("\n")

    def calculate_share_of_range_SOR_Category(self):

        df_tp_ps_kpis = self.get_template_details(KPI_SHEET)
        df_tp_ps_kpis = df_tp_ps_kpis[df_tp_ps_kpis[KPI_TYPE] == 'SOR_Category']

        kpi_output = "kpi_parent_name:{}, kpi_name:{}, numerator_result:{},"
        kpi_output += "denominator_result:{}, result:{}"

        for index, row in df_tp_ps_kpis.iterrows():
            kpi = self.kpi_static_data[(self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
                                       & (self.kpi_static_data[TYPE] == row[KPI_NAME]) & (
                                       self.kpi_static_data['delete_time'].isnull())]

            if kpi.empty:
                print("KPI Name:{} not found in DB".format(row[KPI_NAME]))
            else:
                print("KPI Name:{}".format(row[KPI_NAME]))
                kpi_fk = int(kpi['pk'])
                kpi_parent_name = str(row[KPI_PARENT])
                kpi_name = str(row[KPI_NAME])

                numerator_filter = str(row[NUMERATOR_FILTER])
                denominator_filter = str(row[DENOMINATOR_FILTER])

                numerator_entities = [MAPPINGS[x.strip()] for x in str(row[NUMERATOR_ENTITIES]).split(',')]
                denominator_entities = [MAPPINGS[x.strip()] for x in str(row[DENOMINATOR_ENTITIES]).split(',')]

                denominator = int(len((self.scif.query(denominator_filter)['product_fk']).drop_duplicates()))

                numerator_fk = MAPPINGS[str(row[NUMERATOR_FK]).strip()]
                denominator_fk = MAPPINGS[str(row[DENOMINATOR_FK]).strip()]

                list_numerator_columns = [MAPPINGS[x.strip()] for x in str(row[NUMERATOR_ENTITIES]).split(',')]
                list_numerator_columns.append('product_fk')

                list_denominator_columns = [MAPPINGS[x.strip()] for x in str(row[DENOMINATOR_ENTITIES]).split(',')]
                list_denominator_columns.append('product_fk')

                df_denominator = pd.DataFrame(
                    self.scif.query(denominator_filter)[list_denominator_columns]).drop_duplicates()
                df_denominator = pd.DataFrame(
                    df_denominator.groupby(denominator_entities).size().reset_index(name='count'))

                df_numerator = pd.DataFrame(self.scif.query(numerator_filter)[list_numerator_columns]).drop_duplicates()
                df_numerator = pd.DataFrame(df_numerator.groupby(numerator_entities).size().reset_index(name='count'))

                df_result = df_numerator.merge(df_denominator, how='inner', on=denominator_fk)

                if df_result.empty:
                    print("No records for kpi_fk:{} & filter:{}".format(kpi_fk, numerator_filter))
                    df_temp = pd.DataFrame([[MANUFACTURER_FK,RYO_FK, 0, 0],[MANUFACTURER_FK, TMC_FK, 0, 0]],
                                                    columns=['manufacturer_fk', 'category_fk', 'count_x', 'count_y'])
                    df_result = df_result.append(df_temp)

                if len(df_result)<2:

                    df_temp = df_result.loc[df_result['category_fk'] == TMC_FK]
                    if df_temp.empty:
                        df_temp = pd.DataFrame([[MANUFACTURER_FK, TMC_FK, 0, 0]],
                                     columns=['manufacturer_fk', 'category_fk', 'count_x', 'count_y'])
                        df_result = df_result.append(df_temp)

                    df_temp = df_result.loc[df_result['category_fk'] == RYO_FK]
                    if df_temp.empty:
                        df_temp = pd.DataFrame([[MANUFACTURER_FK, RYO_FK, 0, 0]],
                                     columns=['manufacturer_fk', 'category_fk', 'count_x', 'count_y'])
                        df_result = df_result.append(df_temp)

                for numerator_index, numerator_row in df_result.iterrows():
                    numerator_id = int(numerator_row[numerator_fk])
                    denominator_id = int(numerator_row[denominator_fk])

                    numerator = int(numerator_row['count_x'])
                    denominator = int(numerator_row['count_y'])

                    try:
                        score = round(float(numerator) / float(denominator), ROUNDING_DIGITS)
                    except:
                        score = 0.0

                    result = score

                    if len(kpi_parent_name.strip()) == 0:
                        self.common.write_to_db_result(fk=kpi_fk,
                                                       numerator_id=numerator_id,
                                                       numerator_result=numerator,
                                                       denominator_id=denominator_id,
                                                       denominator_result=denominator,
                                                       result=result,
                                                       score=score,
                                                       identifier_result=kpi_name,
                                                       should_enter=False)
                    else:
                        self.common.write_to_db_result(fk=kpi_fk,
                                                       numerator_id=numerator_id,
                                                       numerator_result=numerator,
                                                       denominator_id=denominator_id,
                                                       denominator_result=denominator,
                                                       result=result,
                                                       score=score,
                                                       identifier_parent=kpi_parent_name,
                                                       identifier_result=kpi_name,
                                                       should_enter=True)
                    print("fk:{}".format(kpi_fk))
                    print("numerator_id:{}".format(numerator_id))
                    print("numerator_result:{}".format(numerator))
                    print("denominator_id:{}".format(denominator_id))
                    print("denominator_result:{}".format(denominator))
                    print("result:{}".format(result))
                    print("score:{}".format(score))
                    print("identifier_parent:{}".format(kpi_parent_name))
                    print("identifier_result:{}".format(kpi_name))
                    print("\n")

    def calculate_share_of_range_SOR_Sub_Category(self):
        df_tp_ps_kpis = self.get_template_details(KPI_SHEET)
        df_tp_ps_kpis = df_tp_ps_kpis[df_tp_ps_kpis[KPI_TYPE]=='SOR_Sub_Category']

        kpi_output =  "kpi_parent_name:{}, kpi_name:{}, numerator_result:{},"
        kpi_output += "denominator_result:{}, result:{}"

        for index, row in df_tp_ps_kpis.iterrows():
            kpi = self.kpi_static_data[(self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
                                       & (self.kpi_static_data[TYPE] == row[KPI_NAME]) & (
                                       self.kpi_static_data['delete_time'].isnull())]
            if kpi.empty:
                print("KPI Name:{} not found in DB".format(row[KPI_NAME]))
            else:
                print("KPI Name:{}".format(row[KPI_NAME]))
                kpi_fk = int(kpi['pk'])
                kpi_parent_name = str(row[KPI_PARENT])
                kpi_name = str(row[KPI_NAME])

                numerator_filter = str(row[NUMERATOR_FILTER])
                denominator_filter = str(row[DENOMINATOR_FILTER])

                numerator_entities = [MAPPINGS[x.strip()] for x in str(row[NUMERATOR_ENTITIES]).split(',')]
                denominator_entities = [MAPPINGS[x.strip()] for x in str(row[DENOMINATOR_ENTITIES]).split(',')]

                denominator = int(len((self.scif.query(denominator_filter)['product_fk']).drop_duplicates()))

                numerator_fk = MAPPINGS[str(row[NUMERATOR_FK]).strip()]
                denominator_fk = MAPPINGS[str(row[DENOMINATOR_FK]).strip()]

                list_numerator_columns = [MAPPINGS[x.strip()] for x in str(row[NUMERATOR_ENTITIES]).split(',')]
                list_numerator_columns.append('product_fk')

                list_denominator_columns = [MAPPINGS[x.strip()] for x in str(row[DENOMINATOR_ENTITIES]).split(',')]
                list_denominator_columns.append('product_fk')

                df_denominator = pd.DataFrame(
                    self.scif.query(denominator_filter)[list_denominator_columns]).drop_duplicates()
                df_denominator = pd.DataFrame(
                    df_denominator.groupby(denominator_entities).size().reset_index(name='count'))

                df_numerator = pd.DataFrame(self.scif.query(numerator_filter)[list_numerator_columns]).drop_duplicates()
                df_numerator = pd.DataFrame(df_numerator.groupby(numerator_entities).size().reset_index(name='count'))

                df_result = df_numerator.merge(df_denominator, how='inner', on=denominator_fk)

                if df_result.empty:

                    dict_filter = dict((n.strip(), v.strip()) for n, v in (a.split('==') for a in numerator_filter.replace("'","").split(" and ")))

                    if dict_filter[CATEGORY]==TMC:
                        denominator_id = TMC_FK
                    elif dict_filter[CATEGORY]==RYO:
                        denominator_id = RYO_FK
                    else:
                        denominator_id = 0

                    numerator_id = MANUFACTURER_FK

                    if len(kpi_parent_name.strip()) == 0:
                        self.common.write_to_db_result(fk=kpi_fk,
                                                       numerator_id=numerator_id,
                                                       numerator_result=0,
                                                       denominator_id=denominator_id,
                                                       denominator_result=0,
                                                       result=0,
                                                       score=0,
                                                       identifier_result=kpi_name,
                                                       should_enter=False)
                    else:
                        self.common.write_to_db_result(fk=kpi_fk,
                                                       numerator_id=numerator_id,
                                                       numerator_result=0,
                                                       denominator_id=denominator_id,
                                                       denominator_result=0,
                                                       result=0,
                                                       score=0,
                                                       identifier_parent=kpi_parent_name,
                                                       identifier_result=kpi_name,
                                                       should_enter=True)
                else:
                    for numerator_index, numerator_row in df_result.iterrows():
                        numerator_id =   int(numerator_row[numerator_fk])
                        denominator_id = int(numerator_row[denominator_fk])

                        numerator = int(numerator_row['count_x'])
                        denominator = int(numerator_row['count_y'])

                        try:
                            score = round(float(numerator) / float(denominator), ROUNDING_DIGITS)
                        except:
                            score = 0.0

                        result = score

                        if len(kpi_parent_name.strip())==0:
                            self.common.write_to_db_result(fk=kpi_fk,
                                                           numerator_id=numerator_id,
                                                           numerator_result=numerator,
                                                           denominator_id=denominator_id,
                                                           denominator_result=denominator,
                                                           result=result,
                                                           score=score,
                                                           identifier_result=kpi_name,
                                                           should_enter=False)
                        else:
                            self.common.write_to_db_result(fk=kpi_fk,
                                                           numerator_id=numerator_id,
                                                           numerator_result=numerator,
                                                           denominator_id=denominator_id,
                                                           denominator_result=denominator,
                                                           result=result,
                                                           score=score,
                                                           identifier_parent=kpi_parent_name,
                                                           identifier_result=kpi_name,
                                                           should_enter=True)
                        print("fk:{}".format(kpi_fk))
                        print("numerator_id:{}".format(numerator_id))
                        print("numerator_result:{}".format(numerator))
                        print("denominator_id:{}".format(denominator_id))
                        print("denominator_result:{}".format(denominator))
                        print("result:{}".format(result))
                        print("score:{}".format(score))
                        print("identifier_parent:{}".format(kpi_parent_name))
                        print("identifier_result:{}".format(kpi_name))
                        print("\n")

    def get_template_details(self, sheet_name):
        template = pd.read_excel(self.excel_file_path, sheet_name=sheet_name)
        return template
