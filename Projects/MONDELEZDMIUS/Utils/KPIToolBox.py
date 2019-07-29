from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from Trax.Utils.Logging.Logger import Log
import pandas as pd
from pandas.io.json.normalize import json_normalize
import json
import numpy
import ast
import os

from KPIUtils_v2.DB.CommonV2 import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from Projects.MONDELEZDMIUS.Utils.Const import Const
# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

__author__ = 'nicolaske'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class MONDELEZDMIUSToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

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
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.manufacturer_fk = self.products['manufacturer_fk'][self.products['manufacturer_name'] ==
                                                                'MONDELEZ INTERNATIONAL, INC.'].iloc[0]
        self.assortment = Assortment(self.data_provider, common=self.common)
        self.store_number = self.get_store_number()
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.custom_entities = self.ps_data_provider.get_custom_entities(Const.PPG_ENTITY_TYPE_FK)

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """

        self.calculate_display_count()
        self.calculate_assortment()
        self.calculate_FD_compliance()
        self.calculate_scripted_compliance()
        self.common.commit_results_data()

    def calculate_display_count(self):
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DISPLAY_COUNT_kpi)

        count_of_scenes = len(self.scif['scene_fk'].unique().tolist())
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=count_of_scenes,
                                       denominator_id=self.store_id, denominator_result=1, result=count_of_scenes,
                                       score=count_of_scenes)

    def calculate_FD_compliance(self):

        score = 0
        compliance_status = Const.NON_COMPLIANT_FK
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.FD_COMPLIANCE_KPI)
        if self.store_assortment.empty:
            # LOG ERROR
            pass
        else:
            filtered_assortment = self.store_assortment[self.store_assortment['kpi_fk_lvl3'] == kpi_fk]
            if not filtered_assortment.empty:
                for i, row in filtered_assortment.iterrows():
                    ppg = row[Const.PPG_COLUMN_NAME]
                    sub_ppg = row[Const.SUB_PPG_COLUMN_NAME]
                    ppg2 = row[Const.PPG_COLUMN_NAME_2]
                    sub_ppg2 = row[Const.SUB_PPG_COLUMN_NAME_2]
                    if not pd.isnull(ppg):
                        product_fk = self.custom_entities['pk'][self.custom_entities['name'] == ppg].iloc[0]
                        filtered_scif_count = len(self.scif[self.scif['PPG'] == ppg])
                        if filtered_scif_count > 0:
                            score = 1
                            compliance_status = Const.COMPLIANT_FK
                        else:
                            score = 0

                            compliance_status = Const.NON_COMPLIANT_FK

                    if not pd.isnull(ppg2):
                        product_fk = self.custom_entities['pk'][self.custom_entities['name'] == ppg2].iloc[0]
                        filtered_scif_count = len(self.scif[self.scif['PPG'] == ppg2])
                        if filtered_scif_count > 0:
                            score = 1
                            compliance_status = Const.COMPLIANT_FK
                        else:
                            score = 0

                            compliance_status = Const.NON_COMPLIANT_FK

                    if not pd.isnull(sub_ppg):
                        product_fk = \
                            self.custom_entities['pk'][self.custom_entities['name'] == sub_ppg].iloc[0]
                        filtered_scif_count = len(self.scif[self.scif['Sub PPG'] == sub_ppg])
                        if filtered_scif_count > 0:
                            score = 1
                            compliance_status = Const.COMPLIANT_FK

                        else:
                            score = 0
                            product_fk = 0
                            compliance_status = Const.NON_COMPLIANT_FK

                    if not pd.isnull(sub_ppg2):
                        product_fk = \
                            self.custom_entities['pk'][self.custom_entities['name'] == sub_ppg2].iloc[0]
                        filtered_scif_count = len(self.scif[self.scif['Sub PPG'] == sub_ppg2])
                        if filtered_scif_count > 0:
                            score = 1
                            compliance_status = Const.COMPLIANT_FK

                        else:
                            score = 0
                            product_fk = 0
                            compliance_status = Const.NON_COMPLIANT_FK

                    self.common.write_to_db_result(fk=kpi_fk, numerator_id=product_fk, numerator_result=score,
                                                   denominator_id=self.store_id, denominator_result=1,
                                                   result=compliance_status, score=score)

    def calculate_scripted_compliance(self):
        score = 0
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.SCRIPTED_COMPLIANCE_KPI)
        if self.store_assortment.empty:
            # LOG ERROR
            pass
        else:
            filtered_assortment = self.store_assortment[self.store_assortment['kpi_fk_lvl3'] == kpi_fk]
            if not filtered_assortment.empty:
                for i, row in filtered_assortment.iterrows():
                    ppg = row[Const.PPG_COLUMN_NAME]
                    sub_ppg = row[Const.SUB_PPG_COLUMN_NAME]
                    ppg2 = row[Const.PPG_COLUMN_NAME_2]
                    sub_ppg2 = row[Const.SUB_PPG_COLUMN_NAME_2]
                    if not pd.isnull(ppg):
                        product_fk = self.custom_entities['pk'][self.custom_entities['name'] == ppg].iloc[0]
                        filtered_scif_count = len(self.scif[self.scif['PPG'] == ppg])
                        if filtered_scif_count > 0:
                            score = 1
                            compliance_status = Const.COMPLIANT_FK
                        else:
                            score = 0

                            compliance_status = Const.NON_COMPLIANT_FK

                    if not pd.isnull(ppg2):
                        product_fk = self.custom_entities['pk'][self.custom_entities['name'] == ppg2].iloc[0]
                        filtered_scif_count = len(self.scif[self.scif['PPG'] == ppg2])
                        if filtered_scif_count > 0:
                            score = 1
                            compliance_status = Const.COMPLIANT_FK
                        else:
                            score = 0

                            compliance_status = Const.NON_COMPLIANT_FK

                    if not pd.isnull(sub_ppg):
                        product_fk = \
                            self.custom_entities['pk'][self.custom_entities['name'] == sub_ppg].iloc[0]
                        filtered_scif_count = len(self.scif[self.scif['Sub PPG'] == sub_ppg])
                        if filtered_scif_count > 0:
                            score = 1
                            compliance_status = Const.COMPLIANT_FK

                        else:
                            score = 0
                            product_fk = 0
                            compliance_status = Const.NON_COMPLIANT_FK

                    if not pd.isnull(sub_ppg2):
                        product_fk = \
                            self.custom_entities['pk'][self.custom_entities['name'] == sub_ppg2].iloc[0]
                        filtered_scif_count = len(self.scif[self.scif['Sub PPG'] == sub_ppg2])
                        if filtered_scif_count > 0:
                            score = 1
                            compliance_status = Const.COMPLIANT_FK

                        else:
                            score = 0
                            product_fk = 0
                            compliance_status = Const.NON_COMPLIANT_FK

                    self.common.write_to_db_result(fk=kpi_fk, numerator_id=product_fk, numerator_result=score,
                                                   denominator_id=self.store_id, denominator_result=1,
                                                   result=compliance_status, score=score)

    def calculate_assortment(self):
        self.store_assortment = self.assortment.store_assortment
        self.store_assortment.reset_index(inplace=True)
        if self.store_assortment.empty:
            return
        else:
            ppg_list = [ast.literal_eval(x) for x in self.store_assortment['additional_attributes'].to_list() if x]
            df_assortment = pd.DataFrame(ppg_list)
            merged_assortment = self.store_assortment.join(df_assortment)
            self.store_assortment = merged_assortment

            # self.store_assortment['additional_attributes'] = self.store_assortment['additional_attributes'].apply(json.loads)
            # self.store_assortment[['ppg', 'value']] = json_normalize(self.store_assortment['additional_attributes'])

    def get_store_assortment(self, policy_filter=None):
        """
        fetch all active assortment for store type in current date
        :param policy_filter: str, for filtering policy (if we know its name)
        :return:
        """
        # TODO parse store policy and to where 1.date 2. store attreibute to match policy
        # Log.info("get_store_assortment query_v2")
        policy_str = ''
        if policy_filter:
            policy_str = " and pol.policy_name = '{}'".format(policy_filter)
        query = """select p.pk as product_fk, p.name, p.ean_code,
                    atp.assortment_additional_attribute as additional_attributes, atp.start_date,
                   pol.policy, pol.policy_type, ass.*
            from (select group3.*, ass3.kpi_fk as kpi_fk_lvl1
                from
                    (SELECT
                        group2.assortment_fk, group2.assortment_name, group2.kpi_fk as kpi_fk_lvl3, coalesce(group2.store_policy_group_fk, 
                            (select store_policy_group_fk from pservice.assortment where pk = group2.assortment_group_fk)) as store_policy_group_fk,
                        group2.assortment_group_fk, coalesce(group1.kpi_fk, 
                            (select kpi_fk from pservice.assortment where pk = group2.assortment_group_fk)) as kpi_fk_lvl2, 
                        group1.target, group1.start_date as group_target_date, group1.assortment_group_fk as assortment_super_group_fk, group1.super_group_target
                    FROM
                        (select atag1.*, ass1.assortment_name, ass1.kpi_fk, ass1.store_policy_group_fk, ass1.target as super_group_target 
                        from pservice.assortment_to_assortment_group as atag1 , 
                        pservice.assortment as ass1
                        where atag1.assortment_fk = ass1.pk and atag1.end_date is null) as group1
                        right join
                        (select atag2.*, ass2.assortment_name, ass2.kpi_fk, ass2.store_policy_group_fk 
                        from pservice.assortment_to_assortment_group as atag2,
                        pservice.assortment as ass2
                        where atag2.assortment_fk = ass2.pk and atag2.end_date is null) as group2
                        on group1.assortment_fk = group2.assortment_group_fk) as group3
                    left join
                    pservice.assortment as ass3
                    on ass3.pk = group3.assortment_super_group_fk) as ass,
                pservice.assortment_to_product as atp,
                static_new.product as p,
                pservice.policy as pol
            where atp.product_fk = p.pk and
            atp.assortment_fk = ass.assortment_fk and
            pol.pk = ass.store_policy_group_fk
            and ((atp.end_date is null and '{0}' >= atp.start_date) or
            ('{0}' between atp.start_date and atp.end_date)){1};
        """.format(self.visit_date, policy_str)
        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        res = cur.fetchall()
        headers = ['product_fk', 'name', 'ean_code', 'additional_attributes', 'start_date', 'policy', 'policy_type',
                   'assortment_fk', 'assortment_name', 'kpi_fk_lvl3', 'store_policy_group_fk', 'assortment_group_fk',
                   'kpi_fk_lvl2', 'target', 'group_target_date', 'assortment_super_group_fk', 'super_group_target',
                   'kpi_fk_lvl1']
        df = pd.DataFrame(list(res), columns=headers)
        df = df.drop_duplicates(subset=['product_fk', 'assortment_fk', 'assortment_group_fk',
                                        'assortment_super_group_fk'], keep='last')

        # df['additional_atttributes'] =

        return df

    def get_store_number(self):
        query = """select store_number_1 from probedata.session ps 
            left join static.stores store on store.pk = ps.store_fk
            where session_uid = '{}'
                """.format(self.session_uid, )
        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        res = cur.fetchall()

        df = pd.DataFrame(list(res))
        return int(df.iloc[0])