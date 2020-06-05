import pandas as pd
from Trax.Cloud.Services.Connector.Logger import Log
from Projects.DIAGEOUS_SAND2.Utils.menu_count.consts import Consts
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from Projects.DIAGEOUS_SAND2.Utils.menu_count.Fetcher import DiageoQueries
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers


class MenuToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, common):
        GlobalSessionToolBox.__init__(self, data_provider, None)
        # self.matches = self.get_filtered_matches()
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalcAdmin)
        self.custom_entity = self.get_custom_entity()

        self.targets = self.ps_data_provider.get_kpi_external_targets(key_fields=['store_number_1', 'ean_code'])
        self.store_number = self.store_info.store_number_1
        self.common = common

    def _get_filtered_match_display_in_scene(self):
        """ This method filters match display in scene - it saves only "close" and "open" tags"""
        mdis = self.data_provider.match_display_in_scene.loc[
            self.data_provider.match_display_in_scene.display_name.str.contains("Open|Close|open|close")]
        return mdis

    def main_calculation(self):
        """This method calculates the entire Case Count KPIs set."""
        self.get_relevant_targets()
        self.menu_count()

    def menu_count(self):
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.MENU_KPI_CHILD)
        parent_kpi = self.get_kpi_fk_by_kpi_type(Consts.TOTAL_MENU_KPI_SCORE)
        # we need to save a second set of KPIs with heirarchy for the mobile report
        kpi_fk_mr = self.get_kpi_fk_by_kpi_type(Consts.MENU_KPI_CHILD_MR)
        parent_kpi_mr = self.get_kpi_fk_by_kpi_type(Consts.TOTAL_MENU_KPI_SCORE_MR)

        if self.targets.empty:
            return
        try:
            menu_ean_codes = self.targets.ean_code.unique().tolist()
        except AttributeError:
            Log.warning('Menu Count targets are corrupt for this store')
            return

        filtered_scif = self.scif[self.scif['template_group'].str.contains('Menu')]
        present_menu_scif_sub_brands = filtered_scif.sub_brand.unique().tolist()
        passed_eans = 0

        # found_ean_codes = filtered_scif.product_ean_code.unique().tolist()

        for ean_code in menu_ean_codes:
            result = 0
            sub_brand = self.all_products['sub_brand'][self.all_products['product_ean_code'] == ean_code].iloc[0]
            product_fk = self.all_products['product_fk'][self.all_products['product_ean_code'] == ean_code].iloc[0]

            custom_entity_df = self.custom_entity['pk'][self.custom_entity['name'] == sub_brand]
            if custom_entity_df.empty:
                custom_entity_pk = -1
            else:
                custom_entity_pk = custom_entity_df.iloc[0]

            if sub_brand in present_menu_scif_sub_brands:
                result = 1
                passed_eans += 1

            self.write_to_db(fk=kpi_fk_mr, numerator_id=product_fk, numerator_result=0, denominator_result=0,
                             denominator_id=custom_entity_pk,
                             result=result, score=0, identifier_parent=parent_kpi_mr, identifier_result=kpi_fk_mr,
                             should_enter=True)

            self.write_to_db(fk=kpi_fk, numerator_id=product_fk, numerator_result=0, denominator_result=0,
                             denominator_id=custom_entity_pk, result=result, score=0)

        target_eans = len(menu_ean_codes)
        self.write_to_db(fk=parent_kpi_mr, numerator_id=self.manufacturer_fk, numerator_result=0, denominator_result=0,
                         denominator_id=self.store_id,
                         result=passed_eans, score=0, target=target_eans, identifier_result=parent_kpi_mr)

        self.write_to_db(fk=parent_kpi, numerator_id=self.manufacturer_fk, numerator_result=0, denominator_result=0,
                         denominator_id=self.store_id,
                         result=passed_eans, score=0, target=target_eans)

    def get_relevant_targets(self):
        self.targets = self.targets[(self.targets['store_number_1'] == self.store_number.iloc[0])
                                    & (self.targets['operation_type'] == 'menu_brand')]

    def get_custom_entity(self):
        query = DiageoQueries.get_custom_entities_query()
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result
