import os
import pandas as pd

from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

__author__ = 'nidhin'

KPI_SHEET_NAME = 'kpi_list'
PS_KPI_FAMILY = 19
KPI_FAMILY = 'kpi_family_fk'
TYPE = 'type'
KPI_TYPE = 'kpi_type'
KPI_NAME = 'kpi_name'

PRODUCT_PRESENCE_FROM_TARGET = 'PRODUCT_PRESENCE_FROM_TARGET'
PRODUCT_POSITION_FROM_TARGET = 'PRODUCT_POSITION_FROM_TARGET'
PRODUCT_FACING_FROM_TARGET = 'PRODUCT_FACING_FROM_TARGET'
OVERALL_RESULT_FROM_TARGET = 'OVERALL_RESULT_FROM_TARGET'
OVERALL_SCORE_FROM_TARGET = 'OVERALL_SCORE_FROM_TARGET'


class JRIJPToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.match_display_in_scene = self.data_provider.match_display_in_scene
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.templates = self.data_provider[Data.TEMPLATES]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.external_targets = self.ps_data_provider.get_kpi_external_targets()
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.templates_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        self.excel_file_path = os.path.join(self.templates_path, 'Template.xlsx')

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        Important:
            The name of the KPI is used to name the function to calculate it.
            if kpi_name is *test_calc*; the function will be *calculate_test_calc*
        """
        self.calculate_config_related()
        self.parse_and_send_kpi_to_calc()
        self.common.commit_results_data()
        return

    def calculate_config_related(self):
        if self.external_targets.empty:
            Log.info("Not calculating Config related KPIs for Canvas."
                     "External Targets empty while running session: {}".format(self.session_uid))
            return True
        product_presence_from_target_pk = self.kpi_static_data[
            (self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
            & (self.kpi_static_data[TYPE] == PRODUCT_PRESENCE_FROM_TARGET)
            & (self.kpi_static_data['delete_time'].isnull())].iloc[0].pk
        product_position_from_target_pk = self.kpi_static_data[
            (self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
            & (self.kpi_static_data[TYPE] == PRODUCT_POSITION_FROM_TARGET)
            & (self.kpi_static_data['delete_time'].isnull())].iloc[0].pk
        product_facing_from_target_pk = self.kpi_static_data[
            (self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
            & (self.kpi_static_data[TYPE] == PRODUCT_FACING_FROM_TARGET)
            & (self.kpi_static_data['delete_time'].isnull())].iloc[0].pk
        overall_result_from_target_pk = self.kpi_static_data[
            (self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
            & (self.kpi_static_data[TYPE] == OVERALL_RESULT_FROM_TARGET)
            & (self.kpi_static_data['delete_time'].isnull())].iloc[0].pk
        overall_score_from_target_pk = self.kpi_static_data[
            (self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
            & (self.kpi_static_data[TYPE] == OVERALL_SCORE_FROM_TARGET)
            & (self.kpi_static_data['delete_time'].isnull())].iloc[0].pk
        match_prod_in_scene_data = self.match_product_in_scene \
            .merge(self.scene_info, on='scene_fk', suffixes=('', '_scene')) \
            .merge(self.templates, on='template_fk', suffixes=('', '_template'))
        self.external_targets.fillna('', inplace=True)
        for index, each_target in self.external_targets.iterrows():
            _each_target_dict = each_target.to_dict()
            group_fk = _each_target_dict.get('product_group_fk')
            product_fks = _each_target_dict.get('product_fks')
            if type(product_fks) != list:
                product_fks = [product_fks]
            best_shelf_position = _each_target_dict.get('best_shelf_position')
            if type(best_shelf_position) != list:
                best_shelf_position = [best_shelf_position]
            min_product_facing = _each_target_dict.get('min_product_facing', 0)
            template_fks = _each_target_dict.get('template_fks')
            if template_fks and type(template_fks) != list:
                template_fks = [template_fks]
            stacking_exclude = _each_target_dict.get('stacking_exclude')
            min_group_product_facing = _each_target_dict.get('group_facings_count', 0)
            # get mpis based on details
            filtered_mpis = match_prod_in_scene_data
            if template_fks:
                filtered_mpis = filtered_mpis[match_prod_in_scene_data['template_fk'].isin(template_fks)]
            if stacking_exclude == '1':
                filtered_mpis = filtered_mpis[match_prod_in_scene_data['stacking_layer'] == 1]
            product_presence_data = self.calculate_product_presence(
                kpi_pk=product_presence_from_target_pk,
                filtere_mpis=filtered_mpis,
                group_fk=group_fk,
                product_fks=product_fks,
                min_product_facing=min_product_facing
            )
            product_position_data = self.calculate_product_position(
                kpi_pk=product_position_from_target_pk,
                filtere_mpis=filtered_mpis,
                group_fk=group_fk,
                product_fks=product_fks,
                best_shelf_position=best_shelf_position
            )
            product_facings_data = self.calculate_product_facings(
                kpi_pk=product_facing_from_target_pk,
                filtere_mpis=filtered_mpis,
                group_fk=group_fk,
                product_fks=product_fks
            )
            self.calculate_overall_result_and_score(result_kpi_pk=overall_result_from_target_pk,
                                                    score_kpi_fk=overall_score_from_target_pk,
                                                    group_fk=group_fk,
                                                    product_presence_data=product_presence_data,
                                                    product_position_data=product_position_data,
                                                    product_facings_data=product_facings_data,
                                                    best_shelf_position=best_shelf_position,
                                                    min_group_product_facing=min_group_product_facing
                                                    )
        pass

    def calculate_product_presence(self, kpi_pk, filtere_mpis, group_fk, product_fks, min_product_facing):
        data = {}
        for each_product in product_fks:
            prod_data_in_mpis = filtere_mpis[filtere_mpis['product_fk'] == each_product]
            result = 0
            if len(prod_data_in_mpis) >= int(min_product_facing):
                result = 1
            Log.info("Saving product presence for product: {product} as {result} in session {sess} in group: {group}"
                     .format(product=each_product,
                             result=result,
                             sess=self.session_uid,
                             group=group_fk
                             ))
            data[each_product] = result
            self.common.write_to_db_result(fk=kpi_pk,
                                           numerator_id=group_fk,
                                           denominator_id=each_product,
                                           context_id=self.all_products[self.all_products['product_fk']
                                                                        ==each_product].category_fk.iloc[0],
                                           result=result,
                                           score=result,
                                           )
        return data

    def calculate_product_position(self, kpi_pk, filtere_mpis, group_fk, product_fks, best_shelf_position):
        data = {}
        for each_product in product_fks:
            prod_data_in_mpis = filtere_mpis[filtere_mpis['product_fk'] == each_product]
            result = 0  # best shelf from top
            score = 0  # best shelf position from top in CONFIG?
            if prod_data_in_mpis.empty:
                Log.info("Position KPI => Product: {} not found in session: {} for group: {}".format(
                    each_product, self.session_uid, group_fk
                ))
                result = 0
                score = 0
            else:
                prod_data_in_mpis_sorted = prod_data_in_mpis.sort_values(by=['shelf_number'])
                result = prod_data_in_mpis_sorted.iloc[0]['shelf_number']  # => presence_lowest_shelf
                if result in [int(x) for x in best_shelf_position if x.strip()]:
                    score = 1

            Log.info("Saving product position for product: {product} as"
                     " lowest={result}/is in config={score} in session"
                     " {sess} in group: {group}"
                     .format(product=each_product,
                             result=result,
                             score=score,
                             sess=self.session_uid,
                             group=group_fk
                             ))
            data[each_product] = (result, score)
            self.common.write_to_db_result(fk=kpi_pk,
                                           numerator_id=group_fk,
                                           denominator_id=each_product,
                                           context_id=self.all_products[self.all_products['product_fk']
                                                                        ==each_product].category_fk.iloc[0],
                                           result=result,
                                           score=score,
                                           )
        return data

    def calculate_product_facings(self, kpi_pk, filtere_mpis, group_fk, product_fks):
        data = {}
        for each_product in product_fks:
            prod_data_in_mpis = filtere_mpis[filtere_mpis['product_fk'] == each_product]
            if prod_data_in_mpis.empty:
                Log.info("Facings KPI => Product: {} not found in session: {} for group: {}".format(
                    each_product, self.session_uid, group_fk
                ))
                result = 0
            else:
                result = len(prod_data_in_mpis)
            Log.info("Saving product facings for product: {product} as {result} in session {sess} in group: {group}"
                     .format(product=each_product,
                             result=result,
                             sess=self.session_uid,
                             group=group_fk
                             ))
            data[each_product] = result
            self.common.write_to_db_result(fk=kpi_pk,
                                           numerator_id=group_fk,
                                           denominator_id=each_product,
                                           context_id=self.all_products[self.all_products['product_fk']
                                                                        ==each_product].category_fk.iloc[0],
                                           result=result,
                                           score=result,
                                           )
        return data

    def calculate_overall_result_and_score(self, result_kpi_pk, score_kpi_fk, group_fk,
                                           product_presence_data, product_position_data,
                                           product_facings_data, best_shelf_position,
                                           min_group_product_facing):
        numerator_result = 1 in product_presence_data.values()
        # product_position_data -> (min_shelf, is_in_config)
        min_level_of_product = 0
        if product_position_data:
            _score_products_presence = filter(lambda x: x[1] == 1, product_position_data.values())
            if _score_products_presence:
                # find min result among the in config ones
                _score_products_presence.sort(key=lambda x: x[0])
                min_level_of_product = int(_score_products_presence[0][0])
            else:
                # score is all 0
                _score_products_presence_present = filter(lambda x: x[0] != 0, product_position_data.values())
                _score_products_presence_present.sort(key=lambda x: x[0])
                if _score_products_presence_present:
                    min_level_of_product = _score_products_presence_present[0][0]
        else:
            Log.info("Product presence information is empty for any product in group = {group_fk}".format(
                group_fk=group_fk
            ))
        Log.info("Saving Overall Result for group: {group_fk} in session {sess}"
                 .format(group_fk=group_fk,
                         sess=self.session_uid,
                         ))
        self.common.write_to_db_result(fk=result_kpi_pk,
                                       numerator_id=group_fk,
                                       denominator_id=self.store_id,
                                       context_id=self.store_id,
                                       numerator_result=int(numerator_result),  # bool to int
                                       denominator_result=min_level_of_product,
                                       result=sum(product_facings_data.values()),  # bool to int
                                       )
        Log.info("Saving Overall Score for group: {group_fk} in session {sess}"
                 .format(group_fk=group_fk,
                         sess=self.session_uid,
                         ))
        is_in_best_shelf = False
        if best_shelf_position and min_level_of_product:
            if type(best_shelf_position) != list:
                best_shelf_position = [best_shelf_position]
            is_in_best_shelf = min_level_of_product in map(lambda x: int(x), best_shelf_position)
        has_minumum_facings_per_config = False
        if not min_group_product_facing or not min_group_product_facing.strip():
            min_group_product_facing = 0
        if sum(product_facings_data.values()) >= int(min_group_product_facing):
            has_minumum_facings_per_config = True
        self.common.write_to_db_result(fk=score_kpi_fk,
                                       numerator_id=group_fk,
                                       denominator_id=self.store_id,
                                       context_id=self.store_id,
                                       numerator_result=int(numerator_result),  # bool to int
                                       denominator_result=int(is_in_best_shelf),  # bool to int
                                       result=int(has_minumum_facings_per_config),  # bool to int
                                       score=int(all([numerator_result, is_in_best_shelf,
                                                      has_minumum_facings_per_config]))   # bool to int
                                       )

    def parse_and_send_kpi_to_calc(self):
        kpi_sheet = self.get_template_details(KPI_SHEET_NAME)
        for index, kpi_sheet_row in kpi_sheet.iterrows():
            kpi = self.kpi_static_data[(self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
                                       & (self.kpi_static_data[TYPE] == kpi_sheet_row[KPI_TYPE])
                                       & (self.kpi_static_data['delete_time'].isnull())]
            if kpi.empty:
                Log.info("KPI Name:{} not found in DB".format(kpi_sheet_row[KPI_NAME]))
            else:
                Log.info("KPI Name:{} found in DB".format(kpi_sheet_row[KPI_NAME]))
                kpi_method_to_calc = getattr(self, 'calculate_{kpi}'.format(kpi=kpi_sheet_row[KPI_NAME].lower()), None)
                if not kpi_method_to_calc:
                    Log.warning("Method not defined for KPI Name:{}.".format(kpi_sheet_row[KPI_NAME].lower()))
                    pass
                kpi_fk = kpi.pk.values[0]
                kpi_method_to_calc(kpi_fk)

    def calculate_count_posm_per_scene(self, kpi_fk):
        if self.match_display_in_scene.empty:
            Log.info("No POSM detected at scene level for session: {}".format(self.session_uid))
            return False
        grouped_data = self.match_display_in_scene.groupby(['scene_fk', 'display_fk'])
        for data_tup, scene_data_df in grouped_data:
            scene_fk, display_fk = data_tup
            posm_count = len(scene_data_df)
            template_fk = self.scene_info[self.scene_info['scene_fk'] == scene_fk].get('template_fk')
            if not template_fk.empty:
                cur_template_fk = int(template_fk)
            else:
                Log.info("JRIJP: Scene ID {scene} is not complete and not found in scene Info.".format(
                    scene=scene_fk))
                continue
            self.common.write_to_db_result(fk=kpi_fk,
                                           numerator_id=display_fk,
                                           denominator_id=self.store_id,
                                           context_id=cur_template_fk,
                                           result=posm_count,
                                           score=scene_fk)

    def calculate_facings_in_cell_per_product(self, kpi_fk):
        match_prod_scene_data = self.match_product_in_scene.merge(
            self.products, how='left', on='product_fk', suffixes=('', '_prod'))
        grouped_data = match_prod_scene_data.query(
            '(stacking_layer==1) or (product_type=="POS")'
        ).groupby(
            ['scene_fk', 'bay_number', 'shelf_number', 'product_fk']
        )
        for data_tup, scene_data_df in grouped_data:
            scene_fk, bay_number, shelf_number, product_fk = data_tup
            facings_count_in_cell = len(scene_data_df)
            cur_template_fk = int(self.scene_info[self.scene_info['scene_fk'] == scene_fk].get('template_fk'))
            self.common.write_to_db_result(fk=kpi_fk,
                                           numerator_id=product_fk,
                                           denominator_id=self.store_id,
                                           context_id=cur_template_fk,
                                           numerator_result=bay_number,
                                           denominator_result=shelf_number,
                                           result=facings_count_in_cell,
                                           score=scene_fk)

    def get_template_details(self, sheet_name):
        template = pd.read_excel(self.excel_file_path, sheetname=sheet_name)
        return template
