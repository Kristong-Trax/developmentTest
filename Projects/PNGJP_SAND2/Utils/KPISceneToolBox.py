import numpy as np
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
from Trax.Utils.Logging.Logger import Log
from Projects.PNGJP_SAND2.Utils.TemplateParser import PNGJPTemplateParser

__author__ = 'prasanna'

# The KPIs for Layout Compliance
PGJAPAN_BLOCK_COMPLIANCE_BY_SCENE = 'PGJAPAN_BLOCK_COMPLIANCE_BY_SCENE'
PGJAPAN_GOLDEN_ZONE_COMPLIANCE_BY_SCENE = 'PGJAPAN_GOLDEN_ZONE_COMPLIANCE_BY_SCENE'

# other constants
KPI_TYPE_COL = 'type'


class PNGJPSceneToolBox:

    def __init__(self, data_provider, output, common):
        self.output = output
        self.data_provider = data_provider
        self.common = common
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.templates = self.data_provider[Data.TEMPLATES]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.scif = self.data_provider.scene_item_facts
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.store_info.iloc[0].store_fk
        self.store_type = self.data_provider.store_type
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.template_parser = PNGJPTemplateParser(self.data_provider, self.rds_conn)

        self.targets_from_template = self.template_parser.get_targets()
        self.custom_entity_data = self.template_parser.get_custom_entity()

        self.match_display_in_scene = self.data_provider.match_display_in_scene
        self.current_scene_fk = self.scene_info.iloc[0].scene_fk
        self.store_banner_name = self.store_info.iloc[0].additional_attribute_20
        self.template_name = self.templates.iloc[0].template_name
        self.match_product_data = self.match_product_in_scene.merge(self.products, on='product_fk', how='left')
        self.block = Block(self.data_provider, self.output)

    @staticmethod
    def ensure_as_list(template_fks):
        if isinstance(template_fks, list):
            ext_target_template_fks = template_fks
        else:
            ext_target_template_fks = list([template_fks])
        return ext_target_template_fks

    def check_if_the_kpi_is_available(self, kpi_name):
        status = True
        res_df = self.kpi_static_data[self.kpi_static_data[KPI_TYPE_COL] == kpi_name]
        if res_df.empty:
            status = False
            Log.warning("Error: KPI {} not found in static.kpi_level_2 table.".format(kpi_name))
        return status

    def calculate_layout_compliance(self):
        current_scene_fk = self.scene_info.iloc[0].scene_fk
        Log.info('Calculate Layout Compliance for session: {sess} - scene: {scene}'
                 .format(sess=self.session_uid, scene=current_scene_fk))

        if self.targets_from_template["Block"].empty and self.targets_from_template["Golden Zone"].empty:
            Log.warning('Unable to calculate PNGJP_LAYOUT_COMPLIANCE_KPIs: external targets are empty')
            return

        try:
            self.calculate_pngjp_block_compliance()
        except Exception as e:
            Log.error("Error : {}".format(e))

        try:
            self.calculate_pngjp_golden_zone_compliance()
        except Exception as e:
            Log.error("Error : {}".format(e))

    def calculate_pngjp_block_compliance(self):
        if not self.check_if_the_kpi_is_available(PGJAPAN_BLOCK_COMPLIANCE_BY_SCENE):
            Log.warning('Unable to calculate PGJAPAN_BLOCK_COMPLIANCE_BY_SCENE: KPIs are not in kpi_level_2')
            return

        kpi_details = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == PGJAPAN_BLOCK_COMPLIANCE_BY_SCENE)
            & (self.kpi_static_data['delete_time'].isnull())]

        Log.info("Calculating {kpi} for session: {sess} and scene: {scene}".format(
            kpi=kpi_details.iloc[0][KPI_TYPE_COL],
            sess=self.session_uid,
            scene=self.current_scene_fk,
        ))
        block_targets = self.targets_from_template["Block"]
        # if no targets return
        if block_targets.empty:
            Log.warning('There is no target policy in the template for calculating {}'.format(
                kpi_details.iloc[0][KPI_TYPE_COL]
            ))
            return False
        else:
            for idx, each_target in block_targets.iterrows():
                # check for template match in target
                product_group_df = self.custom_entity_data[
                    self.custom_entity_data['pk'].isin(each_target.product_group_name_fks)
                ]
                if product_group_df.empty:
                    Log.warning("Product group name: {} not found in custom_entity".format(
                        each_target['Product Group Name']
                    ))
                    continue
                product_group_name = product_group_df.name.iloc[0]
                product_group_fk = product_group_df.pk.iloc[0]
                current_template_fk = self.templates.iloc[0].template_fk
                category_fk = each_target.category_fks[0]

                if current_template_fk not in self.ensure_as_list(each_target.template_fks):
                    Log.info("""Session: {sess}; Scene:{scene}. Scene Type not matching [{k} not in {v}] 
                    target for calculating {kpi}."""
                             .format(sess=self.session_uid,
                                     scene=self.current_scene_fk,
                                     kpi=kpi_details.iloc[0][KPI_TYPE_COL],
                                     k=self.templates.iloc[0].template_fk,
                                     v=each_target.template_fks,
                                     ))
                    continue
                else:
                    result = score = 0
                    total_facings_count = biggest_block_facings_count = 0
                    # TODO: Ensure if its .8 or 80
                    block_threshold_perc = each_target.block_threshold_perc

                    Log.info(
                        "Calculating blocking KPI for category: {cat} - filter {population_filter}".format(
                            cat=each_target.category_fks,
                            population_filter=each_target.population_filter
                        ))
                    stacking_include = each_target.stacking_include

                    # able to pass sub cat and super brand[?] // or get the prods and pass
                    population_filters = each_target.population_filter
                    location_filters = {'scene_fk': [self.current_scene_fk]}
                    additional_filters = {
                        'minimum_facing_for_block': 1,
                        'minimum_block_ratio': 0,
                        'include_stacking': stacking_include,
                        'check_vertical_horizontal': True,
                    }
                    block_res = self.block.network_x_block_together(
                        population_filters, location_filters, additional_filters
                    )
                    block_res.dropna(subset=['total_facings'], inplace=True)
                    block_res = block_res.query('is_block==True')
                    if block_res.empty:
                        Log.info(
                            "Fail: Cannot find the block for the population filter: {population_filter} "
                            "category: {cat}.".format(
                                cat=category_fk,
                                population_filter=each_target.population_filter
                            ))
                        continue
                    else:
                        Log.info(
                            "Found block for the population filter: {population_filter} "
                            "category: {cat}. Check and save.".format(
                                cat=category_fk,
                                population_filter=each_target.population_filter
                            ))
                        biggest_cluster = block_res.sort_values(by='block_facings', ascending=False).head(1)
                        biggest_block_facings_count = float(biggest_cluster['block_facings'])
                        total_facings_count = float(biggest_cluster['total_facings'])
                        if total_facings_count:
                            result = round((biggest_block_facings_count / total_facings_count) * 100, 2)
                        if result >= block_threshold_perc:
                            score = 1
                        try:
                            g = biggest_cluster.cluster.iloc[0]
                            shelf_spread_count_for_biggest_block = len(list(list(g.nodes("shelf_number"))[0][-1]))
                        except Exception as e:
                            Log.error("Error: {}".format(e))
                            shelf_spread_count_for_biggest_block = 0

                    self.common.write_to_db_result(
                        fk=kpi_details.iloc[0].pk,
                        numerator_id=product_group_fk,
                        denominator_id=current_template_fk,
                        context_id=category_fk,
                        numerator_result=biggest_block_facings_count,
                        denominator_result=total_facings_count,
                        target=block_threshold_perc,
                        result=result,
                        score=score,
                        weight=shelf_spread_count_for_biggest_block,
                        by_scene=True,
                    )

    def calculate_pngjp_golden_zone_compliance(self):
        if not self.check_if_the_kpi_is_available(PGJAPAN_GOLDEN_ZONE_COMPLIANCE_BY_SCENE):
            Log.warning('Unable to calculate PGJAPAN_GOLDEN_ZONE_COMPLIANCE_BY_SCENE: KPIs are not in kpi_level_2')
            return

        kpi_details = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == PGJAPAN_GOLDEN_ZONE_COMPLIANCE_BY_SCENE)
            & (self.kpi_static_data['delete_time'].isnull())]

        Log.info("Calculating {kpi} for session: {sess} and scene: {scene}".format(
            kpi=kpi_details.iloc[0][KPI_TYPE_COL],
            sess=self.session_uid,
            scene=self.current_scene_fk,
        ))
        goldenzone_targets = self.targets_from_template["Golden Zone"]

        def _get_shelf_range(sh):
            """Input => string ~ '1_2_shelf
                Output => xrange ~ xrange(1,3)
            '"""
            split_sh = sh.split('_')[:2]
            int_sh = map(int, split_sh)
            int_sh[1] = int_sh[1] + 1
            return xrange(*int_sh)

        # if no targets return
        if goldenzone_targets.empty:
            Log.warning('There is no target policy for calculating {}'.format(
                kpi_details.iloc[0][KPI_TYPE_COL]
            ))
            return False
        else:
            # target_shelf_config_keys => '1_5_shelf, 6_7_shelf, above_12_shelf etc'
            for idx, each_target in goldenzone_targets.iterrows():

                product_group_df = self.custom_entity_data[
                    self.custom_entity_data['pk'].isin(each_target.product_group_name_fks)
                ]
                if product_group_df.empty:
                    Log.warning("Product group name: {} not found in custom_entity".format(
                        each_target['Product Group Name']
                    ))
                    continue
                product_group_name = product_group_df.name.iloc[0]
                product_group_fk = product_group_df.pk.iloc[0]
                current_template_fk = self.templates.iloc[0].template_fk
                category_fk = each_target.category_fks[0]

                if current_template_fk not in self.ensure_as_list(each_target.template_fks):
                    Log.info("""Session: {sess}; Scene:{scene}. Scene Type not matching [{k} not in {v}] 
                                    target for calculating {kpi}."""
                             .format(sess=self.session_uid,
                                     scene=self.current_scene_fk,
                                     kpi=kpi_details.iloc[0][KPI_TYPE_COL],
                                     k=self.templates.iloc[0].template_fk,
                                     v=each_target.template_fks,
                                     ))
                    continue

                result = score = 0
                target_shelf_config_keys = each_target[each_target.keys().map(lambda x: x.endswith('_shelf'))]
                population_filter = each_target.population_filter
                stacking_include = each_target.stacking_include
                # numerator - Cumulative no of Facings "of the brand and sub category" available at desired shelf
                numerator = 0  # number of facings available in desired shelf

                stack_filtered_mpis = self.match_product_data
                if not stacking_include:
                    # consider only stacking layer 1 products
                    stack_filtered_mpis = self.match_product_data[self.match_product_data['stacking_layer'] == 1]


                # Apply filters based on population_filter
                boolean_masks = []
                for column_name, values in population_filter.items():
                    boolean_masks.append(
                        stack_filtered_mpis[column_name].isin(values)
                    )
                population_filter_final_mask = reduce(lambda x, y: x & y, boolean_masks)
                denominator = len(stack_filtered_mpis[population_filter_final_mask])
                # denominator - total number of facings "of the brand and sub category" available in whole scene

                if denominator:
                    for bay_number, grouped_bay_data in stack_filtered_mpis.groupby('bay_number'):
                        Log.info("Running {kpi} for bay {bay}".format(
                            kpi=kpi_details.iloc[0][KPI_TYPE_COL],
                            bay=bay_number
                        ))
                        # min_shelf = self.match_product_data['shelf_number'].unique().min()
                        max_shelf = grouped_bay_data['shelf_number'].unique().max()
                        shelf_config_key = None
                        for each_shelf_conf in target_shelf_config_keys.keys():
                            if each_shelf_conf.startswith('above'):
                                above_shelf = int(each_shelf_conf.split('_')[1])
                                if max_shelf > above_shelf:
                                    # if strictly greater ~ above_12_shelf
                                    # satisfies when shelf is 13 and above
                                    shelf_config_key = each_shelf_conf
                                    break
                            elif max_shelf in _get_shelf_range(each_shelf_conf):
                                shelf_config_key = each_shelf_conf
                                break
                        if not shelf_config_key:
                            Log.error(""" Session: {sess}; Scene:{scene}. 
                                      There is no shelf policy for calculating {kpi}.""".format(
                                sess=self.session_uid,
                                scene=self.current_scene_fk,
                                kpi=kpi_details.iloc[0][KPI_TYPE_COL]
                            ))
                            # TODO: Think about it ? return or continue?
                            return False

                        # find the brand products in the shelf_config_key
                        interested_shelves = each_target[shelf_config_key]
                        Log.info("Using {shelf_config} => shelves to check {shelves} for bay: {bay}".format(
                            shelf_config=shelf_config_key,
                            shelves=interested_shelves,
                            bay=bay_number
                        ))
                        per_bay_numerator = len(
                            grouped_bay_data[
                                (grouped_bay_data['shelf_number'].isin(interested_shelves)) &
                                (population_filter_final_mask)
                                ]
                        )
                        numerator = numerator + per_bay_numerator
                    if denominator:
                        result = round((numerator / float(denominator)) * 100, 2)
                    if result >= each_target.target_perc:
                        score = 1
                else:
                    Log.info(
                        "{kpi}: Fail: Cannot find any products for the population filter: {population_filter} "
                        "category: {cat}.".format(
                            kpi=kpi_details.iloc[0][KPI_TYPE_COL],
                            cat=category_fk,
                            population_filter=each_target.population_filter
                        ))
                    continue

                self.common.write_to_db_result(
                    fk=kpi_details.iloc[0].pk,
                    numerator_id=product_group_fk,
                    denominator_id=current_template_fk,
                    context_id=category_fk,
                    numerator_result=numerator,
                    denominator_result=denominator,
                    result=result,
                    score=score,
                    target=each_target.target_perc,
                    by_scene=True,
                )
