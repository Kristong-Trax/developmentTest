import numpy as np
import pandas as pd

from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block

__author__ = 'nidhinb'

# The KPIs for Layout Compliance
GSK_LAYOUT_COMPLIANCE_BLOCK = 'GSK_LAYOUT_COMPLIANCE_BLOCK'
GSK_LAYOUT_COMPLIANCE_BRAND_FSOS = 'GSK_LAYOUT_COMPLIANCE_BRAND_FSOS'
GSK_LAYOUT_COMPLIANCE_POSITION = 'GSK_LAYOUT_COMPLIANCE_POSITION'
GSK_LAYOUT_COMPLIANCE_SEQUENCE = 'GSK_LAYOUT_COMPLIANCE_SEQUENCE'
GSK_LAYOUT_COMPLIANCE_SBRAND_FSOS = 'GSK_LAYOUT_COMPLIANCE_SBRAND_FSOS'

# other constants
KPI_TYPE_COL = 'type'


class SceneLayoutComplianceCalc(object):

    def __init__(self, scene_toolbox_obj):
        self.__dict__.update(scene_toolbox_obj.__dict__)
        self.current_scene_fk = self.scene_info.iloc[0].scene_fk
        self.store_banner_name = self.store_info.iloc[0].additional_attribute_20
        self.template_name = self.templates.iloc[0].template_name
        self.custom_entity_data = self.get_relevant_custom_entity_data()
        self.match_product_data = self.match_product_in_scene.merge(self.products, on='product_fk', how='left')
        self.block = Block(self.data_provider, self.output)

    def ensure_as_list(self, template_fks):
        if isinstance(template_fks, list):
            ext_target_template_fks = template_fks
        else:
            ext_target_template_fks = list([template_fks])
        return ext_target_template_fks

    def check_if_all_kpis_available(self):
        layout_compliance_kpis = [
            GSK_LAYOUT_COMPLIANCE_BLOCK,
            GSK_LAYOUT_COMPLIANCE_BRAND_FSOS,
            GSK_LAYOUT_COMPLIANCE_POSITION,
            GSK_LAYOUT_COMPLIANCE_SEQUENCE,
            GSK_LAYOUT_COMPLIANCE_SBRAND_FSOS
        ]
        kpis_not_found = []
        for kpi_name in layout_compliance_kpis:
            res_df = self.kpi_static_data[self.kpi_static_data[KPI_TYPE_COL] == kpi_name]
            if res_df.empty:
                kpis_not_found.append(kpi_name)
                Log.warning("Error: KPI {} not found in static.kpi_level_2 table.".format(kpi_name))

        status = True if len(kpis_not_found) == 0 else False
        return status

    def get_relevant_custom_entity_data(self):
        Log.info("Getting custom entity data for the present super brands and store banner...")
        columns_to_check = ["store_banner_pk", "super_brand_pk"]
        status = True
        for column in columns_to_check:
            if column not in self.targets.columns:
                Log.error("Error: {} not found in external targets".format(column))
                status = False

        if not status:
            return pd.DataFrame()

        query = """ select * from static.custom_entity where pk in {custom_entity_pks};"""
        custom_entity_data = pd.read_sql_query(query.format(
            custom_entity_pks=tuple(np.concatenate((self.targets['store_banner_pk'].dropna().unique().astype('int'),
                                                    self.targets['super_brand_pk'].dropna().unique().astype('int'))))),
            self.rds_conn.db)
        return custom_entity_data

    def calculate_all(self):
        if not self.check_if_all_kpis_available():
            Log.warning('Unable to calculate GSK_LAYOUT_COMPLIANCE_KPIs: KPIs are not in kpi_level_2')
            return

        if self.targets.empty:
            Log.warning('Unable to calculate GSK_LAYOUT_COMPLIANCE_KPIs: external targets are empty')
            return

        gsk_layout_compliance_brand_fsos = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_LAYOUT_COMPLIANCE_BRAND_FSOS)
            & (self.kpi_static_data['delete_time'].isnull())]
        gsk_layout_compliance_position = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_LAYOUT_COMPLIANCE_POSITION)
            & (self.kpi_static_data['delete_time'].isnull())]
        gsk_layout_compliance_sbrand_fsos = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_LAYOUT_COMPLIANCE_SBRAND_FSOS)
            & (self.kpi_static_data['delete_time'].isnull())]
        gsk_layout_compliance_sequence = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_LAYOUT_COMPLIANCE_SEQUENCE)
            & (self.kpi_static_data['delete_time'].isnull())]
        gsk_layout_compliance_block = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_LAYOUT_COMPLIANCE_BLOCK)
            & (self.kpi_static_data['delete_time'].isnull())]

        try:
            self.calculate_gsk_layout_compliance_brand_fsos(kpi_details=gsk_layout_compliance_brand_fsos)
        except Exception as e:
            Log.error("Error : {}".format(e))

        try:
            self.calculate_gsk_layout_compliance_block(kpi_details=gsk_layout_compliance_block)
        except Exception as e:
            Log.error("Error : {}".format(e))

        try:
            self.calculate_gsk_layout_compliance_sequence(kpi_details=gsk_layout_compliance_sequence)
        except Exception as e:
            Log.error("Error : {}".format(e))

        try:
            self.calculate_gsk_layout_compliance_super_brand_fsos(kpi_details=gsk_layout_compliance_sbrand_fsos)
        except Exception as e:
            Log.error("Error : {}".format(e))

        try:
            self.calculate_gsk_layout_compliance_position(kpi_details=gsk_layout_compliance_position)
        except Exception as e:
            Log.error("Error : {}".format(e))

    def calculate_gsk_layout_compliance_block(self, kpi_details):
        Log.info("Calculating {kpi} for session: {sess} and scene: {scene}".format(
            kpi=kpi_details.iloc[0][KPI_TYPE_COL],
            sess=self.session_uid,
            scene=self.current_scene_fk,
        ))
        block_targets = self.targets[
            self.targets['kpi_fk'] == kpi_details['pk'].iloc[0]]
        # if no targets return
        if block_targets.empty:
            Log.warning('There is no target policy for calculating {}'.format(
                kpi_details.iloc[0][KPI_TYPE_COL]
            ))
            return False
        else:
            for idx, each_target in block_targets.iterrows():
                # check for banner and template match in target
                target_banner_name = self.custom_entity_data[
                    self.custom_entity_data['pk'] == each_target.store_banner_pk].name.iloc[0]
                if self.templates.iloc[0].template_fk not in self.ensure_as_list(each_target.template_fks) or \
                        target_banner_name != self.store_banner_name:
                    Log.info("""Session: {sess}; Scene:{scene}. Scene Type not matching [{k} not in {v}] 
                    or banner of current store -> {store_b} != target banner -> {targ_b}
                    target for calculating {kpi}."""
                             .format(sess=self.session_uid,
                                     scene=self.current_scene_fk,
                                     kpi=kpi_details.iloc[0][KPI_TYPE_COL],
                                     store_b=self.store_banner_name,
                                     targ_b=target_banner_name,
                                     k=self.templates.iloc[0].template_fk,
                                     v=each_target.template_fks,
                                     ))
                    continue
                else:
                    result = score = 0
                    total_facings_count = biggest_block_facings_count = 0
                    block_threshold_perc = each_target.block_threshold_perc
                    super_brand_pk = each_target.super_brand_pk
                    store_banner_pk = each_target.store_banner_pk
                    super_brand_custom_entity = self.custom_entity_data[
                        self.custom_entity_data['pk'] == super_brand_pk]
                    sub_category_pk = each_target.sub_category_fk
                    Log.info(
                        "Calculating brand blocked for super brand: {super_b} [super_id] & sub category: {scat}".format(
                            super_b=super_brand_custom_entity.name.iloc[0],
                            super_id=super_brand_pk,
                            scat=sub_category_pk,
                        ))
                    stacking_include = bool(int(each_target.stacking_include))
                    # able to pass sub cat and super brand[?] // or get the prods and pass
                    block_filters = {'sub_category_fk': [float(sub_category_pk)],
                                     'Super Brand': [super_brand_custom_entity.name.iloc[0]]
                                     }
                    location_filters = {'scene_fk': [self.current_scene_fk]}
                    additional_filters = {
                        'minimum_facing_for_block': 1,
                        'minimum_block_ratio': 0,
                        'include_stacking': stacking_include,
                        'check_vertical_horizontal': True,
                    }
                    block_res = self.block.network_x_block_together(
                        block_filters, location_filters, additional_filters
                    )
                    block_res.dropna(subset=['total_facings'], inplace=True)
                    block_res = block_res.query('is_block==True')
                    if block_res.empty:
                        Log.info(
                            "Fail: Cannot find brand blocked for super brand: {super_b} "
                            "[super_id] & sub category: {scat}. Save as a Fail.".format(
                                super_b=super_brand_custom_entity.name.iloc[0],
                                super_id=super_brand_pk,
                                scat=sub_category_pk,
                            ))
                        continue
                    else:
                        Log.info(
                            "Found brand blocked for super brand: {super_b} "
                            "[super_id] & sub category: {scat}. Check and save.".format(
                                super_b=super_brand_custom_entity.name.iloc[0],
                                super_id=super_brand_pk,
                                scat=sub_category_pk,
                            ))
                        biggest_cluster = block_res.sort_values(by='block_facings', ascending=False).head(1)
                        biggest_block_facings_count = float(biggest_cluster['block_facings'])
                        total_facings_count = float(biggest_cluster['total_facings'])
                        if total_facings_count:
                            result = round((biggest_block_facings_count / total_facings_count) * 100, 2)
                        if result >= block_threshold_perc:
                            score = 1
                    self.common.write_to_db_result(
                        fk=kpi_details.iloc[0].pk,
                        numerator_id=store_banner_pk,
                        denominator_id=super_brand_pk,
                        context_id=sub_category_pk,
                        numerator_result=biggest_block_facings_count,
                        denominator_result=total_facings_count,
                        target=block_threshold_perc,
                        result=result,
                        score=score,
                        by_scene=True,
                    )

    def calculate_gsk_layout_compliance_sequence(self, kpi_details):
        Log.info("Calculating {kpi} for session: {sess} and scene: {scene}".format(
            kpi=kpi_details.iloc[0][KPI_TYPE_COL],
            sess=self.session_uid,
            scene=self.current_scene_fk,
        ))
        sequence_targets = self.targets[self.targets['kpi_fk'] == kpi_details['pk'].iloc[0]]
        # if no targets return
        if sequence_targets.empty:
            Log.warning('There is no target policy for calculating {}'.format(
                kpi_details.iloc[0][KPI_TYPE_COL]
            ))
            return False
        else:
            for idx, each_target in sequence_targets.iterrows():
                # check for template and banner match in target
                target_banner_name = self.custom_entity_data[
                    self.custom_entity_data['pk'] == each_target.store_banner_pk].name.iloc[0]
                if self.templates.iloc[0].template_fk not in self.ensure_as_list(each_target.template_fks) or \
                        target_banner_name != self.store_banner_name:
                    Log.info("""Session: {sess}; Scene:{scene}. Scene Type not matching [{k} not in {v}] 
                                or banner of current store -> {store_b} != target banner -> {targ_b}
                             target for calculating {kpi}."""
                             .format(sess=self.session_uid,
                                     scene=self.current_scene_fk,
                                     kpi=kpi_details.iloc[0][KPI_TYPE_COL],
                                     store_b=self.store_banner_name,
                                     targ_b=target_banner_name,
                                     k=self.templates.iloc[0].template_fk,
                                     v=each_target.template_fks,
                                     ))
                    continue
                store_banner_pk = each_target.store_banner_pk
                brand_pk_to_check = each_target.brand_pk
                sequence_brand_pks = each_target.sequence_brand_pks
                condition = each_target.condition
                sub_category_pk = each_target.sub_category_fk
                stacking_include = bool(int(each_target.stacking_include))
                stack_filtered_mpis = self.match_product_data
                Log.info("Checking if brand: {br} is present {condition} as in sequence : {seq}.".
                         format(br=brand_pk_to_check,
                                condition=condition,
                                seq=sequence_brand_pks))
                if brand_pk_to_check not in sequence_brand_pks:
                    Log.error(
                        """ KPI:{kpi}. Session: {sess}; Scene:{scene}. brand to check {brand} not in list {br_lst}."""
                            .format(sess=self.session_uid,
                                    scene=self.current_scene_fk,
                                    kpi=kpi_details.iloc[0][KPI_TYPE_COL],
                                    brand=brand_pk_to_check,
                                    br_lst=sequence_brand_pks
                                    ))
                    continue
                if not stacking_include:
                    # consider only stacking layer 1 products
                    stack_filtered_mpis = self.match_product_data[self.match_product_data['stacking_layer'] == 1]
                interested_brand_prod_data = stack_filtered_mpis[
                    (stack_filtered_mpis['brand_fk'] == brand_pk_to_check)
                    & (stack_filtered_mpis['sub_category_fk'] == sub_category_pk)
                    ]
                if interested_brand_prod_data.empty:
                    Log.error(
                        """ KPI:{kpi}. Session: {sess}; Scene:{scene}. brand to check {brand} is not present.""".format(
                            sess=self.session_uid,
                            scene=self.current_scene_fk,
                            kpi=kpi_details.iloc[0][KPI_TYPE_COL],
                            brand=brand_pk_to_check,
                        ))
                    continue
                # Check only in predecessor brands
                predecessor_brands = sequence_brand_pks[:sequence_brand_pks.index(brand_pk_to_check)]
                predecessor_brand_shelf_sorted = stack_filtered_mpis[
                    (stack_filtered_mpis['brand_fk'].isin(predecessor_brands))
                    & (stack_filtered_mpis['sub_category_fk'] == sub_category_pk)
                    ][['brand_fk', 'shelf_number']].sort_values(['shelf_number'])
                result = 0  # initialize as fail
                if predecessor_brand_shelf_sorted.empty:
                    # predecessor brands are not present; PASS
                    Log.info("No Predecessor Brands {} are present. PASS".format(
                        predecessor_brands))
                    result = 1
                else:
                    Log.info("Starting to checking> brand: {br} is present below or same level as in sequence : {seq}.".
                             format(br=brand_pk_to_check,
                                    seq=sequence_brand_pks))
                    min_shelf_of_brand = stack_filtered_mpis[
                        (stack_filtered_mpis['brand_fk'] == brand_pk_to_check) &
                        (stack_filtered_mpis['sub_category_fk'] == sub_category_pk)
                    ]['shelf_number'].min()
                    idx_brand_start_check = sequence_brand_pks.index(brand_pk_to_check) - 1
                    while idx_brand_start_check >= 0:
                        predecessor_brand_to_check = sequence_brand_pks[idx_brand_start_check]
                        Log.info("Checking if predecessor brand: {} is present.".format(predecessor_brand_to_check))
                        if predecessor_brand_shelf_sorted[predecessor_brand_shelf_sorted['brand_fk']
                                                          == predecessor_brand_to_check].empty:
                            # This is the logic to reduce index and continue while loop
                            Log.info("Brand {} is not present. Check next brand in predecessor".format(
                                predecessor_brand_to_check))
                            result = 1  # PASS unless proved otherwise.
                            idx_brand_start_check -= 1
                            continue
                        # check for `predecessor_brand_to_check` present below the level of every brand_pk_to_check
                        Log.info("Check if Brand to check {br_c} is present below the level of brand in sequence {seq}".
                                 format(br_c=brand_pk_to_check, seq=predecessor_brand_to_check))
                        max_shelf_of_predecessor_brand = predecessor_brand_shelf_sorted[
                            predecessor_brand_shelf_sorted['brand_fk'] == predecessor_brand_to_check][
                            'shelf_number'].max()
                        if min_shelf_of_brand >= max_shelf_of_predecessor_brand:
                            Log.info("PASS: brand: {brand} is below or same level as brand: {predecessor}.".format(
                                brand=brand_pk_to_check,
                                predecessor=predecessor_brand_to_check
                            ))
                            result = 1
                            break
                        else:
                            Log.info("FAIL: brand: {brand} is above level of brand: {predecessor}.".format(
                                brand=brand_pk_to_check,
                                predecessor=predecessor_brand_to_check
                            ))
                            result = 0
                            break
                        # end of check
                self.common.write_to_db_result(
                    fk=kpi_details.iloc[0].pk,
                    numerator_id=store_banner_pk,
                    denominator_id=brand_pk_to_check,
                    context_id=sub_category_pk,
                    result=result,
                    numerator_result=result,
                    denominator_result=1,
                    score=result,
                    target=1,
                    by_scene=True,
                )

    def calculate_gsk_layout_compliance_super_brand_fsos(self, kpi_details):
        Log.info("Calculating {kpi} for session: {sess} and scene: {scene}".format(
            kpi=kpi_details.iloc[0][KPI_TYPE_COL],
            sess=self.session_uid,
            scene=self.current_scene_fk,
        ))
        brand_fsos_targets = self.targets[
            self.targets['kpi_fk'] == kpi_details['pk'].iloc[0]]
        # if no targets return
        if brand_fsos_targets.empty:
            Log.warning('There is no target policy for calculating {}'.format(
                kpi_details.iloc[0][KPI_TYPE_COL]
            ))
            return False
        else:
            for idx, each_target in brand_fsos_targets.iterrows():
                # check for template and banner match in target
                target_banner_name = self.custom_entity_data[
                    self.custom_entity_data['pk'] == each_target.store_banner_pk].name.iloc[0]
                if self.templates.iloc[0].template_fk not in self.ensure_as_list(each_target.template_fks) or \
                        target_banner_name != self.store_banner_name:
                    Log.info("""Session: {sess}; Scene:{scene}. Scene Type not matching [{k} not in {v}] 
                                or banner of current store -> {store_b} != target banner -> {targ_b}
                             target for calculating {kpi}."""
                             .format(sess=self.session_uid,
                                     scene=self.current_scene_fk,
                                     kpi=kpi_details.iloc[0][KPI_TYPE_COL],
                                     store_b=self.store_banner_name,
                                     targ_b=target_banner_name,
                                     k=self.templates.iloc[0].template_fk,
                                     v=each_target.template_fks,
                                     ))
                    continue
                numerator = result = score = 0
                store_banner_pk = each_target.store_banner_pk
                super_brand_pk = each_target.super_brand_pk
                brand_pk = each_target.brand_pk
                sub_category_pk = each_target.sub_category_fk
                stacking_include = bool(int(each_target.stacking_include))
                facings_field = 'facings'
                if not stacking_include:
                    # consider only stacking layer 1 products
                    facings_field = 'facings_ign_stack'
                scif_with_products = self.scif.merge(self.products, on='product_fk',
                                                     how='left', suffixes=('', '_prod'))
                super_brand_custom_entity = self.custom_entity_data[self.custom_entity_data['pk'] == super_brand_pk]
                if super_brand_custom_entity.empty:
                    # should never happen
                    Log.error('Super Brand not found. Custom Entity Not loaded with a recent template update.')
                    continue
                denominator = scif_with_products[
                    (scif_with_products['sub_category_fk'] == sub_category_pk)][facings_field].sum()
                if denominator:
                    super_brand_name = super_brand_custom_entity.name.iloc[0]
                    numerator = scif_with_products[
                        (scif_with_products['Super Brand'] == super_brand_name) &
                        (scif_with_products['sub_category_fk'] == sub_category_pk)][facings_field].sum()
                    result = round((numerator / float(denominator)) * 100, 2)
                else:
                    Log.info("{kpi}: No products with sub cat: {scat} found. Save zero.".format(
                        kpi=kpi_details.iloc[0][KPI_TYPE_COL],
                        scat=sub_category_pk
                    ))
                    continue
                if result >= each_target.threshold:
                    score = 1
                self.common.write_to_db_result(
                    fk=kpi_details.iloc[0].pk,
                    numerator_id=store_banner_pk,
                    denominator_id=super_brand_pk,
                    context_id=sub_category_pk,
                    numerator_result=numerator,
                    denominator_result=denominator,
                    result=result,
                    score=score,
                    target=each_target.threshold,
                    by_scene=True,
                )

    def calculate_gsk_layout_compliance_position(self, kpi_details):
        Log.info("Calculating {kpi} for session: {sess} and scene: {scene}".format(
            kpi=kpi_details.iloc[0][KPI_TYPE_COL],
            sess=self.session_uid,
            scene=self.current_scene_fk,
        ))
        position_targets = self.targets[
            self.targets['kpi_fk'] == kpi_details['pk'].iloc[0]]

        def _get_shelf_range(sh):
            """Input => string ~ '1_2_shelf
                Output => xrange ~ xrange(1,3)
            '"""
            split_sh = sh.split('_')[:2]
            int_sh = map(int, split_sh)
            int_sh[1] = int_sh[1] + 1
            return xrange(*int_sh)

        # if no targets return
        if position_targets.empty:
            Log.warning('There is no target policy for calculating {}'.format(
                kpi_details.iloc[0][KPI_TYPE_COL]
            ))
            return False
        else:
            # target_shelf_config_keys => '1_5_shelf, 6_7_shelf, above_12_shelf etc'
            for idx, each_target in position_targets.iterrows():
                result = score = 0
                target_shelf_config_keys = each_target[each_target.keys().map(lambda x: x.endswith('_shelf'))]
                store_banner_pk = each_target.store_banner_pk
                sub_category_pk = each_target.sub_category_fk
                brand_pk = each_target.brand_pk
                stacking_include = bool(int(each_target.stacking_include))
                # numerator - Cumulative no of Facings "of the brand and sub category" available at desired shelf
                numerator = 0  # number of facings available in desired shelf
                # check for banner and template match in target
                target_banner_name = self.custom_entity_data[
                    self.custom_entity_data['pk'] == each_target.store_banner_pk].name.iloc[0]
                if self.templates.iloc[0].template_fk not in self.ensure_as_list(each_target.template_fks) or \
                        target_banner_name != self.store_banner_name:
                    Log.info("""Session: {sess}; Scene:{scene}. Scene Type not matching [{k} not in {v}] 
                                                or banner of current store -> {store_b} != target banner -> {targ_b}
                                             target for calculating {kpi}."""
                             .format(sess=self.session_uid,
                                     scene=self.current_scene_fk,
                                     kpi=kpi_details.iloc[0][KPI_TYPE_COL],
                                     store_b=self.store_banner_name,
                                     targ_b=target_banner_name,
                                     k=self.templates.iloc[0].template_fk,
                                     v=each_target.template_fks,
                                     ))
                    continue
                stack_filtered_mpis = self.match_product_data
                if not stacking_include:
                    # consider only stacking layer 1 products
                    stack_filtered_mpis = self.match_product_data[self.match_product_data['stacking_layer'] == 1]
                # denominator - total number of facings "of the brand and sub category" available in whole scene
                denominator = len(stack_filtered_mpis[
                                      (stack_filtered_mpis['brand_fk'] == brand_pk) &
                                      (stack_filtered_mpis['sub_category_fk'] == sub_category_pk)])
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
                                (grouped_bay_data['brand_fk'] == brand_pk) &
                                (grouped_bay_data['sub_category_fk'] == sub_category_pk)
                                ]
                        )
                        numerator = numerator + per_bay_numerator
                    if denominator:
                        result = round((numerator / float(denominator)) * 100, 2)
                    if result >= each_target.target_perc:
                        score = 1
                else:
                    Log.info("{kpi}: No products with sub cat: {scat} brand: {brand} found. Save zero.".format(
                        kpi=kpi_details.iloc[0][KPI_TYPE_COL],
                        scat=sub_category_pk,
                        brand=brand_pk,
                    ))
                    continue

                self.common.write_to_db_result(
                    fk=kpi_details.iloc[0].pk,
                    numerator_id=store_banner_pk,
                    denominator_id=brand_pk,
                    context_id=sub_category_pk,
                    numerator_result=numerator,
                    denominator_result=denominator,
                    result=result,
                    score=score,
                    target=each_target.target_perc,
                    by_scene=True,
                )

    def calculate_gsk_layout_compliance_brand_fsos(self, kpi_details):
        Log.info("Calculating {kpi} for session: {sess} and scene: {scene}".format(
            kpi=kpi_details.iloc[0][KPI_TYPE_COL],
            sess=self.session_uid,
            scene=self.current_scene_fk,
        ))
        brand_fsos_targets = self.targets[
            self.targets['kpi_fk'] == kpi_details['pk'].iloc[0]]
        # if no targets return
        if brand_fsos_targets.empty:
            Log.warning('There is no target policy for calculating {}'.format(
                kpi_details.iloc[0][KPI_TYPE_COL]
            ))
            return False
        else:
            for idx, each_target in brand_fsos_targets.iterrows():
                # check for banner and template match in target
                target_banner_name = self.custom_entity_data[
                    self.custom_entity_data['pk'] == each_target.store_banner_pk].name.iloc[0]
                if self.templates.iloc[0].template_fk not in self.ensure_as_list(each_target.template_fks) or \
                        target_banner_name != self.store_banner_name:
                    Log.info("""Session: {sess}; Scene:{scene}. Scene Type not matching [{k} not in {v}] 
                                                or banner of current store -> {store_b} != target banner -> {targ_b}
                                             target for calculating {kpi}."""
                             .format(sess=self.session_uid,
                                     scene=self.current_scene_fk,
                                     kpi=kpi_details.iloc[0][KPI_TYPE_COL],
                                     store_b=self.store_banner_name,
                                     targ_b=target_banner_name,
                                     k=self.templates.iloc[0].template_fk,
                                     v=each_target.template_fks,
                                     ))
                    continue
                numerator = result = score = 0
                store_banner_pk = each_target.store_banner_pk
                super_brand_pk = each_target.super_brand_pk
                brand_pk = each_target.brand_pk
                sub_category_pk = each_target.sub_category_fk
                stacking_include = bool(int(each_target.stacking_include))
                facings_field = 'facings'
                if not stacking_include:
                    # consider only stacking layer 1 products
                    facings_field = 'facings_ign_stack'
                super_brand_custom_entity = self.custom_entity_data[self.custom_entity_data['pk'] == super_brand_pk]
                if super_brand_custom_entity.empty:
                    # should never happen
                    Log.error('Super Brand not found. Custom Entity Not loaded with a recent template update.')
                    continue
                super_brand_name = super_brand_custom_entity.name.iloc[0]
                scif_with_products = self.scif.merge(self.products, on='product_fk',
                                                     how='left', suffixes=('', '_prod'))
                denominator = scif_with_products[
                    (scif_with_products['Super Brand'] == super_brand_name) &
                    (scif_with_products['sub_category_fk'] == sub_category_pk)][facings_field].sum()
                numerator = self.scif[
                    (self.scif['brand_fk'] == brand_pk) &
                    (self.scif['sub_category_fk'] == sub_category_pk)][facings_field].sum()
                if denominator:
                    result = round((numerator / float(denominator)) * 100, 2)
                else:
                    if numerator == 0:
                        Log.info("{kpi}: No products with sub cat: {scat} brand: {brand} found. Save zero.".format(
                            kpi=kpi_details.iloc[0][KPI_TYPE_COL],
                            scat=sub_category_pk,
                            brand=brand_pk,
                        ))
                        continue

                if result >= each_target.threshold:
                    score = 1
                self.common.write_to_db_result(
                    fk=kpi_details.iloc[0].pk,
                    numerator_id=store_banner_pk,
                    denominator_id=brand_pk,
                    context_id=sub_category_pk,
                    numerator_result=numerator,
                    denominator_result=denominator,
                    result=result,
                    score=score,
                    target=each_target.threshold,
                    by_scene=True,
                )
