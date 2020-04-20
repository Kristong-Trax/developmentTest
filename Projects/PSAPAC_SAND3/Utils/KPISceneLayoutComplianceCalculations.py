import numpy as np
import pandas as pd

from Trax.Utils.Logging.Logger import Log

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

    def get_relevant_custom_entity_data(self):
        Log.info("Getting custom entity data for the present super brands and store banner...")
        query = """ select * from static.custom_entity where pk in {custom_entity_pks};"""
        custom_entity_data = pd.read_sql_query(query.format(
            custom_entity_pks=tuple(np.concatenate((self.targets['store_banner_pk'].dropna().unique().astype('int'),
                                                    self.targets['super_brand_pk'].dropna().unique().astype('int'))))),
            self.rds_conn.db)
        return custom_entity_data

    def calculate_all(self):
        gsk_layout_compliance_brand_fsos = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_LAYOUT_COMPLIANCE_BRAND_FSOS)
            & (self.kpi_static_data['delete_time'].isnull())]
        gsk_layout_compliance_position = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_LAYOUT_COMPLIANCE_POSITION)
            & (self.kpi_static_data['delete_time'].isnull())]
        gsk_layout_compliance_sbrand_fsos = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_LAYOUT_COMPLIANCE_SBRAND_FSOS)
            & (self.kpi_static_data['delete_time'].isnull())]
        # TODO
        gsk_layout_compliance_block = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_LAYOUT_COMPLIANCE_BLOCK)
            & (self.kpi_static_data['delete_time'].isnull())]
        gsk_layout_compliance_sequence = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_LAYOUT_COMPLIANCE_SEQUENCE)
            & (self.kpi_static_data['delete_time'].isnull())]

        self.calculate_gsk_layout_compliance_position(kpi_details=gsk_layout_compliance_position)
        self.calculate_gsk_layout_compliance_brand_fsos(kpi_details=gsk_layout_compliance_brand_fsos)

    def calculate_gsk_layout_compliance_position(self, kpi_details):
        Log.info("Calculating {}".format(kpi_details.iloc[0][KPI_TYPE_COL]))
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
                exclude_stacked_products = bool(int(each_target.stacking_exclude))
                # numerator - Cumulative no of Facings "of the brand and sub category" available at desired shelf
                numerator = 0  # number of facings available in desired shelf
                if self.templates.iloc[0].template_fk not in each_target.template_fks:
                    Log.info('Session: {sess}; Scene:{scene}. Scene Type not matching [{k} not in {v}] '
                             'target for calculating {kpi}.'
                             .format(sess=self.session_uid,
                                     scene=self.current_scene_fk,
                                     kpi=kpi_details.iloc[0][KPI_TYPE_COL],
                                     k=self.templates.iloc[0].template_fk,
                                     v=each_target.template_fks,
                                     ))
                    continue
                stack_filtered_mpis = self.match_product_data
                if exclude_stacked_products:
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
        Log.info("Calculating {}".format(kpi_details.iloc[0][KPI_TYPE_COL]))
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
                # check for template match in target
                if self.templates.iloc[0].template_fk not in each_target.template_fks:
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
                store_banner_pk = each_target.store_banner_pk
                super_brand_pk = each_target.super_brand_pk
                brand_pk = each_target.brand_pk
                sub_category_pk = each_target.sub_category_fk
                exclude_stacked_products = bool(int(each_target.stacking_exclude))
                facings_field = 'facings'
                if exclude_stacked_products:
                    # consider only stacking layer 1 products
                    facings_field = 'facings_ign_stack'
                numerator = self.scif[
                    (self.scif['brand_fk']==brand_pk) &
                    (self.scif['sub_category_fk']==sub_category_pk)][facings_field].sum()
                scif_with_products = self.scif.merge(self.products, on='product_fk',
                                                     how='left', suffixes=('', '_prod'))
                super_brand_custom_entity = self.custom_entity_data[self.custom_entity_data['pk']==super_brand_pk]
                if super_brand_custom_entity.empty:
                    Log.error('Super Brand not found. Custom Entity Not loaded with a recent template update.')
                super_brand_name = super_brand_custom_entity.name.iloc[0]
                denominator = scif_with_products[
                    (scif_with_products['Super Brand']==super_brand_name) &
                    (scif_with_products['sub_category_fk']==sub_category_pk)][facings_field].sum()
                if denominator:
                    result = numerator/float(denominator)
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
