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
        gsk_layout_compliance_block = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_LAYOUT_COMPLIANCE_BLOCK)
            & (self.kpi_static_data['delete_time'].isnull())]
        gsk_layout_compliance_brand_fsos = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_LAYOUT_COMPLIANCE_BRAND_FSOS)
            & (self.kpi_static_data['delete_time'].isnull())]
        gsk_layout_compliance_position = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_LAYOUT_COMPLIANCE_POSITION)
            & (self.kpi_static_data['delete_time'].isnull())]
        gsk_layout_compliance_sequence = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_LAYOUT_COMPLIANCE_SEQUENCE)
            & (self.kpi_static_data['delete_time'].isnull())]
        gsk_layout_compliance_sbrand_fsos = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_LAYOUT_COMPLIANCE_SBRAND_FSOS)
            & (self.kpi_static_data['delete_time'].isnull())]

        self.calculate_gsk_layout_compliance_brand_fsos(kpi_details=gsk_layout_compliance_brand_fsos)

    def calculate_gsk_layout_compliance_brand_fsos(self, kpi_details):
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
                    by_scene=True,
                )
