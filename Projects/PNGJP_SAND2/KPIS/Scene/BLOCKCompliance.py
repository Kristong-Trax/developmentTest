from Projects.PNGJP_SAND2.KPIS.BlockGoldenUtil import PNGJP_SAND2BlockGoldenUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Utils.Logging.Logger import Log
from collections import defaultdict
import pandas as pd


class BLOCKComplianceByScene(UnifiedCalculationsScript):
    PGJAPAN_BLOCK_COMPLIANCE_BY_SCENE = 'PGJAPAN_BLOCK_COMPLIANCE_BY_SCENE'
    KPI_TYPE_COL = 'type'

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(BLOCKComplianceByScene, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PNGJP_SAND2BlockGoldenUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        try:
            self.calculate_pngjp_block_compliance()
        except Exception as e:
            Log.error("Error : {}".format(e))

    def calculate_pngjp_block_compliance(self):
        current_scene_fk = self.util.scene_info.iloc[0].scene_fk
        Log.info('Calculate Layout Compliance for session: {sess} - scene: {scene}'
                 .format(sess=self.util.session_uid, scene=current_scene_fk))

        if not self.util.check_if_the_kpi_is_available(self.PGJAPAN_BLOCK_COMPLIANCE_BY_SCENE):
            Log.warning('Unable to calculate PGJAPAN_BLOCK_COMPLIANCE_BY_SCENE: KPIs are not in kpi_level_2')
            return

        kpi_details = self.util.kpi_static_data[
            (self.util.kpi_static_data[self.KPI_TYPE_COL] == self.PGJAPAN_BLOCK_COMPLIANCE_BY_SCENE)
            & (self.util.kpi_static_data['delete_time'].isnull())]

        Log.info("Calculating {kpi} for session: {sess} and scene: {scene}".format(
            kpi=kpi_details.iloc[0][self.KPI_TYPE_COL],
            sess=self.util.session_uid,
            scene=self.util.current_scene_fk,
        ))
        block_targets = self.util.targets_from_template["Block"]
        # if no targets return
        if block_targets.empty:
            Log.warning('There is no target policy in the template for calculating {}'.format(
                kpi_details.iloc[0][self.KPI_TYPE_COL]
            ))
            return False
        else:
            # merge block_targets with ext_targets
            block_targets = block_targets.merge(self.util.external_targets,
                                                left_on=['KPI Name', 'Product Group Name'],
                                                right_on=["kpi_type", "Product_Group_Name"],
                                                how="left",
                                                suffixes=['', '_y'])
            for idx, each_target in block_targets.iterrows():
                # check for template match in target
                context_fk = 0 if pd.isnull(each_target.pk) else each_target.pk
                product_group_df = self.util.custom_entity_data[
                    self.util.custom_entity_data['pk'].isin(each_target.product_group_name_fks)
                ]
                if product_group_df.empty:
                    Log.warning("Product group name: {} not found in custom_entity".format(
                        each_target['Product Group Name']
                    ))
                    continue
                product_group_fk = product_group_df.pk.iloc[0]
                current_template_fk = self.util.templates.iloc[0].template_fk

                if current_template_fk not in self.util.ensure_as_list(each_target.template_fks):
                    Log.info("""Session: {sess}; Scene:{scene}. Scene Type not matching [{k} not in {v}] 
                        target for calculating {kpi}."""
                             .format(sess=self.util.session_uid,
                                     scene=self.util.current_scene_fk,
                                     kpi=kpi_details.iloc[0][self.KPI_TYPE_COL],
                                     k=self.util.templates.iloc[0].template_fk,
                                     v=each_target.template_fks,
                                     ))
                    continue
                else:
                    result = score = 0
                    total_facings_count = biggest_block_facings_count = 0
                    block_threshold_perc = each_target.block_threshold_perc

                    Log.info(
                        "Calculating blocking KPI for - filter {population_filter}".format(
                            population_filter=each_target.population_filter
                        ))

                    if not each_target.population_filter:
                        Log.warning(
                            "Population filter is empty for - filter {population_filter}".format(
                                population_filter=each_target.population_filter
                            ))
                        continue

                    include_stacking = each_target.include_stacking
                    include_empty = each_target.include_empty
                    include_other = each_target.include_other

                    # able to pass sub cat and super brand[?] // or get the prods and pass
                    population_filters = each_target.population_filter

                    boolean_masks = []
                    for column_name, values in population_filters.items():
                        boolean_masks.append(
                            self.util.match_product_data[column_name].isin(values)
                        )
                    population_filter_final_mask = reduce(lambda x, y: x & y, boolean_masks)
                    if self.util.match_product_data[population_filter_final_mask].empty:
                        Log.info(
                            "{kpi}: Fail: Cannot find any products for the population filter: {population_filter} "
                                .format(
                                kpi=kpi_details.iloc[0][self.KPI_TYPE_COL],
                                population_filter=each_target.population_filter
                            ))
                        continue

                    location_filters = {'scene_fk': [self.util.current_scene_fk]}

                    allowed_products = defaultdict(list)
                    if include_empty:
                        allowed_products['product_type'].append('Empty')
                    if include_other:
                        allowed_products['product_type'].append("Other")
                    allowed_products = dict(allowed_products)

                    additional_filters = {
                        'minimum_facing_for_block': 1,
                        'minimum_block_ratio': 0,
                        'include_stacking': include_stacking,
                        'check_vertical_horizontal': True,
                        'allowed_products_filters': allowed_products
                    }
                    block_res = self.util.block.network_x_block_together(
                        population_filters, location_filters, additional_filters
                    )
                    block_res.dropna(subset=['total_facings'], inplace=True)
                    block_res = block_res.query('is_block==True')
                    if block_res.empty:
                        Log.info(
                            "Fail: Cannot find the block for the population filter: {population_filter} ".format(
                                population_filter=each_target.population_filter
                            ))
                        continue
                    else:
                        Log.info(
                            "Found block for the population filter: {population_filter} "
                            "Check and save.".format(
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
                            # TODO: fix if its max or len
                            shelf_spread_count_for_biggest_block = len(list(list(g.nodes("shelf_number"))[0][-1]))
                        except Exception as e:
                            Log.error("Error: {}".format(e))
                            shelf_spread_count_for_biggest_block = 0

                    self.write_to_db_result(
                        fk=kpi_details.iloc[0].pk,
                        numerator_id=product_group_fk,
                        denominator_id=current_template_fk,
                        context_id=context_fk,
                        numerator_result=biggest_block_facings_count,
                        denominator_result=total_facings_count,
                        target=block_threshold_perc,
                        result=result,
                        score=score,
                        weight=shelf_spread_count_for_biggest_block,
                        by_scene=True,
                    )
