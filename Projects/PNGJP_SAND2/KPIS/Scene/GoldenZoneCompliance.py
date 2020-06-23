from Projects.PNGJP_SAND2.KPIS.BlockGoldenUtil import PNGJP_SAND2BlockGoldenUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Utils.Logging.Logger import Log


class GoldenZoneComplianceByScene(UnifiedCalculationsScript):

    PGJAPAN_GOLDEN_ZONE_COMPLIANCE_BY_SCENE = 'PGJAPAN_GOLDEN_ZONE_COMPLIANCE_BY_SCENE'
    KPI_TYPE_COL = 'type'

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(GoldenZoneComplianceByScene, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PNGJP_SAND2BlockGoldenUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        try:
            self.calculate_pngjp_golden_zone_compliance()
        except Exception as e:
            Log.error("Error : {}".format(e))

    def calculate_pngjp_golden_zone_compliance(self):
        current_scene_fk = self.util.scene_info.iloc[0].scene_fk
        Log.info('Calculate Layout Compliance for session: {sess} - scene: {scene}'
                 .format(sess=self.util.session_uid, scene=current_scene_fk))

        if not self.util.check_if_the_kpi_is_available(self.PGJAPAN_GOLDEN_ZONE_COMPLIANCE_BY_SCENE):
            Log.warning('Unable to calculate PGJAPAN_GOLDEN_ZONE_COMPLIANCE_BY_SCENE: KPIs are not in kpi_level_2')
            return

        kpi_details = self.util.kpi_static_data[
            (self.util.kpi_static_data[self.KPI_TYPE_COL] == self.PGJAPAN_GOLDEN_ZONE_COMPLIANCE_BY_SCENE)
            & (self.util.kpi_static_data['delete_time'].isnull())]

        Log.info("Calculating {kpi} for session: {sess} and scene: {scene}".format(
            kpi=kpi_details.iloc[0][self.KPI_TYPE_COL],
            sess=self.util.session_uid,
            scene=self.util.current_scene_fk,
        ))
        goldenzone_targets = self.util.targets_from_template["Golden Zone"]

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
                kpi_details.iloc[0][self.KPI_TYPE_COL]
            ))
            return False
        else:
            # target_shelf_config_keys => '1_5_shelf, 6_7_shelf, above_12_shelf etc'
            for idx, each_target in goldenzone_targets.iterrows():

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

                if not each_target.population_filter:
                    Log.warning(
                        "Population filter is empty for filter {population_filter}".format(
                            population_filter=each_target.population_filter
                        ))
                    continue

                result = score = 0
                target_shelf_config_keys = each_target[each_target.keys().map(lambda x: x.endswith('_shelf'))]
                population_filter = each_target.population_filter
                include_stacking = each_target.include_stacking
                numerator = 0  # number of facings available in desired shelf

                stack_filtered_mpis = self.util.match_product_data
                if not include_stacking:
                    # consider only stacking layer 1 products
                    stack_filtered_mpis = self.util.match_product_data[
                        self.util.match_product_data['stacking_layer'] == 1]

                # Apply filters based on population_filter
                boolean_masks = []
                for column_name, values in population_filter.items():
                    boolean_masks.append(
                        stack_filtered_mpis[column_name].isin(values)
                    )
                population_filter_final_mask = reduce(lambda x, y: x & y, boolean_masks)
                denominator = len(stack_filtered_mpis[population_filter_final_mask])
                total_no_of_skus_in_scene = len(stack_filtered_mpis)

                # denominator - total number of facings "by the population filter" available in whole scene
                if denominator:
                    for bay_number, grouped_bay_data in stack_filtered_mpis.groupby('bay_number'):
                        Log.info("Running {kpi} for bay {bay}".format(
                            kpi=kpi_details.iloc[0][self.KPI_TYPE_COL],
                            bay=bay_number
                        ))
                        # min_shelf = self.util.match_product_data['shelf_number'].unique().min()
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
                                sess=self.util.session_uid,
                                scene=self.util.current_scene_fk,
                                kpi=kpi_details.iloc[0][self.KPI_TYPE_COL]
                            ))
                            continue

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
                        "{kpi}: Fail: Cannot find any products for the population filter: {population_filter} ".format(
                            kpi=kpi_details.iloc[0][self.KPI_TYPE_COL],
                            population_filter=each_target.population_filter
                        ))
                    continue

                self.write_to_db_result(
                    fk=kpi_details.iloc[0].pk,
                    numerator_id=product_group_fk,
                    denominator_id=current_template_fk,
                    numerator_result=numerator,
                    denominator_result=denominator,
                    result=result,
                    score=score,
                    weight=total_no_of_skus_in_scene,
                    target=each_target.target_perc,
                    by_scene=True,
                )
