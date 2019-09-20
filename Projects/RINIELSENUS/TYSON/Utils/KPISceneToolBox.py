from Trax.Algo.Calculations.Core.DataProvider import Data

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'krishna'


class TYSONToolBox:
    def __init__(self, data_provider, common, output):
        self.output = output
        self.data_provider = data_provider
        self.common = common
        # self.project_name = self.data_provider.project_name
        # self.session_uid = self.data_provider.session_uid
        # #self.products = self.data_provider[Data.PRODUCTS]
        # self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        # self.match_product_in_scene = self.data_provider[Data.MATCHES]
        # self.visit_date = self.data_provider[Data.VISIT_DATE]
        # self.session_info = self.data_provider[Data.SESSION_INFO]
        # self.scene_info = self.data_provider[Data.SCENES_INFO]
        # self.template_info = self.data_provider.all_templates
        # self.store_id = self.data_provider[Data.STORE_FK]

        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        # self.match_display_in_scene = self.data_provider._display_set_match_display_in_scene(self, level, uid)
        self.match_display_in_scene = self.data_provider.match_display_in_scene



    def calculate_linear_feet(self):
        '''
        :return: the df of numerator which is grouped by product_fk and
                    the denominator which is grouped by template_fk and product_fk.
                    In which the final result is the numerator over the denominator.
        '''


        # Calls the updated scif the remove_substitue method
        filtered_scif = self.scif[
            ['scene_id', 'template_fk', 'template_name', 'product_fk', 'net_len_add_stack', 'net_len_ign_stack']]

        filtered_scif[filtered_scif['template_name'].isin(["Frozen Meat and Frozen Breakfast - Secondary Locations",
                                                           'Frozen Meat and Frozen Breakfast - Main Aisle'])]
        if filtered_scif.empty:
            pass

        else:
            filtered_scif.dropna(subset=['net_len_add_stack'])


            # Need to display name as the "bunker cooler" in display name determines
            # whether the linear feet is stacked or not

            if not self.match_display_in_scene.empty:
                relevant_match_display_in_scene = self.match_display_in_scene[['scene_fk', 'display_name']]
                scif_with_display_name = filtered_scif.merge(relevant_match_display_in_scene, left_on='scene_id',
                                                             right_on='scene_fk')
                scif_with_display_name = scif_with_display_name.drop(columns=['scene_fk', 'scene_id'])

            else:
                scif_with_display_name = filtered_scif
                scif_with_display_name['display_name'] = "Not a Bunker Cooler"




            scif_with_display_name.loc[scif_with_display_name['display_name'] == 'Bunker Cooler', 'final_linear_feet'] = \
                scif_with_display_name.loc[scif_with_display_name['display_name'] == 'Bunker Cooler', 'net_len_add_stack']

            scif_with_display_name.loc[scif_with_display_name['display_name'] != 'Bunker Cooler', 'final_linear_feet'] = \
                scif_with_display_name.loc[scif_with_display_name['display_name'] != 'Bunker Cooler', 'net_len_ign_stack']

            scif_with_display_name = scif_with_display_name.drop(columns=['net_len_ign_stack', 'net_len_add_stack'])

            denominator_results = scif_with_display_name.groupby('template_fk', as_index=False)[
                ['final_linear_feet']].sum().rename(columns={'final_linear_feet': 'denominator_result'})

            numerator_result = scif_with_display_name.groupby(
                ['template_fk', 'product_fk'], as_index=False).sum().rename(
                columns={'final_linear_feet': 'numerator_result'})

            results = numerator_result.merge(denominator_results)
            results['result'] = (results['numerator_result'] / results['denominator_result'])

            results['result'] = (results['numerator_result'] / results['denominator_result'])

            kpi_fk = self.common.get_kpi_fk_by_kpi_type('Tyson_SOS')

            for row in results.itertuples():
                self.common.write_to_db_result(fk=kpi_fk, score=1, result=getattr(row, 'result'), should_enter=True,
                                               numerator_result=getattr(row, 'numerator_result'),
                                               denominator_result=getattr(row, 'denominator_result'),
                                               numerator_id=getattr(row, 'product_fk'),
                                               denominator_id=getattr(row, 'template_fk'),
                                               by_scene=True)


    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """

        self.calculate_linear_feet()
        return
