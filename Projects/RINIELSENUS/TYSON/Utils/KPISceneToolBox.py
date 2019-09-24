from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.Utils.Consts.DataProvider import TemplatesConsts
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts

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

        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.match_display_in_scene = self.data_provider.match_display_in_scene

    def calculate_linear_feet(self):
        '''
        This Function calculates the linear sos of products in scenes :
        'Frozen Meat and Frozen Breakfast-Secondary Locations' and 'Frozen Meat and Frozen Breakfast - Main Aisle'.
        The numerator is grouped by product_fk and denominator is grouped by scene_if.
        If the scene display name is 'Bunker Cooler', then we taking stacking into account. Else, stacking is not considerd
        in terms of linear sos.
        :return:
        '''

        filtered_scif = self.scif[
            [ScifConsts.SCENE_ID, ScifConsts.TEMPLATE_FK, ScifConsts.TEMPLATE_NAME, ScifConsts.PRODUCT_FK,
             ScifConsts.NET_LEN_ADD_STACK, ScifConsts.NET_LEN_IGN_STACK]]

        filtered_scif[
            filtered_scif[ScifConsts.TEMPLATE_NAME].isin(["Frozen Meat and Frozen Breakfast - Secondary Locations",
                                                         'Frozen Meat and Frozen Breakfast - Main Aisle'])]
        if filtered_scif.empty:
            return

        filtered_scif = filtered_scif.dropna(subset=[ScifConsts.NET_LEN_ADD_STACK], how='all')

        # Need to display name as the "bunker cooler" in display name determines
        # whether the linear feet is stacked or not

        if not self.match_display_in_scene.empty:
            relevant_match_display_in_scene = self.match_display_in_scene[[ScifConsts.SCENE_FK, 'display_name']]
            scif_with_display_name = filtered_scif.merge(relevant_match_display_in_scene, left_on=ScifConsts.SCENE_ID,
                                                         right_on=ScifConsts.SCENE_FK)
            scif_with_display_name = scif_with_display_name.drop(columns=[ScifConsts.SCENE_FK, ScifConsts.SCENE_ID])

        else:
            scif_with_display_name = filtered_scif

            scif_with_display_name['display_name'] = "Not a Bunker Cooler"

        scif_with_display_name.loc[scif_with_display_name['display_name'] == 'Bunker Cooler', 'final_linear_feet'] = \
            scif_with_display_name.loc[
                scif_with_display_name['display_name'] == 'Bunker Cooler', 'net_len_add_stack']

        scif_with_display_name.loc[scif_with_display_name['display_name'] != 'Bunker Cooler', 'final_linear_feet'] = \
            scif_with_display_name.loc[
                scif_with_display_name['display_name'] != 'Bunker Cooler', ScifConsts.NET_LEN_IGN_STACK]

        scif_with_display_name = scif_with_display_name.drop(columns=[ScifConsts.NET_LEN_IGN_STACK,
                                                                      ScifConsts.NET_LEN_ADD_STACK])

        denominator_results = scif_with_display_name.groupby(ScifConsts.TEMPLATE_FK, as_index=False)[
            ['final_linear_feet']].sum().rename(columns={'final_linear_feet': 'denominator_result'})

        numerator_result = scif_with_display_name.groupby(
            [ScifConsts.TEMPLATE_FK, ScifConsts.PRODUCT_FK], as_index=False).sum().rename(
            columns={'final_linear_feet': 'numerator_result'})

        results = numerator_result.merge(denominator_results)
        results['result'] = (results['numerator_result'] / results['denominator_result'])

        results['result'] = (results['numerator_result'] / results['denominator_result'])
        results = results[results['result'] != 0]

        kpi_fk = self.common.get_kpi_fk_by_kpi_type('Tyson_SOS')
        a = 1
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
