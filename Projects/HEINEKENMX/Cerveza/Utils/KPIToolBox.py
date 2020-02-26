
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
import pandas as pd

from Projects.HEINEKENMX.Cerveza.Data.LocalConsts import Consts

# from KPIUtils_v2.Utils.Consts.DataProvider import
# from KPIUtils_v2.Utils.Consts.DB import
# from KPIUtils_v2.Utils.Consts.PS import
# from KPIUtils_v2.Utils.Consts.GlobalConsts import
# from KPIUtils_v2.Utils.Consts.Messages import
# from KPIUtils_v2.Utils.Consts.Custom import
# from KPIUtils_v2.Utils.Consts.OldDB import

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'huntery'


class CervezaToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output, common):
        GlobalSessionToolBox.__init__(self, data_provider, output, common)
        self.gz = self.store_info['additional_attribute_4'].iloc[0]
        self.city = self.store_info['address_city'].iloc[0]
        self.relevant_targets = self._get_relevant_external_targets()

    def main_calculation(self):
        for scene_type in self.relevant_targets[Consts.TEMPLATE_SCENE_TYPE].unique().tolist():
            if scene_type in self.scif['template_name'].unique().tolist():
                # will there ever be more than one of each scene type? assuming no for now
                scene_fk = self.scif[self.scif['template_name'] == scene_type]['scene_id'].mode()
                scene_mpis = self.matches[self.matches['scene_fk'] == scene_fk]
                scene_realogram = CervezaRealogram(scene_mpis, scene_type, self.relevant_targets)
        return

    def _get_relevant_external_targets(self):
        template_df = pd.read_excel(Consts.TEMPLATE_PATH, sheetname='Planograma_cerveza', header=1)
        template_df = template_df[(template_df['GZ'].str.encode('utf-8') == self.gz.encode('utf-8')) &
                                  (template_df['Ciudad'].str.encode('utf-8') == self.city.encode('utf-8'))]

        template_df['Puertas'] = template_df['Puertas'].fillna(1)
        return template_df


class CervezaRealogram(object):
    def __init__(self, mpis, scene_type, planogram_template_data):
        self.scene_fk = self._get_scene_fk(mpis)
        self.mpis = mpis[mpis['scene_fk'] == self.scene_fk]
        self.scene_type = scene_type
        self.planograms = self._generate_planograms_by_door(planogram_template_data)
        self.realogram = self._generate_realogram()
        self.correctly_placed_skus = self._calculate_correctly_placed_skus()

    def _get_scene_fk(self, mpis):
        # TODO: raise exception if more than one scene
        return mpis['scene_fk'].iloc[0]

    def _generate_planograms_by_door(self, planogram_template_data):
        planograms = {}
        relevant_template = \
            planogram_template_data[planogram_template_data[Consts.TEMPLATE_SCENE_TYPE] == self.scene_type]

        for door in relevant_template[Consts.TEMPLATE_DOOR_ID].unique().tolist():
            door_template = relevant_template[relevant_template[Consts.TEMPLATE_DOOR_ID] == door]
            planograms[door] = self._expand_template_data_into_planogram(door_template)

        return planograms

    @staticmethod
    def _expand_template_data_into_planogram(template_data):
        planogram = pd.DataFrame(columns=['leading_product_fk', 'shelf', 'sequence_number'])
        for idx, product_line in template_data.sort_values(
                by=[Consts.TEMPLATE_SHELF_NUMBER, Consts.TEMPLATE_SEQUENCE_NUMBER]).itertuples():
            for i in range(int(getattr(product_line, Consts.TEMPLATE_FACINGS_COUNT))):
                planogram.loc[:, planogram.columns.tolist()] = \
                    [product_line.leading_product_fk,
                     getattr(product_line, Consts.TEMPLATE_SHELF_NUMBER),
                     getattr(product_line, Consts.TEMPLATE_SEQUENCE_NUMBER) + i]
        return planogram

    def _generate_realogram(self):
        realograms = []
        for door_id, door_planogram in self.planograms:
            door_realogram = self.mpis[self.mpis['bay_number'] == door_id]
            door_realogram = pd.merge(door_realogram, door_planogram,
                                      left_on=['leading_product_fk', 'shelf_number', 'facing_sequence_number'],
                                      right_on=['leading_product_fk', 'shelf', 'sequence_number'])
            realograms.append(door_realogram)
        return pd.concat(realograms)

    def _calculate_correctly_placed_skus(self):
        return self.realogram[(self.realogram['shelf_number'] == self.realogram['shelf']) &
                              (self.realogram['facing_sequence_number'] == self.realogram['sequence_number'])]

    def _calculate_extra_skus(self):
        pass


