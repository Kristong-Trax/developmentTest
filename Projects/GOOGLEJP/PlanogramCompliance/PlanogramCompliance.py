from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import PlanogramComplianceBaseClass
from Trax.Apps.Services.KEngine.PRE.Algo.PlanogramCompliance import get_tag_planogram_compliance
import itertools
import pandas as pd
from Trax.Apps.Services.KEngine.PRE.Resources.Constants import Keys
from Trax.Algo.Calculations.Core.Constants import PlanogramTagCompliance
from Trax.Utils.Logging.Logger import Log

__author__ = 'Shivi'

IRRELEVANT = "Irrelevant"
SCENE_BAY = "scene_bay"
POG_BAY = "pog_bay"
BAYS_COUPLE = "bays_couple"
SCORE = "score"
TAG_COMPLIANCE = "tag_compliance"


class PlanogramCompliance(PlanogramComplianceBaseClass):

    def get_compliance(self, manual_planogram_data=None, manual_scene_data=None):
        planogram_matches = manual_planogram_data if manual_planogram_data else self._data_provider.planogram_data
        scene_matches = manual_scene_data if manual_scene_data else self._data_provider.matches
        scene_matches = self.filter_irrelevant_out(scene_matches)
        return self.get_iterated_position(scene_matches, planogram_matches)

    @staticmethod
    def filter_irrelevant_out(scene_matches):
        irrelevant_fks = scene_matches[scene_matches[Keys.PRODUCT_TYPE] == IRRELEVANT][Keys.MATCH_FK].tolist()
        for match_fk in irrelevant_fks:
            irrelevant_data = scene_matches[scene_matches[Keys.MATCH_FK] == match_fk].iloc[0]
            bay_number = irrelevant_data[Keys.BAY_NUMBER]
            shelf_number = irrelevant_data[Keys.SHELF_NUMBER_FROM_BOTTOM]
            sequence_number = irrelevant_data[Keys.FACING_SEQUENCE_NUMBER]
            scene_matches.loc[(scene_matches[Keys.BAY_NUMBER] == bay_number) &
                              (scene_matches[Keys.SHELF_NUMBER_FROM_BOTTOM] == shelf_number) &
                              (scene_matches[Keys.FACING_SEQUENCE_NUMBER] > sequence_number),
                              Keys.FACING_SEQUENCE_NUMBER] -= 1
        scene_matches = scene_matches[~(scene_matches[Keys.PRODUCT_TYPE] == IRRELEVANT)][Keys.MATCH_FK].tolist()
        return scene_matches

    @staticmethod
    def _get_df_of_bay(df, bay_number):
        df_answer = df[df[Keys.BAY_NUMBER] == bay_number]
        df_answer[Keys.BAY_NUMBER] = 1
        return df_answer

    def get_iterated_position(self, scene_matches, planogram_matches):
        """
        For multi-sided POGs. It iterates about all the possible permutations of the scene bays and the POG bays,
        and returns the tag_compliance of the permutation with the best score (most of "in position" products).
        """
        scene_bays = scene_matches[Keys.BAY_NUMBER].unique().tolist()
        pog_bays = planogram_matches[Keys.BAY_NUMBER].unique().tolist()
        all_combinations = pd.DataFrame()
        for combination in list(itertools.product(scene_bays, pog_bays)):
            scene_bay, pog_bay = combination
            scene_bay_data = self._get_df_of_bay(scene_matches, scene_bay)
            pog_bay_data = self._get_df_of_bay(planogram_matches, pog_bay)
            can_match = self.can_match(pog_bay_data=pog_bay_data, scene_bay_data=scene_bay_data)
            if not can_match:
                continue
            tag_compliance = get_tag_planogram_compliance(scene_bay_data, pog_bay_data)
            score = float(len(
                tag_compliance[tag_compliance[Keys.COMPLIANCE_STATUS_FK] == PlanogramTagCompliance.CORRECTLY_POSITIONED]
            )) / len(tag_compliance)
            all_combinations = all_combinations.append(
                {BAYS_COUPLE: (scene_bay, pog_bay), SCORE: score, TAG_COMPLIANCE: tag_compliance},
                ignore_index=True)
        permutations, combinations = [], all_combinations[BAYS_COUPLE].tolist()
        for permutation in [zip(x, pog_bays) for x in itertools.permutations(scene_bays, len(pog_bays))]:
            should_insert = True
            for combination in permutation:
                if combination not in combinations:
                    should_insert = False
                    break
            if should_insert:
                permutations.append(permutation)
        final_tag_compliance, highest_score = None, 0
        for permutation in permutations:
            score = 0
            temp_final_tag_compliance = pd.DataFrame()
            for combination in permutation:
                combination_data = all_combinations[all_combinations[BAYS_COUPLE] == combination]
                score += combination_data[SCORE].sum()
                temp_final_tag_compliance = temp_final_tag_compliance.append(combination_data[TAG_COMPLIANCE].iloc[0],
                                                                             ignore_index=True)
            if score > highest_score:
                highest_score = score
                final_tag_compliance = temp_final_tag_compliance
        return final_tag_compliance

    @staticmethod
    def can_match(pog_bay_data, scene_bay_data):
        shelves_distance = abs(
            pog_bay_data[Keys.SHELF_NUMBER_FROM_BOTTOM].max() - scene_bay_data[Keys.SHELF_NUMBER_FROM_BOTTOM].max())
        sequences_distance = abs(
            pog_bay_data[Keys.FACING_SEQUENCE_NUMBER].max() - scene_bay_data[Keys.FACING_SEQUENCE_NUMBER].max())
        if shelves_distance > 1 or sequences_distance > 1:
            return False
        return True

# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# if __name__ == '__main__':
#     LoggerInitializer.init('POG compliance test')
#     Config.init()
#     planogram_data = pd.read_csv("/home/elyashiv/Desktop/backup/POGs/pog2.csv")
#     scene_data = pd.read_csv("/home/elyashiv/Desktop/backup/POGs/scene2.csv")
#     pog = PlanogramCompliance(data_provider=None)
#     compliances = pog.get_compliance(planogram_data=planogram_data, scene_data=scene_data)
