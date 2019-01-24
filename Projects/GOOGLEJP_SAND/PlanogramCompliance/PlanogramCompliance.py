from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import PlanogramComplianceBaseClass
from Trax.Apps.Services.KEngine.PRE.Algo.PlanogramCompliance import get_tag_planogram_compliance
# import itertools
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
SUM = "SUM"


class GOOGLEJP_SANDPlanogramCompliance(PlanogramComplianceBaseClass):

    def get_compliance(self, manual_planogram_data=None, manual_scene_data=None):
        try:
            self.planogram_matches = self._data_provider.planogram_data if manual_planogram_data is\
                                                                      None else manual_planogram_data
            self.scene_matches = self._data_provider.matches if manual_scene_data is None else manual_scene_data
            self._filter_irrelevant_out()
            self.scene_bays = self.scene_matches[Keys.BAY_NUMBER].unique().tolist()
            self.pog_bays = self.planogram_matches[Keys.BAY_NUMBER].unique().tolist()
            if len(self.scene_bays) == 1 and self.scene_bays == self.pog_bays:
                tag_compliance, score = self._local_get_tag_planogram_compliance(self.scene_matches, self.planogram_matches)
                return tag_compliance
            return self._get_iterated_position_trial()
        except Exception as e:
            Log.error(e.message)
            return pd.DataFrame(columns=[Keys.MATCH_FK, Keys.COMPLIANCE_STATUS_FK])

    def _filter_irrelevant_out(self):
        """
        This function filters the Irrelevant products out of the DF and move the other products to their real sequence.
        """
        irrelevant_fks = self.scene_matches[self.scene_matches[Keys.PRODUCT_TYPE] == IRRELEVANT][Keys.MATCH_FK].tolist()
        for match_fk in irrelevant_fks:
            irrelevant_data = self.scene_matches[self.scene_matches[Keys.MATCH_FK] == match_fk].iloc[0]
            bay_number = irrelevant_data[Keys.BAY_NUMBER]
            shelf_number = irrelevant_data[Keys.SHELF_NUMBER_FROM_BOTTOM]
            sequence_number = irrelevant_data[Keys.FACING_SEQUENCE_NUMBER]
            self.scene_matches.loc[
                (self.scene_matches[Keys.BAY_NUMBER] == bay_number) &
                (self.scene_matches[Keys.SHELF_NUMBER_FROM_BOTTOM] == shelf_number) &
                (self.scene_matches[Keys.FACING_SEQUENCE_NUMBER] > sequence_number),
                Keys.FACING_SEQUENCE_NUMBER] -= 1
        self.scene_matches = self.scene_matches[~(self.scene_matches[Keys.PRODUCT_TYPE] == IRRELEVANT)]

    def _get_iterated_position_trial(self):
        """
        For multi-sided POGs. It iterates about all the possible permutations of the scene bays and the POG bays,
        and returns the tag_compliance of the permutation with the best score (most of "in position" products).
        """
        all_combinations_shelves_match = self._get_matches_df(Keys.SHELF_NUMBER_FROM_BOTTOM)
        all_combinations_sequence_match = self._get_matches_df(Keys.FACING_SEQUENCE_NUMBER)
        self.all_combinations_matches = all_combinations_shelves_match & all_combinations_sequence_match
        self.all_combinations_scores, self.all_combinations_compliances = self._get_compliances_and_scores_df()
        tag_compliance = self._get_final_compliance()
        return tag_compliance

    def _get_matches_df(self, column):
        all_combinations_match = pd.DataFrame(index=self.pog_bays, columns=self.scene_bays)
        for bay_number in self.scene_bays:
            max_amount = self.scene_matches[self.scene_matches[Keys.BAY_NUMBER] == bay_number][column].max()
            all_combinations_match[bay_number] = max_amount
        all_combinations_match = all_combinations_match.T
        for bay_number in self.pog_bays:
            max_amount = self.planogram_matches[self.planogram_matches[Keys.BAY_NUMBER] == bay_number][column].max()
            all_combinations_match[bay_number] -= max_amount
        for bay_number in self.pog_bays:
            all_combinations_match[bay_number] = all_combinations_match[bay_number].isin([-1, 0, 1])
        return all_combinations_match

    def _get_compliances_and_scores_df(self):
        self.all_combinations_compliances = pd.DataFrame(index=self.scene_bays, columns=self.pog_bays)
        self.all_combinations_scores = pd.DataFrame(index=self.scene_bays, columns=self.pog_bays, data=0.0)
        for pog_bay in self.pog_bays:
            for scene_bay in self.all_combinations_matches[self.all_combinations_matches[pog_bay]].index.tolist():
                tag_compliance, score = self._get_compliance_and_score_by_bays(scene_bay, pog_bay)
                self.all_combinations_scores[pog_bay][scene_bay] = score
                self.all_combinations_compliances[pog_bay][scene_bay] = tag_compliance
        return self.all_combinations_scores, self.all_combinations_compliances

    def _get_compliance_and_score_by_bays(self, scene_bay, pog_bay):
        scene_bay_data = self._get_df_of_bay(self.scene_matches, scene_bay)
        pog_bay_data = self._get_df_of_bay(self.planogram_matches, pog_bay)
        return self._local_get_tag_planogram_compliance(scene_bay_data, pog_bay_data)

    @staticmethod
    def _get_df_of_bay(df, bay_number):
        """
        Takes DF and bay, and returns the DF of this bay alone (and the bay_number is 1)
        """
        df_answer = df[df[Keys.BAY_NUMBER] == bay_number]
        df_answer[Keys.BAY_NUMBER] = 1
        return df_answer

    def _get_final_compliance(self):
        self.left_scene_bays, self.left_pog_bays = self.scene_bays[:], self.pog_bays[:]
        self.all_combinations_matches[SUM] = 0
        for scene_bay in self.scene_bays:
            self.all_combinations_matches.loc[scene_bay, SUM] = sum(
                i for i in self.all_combinations_matches.loc[scene_bay].values)
        self.all_combinations_matches = self.all_combinations_matches.sort_values(by=SUM)
        final_compliance_tag = pd.DataFrame()
        final_compliance_tag = self._get_final_compliance_scored_couples_part(final_compliance_tag)
        final_compliance_tag = self._get_final_compliance_unscored_couples_part(final_compliance_tag)
        final_compliance_tag = self._get_final_compliance_unmatched_couples_part(final_compliance_tag)
        return final_compliance_tag

    def _get_final_compliance_scored_couples_part(self, final_compliance_tag):
        for scene_bay in self.all_combinations_matches.index:
            line = self.all_combinations_matches.loc[scene_bay]
            if line[SUM] == 0 or True not in line.drop(SUM).values:
                continue
            scores = self.all_combinations_scores.loc[scene_bay].sort_values(ascending=False)
            score = scores.iloc[0]
            if score == 0:
                continue
            pog_bay = scores.index[0]
            final_compliance_tag = final_compliance_tag.append(self.all_combinations_compliances[pog_bay][scene_bay],
                                                               ignore_index=True)
            self._delete_bay_from_dfs(scene_bay, pog_bay)
        return final_compliance_tag

    def _get_final_compliance_unscored_couples_part(self, final_compliance_tag):
        for scene_bay in self.all_combinations_matches.index:
            line = self.all_combinations_matches.loc[scene_bay]
            if line[SUM] == 0 or True not in line.drop(SUM).values:
                continue
            pog_bay = line.drop(SUM).sort_values(ascending=False).index[0]
            final_compliance_tag = final_compliance_tag.append(self.all_combinations_compliances[pog_bay][scene_bay],
                                                               ignore_index=True)
            self._delete_bay_from_dfs(scene_bay, pog_bay)

        return final_compliance_tag

    def _get_final_compliance_unmatched_couples_part(self, final_compliance_tag):
        for scene_bay in self.left_scene_bays[:]:
            if len(self.left_pog_bays) == 0:
                break
            pog_bay = self.left_pog_bays[0]
            tag_compliance, temp_score = self._get_compliance_and_score_by_bays(scene_bay, pog_bay)
            final_compliance_tag = final_compliance_tag.append(tag_compliance, ignore_index=True)
            self._delete_bay_from_dfs(scene_bay, pog_bay)
        return final_compliance_tag

    def _delete_bay_from_dfs(self, scene_bay, pog_bay):
        self.left_pog_bays.remove(pog_bay)
        self.left_scene_bays.remove(scene_bay)
        self.all_combinations_scores = self.all_combinations_scores.drop(scene_bay)
        self.all_combinations_matches = self.all_combinations_matches.drop(scene_bay)
        self.all_combinations_scores = self.all_combinations_scores.drop(pog_bay, axis=1)
        self.all_combinations_matches = self.all_combinations_matches.drop(pog_bay, axis=1)

    # def _get_final_compliance2(self):
    #     # couples = []
    #     final_compliance_tag = pd.DataFrame()
    #     for pog_bay in self.pog_bays:
    #         pog_matches = self.all_combinations_scores[pog_bay].sort_values(ascending=False)
    #         # match_score = pog_matches.iloc[0]
    #         scene_bay = pog_matches.index[0]
    #         self.all_combinations_scores.drop(scene_bay)
    #         final_compliance_tag = final_compliance_tag.append(self.all_combinations_compliances[pog_bay][scene_bay],
    #                                                            ignore_index=True)
    #         # couples.append((pog_bay, scene_bay))
    #     return final_compliance_tag

    @staticmethod
    def _local_get_tag_planogram_compliance(scene_data, planogram_data):
        """
        Checks if there is one missing shelf, and calculates the percentage of the correct products and the tags
        """
        tag_compliance = get_tag_planogram_compliance(scene_data, planogram_data)
        score = float(len(
            tag_compliance[tag_compliance[Keys.COMPLIANCE_STATUS_FK] == PlanogramTagCompliance.CORRECTLY_POSITIONED]
                )) / len(tag_compliance)
        if score < 1 and scene_data[Keys.SHELF_NUMBER_FROM_BOTTOM].max() < planogram_data[
            Keys.SHELF_NUMBER_FROM_BOTTOM].max():
            scene_data[Keys.SHELF_NUMBER_FROM_BOTTOM] += 1
            temp_tag_compliance = get_tag_planogram_compliance(scene_data, planogram_data)
            temp_score = float(len(
                tag_compliance[tag_compliance[Keys.COMPLIANCE_STATUS_FK] == PlanogramTagCompliance.CORRECTLY_POSITIONED]
            )) / len(tag_compliance)
            if temp_score > score:
                score = temp_score
                tag_compliance = temp_tag_compliance
        return tag_compliance, score

    # def get_iterated_position(self):
    #     """
    #     For multi-sided POGs. It iterates about all the possible permutations of the scene bays and the POG bays,
    #     and returns the tag_compliance of the permutation with the best score (most of "in position" products).
    #     """
    #     all_combinations = pd.DataFrame()
    #     for combination in list(itertools.product(self.scene_bays, self.pog_bays)):
    #         scene_bay, pog_bay = combination
    #         scene_bay_data = self._get_df_of_bay(self.scene_matches, scene_bay)
    #         pog_bay_data = self._get_df_of_bay(self.planogram_matches, pog_bay)
    #         can_match = self.can_match(pog_bay_data=pog_bay_data, scene_bay_data=scene_bay_data)
    #         if not can_match:
    #             continue
    #         tag_compliance = get_tag_planogram_compliance(scene_bay_data, pog_bay_data)
    #         score = float(len(
    #             tag_compliance[tag_compliance[Keys.COMPLIANCE_STATUS_FK] == PlanogramTagCompliance.CORRECTLY_POSITIONED]
    #         )) / len(tag_compliance)
    #         all_combinations = all_combinations.append(
    #             {BAYS_COUPLE: (scene_bay, pog_bay), SCORE: score, TAG_COMPLIANCE: tag_compliance},
    #             ignore_index=True)
    #     permutations, combinations = [], all_combinations[BAYS_COUPLE].tolist()
    #     for permutation in [zip(x, self.pog_bays) for x in itertools.permutations(self.scene_bays, len(self.pog_bays))]:
    #         should_insert = True
    #         for combination in permutation:
    #             if combination not in combinations:
    #                 should_insert = False
    #                 break
    #         if should_insert:
    #             permutations.append(permutation)
    #     final_tag_compliance, highest_score = None, 0
    #     for permutation in permutations:
    #         score = 0
    #         temp_final_tag_compliance = pd.DataFrame()
    #         for combination in permutation:
    #             combination_data = all_combinations[all_combinations[BAYS_COUPLE] == combination]
    #             score += combination_data[SCORE].sum()
    #             temp_final_tag_compliance = temp_final_tag_compliance.append(combination_data[TAG_COMPLIANCE].iloc[0],
    #                                                                          ignore_index=True)
    #         if score > highest_score:
    #             highest_score = score
    #             final_tag_compliance = temp_final_tag_compliance
    #     return final_tag_compliance
    #
    # @staticmethod
    # def can_match(pog_bay_data, scene_bay_data):
    #     """
    #     Checks if these bays can match - their sequences and shelves should be in the same amount (distance of max 1)
    #     :param pog_bay_data: DF
    #     :param scene_bay_data: DF
    #     :return: Bool
    #     """
    #     shelves_distance = abs(
    #         pog_bay_data[Keys.SHELF_NUMBER_FROM_BOTTOM].max() - scene_bay_data[Keys.SHELF_NUMBER_FROM_BOTTOM].max())
    #     sequences_distance = abs(
    #         pog_bay_data[Keys.FACING_SEQUENCE_NUMBER].max() - scene_bay_data[Keys.FACING_SEQUENCE_NUMBER].max())
    #     if shelves_distance > 1 or sequences_distance > 1:
    #         return False
    #     return True

from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
if __name__ == '__main__':
    LoggerInitializer.init('POG compliance test')
    Config.init()
    path = "/home/elyashiv/Desktop/backup/POGs/8/"
    planogram_data = pd.read_csv(path + "pog.csv")
    scene_data = pd.read_csv(path + "scene.csv")
    pog = GOOGLEJP_SANDPlanogramCompliance(data_provider=None)
    compliances = pog.get_compliance(manual_planogram_data=planogram_data, manual_scene_data=scene_data)
    print compliances
