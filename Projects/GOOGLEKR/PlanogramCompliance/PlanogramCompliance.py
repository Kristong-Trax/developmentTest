from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import PlanogramComplianceBaseClass
from Trax.Apps.Services.KEngine.PRE.Algo.PlanogramCompliance import get_tag_planogram_compliance
import itertools
import pandas as pd
from Trax.Apps.Services.KEngine.PRE.Resources.Constants import Keys
from Trax.Algo.Calculations.Core.Constants import PlanogramTagCompliance
from Trax.Utils.Logging.Logger import Log

__author__ = 'Shivi'


class PlanogramCompliance(PlanogramComplianceBaseClass):

    def get_compliance(self):
        planogram_data = self._data_provider.planogram_data
        scene_data = self._data_provider.matches
        return self.get_iterated_position(scene_data, planogram_data)

    @staticmethod
    def _get_df_of_bay(df, bay_number):
        df_answer = df[df['bay_number'] == bay_number]
        df_answer['bay_number'] = 1
        return df_answer

    def get_iterated_position(self, scene_data, planogram_data):
        """
        For multi-sided POGs. It iterates about all the possible permutations of the scene bays and the POG bays,
        and returns the tag_compliance of the permutation with the best score (most of "in position" products).
        """
        scene_bays = scene_data['bay_number'].unique().tolist()
        pog_bays = planogram_data['bay_number'].unique().tolist()
        all_combinations = pd.DataFrame()
        combinations = list(itertools.product(pog_bays, scene_bays))
        for combination in combinations:
            scene_bay, pog_bay = combination[0], combination[1]
            scene_bay_data = self._get_df_of_bay(scene_data, scene_bay)
            pog_bay_data = self._get_df_of_bay(planogram_data, pog_bay)
            tag_compliance = get_tag_planogram_compliance(scene_bay_data, pog_bay_data)
            score = float(len(
                tag_compliance[tag_compliance[Keys.COMPLIANCE_STATUS_FK] == PlanogramTagCompliance.CORRECTLY_POSITIONED]
            )) / len(tag_compliance)
            all_combinations = all_combinations.append(
                {"scene_bay": scene_bay, "pog_bay": pog_bay, "score": score, "tag_compliance": tag_compliance},
                ignore_index=True)
        permutations = [zip(x, pog_bays) for x in itertools.permutations(scene_bays, len(pog_bays))]
        final_tag_compliance, highest_score = None, 0
        for permutation in permutations:
            score = 0
            temp_final_tag_compliance = pd.DataFrame()
            for combination in permutation:
                combination_data = all_combinations[(all_combinations['scene_bay'] == combination[0]) &
                                                    (all_combinations['pog_bay'] == combination[1])]
                score += combination_data['score'].sum()
                temp_final_tag_compliance = temp_final_tag_compliance.append(combination_data['tag_compliance'][0],
                                                                             ignore_index=True)
            if score > highest_score:
                highest_score = score
                final_tag_compliance = temp_final_tag_compliance
        return final_tag_compliance
