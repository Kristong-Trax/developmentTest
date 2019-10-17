from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import PlanogramComplianceBaseClass
from KPIUtils_v2.Calculations.PlanogramAlgorithms.PlanogramComplianceMultiSided import PlanogramComplianceMultiSided

__author__ = 'Shivi'

IRRELEVANT = "Irrelevant"
SCENE_BAY = "scene_bay"
POG_BAY = "pog_bay"
BAYS_COUPLE = "bays_couple"
SCORE = "score"
TAG_COMPLIANCE = "tag_compliance"
SUM = "SUM"


class PlanogramCompliance(PlanogramComplianceBaseClass):

    def get_compliance(self, manual_planogram_data=None, manual_scene_data=None):
        """
        This function filters the irrelevant products out, creates a matrix that matches the bays of the POG and the
        scene and scores them, find the best way to match the bays and returns the match tags.
        :param manual_planogram_data: match_product_in_planogram (just for testing)
        :param manual_scene_data: match_product_in_scene (just for testing)
        :return: DF of match_product_in_scene_fk with the tags
        """
        planogram_matches = self._data_provider.planogram_data if manual_planogram_data is\
                                                                  None else manual_planogram_data
        scene_matches = self._data_provider.matches if manual_scene_data is None else manual_scene_data
        compliances = PlanogramComplianceMultiSided(self._data_provider, planogram_matches, scene_matches)
        tag_compliance = compliances.get_google_compliance()
        return tag_compliance

# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# if __name__ == '__main__':
#     LoggerInitializer.init('POG compliance test')
#     Config.init()
#     path = "/home/elyashiv/Desktop/backup/POGs/13/"
#     planogram_data = pd.read_csv(path + "pog.csv")
#     scene_data = pd.read_csv(path + "scene.csv")
#     pog = PlanogramCompliance(data_provider=None)
#     compliances = pog.get_compliance(manual_planogram_data=planogram_data, manual_scene_data=scene_data)
#     print compliances
