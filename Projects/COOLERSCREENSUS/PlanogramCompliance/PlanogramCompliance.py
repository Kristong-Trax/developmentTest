from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import PlanogramComplianceBaseClass
from Trax.Algo.Calculations.Core.Constants import ProductTypes, PlanogramTagCompliance
from Trax.Apps.Services.KEngine.PRE.Algo.PlanogramCompliance import get_tag_planogram_compliance
import pandas as pd
from Trax.Apps.Services.KEngine.PRE.Resources.Constants import Keys
from Trax.Utils.Logging.Logger import Log

INCERTAINLY_POSITIONNED = 5
PLANOGRAM_BRAND_FK = 'planogram_brand_fk'
PLANOGRAM_PRODUCT_FK = 'planogram_product_fk'

__author__ = 'JonathanC'


class PlanogramCompliance(PlanogramComplianceBaseClass):

    def get_compliance(self, manual_planogram_data=None, manual_scene_data=None):
        """
        This function filters the irrelevant products out, creates a matrix that matches the bays of the POG and the
        scene and scores them, find the best way to match the bays and returns the match tags.
        :return: DF of match_product_in_scene_fk with the tags
        """
        tag_compliance = pd.DataFrame(columns=[Keys.MATCH_FK, Keys.COMPLIANCE_STATUS_FK])
        try:
            self.planogram_matches = self._get_planogram_data() if manual_planogram_data is \
                                                                           None else manual_planogram_data
            self.scene_matches = self._get_matches() if manual_scene_data is None else manual_scene_data
            self.planogram_matches = self.planogram_matches[self.planogram_matches[Keys.STACKING_LAYER] == 1]
            self.planogram_matches = self.planogram_matches[
                [Keys.BAY_NUMBER, Keys.SHELF_NUMBER_FROM_BOTTOM, Keys.FACING_SEQUENCE_NUMBER, Keys.BRAND_FK,
                 Keys.PRODUCT_FK]]
            self.planogram_matches.rename(columns={Keys.PRODUCT_FK: PLANOGRAM_PRODUCT_FK}, inplace=True)
            self.planogram_matches.rename(columns={Keys.BRAND_FK: PLANOGRAM_BRAND_FK}, inplace=True)
            self.scene_matches = self.scene_matches.merge(self.planogram_matches, how='left')
            planogram_products = set(self.planogram_matches[PLANOGRAM_PRODUCT_FK].tolist())
            tag_compliance = self.local_get_tag_planogram_compliance(self.scene_matches, planogram_products)
        except Exception as e:
            Log.error("Calculated compliance has failed: " + e.message)
        return tag_compliance[[Keys.MATCH_FK, Keys.COMPLIANCE_STATUS_FK]]

    def local_get_tag_planogram_compliance(self, scene_planogram_data, planogram_products):
        matches = set()

        empty_tags = scene_planogram_data[scene_planogram_data[Keys.PRODUCT_TYPE] == ProductTypes.P_TYPE_EMPTY]
        empty_tags[Keys.COMPLIANCE_STATUS_FK] = PlanogramTagCompliance.EMPTY
        matches.update(empty_tags[Keys.MATCH_FK].tolist())

        correctly_positioned_tags = scene_planogram_data[
            (scene_planogram_data[Keys.PRODUCT_FK] == scene_planogram_data[PLANOGRAM_PRODUCT_FK]) & (
                ~scene_planogram_data[Keys.MATCH_FK].isin(matches))]
        correctly_positioned_tags[Keys.COMPLIANCE_STATUS_FK] = PlanogramTagCompliance.CORRECTLY_POSITIONED
        matches.update(correctly_positioned_tags[Keys.MATCH_FK].tolist())

        incertainly_positioned_tags = scene_planogram_data[
            (scene_planogram_data[Keys.BRAND_FK] == scene_planogram_data[PLANOGRAM_BRAND_FK]) & (
                ~scene_planogram_data[Keys.MATCH_FK].isin(matches))]
        incertainly_positioned_tags[Keys.COMPLIANCE_STATUS_FK] = INCERTAINLY_POSITIONNED

        tags = pd.concat([empty_tags, correctly_positioned_tags, incertainly_positioned_tags])
        tagged_matches = set(tags[Keys.MATCH_FK].tolist())
        not_in_position_tags = scene_planogram_data[~scene_planogram_data[Keys.MATCH_FK].isin(tagged_matches)]
        not_in_position_tags[Keys.COMPLIANCE_STATUS_FK] = PlanogramTagCompliance.NOT_IN_POSITION
        return pd.concat([tags, not_in_position_tags])

    def _get_planogram_data(self):
        query = """SELECT 
                        mpip.pk as planogram_match_fk, mpip.pk as scene_match_fk, mpip.product_fk, p.brand_fk,
                        mpip.bay_number, mpip.shelf_number, mpip.facing_sequence_number, mpip.x_mm, mpip.y_mm, 
                        mpip.shelf_number_from_bottom, mpip.width_mm, mpip.height_mm, mpip.status, mpip.stacking_layer
                    FROM static.match_product_in_planogram mpip
                    LEFT JOIN static_new.product p on p.pk = mpip.product_fk
                    WHERE mpip.planogram_fk =:planogram_fk
                """

        return self._data_provider._perform_query(query,
                                                  **{'planogram_fk': self._data_provider.planogram_fk})

    def _get_matches(self):
        query = """SELECT 
                        mpis.pk as match_fk, mpis.pk as scene_match_fk, mpis.product_fk, p.type as product_type,
                        p.brand_fk, mpis.shelf_number, mpis.bay_number, mpis.shelf_number_from_bottom, 
                        mpis.facing_sequence_number, mpis.stacking_layer, mpis.status, mpis.x_mm, mpis.y_mm, 
                        mpis.width_mm, mpis.height_mm
                     FROM probedata.match_product_in_scene mpis
                     JOIN static_new.product p ON mpis.product_fk = p.pk
                     WHERE mpis.scene_fk =:scene_id
                """
        return self._data_provider._perform_query(query,
                                                  **{'scene_id': self._data_provider._scene_id})


# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# if __name__ == '__main__':
#     LoggerInitializer.init('POG compliance test')
#     Config.init()
#     path = "/home/jonathanc/Documents/Datas/Tests/"
#     planogram_data = pd.read_csv(path + "pogs.csv")
#     scene_data = pd.read_csv(path + "matches.csv")
#     pog = PlanogramCompliance(data_provider=None)
#     compliances = pog.get_compliance(manual_planogram_data=planogram_data, manual_scene_data=scene_data)
#     print compliances
