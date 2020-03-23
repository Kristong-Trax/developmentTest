
from Trax.Utils.Logging.Logger import Log

from Projects.NESTLEUS.Utils.KPIToolBox import NESTLEUSToolBox

from KPIUtils_v2.DB.Common import Common

from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = 'nicolaske'


class Generator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = NESTLEUSToolBox(self.data_provider, self.output)
        self.common = Common(data_provider)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')

        df_scif = self.tool_box.scif
        fk_template_water_aisle = 2
        fk_template_water_display = 7

        fk_kpi_level_2 = {
            'facings': 909,
            'facings_ign_stack': 910,
            'net_len_split_stack': 911,
            'net_len_ign_stack': 912
        }

        def calculate_facing_count_and_linear_feet(id_scene_type):
            df_scene = df_scif[df_scif['template_fk'] == id_scene_type]
            sums = {key: df_scene[key].sum() for key, _ in fk_kpi_level_2.items()}

            for row in df_scene.itertuples():
                for key, fk in fk_kpi_level_2.items():
                    numerator = getattr(row, key) # row[key] #row.get(key) # this seems awkward
                    denominator = sums.get(key)
                    result = numerator / denominator

                    self.common.write_to_db_result(
                        fk=row.pk,
                        level="",
                        score=result,
                        kpi_level_2_fk=fk,
                        session_fk=row.session_id,
                        numerator_id=row.item_id,
                        numerator_result=numerator,
                        denominator_id=row.store_id,
                        denominator_result=denominator,
                        result=result
                    )

        calculate_facing_count_and_linear_feet(id_scene_type=fk_template_water_aisle)
        calculate_facing_count_and_linear_feet(id_scene_type=fk_template_water_display)

        # for kpi_set_fk in self.tool_box.new_kpi_static_data['pk'].unique().tolist():
        #     print("kpi_set")
        #     score = self.tool_box.main_calculation(kpi_set_fk=kpi_set_fk)
        #     # self.common.write_to_db_result(kpi_set_fk, self.tool_box.LEVEL1, score)
        # self.tool_box.common.commit_results_data()
        self.tool_box.calculate_assortment()
        # self.tool_box.commit_assortment_results_without_delete()
        self.tool_box.commit_assortment_results()

