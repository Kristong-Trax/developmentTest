
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import PlanogramBaseClass
from Projects.BATAU_SAND.KPIPlanogramGenerator import PlanogramGenerator

__author__ = 'sathiyanarayanan'


class PlanogramCalculations(PlanogramBaseClass):
    def __init__(self, data_provider):
        super(PlanogramCalculations, self).__init__(data_provider)
        self.planogram_generator = PlanogramGenerator(self._data_provider)

    def calculate_planogram(self):
        self.planogram_generator.planogram_score()
