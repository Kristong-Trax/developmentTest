import abc

from Trax.Algo.Calculations.Core.Shortcuts import BaseCalculationsGroup


class FEMSACalculationsGroup(BaseCalculationsGroup):
    @abc.abstractproperty
    def run_calculations(self):
        raise NotImplementedError('This method must be overridden')





