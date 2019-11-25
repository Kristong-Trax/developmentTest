import pandas as pd
from Projects.GMIUS.Snacks.Utils.Const import Const
from Projects.GMIUS.Utils.CustomError import NoDataForBlockError


__author__ = 'Sam'


def empty_scif_decorator(func):
    def arg_wrapper(instance, kpi_name, kpi_line, relevant_scif, general_filters):
        special_result = [{'score': 0, 'result': kpi_line['Fail Result']}]
        if relevant_scif.empty:  # This is only possible if the run empty line on the KPIs tab in the template is set to Y
            result = special_result
        else:
            special = False
            if 'Fail Result' in kpi_line and not pd.isna(kpi_line['Fail Result']):
                special = True
            try:
                result = func(instance, kpi_name, kpi_line, relevant_scif, general_filters)
                if result is None and special:
                    result = special_result
            except NoDataForBlockError:
                if special:
                    result = special_result
        return result

    return arg_wrapper