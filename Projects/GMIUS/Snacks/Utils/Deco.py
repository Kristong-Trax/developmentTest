__author__ = 'Sam'


def empty_scif_decorator(func):
    def arg_wrapper(instance, kpi_name, kpi_line, relevant_scif, general_filters):
        if relevant_scif.empty:
            return {'score': 1, 'result': kpi_line['Fail Result']}
        return func(instance, kpi_name, kpi_line, relevant_scif, general_filters)
    return arg_wrapper