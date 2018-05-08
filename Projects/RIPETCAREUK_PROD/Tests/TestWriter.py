import pandas as pd


class TestKpiResultsWriter(object):

    def __init__(self):
        self._kpi_results = pd.DataFrame()

    def write_to_db_level_3_result(self, atomic_kpi_name, kpi_name, kpi_set_name, score, threshold, result):
        attributes = pd.DataFrame.from_records(
            [
                {
                    'set_name': kpi_set_name,
                    'atomic_kpi_name': atomic_kpi_name,
                    'kpi_name': kpi_name,
                    'score': score,
                    'threshold': threshold,
                    'result': result
                }
            ]
        )
        self._kpi_results = self._kpi_results.append(attributes)

    def write_to_db_level_2_result(self, kpi_name, set_name, score):
        attributes = pd.DataFrame.from_records(
            [
                {
                    'set_name': set_name,
                    'kpi_name': kpi_name,
                    'score': score,
                }
            ]
        )
        self._kpi_results = self._kpi_results.append(attributes)

    def write_to_db_level_1_result(self, set_name, score):
        attributes = pd.DataFrame.from_records(
            [
                {
                    'set_name': set_name,
                    'score': score,
                }
            ]
        )
        self._kpi_results = self._kpi_results.append(attributes)

    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        pass
