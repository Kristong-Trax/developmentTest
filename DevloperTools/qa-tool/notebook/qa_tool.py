import sys
import MySQLdb
import pandas as pd
import matplotlib.pyplot as plt

from Trax.Utils.Conf.Configuration import Config
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers


class qa:
    def __init__(self, project, start_date=None , end_date=None ,config_file='~/theGarage/Trax/Apps/Services/KEngine/k-engine-prod.config'):
        self._project = project
        self._config_file = config_file
        self._dbUser = DbUsers.CalculationEng
        self._env = Config.PROD
        self.start_date = start_date
        self.end_date = end_date
        # fetch db data
        self.static_kpi = self._get_static_kpi()
        self.kpi_results = self._get_kpi_results()
        self.merged_kpi_results = self.static_kpi.merge(self.kpi_results, left_on='pk', right_on='kpi_level_2_fk', how='left')
        self.expected = pd.read_csv('expected.csv')

    def _get_static_kpi(self):
        connector = ProjectConnector(self._project,self._dbUser)
        try:
            static = '''SELECT * FROM  static.kpi_level_2;'''
            return pd.read_sql_query(static, connector.db)
        except Exception as e:
            print e.message
        finally:
            connector.disconnect_rds()

    def _get_kpi_results(self):
        connector = ProjectConnector(self._project, self._dbUser)
        try:
            results = '''    
                    SELECT 
                        *
                    FROM
                        report.kpi_level_2_results,
                        probedata.session
                    WHERE
                        probedata.session.pk = report.kpi_level_2_results.session_fk
                            AND probedata.session.visit_date BETWEEN '{}' AND '{}';'''.format(self.start_date, self.end_date)
            return pd.read_sql_query(results, connector.db)
        except Exception as e:
            print e.message
        finally:
            connector.disconnect_rds()

    def test_uncalculated_kpi(self):
        """get list of kpi names that doesnt have any result """
        kpi_results2 = self.static_kpi.merge(self.kpi_results, left_on='pk', right_on='kpi_level_2_fk', how='left')
        kpi_results2 = kpi_results2.loc[kpi_results2['kpi_calculation_stage_fk'] == 3]
        print '## unclaculated kpi list ##'
        print kpi_results2[kpi_results2.isnull().result].client_name

    def test_invalid_precent_results(self):
        """kpi result should be percent between 0-1 """
        res = self.merged_kpi_results.loc[(self.merged_kpi_results['kpi_calculation_stage_fk'] == 3) & (self.merged_kpi_results['is_percent'] == 1)]

        print '##  kpi result should be percent between 0-1 ##'
        print res[(res.result > 1) | (res.result < 0)][['session_fk','client_name' ,'result']]

    def test_result_is_zero(self):
        """ kpi with results is 0 """

        res = self.merged_kpi_results.loc[(self.merged_kpi_results['kpi_calculation_stage_fk'] == 3)]
        print "## % results with 0 ##"
        print len(res[res.result == 0].pk_x) / float(len(res.pk_x)) * 100
        print res[(res.result == 0)][['session_fk', 'client_name', 'result']]

    def test_results_in_expected_range(self):
        """ show list or results that are not in the expected range"""

        for i, row in self.expected.iterrows():
            x = self.merged_kpi_results.loc[(self.merged_kpi_results['client_name'] == row['kpi_name']) & (
                        (self.merged_kpi_results['result'] < row['min']) | (self.merged_kpi_results['result'] > row['max']))]
            print "##### results not in expected range: (" + str(row['min']) + '-' + str(row['max']) + ' )' + row['kpi_name']
            print x[['session_fk', 'client_name', 'result']]

    def test_one_result_in_all_sessions(self):
        """ print kpi names where there is only 1 results in all sessions """

        for i, row in self.static_kpi[self.static_kpi['kpi_calculation_stage_fk'] == 3].iterrows():

            res = self.merged_kpi_results.loc[self.merged_kpi_results['client_name'] == row['client_name']]
            if res.empty:
                pass
            else:
                res = res.groupby(['result'])

            if len(res.count().index) < 2:
                print '## there is only 2 result type for kpi ' + row['client_name']
                print res.count().get_values()



    def run_all_tests(self):
        self.test_invalid_precent_results()
        self.test_uncalculated_kpi()
        self.test_result_is_zero()
        self.test_results_in_expected_range()
        self.test_one_result_in_all_sessions()


    #TODO
    # 1.pull data from prod base on dates
    # 2.plot histogram

if __name__ ==  "__main__":
    Config.init(app_name='ttt', default_env='prod',

                config_file='~/theGarage/Trax/Apps/Services/KEngine/k-engine-prod.config')

    qa_tool = qa('diageous', start_date='2018-05-01', end_date='2018-08-01')