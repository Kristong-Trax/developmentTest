import sys
import MySQLdb
import pandas as pd
import matplotlib.pyplot as plt
import findspark
from memory_profiler import profile

from pyspark import SparkContext , SQLContext
from pyspark.sql import SparkSession
from Trax.Utils.Conf.Configuration import Config
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
import pandas as pd
import pyspark.sql.functions as F
import pyspark
from Trax.Utils.Conf.Configuration import Config
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers


class qa:
    def __init__(self, project, batch_size=80000, start_date=None , end_date=None ,config_file='~/theGarage/Trax/Apps/Services/KEngine/k-engine-prod.config'):

        findspark.init('/home/Ilan/miniconda/envs/garage/lib/python2.7/site-packages/pyspark')
        findspark.add_jars('/usr/local/bin/mysql-connector-java-5.1.46-bin.jar')
        self.spark = SparkSession.builder.appName("run_etl").config("spark.driver.memory","4g")\
                                                            .config("spark.executor.memory", "4g")\
                                                            .config("spark.driver.cores", "4")\
                                                            .config("spark.driver.maxResultSize", "4")\
                                                            .config("spark.ssl.enabled","True") \
                                                            .config("spark.ssl.protocol", "TLSv1.1").getOrCreate()

        self._project = project
        self._config_file = config_file
        self._dbUser = DbUsers.CalculationEng
        self._env = Config.PROD
        self.start_date = start_date
        self.end_date = end_date
        self.batch_size = batch_size
        self.spark = SparkSession.builder.appName("run_etl").config("spark.driver.memory","4g").config("spark.executor.memory", "4g").config("spark.driver.cores", "4").config("spark.driver.maxResultSize", "4").getOrCreate()
        self.connector = ProjectConnector(self._project,self._dbUser)
        self.project_url =  'jdbc:mysql://{}/report'.format(self.connector.project_params['rds_name'])

        #const
        self.results_query = '''    
                            (SELECT 
                                report.kpi_level_2_results.*
                            FROM
                                report.kpi_level_2_results,
                                probedata.session
                            WHERE
                                probedata.session.pk = report.kpi_level_2_results.session_fk
                                    AND probedata.session.visit_date BETWEEN '{}' AND '{}')tmp_kpi_level_2_results '''.format(
            start_date, end_date)

        self.static_query = '''(SELECT * FROM  static.kpi_level_2 where static.kpi_level_2.kpi_calculation_stage_fk = 3) static_kpi '''


        # fetch db data
        self.static_kpi = self._get_static_kpi()
        self.kpi_results = self._get_kpi_results()
        self.merged_kpi_results = self.static_kpi.join(self.kpi_results, self.static_kpi.pk == self.kpi_results.kpi_level_2_fk, how='left')
        self.expected = pd.read_csv('expected.csv')

    def _get_kpi_results_meta_data(self):
        try:
            meta_results = '''    
                        SELECT 
                            count(*) count, 
                            MIN(probedata.session.pk) min,
                            MAX(probedata.session.pk) max
                        FROM
                            report.kpi_level_2_results,
                            probedata.session
                        WHERE
                            probedata.session.pk = report.kpi_level_2_results.session_fk
                                AND probedata.session.visit_date BETWEEN '{}' AND '{}';'''.format(self.start_date, self.end_date)

            return pd.read_sql_query(meta_results, self.connector.db)
        except Exception as e:
            print e.message

    def _get_kpi_results(self):

        kpi_results_meta_data = self._get_kpi_results_meta_data()
        lowerBound = kpi_results_meta_data.loc[0]['min']
        upperBound = kpi_results_meta_data.loc[0]['max']
        count_of_row = kpi_results_meta_data.loc[0]['count']
        if (count_of_row / self.batch_size) > 1:
            number_of_partition = (count_of_row / self.batch_size)
        else:
            number_of_partition = 1
        print "count of rows : " + str(count_of_row)
        print " number of partition:" + str(number_of_partition)
        kpi_results = self.spark.read.jdbc(url=self.project_url,
                                           table=self.results_query,
                                           properties={"user": self.connector.dbuser.username,
                                                          "password": self.connector.dbuser.cred,
                                                          "partitionColumn": "tmp_kpi_level_2_results.session_fk",
                                                          "lowerBound": "{}".format(lowerBound),
                                                          "upperBound": "{}".format(upperBound),
                                                          "numPartitions": "{}".format(number_of_partition),
                                                          "driver": 'com.mysql.jdbc.Driver'}).persist(storageLevel=pyspark.StorageLevel.MEMORY_AND_DISK)

        kpi_results.count()
        return kpi_results

    def _get_static_kpi(self):
        static_kpi = self.spark.read.jdbc(url=self.project_url,
                                          table=self.static_query,
                                          properties={"user": self.connector.dbuser.username,
                                                      "password":  self.connector.dbuser.cred,
                                                      "driver": 'com.mysql.jdbc.Driver'}).persist(storageLevel=pyspark.StorageLevel.MEMORY_AND_DISK)

        static_kpi.count()
        return static_kpi



    def test_uncalculated_kpi(self):
        """get list of kpi names that doesnt have any result """

        # merged_kpi_results.select("session_fk","client_name","result").filter('result is null').show()
        print '## unclaculated kpi list ##'
        self.merged_kpi_results.select("client_name").filter('result is null').groupBy("client_name").count().show()
        self.merged_kpi_results.select("client_name").filter('result is null').groupBy("client_name").count()

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
    # 2.plot histogram
    # 3.set a summery report

if __name__ ==  "__main__":
    Config.init(app_name='ttt', default_env='prod',
                config_file='~/theGarage/Trax/Apps/Services/KEngine/k-engine-prod.config')
    qa_tool = qa('jnjuk', start_date='2018-07-01', end_date='2018-07-2')
    qa_tool.test_uncalculated_kpi()