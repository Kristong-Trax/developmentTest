import os
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


SUMMERY_FILE = "test_restuls.html"

class qa:
    def __init__(self, project, batch_size=300000, start_date=None , end_date=None ,config_file='~/theGarage/Trax/Apps/Services/KEngine/k-engine-prod.config'):

        findspark.init('/home/Ilan/miniconda/envs/garage/lib/python2.7/site-packages/pyspark')
        findspark.add_jars('/usr/local/bin/mysql-connector-java-5.1.46-bin.jar')
        self.spark = SparkSession.builder.appName("run_etl").config("spark.driver.memory","4g")\
                                                            .config("spark.executor.memory", "4g")\
                                                            .config("spark.driver.cores", "4")\
                                                            .config("spark.driver.maxResultSize", "4g")\
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
        self.categories_query = '''(select * from static_new.category) categories '''

        # fetch db data
        self.static_kpi = self._get_static_kpi()
        self.kpi_results = self._get_kpi_results()
        self.categories_df = self._get_categories()
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
        lower_bound = kpi_results_meta_data.loc[0]['min']
        upper_bound = kpi_results_meta_data.loc[0]['max']
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
                                                          "lowerBound": "{}".format(lower_bound),
                                                          "upperBound": "{}".format(upper_bound),
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

    def _get_categories(self):
        categories = self.spark.read.jdbc(url=self.project_url,
                                          table=self.categories_query,
                                          properties={"user": self.connector.dbuser.username,
                                                      "password": self.connector.dbuser.cred,
                                                      "driver": 'com.mysql.jdbc.Driver'}).persist(storageLevel=pyspark.StorageLevel.MEMORY_AND_DISK)

        categories.count()
        return categories

    def test_uncalculated_kpi(self):
        """get list of kpi names that doesnt have any result """

        filtered = self.merged_kpi_results.filter('result is null')
        df = filtered.groupBy("client_name", "result").count().withColumnRenamed('count', 'result_count')
        df2 = filtered.groupBy("client_name").agg(F.countDistinct("session_fk").alias("session count"))
        test_results = df.join(df2, df2.client_name == df.client_name)

        print '## uncalculated kpi list ##'
        test_results.show(1000, False)
        test_results_pandas = test_results.toPandas()
        test_results_pandas.to_csv("results/uncalculated_kpi_list.csv")
        return test_results_pandas.to_html()

    def test_invalid_percent_results(self):
        """kpi result should be percent between 0-1 """
        total_sessions = self.merged_kpi_results.select("session_fk").distinct().count()
        filtered = self.merged_kpi_results.filter('is_percent = 1 and result < 0 or result > 1')
        df = filtered.groupBy("client_name").count() \
                                            .withColumnRenamed('count', 'result_count') \
                                            .withColumnRenamed('client_name', 'name')
        df2 = filtered.groupBy("client_name").agg(F.countDistinct("session_fk").alias("session_count"))
        test_results = df.join(df2, df2.client_name == df.name)

        test_results_pandas = test_results.select('client_name', \
                                                  'result_count', \
                                                  "session_count", \
                                                  ((F.col('session_count') / total_sessions) * 100).alias("session_count%"))\
                                                   .toPandas()
        test_results_pandas.to_csv("results/invalid_percent_results_list.csv")
        return test_results_pandas.to_html()

    def test_result_is_zero(self):
        """ kpi with results is 0 """

        total_sessions = self.merged_kpi_results.select("session_fk").distinct().count()

        filtered = self.merged_kpi_results.filter('result == 0')
        df = filtered.groupBy('client_name').count().withColumnRenamed('count', 'results_zero_count').withColumnRenamed(
            'client_name', 'name')
        df2 = filtered.groupBy("client_name").agg(F.countDistinct("session_fk").alias("session_count"))
        df3 = self.merged_kpi_results.groupBy('client_name').count().withColumnRenamed('count', 'total_count') \
            .withColumnRenamed('client_name', 'name2')
        test_results = df.join(df2, df2.client_name == df.name).join(df3, df3.name2 == df.name)
        # test_results2 = df.join(test_results, test_results.client_name == df.name)
        test_results_pandas = test_results.select('client_name', \
                             'results_zero_count', \
                             # 'total_count',\
                             # "session_count",\
                             ((F.col('results_zero_count') / F.col('total_count')) * 100).alias(
                                 "results%(out of all results)"), \
                             ((F.col('session_count') / total_sessions) * 100).alias(
                                 "session_count%(out of all sessions)") \
                             ).toPandas()
        test_results_pandas.to_csv("results/test_result_is_zero.csv")
        return test_results_pandas.to_html()


    def test_results_stdev(self):
        test_results_pandas = self.merged_kpi_results.groupBy('client_name').agg(F.stddev('result'), \
                                                                         F.mean('result'), \
                                                                         F.min('result'), \
                                                                         F.max('result')).orderBy('client_name').toPandas()
        test_results_pandas.to_csv("results/test_results_stdev.csv")
        return test_results_pandas.to_html()

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

    def test_results_by_category_stddev(self):
        merged_kpi_results_tmp = self.merged_kpi_results.filter('denominator_type_fk = 4')
        categories_results = merged_kpi_results_tmp.join(self.categories_df,
                                                         self.categories_df.pk == merged_kpi_results_tmp.denominator_id)
        test_results_pandas = categories_results.select(categories_results.client_name , \
                                                        categories_results.name.alias("category_name") , \
                                                        categories_results.result).groupBy('client_name',"category_name")\
                                                                    .agg(F.stddev('result'), \
                                                                         F.mean('result'), \
                                                                         F.min('result'), \
                                                                         F.max('result')).orderBy('client_name').toPandas()

        test_results_pandas.to_csv("results/test_results_by_category_stddev.csv")
        return test_results_pandas.to_html()

    def run_all_tests(self):
        with open(SUMMERY_FILE, 'a') as file:
            file.write("<br> <p> test_invalid_percent_results</p>")
            file.write(self.test_invalid_percent_results())

            file.write("<br> <p> test_uncalculated_kpi</p>")
            file.write(self.test_uncalculated_kpi())

            file.write("<br> <p> test_result_is_zero</p>")
            file.write(self.test_result_is_zero())

            file.write("<br> <p> test_result_is_zero</p>")
            file.write(self.test_results_stdev())

            file.write("<br> <p> test_results_by_category_stddev</p>")
            file.write(self.test_results_by_category_stddev())


            # file.write(self.test_results_in_expected_range())
            # file.write(self.test_one_result_in_all_sessions())


    #TODO
    # 2.plot histogram
    # 3.set a summery reports
    # 4.total of rows
    # do not include blade - results in all level filter out  (low priorty )    `
    # show for each count uniqe count of sessions
    # results by category for kpi category
    # histogram by category per kpier
    # add std results  for each kpi
    # inecluded status fk on session = 1

    def get_statistics(self):

        stats = """ 
                <p>
                <br>
                   +---------------------------+    <br>
                    Project Name:  {project}  <br>
                    Dates:  {start_date} - {end_date}  <br>    
                    Total Results: {total_results}   <br>    
                    Total Sessions: {total_sessions}   <br>    
                    Total Kpi: {total_kpi}   <br>       
                   +---------------------------+  <br>
                </p>

              """.format(project=self._project,
                         start_date=self.start_date,
                         end_date=self.end_date,
                         total_results=self.merged_kpi_results.select("result").count(),
                         total_sessions=self.merged_kpi_results.select("session_fk").distinct().count(),
                         total_kpi=self.static_kpi.select("client_name").distinct().count())

        print stats
        with open(SUMMERY_FILE, 'a') as file:
            file.write(stats)




if __name__ ==  "__main__":
    Config.init(app_name='ttt', default_env='prod',
                config_file='~/theGarage/Trax/Apps/Services/KEngine/k-engine-prod.config')
    qa_tool = qa('jnjuk', start_date='2018-07-01', end_date='2018-07-30')
    if not os.path.exists("results"):
        os.mkdir("results")

    if os.path.isfile(SUMMERY_FILE):
        os.remove(SUMMERY_FILE)
    qa_tool.get_statistics()
    qa_tool.run_all_tests()