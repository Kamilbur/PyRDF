from __future__ import print_function

import json
import cloudpickle as pickle
import time

import boto3

from PyRDF.backend.Dist import Dist


class AWS(Dist):
    """
    Backend that executes the computational graph using using `Spark` framework
    for distributed execution.

    """

    MIN_NPARTITIONS = 2

    def __init__(self, config={}):
        '''
        Creates an instance of the Spark backend class.

        Args:
            config (dict, optional): The config options for Spark backend.
                The default value is an empty Python dictionary :obj:`{}`.
                :obj:`config` should be a dictionary of Spark configuration
                options and their values with :obj:'npartitions' as the only
                allowed extra parameter.

        Example::

            config = {
                'npartitions':20,
                'spark.master':'myMasterURL',
                'spark.executor.instances':10,
                'spark.app.name':'mySparkAppName'
            }

        Note:
            If a SparkContext is already set in the current environment, the
            Spark configuration parameters from :obj:'config' will be ignored
            and the already existing SparkContext would be used.

        '''
        super(AWS, self).__init__(config)
        #
        # sparkConf = SparkConf().setAll(config.items())
        # self.sparkContext = SparkContext.getOrCreate(sparkConf)

        # Set the value of 'npartitions' if it doesn't exist
        self.npartitions = self._get_partitions()

    def _get_partitions(self):
        return int(self.npartitions or AWS.MIN_NPARTITIONS)

    def ProcessAndMerge(self, mapper, reducer):
        """
        Performs map-reduce using Spark framework.

        Args:
            mapper (function): A function that runs the computational graph
                and returns a list of values.

            reducer (function): A function that merges two lists that were
                returned by the mapper.

        Returns:
            list: A list representing the values of action nodes returned
            after computation (Map-Reduce).
        """

        ranges = self.build_ranges()

        ds = '''
            # mapper and reducer are pickled
            # TEMP: now i need to inspect ranges to see what files i have to process ?
            # I need to send scripts to lambdas
            # each processing Lambda gets: [(range, mapper) for range in ranges]
            # 1. I run the mapper on ranges and pickles it and outputs pickled version somewhere
            # reducer gets: (outputs, reducer)
            # 2. I run the reducer =>
            # for each partial: unpickle pickled versions |>
            # apply reducer until all partials processed |>
            # pickle the result
            #  and send back to PyRDF
            # TODO: PROVIDE THE WAY TO GET THE DATA OUTPUT
            # 3. unpickle value and return to user
            # Build parallel collection
            # sc = self.sparkContext
            # parallel_collection = sc.parallelize(ranges, self.npartitions)
            # ranges look like [(0,n,fname),(n+1,2*n,fname)]
            '''

        # Map-Reduce using AWS
        pickled_mapper = pickle.dumps(mapper)
        pickled_reducer = pickle.dumps(reducer)

        s3 = boto3.client('s3',region_name='us-east-1')
        lambda_client = boto3.client('lambda',region_name='us-east-1')
        ssm = boto3.client('ssm',region_name='us-east-1')
        s3_output_bucket = ssm.get_parameter('output_bucket')
        if not s3_output_bucket:
            print('AWS backend not initialized!')
            return False

        ssm.put_parameter('ranges_num', len(ranges))
        ssm.put_parameter('reducer', pickled_reducer)

        def invoke_root_lambda(client, root_range, script):
            payload = json.dumps({
                'range': root_range,
                'script': script
            })
            return client.invoke(
                FunctionName='root_lambda',
                InvocationType='Event',
                Payload=bytes(payload, encoding='utf8')
            )

        call_results = []
        for root_range in ranges:
            call_result = invoke_root_lambda(lambda_client, root_range, pickled_mapper)
            call_results.append(call_result)

        while True:
            results = s3.list_objects_v2(Bucket=s3_output_bucket, Prefix='out.pickle')
            if len(results['Contents']) > 0:
                break
            time.sleep(1)

        result = s3.get_object(s3_output_bucket, 'out.pickle')
        reduced_output = pickle.loads(result)
        # return parallel_collection.map(spark_mapper).treeReduce(reducer)
        return reduced_output

    def distribute_files(self, includes_list):
        pass
