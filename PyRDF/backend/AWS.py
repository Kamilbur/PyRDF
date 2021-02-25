from __future__ import print_function

import base64
import json
import logging
import time

import boto3
import cloudpickle as pickle

from PyRDF.backend.Dist import Dist


class AWS(Dist):
    """
    Backend that executes the computational graph using using AWS Lambda
    for distributed execution.
    """

    MIN_NPARTITIONS = 2

    def __init__(self, config={}):
        """
        Config for AWS is same as in Dist backend,
        more support will be added in future.
        """
        super(AWS, self).__init__(config)
        self.logger = logging.getLogger()
        self.npartitions = self._get_partitions()
        self.region = config.get('region') or 'us-east-1'

    def _get_partitions(self):
        return int(self.npartitions or AWS.MIN_NPARTITIONS)

    def ProcessAndMerge(self, mapper, reducer):
        """
        Performs map-reduce using AWS Lambda.

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

        def encode_object(object_to_encode) -> str:
            return str(base64.b64encode(pickle.dumps(object_to_encode)))

        # Make mapper and reducer transferable
        pickled_mapper = encode_object(mapper)
        pickled_reducer = encode_object(reducer)

        # Setup AWS clients
        s3_resource = boto3.resource('s3', region_name=self.region)
        s3_client = boto3.client('s3', region_name=self.region)
        lambda_client = boto3.client('lambda', region_name=self.region)
        ssm_client = boto3.client('ssm', region_name=self.region)

        # Check for existence of infrastructure
        s3_output_bucket = ssm_client.get_parameter(Name='output_bucket')['Parameter']['Value']
        if not s3_output_bucket:
            self.logger.info('AWS backend not initialized!')
            return False

        ssm_client.put_parameter(
            Name='ranges_num',
            Type='String',
            Value=str(len(ranges)),
            Overwrite=True
        )

        ssm_client.put_parameter(
            Name='reducer',
            Type='String',
            Value=str(pickled_reducer),
            Overwrite=True
        )

        def invoke_root_lambda(client, root_range, script):
            payload = json.dumps({
                'range': encode_object(root_range),
                'script': script,
                'start': str(root_range.start),
                'end': str(root_range.end),
                'filelist': str(root_range.filelist),
                'friend_info': str(root_range.friend_info)
            })
            return client.invoke(
                FunctionName='root_lambda',
                InvocationType='Event',
                Payload=bytes(payload, encoding='utf8')
            )

        # Invoke workers with ranges and mapper
        call_results = []
        for root_range in ranges:
            call_result = invoke_root_lambda(lambda_client, root_range, pickled_mapper)
            call_results.append(call_result)

        # while True:
        #     results = s3.list_objects_v2(Bucket=s3_output_bucket, Prefix='out.pickle')
        #     if results['KeyCount'] > 0:
        #         break
        #     self.logger.debug("still waiting")
        #     time.sleep(1)
        # result = s3.get_object(s3_output_bucket, 'out.pickle')

        processing_bucket = ssm_client.get_parameter(Name='processing_bucket')['Parameter']['Value']

        # Wait until all lambdas finished execution
        while True:
            results = s3_client.list_objects_v2(Bucket=processing_bucket)
            if results['KeyCount'] == len(ranges):
                break
            self.logger.info(f'Lambdas finished: {results["KeyCount"]}')
            time.sleep(1)

        # Get names of output files, download and reduce them
        filenames = s3_client.list_objects_v2(Bucket=processing_bucket)['Contents']

        # need better way to do that
        accumulator = pickle.loads(s3_client.get_object(
            Bucket=processing_bucket,
            Key=filenames[0]['Key']
        )['Body'].read())

        for filename in filenames[1:]:
            file = pickle.loads(s3_client.get_object(
                Bucket=processing_bucket,
                Key=filename['Key']
            )['Body'].read())
            accumulator = reducer(accumulator, file)

        # Clean up intermediate objects after we're done
        s3_resource.Bucket(processing_bucket).objects.all().delete()

        return accumulator
        # reduced_output = pickle.loads(result)
        # return reduced_output

    def distribute_files(self, includes_list):
        pass
