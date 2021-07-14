from __future__ import print_function

import base64
import json
import logging
import time
import threading
import concurrent.futures
import os

import boto3
import botocore
import cloudpickle as pickle

from .Dist import Dist


class FlushingLogger:
    def __init__(self):
        self.logger = logging.getLogger()

    def __getattr__(self, name):
        method = getattr(self.logger, name)
        if name in ['info', ]:
            def flushed_method(msg, *args, **kwargs):
                method(msg, *args, **kwargs)
                for h in self.logger.handlers:
                    h.flush()
            return flushed_method
        else:
            return method


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
        self.logger = FlushingLogger() if logging.root.level >= logging.INFO else logging.getLogger()
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
        """
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
        """

        def invoke_root_lambda(root_range, script):
            client = boto3.client('lambda', region_name=self.region)

            def process_response(response):
                if response:
                    if 'Payload' in response:
                        payload = response['Payload']
                        if isinstance(payload, botocore.response.StreamingBody):
                            payload_bytes = payload.read()
                            try:
                                response['Payload'] = json.loads(payload_bytes)
                            except json.JSONDecodeError:
                                response['Payload'] = {}

                return response

            payload = json.dumps({
                'range': encode_object(root_range),
                'script': script,
                'start': str(root_range.start),
                'end': str(root_range.end),
                'filelist': str(root_range.filelist),
                'friend_info': encode_object(root_range.friend_info)
            })

            # Maybe here give info about number of invoked lambda for awsmonitor
            #self.logger.info(f'New lambda - 31')

            while True:
                try:
                    #my_before_time = time.time()
                    response = client.invoke(
                        FunctionName='root_lambda',
                        InvocationType= 'Event', #'RequestResponse',
                        Payload=bytes(payload, encoding='utf8')
                    )
                    #my_after_time = time.time()
                    #self.logger.info(f'Invocation time: {my_after_time - my_before_time}')

                    if 'FunctionError' in response:
                        raise RuntimeError('Lambda runtime error')

                    response = process_response(response)

                    if 'Payload' in response:
                        response_payload = response['Payload']
                        lambda_status = response_payload['statusCode'] if 'statusCode' in response_payload else 200
                        if lambda_status != 200:
                            raise RuntimeError(f'Lambda status code {lambda_status}')
                except botocore.exceptions.ClientError as error:
                    # AWS site errors
                    self.logger.warn(error['Error']['Message'])
                except RuntimeError as error:
                    # Lambda runtime errors and errors from
                    # lambdas that returned statusCode
                    # different than 200
                    print(error)
                except Exception as error:
                    # All other errors
                    print(error)
                else:
                    break
                time.sleep(1)

            return response


        self.logger.info(f'Before lambdas invoke. Number of lambdas: {len(ranges)}')

        processing_bucket = ssm_client.get_parameter(Name='processing_bucket')['Parameter']['Value']

        s3_resource.Bucket(processing_bucket).objects.all().delete()


        invoke_begin = time.time()
        # Invoke workers with ranges and mapper
        call_results = []
        """
        for lambda_num, root_range in enumerate(ranges):
            call_result = invoke_root_lambda(lambda_client, root_range, pickled_mapper)
            call_results.append(call_result)
            self.logger.info(f'New lambda - {lambda_num}')
        """

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(ranges)) as executor:
            executor.submit(AWS.process_execution, s3_client, processing_bucket, len(ranges), self.logger)
            call_results = executor.map(lambda root_range: invoke_root_lambda(root_range, pickled_mapper), ranges)
            executor.shutdown(wait=True)

        wait_begin = time.time()
        self.logger.info('All lambdas have been invoked')

        # while True:
        #     results = s3.list_objects_v2(Bucket=s3_output_bucket, Prefix='out.pickle')
        #     if results['KeyCount'] > 0:
        #         break
        #     self.logger.debug("still waiting")
        #     time.sleep(1)
        # result = s3.get_object(s3_output_bucket, 'out.pickle')

        reduce_begin = time.time()
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

        bench = (
            len(ranges),
            wait_begin-invoke_begin,
            reduce_begin-wait_begin,
            time.time()-reduce_begin
        )

        print(bench)

        return accumulator
        # reduced_output = pickle.loads(result)
        # return reduced_output

    def distribute_files(self, includes_list):
        pass

    @staticmethod
    def process_execution(s3_client, processing_bucket, num_of_lambdas, logger):
        # Wait until all lambdas finished execution
        while True:
            results = s3_client.list_objects_v2(Bucket=processing_bucket)
            logger.info(f'Lambdas finished: {results["KeyCount"]}')
            if results['KeyCount'] == num_of_lambdas:
                break
            time.sleep(1)


