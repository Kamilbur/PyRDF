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

            trials = 3

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

            while trials:
                trials -= 1
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
                    # Lambda runtime errors and lambdas that
                    # returned statusCode different than 200
                    # (maybe also response processing errors)
                    self.logger.warn(error)
                except Exception as error:
                    # All other errors
                    self.logger.warn(error)
                else:
                    break
                time.sleep(1)

            # Note: lambda finishes before s3 object is created
            #self.logger.info('Lambda finished :)')

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
            futures = [executor.submit(invoke_root_lambda, root_range, pickled_mapper) for root_range in ranges]
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
        results = s3_client.list_objects_v2(Bucket=processing_bucket)
        self.logger.info(f'Lambdas finished: {results["KeyCount"]}')
        filenames = s3_client.list_objects_v2(Bucket=processing_bucket)['Contents']

        def get_from_s3(filename):
            s3_client = boto3.client('s3', region_name=self.region)
            return pickle.loads(s3_client.get_object(
                        Bucket=processing_bucket,
                        Key=filename['Key']
                    )['Body'].read())

        files = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(filenames)) as executor:
            futures = [executor.submit(get_from_s3, filename) for filename in filenames]
            executor.shutdown(wait=True)
            files = [future.result() for future in futures]

        to_process = files
        while len(to_process) > 1:
            even_index_files = [to_process[i] for i in range(len(to_process)) if i % 2 == 0]
            odd_index_files = [to_process[i] for i in range(len(to_process)) if i % 2 == 1]

            with concurrent.futures.ThreadPoolExecutor(len(to_process)) as executor:
                futures = [executor.submit(reducer, pair[0], pair[1]) for pair in zip(even_index_files, odd_index_files)]
                executor.shutdown(wait=True)
                to_process = [future.result() for future in futures]

            if len(even_index_files) > len(odd_index_files):
                to_process.append(even_index_files[-1])
            elif len(even_index_files) < len(odd_index_files):
                to_process.append(odd_index_files[-1])

        reduction_result = to_process[0]

        # Clean up intermediate objects after we're done
        s3_resource.Bucket(processing_bucket).objects.all().delete()

        bench = (
            len(ranges),
            wait_begin-invoke_begin,
            reduce_begin-wait_begin,
            time.time()-reduce_begin
        )

        print(bench)

        return reduction_result
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


