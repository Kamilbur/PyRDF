from __future__ import print_function

import base64
import json
import logging
import time
import concurrent.futures
import os
import sys
import ROOT
from pathlib import Path

import boto3
import botocore
import cloudpickle as pickle

from .Dist import Dist

lambda_await_thread_stop = False


class FlushingLogger:
    def __init__(self):
        self.logger = logging.getLogger()

    def __getattr__(self, name):
        method = getattr(self.logger, name)
        if name in ['info', 'warning', 'debug', 'error', 'critical', ]:
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

        # Make mapper and reducer transferable
        pickled_mapper = AWS.encode_object(mapper)
        pickled_reducer = AWS.encode_object(reducer)

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

        self.logger.info(f'Before lambdas invoke. Number of lambdas: {len(ranges)}')

        processing_bucket = ssm_client.get_parameter(Name='processing_bucket')['Parameter']['Value']

        s3_resource.Bucket(processing_bucket).objects.all().delete()

        invoke_begin = time.time()
        # Invoke workers with ranges and mapper

        global lambda_await_thread_stop
        lambda_await_thread_stop = False

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(ranges)) as executor:
            executor.submit(AWS.wait_for_all_lambdas, s3_client, processing_bucket, len(ranges), self.logger)
            futures = [executor.submit(AWS.invoke_root_lambda, root_range, pickled_mapper, self.region, self.logger)
                       for root_range in ranges]
            call_results = [future.result() for future in futures]
            if not all(call_results):
                lambda_await_thread_stop = True

        if lambda_await_thread_stop:
            raise Exception(f'Some lambdas failed after multiple retrials')

        self.logger.info('All lambdas have been invoked')

        # while True:
        #     results = s3.list_objects_v2(Bucket=s3_output_bucket, Prefix='out.pickle')
        #     if results['KeyCount'] > 0:
        #         break
        #     self.logger.debug("still waiting")
        #     time.sleep(1)
        # result = s3.get_object(s3_output_bucket, 'out.pickle')

        download_begin = time.time()

        # Get names of output files, download and reduce them
        filenames = AWS.get_all_objects_from_s3_bucket(s3_client, processing_bucket)
        self.logger.info(f'Lambdas finished: {len(filenames)}')

        tmp_files_directory = '/tmp'
        AWS.remove_all_tmp_root_files(tmp_files_directory)

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(filenames)) as executor:
            futures = [executor.submit(AWS.get_from_s3, filename, self.region, processing_bucket, tmp_files_directory)
                       for filename in filenames]
            files = [future.result() for future in futures]

        reduce_begin = time.time()

        to_process = files
        while len(to_process) > 1:
            even_index_files = [to_process[i] for i in range(len(to_process)) if i % 2 == 0]
            odd_index_files = [to_process[i] for i in range(len(to_process)) if i % 2 == 1]

            with concurrent.futures.ThreadPoolExecutor(len(to_process)) as executor:
                futures = [executor.submit(reducer, pair[0], pair[1]) for pair in
                           zip(even_index_files, odd_index_files)]
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
            download_begin - invoke_begin,
            reduce_begin - download_begin,
            time.time() - reduce_begin
        )

        print(bench)

        return reduction_result

    def distribute_files(self, includes_list):
        pass

    @staticmethod
    def encode_object(object_to_encode) -> str:
        return str(base64.b64encode(pickle.dumps(object_to_encode)))

    @staticmethod
    def get_from_s3(filename, region, bucket_name, directory):
        s3_client = boto3.client('s3', region_name=region)
        local_filename = os.path.join(directory, filename['Key'])
        s3_client.download_file(bucket_name, filename['Key'], local_filename)

        tfile = ROOT.TFile(local_filename, 'OPEN')
        result = []
        # Get all objects from TFile
        for key in tfile.GetListOfKeys():
            result.append(key.ReadObj())
            result[-1].SetDirectory(0)
        tfile.Close()

        # Remove temporary root file
        Path(local_filename).unlink()

        return result

    @staticmethod
    def get_all_objects_from_s3_bucket(s3_client, bucket_name):
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        result = response.get('Contents', [])
        while response.get('IsTruncated'):
            cont_token = response.get('NextContinuationToken')
            response = s3_client.list_objects_v2(Bucket=bucket_name, ContinuationToken=cont_token)
            result += response.get('Contents', [])
        return result

    @staticmethod
    def invoke_root_lambda(root_range, script, region, logger):
        """
        Invoke root lambda.
        Args:
            root_range (Range): Range of data.
            script (function): A function that performs an operation on
                a range of data.
            region (str): String containing AWS region.
            logger (logging.Logger):
        Returns:
            bool: True if lambda invocation and execution was
                successful.
        """

        trials = 3
        client = boto3.client('lambda', region_name=region)

        payload = json.dumps({
            'range': AWS.encode_object(root_range),
            'script': script,
            'start': str(root_range.start),
            'end': str(root_range.end),
            'filelist': str(root_range.filelist),
            'friend_info': AWS.encode_object(root_range.friend_info)
        })

        # Maybe here give info about number of invoked lambda for awsmonitor

        while trials >= 0:
            trials -= 1
            try:
                response = client.invoke(
                    FunctionName='root_lambda',
                    InvocationType='RequestResponse',
                    Payload=bytes(payload, encoding='utf8')
                )

                try:
                    response['Payload'] = json.loads(response['Payload'].read())
                except Exception:
                    response['Payload'] = {}

                if 'FunctionError' in response or response['Payload'].get('statusCode') == 500:
                    try:
                        # Get error specification and remove additional quotas (side effect of serialization)
                        error_type = response['Payload']['errorType'][1:-1]
                        error_message = response['Payload']['errorMessage'][1:-1]
                        exception = getattr(sys.modules['builtins'], error_type)
                        msg = f"Lambda raised an exception: {error_message}"
                    except Exception:
                        exception = RuntimeError
                        msg = (f"Lambda raised an exception: (type={response['Payload']['errorType']},"
                               f"message={response['Payload']['errorMessage']})")
                    raise exception(msg)

            except botocore.exceptions.ClientError as error:
                # AWS site errors
                logger.warning(error['Error']['Message'])
            except Exception as error:
                # All other errors
                logger.warning(str(error) + " (" + type(error).__name__ + ")")
            else:
                return True
            time.sleep(1)

        # Note: lambda finishes before s3 object is created
        return False

    @staticmethod
    def remove_all_tmp_root_files(directory):
        for file in os.listdir(directory):
            if file.endswith('.root'):
                Path(os.path.join(directory, file)).unlink()

    @staticmethod
    def wait_for_all_lambdas(s3_client, processing_bucket, num_of_lambdas, logger):
        # Wait until all lambdas finished execution
        global lambda_await_thread_stop
        while not lambda_await_thread_stop:
            results = AWS.get_all_objects_from_s3_bucket(s3_client, processing_bucket)
            logger.info(f'Lambdas finished: {len(results)}')
            if len(results) == num_of_lambdas:
                break
            time.sleep(1)

