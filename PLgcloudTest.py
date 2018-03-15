from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import numpy as np
from numpy import array, arange, float32, uint8
from numpy.random import rand
import os
import sys
import time
from ND2Reader import ReadFromND2Vid
from BeamTools import *

import pandas as pd
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from apache_beam.io import filebasedsource, ReadFromText, WriteToText, iobase
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.transforms import PTransform


parser = argparse.ArgumentParser()
known_args, pipeline_args = parser.parse_known_args()

pipeline_args.extend([
  '--runner=DataflowRunner',
  '--project=lai-lab',
  '--staging_location=gs://pipeline-proc/staging',
  '--temp_location=gs://pipeline-proc/tmp',
  '--setup_file=./setup.py',
  # '--extra_package=NetTracker/libhungarian/Hungarian/dist/Hungarian-1.0.tar.gz',
  # '--zone=us-east1-d',
  '--worker_machine_type=n1-standard-1',
  '--max_num_workers=500',
  # '--autoscaling_algorithm=NONE',
  # '--num_workers=100',
  '--disk_size_gb=50',
  '--experiments=shuffle_mode=service',
])
pipeline_options = PipelineOptions(pipeline_args)
# pipeline_options.view_as(SetupOptions).save_main_session = True
class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input', type=str,
                                           default='gs://pipeline-proc/testVids/**.nd2')
        # parser.add_value_provider_argument('--kind', type=str,
        #                                    default='tif')
        parser.add_value_provider_argument('--output', type=str,
                                           default='gs://pipeline-proc/testOutput/')


user_params = pipeline_options.view_as(UserOptions)

with beam.Pipeline(options=pipeline_options) as p:
    timeString = time.ctime()
    nd2Files = (p | 'Read nd2' >> ReadFromND2Vid(user_params.input))
    NNproc = (nd2Files | 'recombine video' >> beam.CombinePerKey(combineStats())
                       | 'to JSON' >> beam.ParDo(toJSON())
                       | 'WriteFullOutput' >> WriteToText(user_params.output,
                                                          shard_name_template='',
                                                          file_name_suffix=timeString + '.txt')
             )
