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
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions



parser = argparse.ArgumentParser()
parser.add_argument('--output', dest='output', default='/Users/jaynewby/Dropbox/CF/PT/beamTest/output/')
parser.add_argument('--input', dest='input', default='/Users/jaynewby/Dropbox/CF/PT/beamTest/testVidsND2')
known_args, pipeline_args = parser.parse_known_args()

pipeline_args.extend([
  '--runner=DirectRunner',
  # '--runner=DataflowRunner',
  # '--project=lai-lab',
  # '--staging_location=gs://pipeline-proc/staging',
  '--staging_location=/Users/jaynewby/Dropbox/CF/PT/beamTest/',
  '--temp_location=/Users/jaynewby/Dropbox/CF/PT/beamTest/temp/',
  '--setup_file=./setup.py',
  # '--extra_package=NetTracker/libhungarian/Hungarian/dist/Hungarian-1.0.tar.gz',
  # '--zone=us-east1-d',
  # '--worker_machine_type=n1-standard-4',
  '--max_num_workers=1'
])
pipeline_options = PipelineOptions(pipeline_args)

with beam.Pipeline(options=pipeline_options) as p:
    #timeString = time.ctime()
    nd2Files = (p | 'Read nd2' >> ReadFromND2Vid(os.path.join(known_args.input, '**.nd2')))
    NNproc = (nd2Files | 'recombine video' >> beam.CombinePerKey(combineStats())
                       | 'to JSON' >> beam.ParDo(toJSON())
                       | 'WriteFullOutput' >> WriteToText(known_args.output, file_name_suffix='.txt')
             )
