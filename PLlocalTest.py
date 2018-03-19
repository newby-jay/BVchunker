from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import numpy as np
from numpy import array, arange, float32, uint8
from numpy.random import rand
import os
import sys
import time
import BVchunker
from BVchunker.ND2Reader import ReadFromND2Vid
from BVchunker.TIFReader import ReadFromTIFVid
from BVchunker.BeamTools import *

import pandas as pd
import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions



parser = argparse.ArgumentParser()
parser.add_argument('--output', dest='output')
parser.add_argument('--input', dest='input')
known_args, pipeline_args = parser.parse_known_args()

pipeline_args.extend([
  '--runner=DirectRunner',
  '--setup_file=./setup.py',
  '--max_num_workers=1'
])
pipeline_options = PipelineOptions(pipeline_args)



class identity(beam.DoFn):
    def __init__(self):
        pass
    def process(self, element):
        assert type(element) == tuple
        k, e = element
        yield element

with beam.Pipeline(options=pipeline_options) as p:
    # files = (p | 'Read nd2' >> ReadFromND2Vid(os.path.join(known_args.input, '**.nd2')))
    files = p | 'Read tif' >> ReadFromTIFVid(os.path.join(known_args.input, '**.tif'))
    goodFiles, badFiles = files
    (
    goodFiles |
        beam.ParDo(stripChunks()) |
        'recombine video' >> beam.CombinePerKey(combineStats()) |
        'to JSON' >> beam.ParDo(toJSON()) |
        'WriteFullOutput' >> WriteToText(known_args.output,
                                         file_name_suffix='.txt')
    )
    (
    badFiles |
        'failed files to JSON' >> beam.ParDo(toJSON()) |
        'failed files output' >> WriteToText(known_args.output,
                                             file_name_suffix='-failedFiles.txt')
    )
