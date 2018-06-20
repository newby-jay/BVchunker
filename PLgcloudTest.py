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
from BVchunker.PIMSReader import ReadFromPIMSVid
from BVchunker.BeamTools import *

import pandas as pd
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from apache_beam.io import filebasedsource, ReadFromText, WriteToText, iobase
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.transforms import PTransform


parser = argparse.ArgumentParser()
parser.add_argument('--input', type=str, default='inputFolder/')
parser.add_argument('--output', type=str, default='outputFolder/')
known_args, pipeline_args = parser.parse_known_args()

pipeline_args.extend([
  '--runner=DataflowRunner',
  '--project=lai-lab',
  '--setup_file=./setup.py',
  '--worker_machine_type=n1-standard-1',
  '--max_num_workers=10',
  '--disk_size_gb=50',
  # '--experiments=shuffle_mode=service',
])
pipeline_options = PipelineOptions(pipeline_args)

class ReduceVideosStats(beam.PTransform):
    def __init__(self, kind, output):
        super(ReduceVideosStats, self).__init__()
        self.kind = kind
        self.time = time.ctime()
        self.output = output
    def expand(self, pvalue):
        outName = self.time+'--'+self.kind+'.txt'
        return (pvalue |
                    'strip chunk keys' >> beam.ParDo(stripChunks()) |
                    'recombine video' >> beam.CombinePerKey(combineStats()) |
                    'to JSON' >> beam.ParDo(toJSON()) |
                    'WriteFullOutput' >> WriteToText(self.output,
                                                     shard_name_template='',
                                                     file_name_suffix=outName))


with beam.Pipeline(options=pipeline_options) as p:

    nd2Files = p | 'Read nd2' >> ReadFromND2Vid(os.path.join(known_args.input, '**.nd2'))
    # tifFiles = p | 'Read tif' >> ReadFromTIFVid(os.path.join(known_args.input, '**.tif'))
    pimsFiles = p | 'Read pims' >> ReadFromPIMSVid(os.path.join(known_args.input, '**.*'))

    nd2Files | 'ND2 Pipeline' >> ReduceVideosStats('nd2', known_args.output)
    # tifFiles | 'TIF Pipeline' >> ReduceVideosStats('tif', known_args.output)
    pimsFiles | 'PIMS Pipeline' >> ReduceVideosStats('pims', known_args.output)
