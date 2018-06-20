from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import numpy as np
from numpy import array, arange, float32, uint8
from numpy.random import rand
import os
import sys
import time
from BVchunker import *
from BVchunker.ND2Reader import ReadFromND2Vid
from BVchunker.TIFReader import ReadFromTIFVid
from BVchunker.PIMSReader import ReadFromPIMSVid

import pandas as pd
import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions



parser = argparse.ArgumentParser()
parser.add_argument('--input', type=str, default='inputFolder/')
parser.add_argument('--output', type=str, default='outputFolder/')
known_args, pipeline_args = parser.parse_known_args()

pipeline_args.extend([
  '--runner=DirectRunner',
  '--setup_file=./setup.py',
  '--max_num_workers=1'
])
pipeline_options = PipelineOptions(pipeline_args)


class ReduceVideosStats(beam.PTransform):
    def __init__(self, kind, output):
        super(ReduceVideosStats, self).__init__()
        self.kind = kind
        self.output = output
    def expand(self, pvalue):
        return (pvalue |
                    'strip chunk keys' >> beam.ParDo(stripChunks()) |
                    'recombine video' >> beam.CombinePerKey(combineStats()) |
                    'to JSON' >> beam.ParDo(toJSON()) |
                    'WriteFullOutput' >> WriteToText(self.output,
                                                    file_name_suffix='--'+self.kind+'.txt')
               )

with beam.Pipeline(options=pipeline_options) as p:

    nd2Files = p | 'Read nd2' >> ReadFromND2Vid(os.path.join(known_args.input, '**.nd2'))
    tifFiles = p | 'Read tif' >> ReadFromTIFVid(os.path.join(known_args.input, '**.tif'))
    pimsFiles = p | 'Read pims' >> ReadFromPIMSVid(os.path.join(known_args.input, '**.*'))

    nd2Files | 'ND2 Pipeline' >> ReduceVideosStats('nd2', known_args.output)
    tifFiles | 'TIF Pipeline' >> ReduceVideosStats('tif', known_args.output)
    pimsFiles | 'PIMS Pipeline' >> ReduceVideosStats('pims', known_args.output)
