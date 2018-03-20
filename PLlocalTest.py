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

    nd2Files = p | 'Read nd2' >> ReadFromND2Vid(known_args.input)
    nd2GoodFiles, nd2BadFiles = nd2Files

    tifFiles = p | 'Read tif' >> ReadFromTIFVid(known_args.input)
    tifGoodFiles, tifBadFiles = tifFiles

    nd2GoodFiles | 'ND2 Pipeline' >> ReduceVideosStats('nd2', known_args.output)
    tifGoodFiles | 'TIF Pipeline' >> ReduceVideosStats('tif', known_args.output)

    # (nd2BadFiles |
    #     'nfailed files to JSON' >> beam.ParDo(toJSON()) |
    #     'nfailed files output' >> WriteToText(known_args.output,
    #                                          file_name_suffix='--nd2-failedFiles.txt')
    # )
    # (tifBadFiles |
    #     'tfailed files to JSON' >> beam.ParDo(toJSON()) |
    #     'tfailed files output' >> WriteToText(known_args.output,
    #                                          file_name_suffix='--tif-failedFiles.txt')
    # )
