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
  '--setup_file=./setup.py',
  '--worker_machine_type=n1-standard-1',
  '--max_num_workers=10',
  '--disk_size_gb=50',
  # '--experiments=shuffle_mode=service',
])
pipeline_options = PipelineOptions(pipeline_args)
class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input', type=str)
        parser.add_value_provider_argument('--output', type=str)
user_params = pipeline_options.view_as(UserOptions)


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

    nd2Files = p | 'Read nd2' >> ReadFromND2Vid(user_params.input)
    nd2GoodFiles, nd2BadFiles = nd2Files

    tifFiles = p | 'Read tif' >> ReadFromTIFVid(user_params.input)
    tifGoodFiles, tifBadFiles = tifFiles

    nd2GoodFiles | 'ND2 Pipeline' >> ReduceVideosStats('nd2', user_params.output)
    tifGoodFiles | 'TIF Pipeline' >> ReduceVideosStats('tif', user_params.output)

    # (nd2BadFiles |
    #     'nfailed files to JSON' >> beam.ParDo(toJSON()) |
    #     'nfailed files output' >> WriteToText(user_params.output,
    #                                          file_name_suffix='--nd2-failedFiles.txt')
    # )
    # (tifBadFiles |
    #     'tfailed files to JSON' >> beam.ParDo(toJSON()) |
    #     'tfailed files output' >> WriteToText(user_params.output,
    #                                          file_name_suffix='--tif-failedFiles.txt')
    # )
