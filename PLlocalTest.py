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
from BVchunker.TIFReader import ReadFrom2DTIFVid
from BVchunker.OMETIFReader import ReadFromOMETIFVid
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
        return (
            pvalue
                | 'strip chunk keys' >> beam.ParDo(stripChunks())
                | 'recombine video' >> beam.CombinePerKey(combineStats())
                | 'to JSON' >> beam.ParDo(toJSON())
                | 'WriteFullOutput' >> WriteToText(
                    self.output,
                    shard_name_template='',
                    file_name_suffix='--'+self.kind+'.txt'))

with beam.Pipeline(options=pipeline_options) as pipeline:
    testPIMS = (
        pipeline
            | 'Read PIMS' >> ReadFromPIMSVid(
                os.path.join(known_args.input, '**.*'))
            | 'PIMS Pipeline' >> ReduceVideosStats('pims', known_args.output))
    testND2 = (
        pipeline
            | 'Read ND2' >> ReadFromND2Vid(
                os.path.join(known_args.input, '**.nd2'))
            | 'ND2 Pipeline' >> ReduceVideosStats('nd2', known_args.output))
    test2DTIF = (
        pipeline
            | 'Read 2D TIF' >> ReadFrom2DTIFVid(
                os.path.join(known_args.input, '**.tif'))
            | '2D TIF Pipeline' >> ReduceVideosStats('tif', known_args.output))
    testOMETIF = (
        pipeline
            | 'Read OME TIF' >> ReadFrom2DTIFVid(
                os.path.join(known_args.input, '**.ome.tif'))
            | 'OME TIF Pipeline' >> ReduceVideosStats('ome.tif', known_args.output))
