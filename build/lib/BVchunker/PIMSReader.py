from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import os
import sys
import time

from numpy import *
from numpy.random import rand
import pandas as pd
from itertools import product
from BVchunker import VideoSplitter, combineTZ, splitBadFiles
from cStringIO import StringIO
import pims

import apache_beam as beam
from apache_beam.transforms import PTransform
from apache_beam.io import filebasedsource, ReadFromText, WriteToText, iobase
from apache_beam.io.iobase import Read
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.options.value_provider import ValueProvider
from apache_beam.options.value_provider import check_accessible
# from apache_beam.transforms import core
# from apache_beam.typehints import TypeVariable, Any, KV, List, Tuple, Dict, with_input_types, with_output_types

class ReadFromPIMSVid(PTransform):
    """A PTransform for reading video files using the PIMS python package."""

    def __init__(self, file_pattern=None):
        """Initializes ``ReadFromPIMSVid``."""
        super(ReadFromPIMSVid, self).__init__()
        self._source = _PIMSSource(file_pattern, min_bundle_size=0,
                                   splittable=False, validate=False)
    def expand(self, pvalue):
        frames = pvalue.pipeline | Read(self._source) | beam.CombinePerKey(combineTZ())
        return frames
    def display_data(self):
        return {'source_dd': self._source}

class _PIMSSource(filebasedsource.FileBasedSource):
    """Read video into chunks using the PIMS python package."""

    def _getSize(self, vfile):
        try:
            return vfile.size
        except:
            vfile.seek(0, 2)
            fs = vfile.tell()
            vfile.seek(0)
            return fs
    def _getSplitter(self, imgMetadata):
        splitter = VideoSplitter(imgMetadata)
        return splitter
    def read_records(self, fileName, range_tracker):
        nextBlockStart = -1
        def split_points_unclaimed(stopPosition):
            if nextBlockStart >= stopPosition:
                return 0
            return iobase.RangeTracker.SPLIT_POINTS_UNKNOWN
        range_tracker.set_split_points_unclaimed_callback(split_points_unclaimed)
        basePath, vidName = os.path.split(fileName)
        _, ext = os.path.splitext(vidName)
        startOffset = range_tracker.start_position()
        with self.open_file(fileName) as vfile:
            # try:
            fileSize = self._getSize(vfile)
            assert fileSize < 10*1024**3
            with open(vidName, mode='w+b') as tmpfile:
                for rl in vfile:
                    tmpfile.write(rl)
            with pims.open(vidName) as frames:
                metadata = [f.metadata for f in frames]
                fshape = frames.frame_shape
                assert len(fshape) == 2 or len(fshape) == 3
                if len(fshape) == 2:
                    shape = (1, fshape[0], fshape[1])
                elif len(fshape) == 3:
                    shape = fshape
                Nz, Ny, Nx = shape
                Nt = len(metadata)
                try:
                    dz = frames.calibrationZ
                    dxy = frames.calibration
                    zscale = frames.calibrationZ/frames.calibration
                    rtimes = array([metadata[t]['t_ms'][0] for t in arange(Nt)])
                    dt = mean(diff(rtimes))/1000.
                    frames.bundle_axes = 'yx'
                    frames.iter_axes = 'tz'
                    ThreeD = True
                except:
                    dz = 1.0
                    dxy = 1.0
                    zscale = 1.0
                    dt = 1.0
                    Nz = 1
                    ThreeD = False
                imgMetadata = {'Nt': Nt, 'Ny': Ny, 'Nx': Nx, 'Nz': Nz,
                               'dxy': dxy, 'dz': dz, 'dt': dt,
                               'fileName': fileName, 'fileSize': fileSize}
                splitter = self._getSplitter(imgMetadata)
                n = 0
                for frame in frames:
                    n += 1
                    for chunk in splitter.iterChunks(n, array(frame)):
                        yield chunk
                os.remove(vidName)
            # except:
            #     yield ('File Not Processed', fileName)
