from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import os
import sys
import time
import tempfile

from numpy import *
from itertools import product
from BVchunker import VideoSplitter, combineTZ, splitBadFiles
import pims
from google.cloud import error_reporting, logging

import apache_beam as beam
from apache_beam.transforms import PTransform
from apache_beam.io import filebasedsource, ReadFromText, WriteToText, iobase
from apache_beam.io.iobase import Read
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.options.value_provider import ValueProvider
from apache_beam.options.value_provider import check_accessible
from apache_beam.io.filesystems import FileSystems

class ReadFromPIMSVid(PTransform):
    """A PTransform for reading 2D video files using the PIMS python package."""

    def __init__(self, file_pattern=None, chunkShape=None, Overlap=None, downSample=1):
        """Initializes ``ReadFromPIMSVid``."""
        super(ReadFromPIMSVid, self).__init__()
        self._source = _PIMSSource(file_pattern, chunkShape, Overlap, downSample)
    def expand(self, pvalue):
        frames = (
            pvalue.pipeline
                | Read(self._source)
                | beam.Partition(splitBadFiles, 2)
           )
        chunks = (
            frames[1]
                | beam.FlatMap(lambda e: [e])
                | beam.CombinePerKey(combineTZ())
           )
        return chunks
    def display_data(self):
        return {'source_dd': self._source}

class _PIMSSource(filebasedsource.FileBasedSource):
    """Read video into chunks using the PIMS python package."""
    def __init__(self, file_pattern, chunkShape=None, Overlap=None, downSample=1):
        super(_PIMSSource, self).__init__(
            file_pattern,
            min_bundle_size=0,
            splittable=False,
            validate=False)
        self.chunkShape = chunkShape
        self.Overlap = Overlap
        self.downSample = downSample
    def _getSplitter(self, imgMetadata):
        if all(self.chunkShape == None) and all(self.Overlap == None):
            splitter = VideoSplitter(imgMetadata, downSample=self.downSample)
        elif all(self.chunkShape != None) and all(self.Overlap == None):
            splitter = VideoSplitter(imgMetadata,
                                    chunkShape=self.chunkShape,
                                    downSample=self.downSample)
        elif all(self.chunkShape == None) and all(self.Overlap != None):
            splitter = VideoSplitter(imgMetadata,
                                    Overlap=self.Overlap,
                                    downSample=self.downSample)
        else:
            splitter = VideoSplitter(imgMetadata,
                                    Overlap=self.Overlap,
                                    chunkShape=self.chunkShape,
                                    downSample=self.downSample)
        return splitter
    def _getSize(self, vfile):
        try:
            return vfile.size
        except:
            vfile.seek(0, 2)
            fs = vfile.tell()
            vfile.seek(0)
            return fs
    def iterChunksPIMS(self, tmpfile, fileName, fileSize):
        with pims.open(tmpfile.name) as frames:
            metadata = [f.metadata for f in frames]
            fshape = frames.frame_shape
            assert len(fshape) == 2 or len(fshape) == 3
            shape = (1, fshape[0], fshape[1])
            isColor = len(fshape) == 3
            Nz, Ny, Nx = shape
            Nt = len(metadata)
            dz = 1.0
            dxy = 1.0
            zscale = 1.0
            dt = 1.0
            Nz = 1
            imgMetadata = {'Nt': Nt, 'Ny': Ny, 'Nx': Nx, 'Nz': Nz,
                           'dxy': dxy, 'dz': dz, 'dt': dt,
                           'fileName': fileName, 'fileSize': fileSize}
            splitter = self._getSplitter(imgMetadata)
            n = 0
            for frame in frames:
                n += 1
                data = array(frame)
                if isColor:
                    assert data.dtype == uint8, 'Color videos must be 8 bit'
                    data = float64(data).sum(axis=2)
                    data -= data.min()
                    # data /= data.max()
                    data = uint16(data)
                for chunk in splitter.iterChunks(n, data):
                    yield chunk
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
        with self.open_file(fileName) as vfile, \
            tempfile.NamedTemporaryFile(mode='w+b', suffix=ext) as tmpfile:
            try:
                fileSize = self._getSize(vfile)
                assert fileSize < 20*1024**3
                for rl in vfile:
                    tmpfile.write(rl)
                tmpfile.seek(0)
                for chunk in self.iterChunksPIMS(tmpfile, fileName, fileSize):
                    yield chunk
            except:
                client = error_reporting.Client()
                client.report('File Not Processed: ' + fileName)
                client.report_exception()

                logging_client = logging.Client()
                log_name = 'pims-reader'
                logger = logging_client.logger(log_name)
                logmessage = {
                'Error': 'File cannot be read',
                'Filename': fileName
                }
                logger.log_struct(logmessage)
                yield ('File Not Processed', fileName)
