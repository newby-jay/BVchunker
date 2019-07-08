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
from pims import bioformats
from pims.bioformats import BioformatsReader as BioformatsReader
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

    def __init__(
        self,
        file_pattern=None,
        chunkShape=None,
        Overlap=None,
        downSample=1):
        """Initializes ``ReadFromPIMSVid``."""
        super(ReadFromPIMSVid, self).__init__()
        self._source = _PIMSSource(
            file_pattern,
            chunkShape,
            Overlap,
            downSample)
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
    def __init__(
        self,
        file_pattern,
        chunkShape=None,
        Overlap=None,
        downSample=1):
        "Initialize"
        super(_PIMSSource, self).__init__(
            file_pattern,
            min_bundle_size=0,
            splittable=True,
            validate=False)
        self.chunkShape = chunkShape
        self.Overlap = Overlap
        self.downSample = downSample
        self.ffmpegList = [
            '.mp4', '.MP4',
            '.avi', '.AVI',
            '.mov', '.MOV']
        self.validExtList = [
            '.tif', '.TIF',
            '.tiff', '.TIFF',
            '.nd2', '.ND2']
        self.validExtList.extend(self.ffmpegList)
        self.imgMetadata = None
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
    # @check_accessible(['_pattern'])
    # def estimate_size(self):
    #     pattern = self._pattern.get()
    #     match_result = FileSystems.match([pattern])[0]
    #     size = 0
    #     for f in match_result.metadata_list:
    #         if f.path[-4:] in ['.mp4', '.MP4']:
    #             size += 100*f.size_in_bytes
    #         else:
    #             size += f.size_in_bytes
    #     return int(size)
    def _getMetadata(self, frames, fileName, fileSize):
        fshape = frames.frame_shape
        assert len(fshape) == 2 or len(fshape) == 3
        shape = (1, fshape[0], fshape[1])
        isColor = len(fshape) == 3
        Nz, Ny, Nx = shape
        Nt = len(frames)
        dz = 1.0
        dxy = 1.0
        zscale = 1.0
        dt = 1.0
        Nz = 1
        self.imgMetadata = {
            'Nt': Nt, 'Ny': Ny, 'Nx': Nx, 'Nz': Nz,
            'dxy': dxy, 'dz': dz, 'dt': dt,
            'fileName': fileName, 'fileSize': fileSize,
            'isColor': isColor}
    def iterChunksPIMS(self, range_tracker, tmpfile, fileName, fileSize):
        basePath, vidName = os.path.split(fileName)
        _, ext = os.path.splitext(vidName)
        if ext in ['.his', '.HIS']:
            opener = pims.Bioformats
        else:
            opener = pims.open
        with opener(tmpfile.name) as frames:
        # with BioformatsReader(tmpfile.name, java_memory='1024m') as frames:
            # if self.imgMetadata is None:
            self._getMetadata(frames, fileName, fileSize)
            Npages = self.imgMetadata['Nt']*self.imgMetadata['Nz']
            assert len(frames) == Npages
            assert len(frames) > 0
            splitter = self._getSplitter(self.imgMetadata)
            d_os = fileSize/Npages
            s = range_tracker.start_position()
            if s is None:
                s = 0
            n = int(ceil(s/d_os))
            while True:
                range_tracker.try_claim(int(n*d_os))
                if n >= Npages:
                    break
                frame = frames[n]
                data = uint16(frame)
                n += 1
                if self.imgMetadata['isColor']:
                    # assert data.dtype == uint8, 'Color videos must be 8 bit'
                    data = float64(data).sum(axis=2)
                    # data -= data.min()
                    # data /= data.max()
                    data = uint16(data)
                    # data = data[..., 0]
                for chunk in splitter.iterChunks(n, data):
                    yield chunk
            assert n >= Npages
    def read_records(self, fileName, range_tracker):
        nextBlockStart = -1
        def split_points_unclaimed(stopPosition):
            if stopPosition <= nextBlockStart:
                return 0
            return iobase.RangeTracker.SPLIT_POINTS_UNKNOWN
        range_tracker.set_split_points_unclaimed_callback(split_points_unclaimed)

        # assert fileName[-4:] in self.validExtList, 'Invalid file type'
        basePath, vidName = os.path.split(fileName)
        _, ext = os.path.splitext(vidName)
        with self.open_file(fileName) as vfile, \
            tempfile.NamedTemporaryFile(mode='w+b', suffix=ext) as tmpfile:
            try:
                fileSize = self._getSize(vfile)
                assert fileSize < 20*1024**3
                for rl in vfile:
                    tmpfile.write(rl)
                tmpfile.seek(0)
                for chunk in self.iterChunksPIMS(
                    range_tracker,
                    tmpfile,
                    fileName,
                    fileSize):
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
