from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from numpy import *
from numpy.random import rand
import os
import sys
import time
import pandas as pd
from itertools import product
import BVchunker
from BVchunker.BeamTools import VideoSplitter, combineTZ, splitBadFiles

import apache_beam as beam
from apache_beam.transforms import PTransform
from apache_beam.io import filebasedsource, ReadFromText, WriteToText, iobase
from apache_beam.io.iobase import Read
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.options.value_provider import ValueProvider
from apache_beam.options.value_provider import check_accessible

class ReadFromND2Vid(PTransform):
    """A ``PTransform`` for reading Nikon nd2 video files."""

    def __init__(self, path, chunkShape=None, Overlap=None, downSample=1):
        """Initializes ``ReadFromND2Vid``."""
        super(ReadFromND2Vid, self).__init__()
        if isinstance(path, basestring):
            path = StaticValueProvider(str, path)
        blob = StaticValueProvider(str, '**.nd2')
        if path.is_accessible() and blob.is_accessible():
            pattern = os.path.join(path.get(), blob.get())
        else:
            pattern = ''
        self._source = _ND2Source(pattern, chunkShape, Overlap, downSample)
    def expand(self, pvalue):
        frames = pvalue.pipeline | Read(self._source) | beam.Partition(splitBadFiles, 2)
        goodFiles = frames[1] | beam.FlatMap(lambda e: [e]) | beam.CombinePerKey(combineTZ())
        return goodFiles, frames[0]

    def display_data(self):
        return {'source_dd': self._source}
class _ND2utils:
    @staticmethod
    def findFooterOffset(vfile):
        pos = vfile.tell()
        filemapOffset = -1
        for line in vfile:
            if 'ND2 FILEMAP SIGNATURE NAME 0001' in line:
                i = line.index('ND2 FILEMAP SIGNATURE NAME 0001')
                filemapOffset = pos  + i
                break
            pos += int(len(line))
        return filemapOffset
    @staticmethod
    def readMetadata(vfile):
        try:
            size = vfile.size
        except:
            vfile.seek(0, 2)
            size = vfile.tell()
        vfile.seek(0)
        mn = vfile.read(3)
        assert mn == '\xda\xce\xbe'
        filemapOffset = 0
        start = max(0, size - 100*1024)
        vfile.seek(start)
        while True:
            filemapOffset = _ND2utils.findFooterOffset(vfile)
            if filemapOffset > 0 or start == 0:
                break
            start = max(0, start - 100*1024)
            assert size - start < 1e8
            vfile.seek(start)
        assert 0 < filemapOffset < size
        vfile.seek(filemapOffset)
        footer = vfile.read(size - filemapOffset)
        ind = footer.index('ImageAttributesLV!') + len('ImageAttributesLV!')
        mdos = frombuffer(footer[ind:ind+16], 'int64')
        vfile.seek(int(mdos[0]) + 16)
        mdAtributesBytes = vfile.read(filemapOffset - mdos[0] - 16)
        ind = footer.index('ImageCalibrationLV|0!') + len('ImageCalibrationLV|0!')
        mdos = frombuffer(footer[ind:ind+16], 'int64')
        vfile.seek(int(mdos[0]) + 16)
        mdCalibrationBytes = vfile.read(2*1024**2)
        ind = footer.index('ImageTextInfoLV!') + len('ImageTextInfoLV!')
        mdos = frombuffer(footer[ind:ind+16], 'int64')
        vfile.seek(int(mdos[0]) + 16)
        metadataBytes = vfile.read(10*1024)
        offsetData = []
        n = 0
        while True:
            key = 'ImageDataSeq|{0}!'.format(int(n))
            i = footer.find(key)
            if i == -1:
                break
            i += len(key)
            mdos = frombuffer(footer[i:i+16], 'int64')
            vi = footer[i:i+16]
            offsetData.append((key, int(mdos[0]), int(mdos[1])))
            n += 1
        Nrecords = n
        mdkeysXY = {'Nx': b'\x00W\x00i\x00d\x00t\x00h\x00\x00\x00',
                    'NxBytes': b'\x00W\x00i\x00d\x00t\x00h\x00B\x00y\x00t\x00e\x00s\x00\x00\x00',
                    'Ny': b'\x00H\x00e\x00i\x00g\x00h\x00t\x00\x00\x00',
                   }
        imgMD = {}
        for key, val in mdkeysXY.iteritems():
            ind = mdAtributesBytes.index(val)
            start = ind + len(val)
            a = mdAtributesBytes[start: start + 2]
            imgMD[key] = frombuffer(a, 'int16')[0]
        mdkeysZ = {'dxy': b'\rd\x00C\x00a\x00l\x00i\x00b\x00r\x00a\x00t\x00i\x00o\x00n\x00\x00\x00',
                  }
        for key, val in mdkeysZ.iteritems():
            ind = mdCalibrationBytes.index(val)
            start = ind + len(val)
            if key == 'dxy':
                a = mdCalibrationBytes[start: start + 8]
                imgMD[key] = frombuffer(a, 'float64')[0]
        mdkeysText = {'Nt': b'\x00T\x00i\x00m\x00e\x00 \x00L\x00o\x00o\x00p\x00:\x00 '}
        ind = metadataBytes.index(b'\x00M\x00e\x00t\x00a\x00d\x00a\x00t\x00a\x00:')
        metadataText = metadataBytes[ind:][1::2]
        ind = metadataText.index(b'\x00\x08')
        metadataText = metadataText[:ind]
        lines = metadataText.split('\r\n')
        imgMD['dz'] = 1.0
        for n, line in enumerate(lines):
            if 'Z Stack Loop:' in line and '- Step:' in lines[n+1]:
                sline = lines[n+1].split(' ')
                imgMD['dz'] = float64(sline[2])
                imgMD['dz units'] = sline[3]
        ind = metadataBytes.index(mdkeysText['Nt'])
        di = len(mdkeysText['Nt'])
        val = metadataBytes[ind + di: ind + di + 8][1::2].split('\r')[0]
        imgMD['Nt'] = int(val)
        imgMD['Nz'] = int(Nrecords/imgMD['Nt'])
        imgMD['raw'] = metadataText
        imgMD['fileSize'] = size
        return offsetData, imgMD
class _ND2Source(filebasedsource.FileBasedSource):
    """An experiment.
    """
    def __init__(self, file_pattern, chunkShape=None, Overlap=None, downSample=1):
        super(_ND2Source, self).__init__(file_pattern, min_bundle_size=0, validate=False)
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
    def read_records(self, fileName, range_tracker):
        nextBlockStart = -1
        def split_points_unclaimed(stopPosition):
            if nextBlockStart >= stopPosition:
                # Next block starts at or after the suggested stop position. Hence
                # there will not be split points to be claimed for the range ending at
                # suggested stop position.
                return 0
            return iobase.RangeTracker.SPLIT_POINTS_UNKNOWN
        range_tracker.set_split_points_unclaimed_callback(split_points_unclaimed)
        ###########
        basePath, vidName = os.path.split(fileName)
        _, ext = os.path.splitext(vidName)
        startOffset = range_tracker.start_position()
        with self.open_file(fileName) as vfile:
            try:
                offsetData, imgMetadata = _ND2utils.readMetadata(vfile)
                fileSize = imgMetadata['fileSize']
                imgMetadata['fileName'] = fileName
                bytesPerPixel = imgMetadata['NxBytes']/imgMetadata['Nx']
                assert bytesPerPixel == 2 # must be 16bit and single channel
                offsets = array([osd[1] for osd in offsetData], 'int')
                inds = offsets.argsort()
                offsets = offsets[inds]
                assert offsets[0] > 0
                blockSizes = array([offsetData[ind][2] for ind in inds], 'int')
                labelShifts = array([len(offsetData[ind][0]) for ind in inds], 'int')
                shape = tuple(int(imgMetadata[k]) for k in ['Nt', 'Ny', 'Nx', 'Nz'])
                assert all(shape > 0)
                Nt, Ny, Nx, Nz = shape
                frameSizeBytes = 2*Nx*Ny
                assert all(diff(offsets) > frameSizeBytes)
                assert offsets[-1] <= fileSize - frameSizeBytes
                recordSize = zeros_like(offsets)
                recordSize[:-1] = diff(offsets)
                recordSize[-1] = diff(offsets).max()
                splitter = self._getSplitter(imgMetadata)
                if startOffset is None:
                    startOffset = offsets[0]
                if not startOffset in offsets:
                    n = searchsorted(offsets, startOffset)
                    if n < offsets.size:
                        startOffset = offsets[n]
                vfile.seek(startOffset)
                while range_tracker.try_claim(vfile.tell()):
                    n = searchsorted(offsets, vfile.tell()) + 1
                    if n > offsets.size:
                        break
                    record = vfile.read(recordSize[n-1])
                    key = 'ImageDataSeq|{0}!'.format(int(n-1))
                    i = record.index(key)
                    # '\xda\xce\xbe\n\xe8\x0f\x00\x00'
                    assert record[:4] == '\xda\xce\xbe\n'
                    frameShift = frombuffer(record[4:12], 'int32')
                    db0 = record[i + frameShift[0]:]
                    # assert len(db0) == frameShift[1]
                    timeStampMaybe = db0[:8]
                    db = db0[8: frameSizeBytes + 8]
                    frame = frombuffer(db, 'uint16').reshape(Ny, Nx)
                    assert all(isfinite(frame))
                    if n < offsets.size:
                        nextBlockStart = offsets[n]
                        startOffset = offsets[n]
                        vfile.seek(startOffset)
                    else:
                        vfile.seek(fileSize)
                    for chunk in splitter.iterChunks(n, frame):
                        yield chunk
            except:
                yield ('File Not Processed', fileName)
