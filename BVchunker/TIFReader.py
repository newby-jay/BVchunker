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
from BVchunker.BeamTools import VideoSplitter, combineTZ
import tifffile as tif
import xml.etree.ElementTree as ET

import apache_beam as beam
from apache_beam.transforms import PTransform
from apache_beam.io import filebasedsource, ReadFromText, WriteToText, iobase
from apache_beam.io.iobase import Read


class ReadFromTIFVid(PTransform):
    """A PTransform for reading TIF video files."""

    def __init__(self, file_pattern=None, chunkShape=None, Overlap=None, downSample=1):
        """Initializes ``ReadFromTIFVid``."""
        super(ReadFromTIFVid, self).__init__()
        self._source = _TIFSource(file_pattern, chunkShape, Overlap, downSample)

    def expand(self, pvalue):
        return pvalue.pipeline | Read(self._source) | beam.CombinePerKey(combineTZ())

    def display_data(self):
        return {'source_dd': self._source}
class _TIFutils:
    def __init__(self):
        self.TAGS = {256: 'ImageWidth',
                    256: 'ImageHeight',
                    258: 'BitsPerSample',
                    259: 'Compression',
                    270: 'ImageDescription',
                    273: 'StripOffsets',
                    278: 'RowsPerStrip',
                    279: 'StripByteCounts',
                    324: 'TileOffsets'}
        self.TIFtypes = {2: 'ascii',
                         3: 'short',
                         4: 'long',
                         5: 'rational'}
        self.fileSize = 0
        self.bufferSize = 5*1024
    def get16int(self, s):
        assert len(s) == 2
        dt = dtype('uint16').newbyteorder(self.E)
        return int(frombuffer(s, dt))
    def get32int(self, s):
        assert len(s) == 4
        dt = dtype('uint32').newbyteorder(self.E)
        return int(frombuffer(s, dt))
    def getIFD(self, buf, A):
        #print(A, len(buf))
        assert 0 < A + 2 < len(buf)
        B = self.get16int(buf[A: A+2])
        entries = []
        for n in arange(B):
            start = A + 2 + 12*n
            assert start + 12 < len(buf)
            entries.append(buf[start:start+12])
        Q = A + 2 + B*12
        assert 0 < Q + 4 <= len(buf)
        Anext = self.get32int(buf[Q: Q + 4])
        return Anext, entries
    def parseEntries(self, entries):
        output = {}
        tifMD = {}
        for e in entries:
            TAG = self.get16int(e[:2])
            tifType = self.get16int(e[2:4])
            N = self.get32int(e[4:8])
            if tifType == 3:
                TAGvalue = self.get16int(e[8:10])
            else:
                TAGvalue = self.get32int(e[8:12])
            tifMD[TAG] = (tifType, N, TAGvalue)
        output['Ny'] = tifMD[257][2]
        output['Nx'] = tifMD[256][2]
        output['mdOffset'] = (tifMD[270][2], tifMD[270][1])
        output['pixelSizeBytes'] = int(tifMD[258][2]/8)
        output['stripOffsets'] = tifMD[273][1:]
        # assert output['pixelSizeBytes'] == int(rowSize/output['Nx'] / 8)
        if 259 in tifMD.keys():
            assert tifMD[259][2] == 1 ## must be uncompressed
        return output
    def getAllOffsets(self, A0, vfile):
        A = A0
        IFDoffsets = []
        frameOffsets = []
        firstFrame = True
        while True:
            vfile.seek(A)
            IFDoffsets.append(A)
            buf = vfile.read(self.bufferSize)
            A, entries = self.getIFD(buf, 0)
            frameMD = self.parseEntries(entries)
            if firstFrame:
                MDfirst = frameMD.copy()
                firstFrame = False
            numStrips, stripOffset = frameMD['stripOffsets']
            if numStrips == 1: # in this case, we live in a sane universe
                frameOffsets.append(stripOffset)
            else: # in this case, we have an offset to the actual offset--it can be anywhere in the file :D
                ind = stripOffset - A
                if not 0 < ind + 4 < len(buf):
                    vfile.seek(stripOffset)
                    buf = vfile.read(4)
                    ind = 0
                frameOffsets.append(self.get32int(buf[ind: ind + 4]))
            if A == 0:
                break
        assert len(IFDoffsets) == len(frameOffsets)
        return array(frameOffsets), MDfirst
    def getNextOffset(self, A0, vfile):
        vfile.seek(A0)
        IFDoffsets.append(A0)
        buf = vfile.read(self.bufferSize)
        A, entries = self.getIFD(buf, 0)
        frameMD = self.parseEntries(entries)
        numStrips, stripOffset = frameMD['stripOffsets']
        if numStrips == 1: # in this case, we live in a sane universe
            frameOffset = stripOffset
        else: # in this case, we have an offset to the actual offset--it can be anywhere in the file :D
            ind = stripOffset - A
            if not 0 < ind + 4 < len(buf):
                vfile.seek(stripOffset)
                buf = vfile.read(4)
                ind = 0
            frameOffset = self.get32int(buf[ind: ind + 4])
        return frameOffset, frameMD
    def readMetadata(self, vfile):
        vfile.seek(0)
        buf = vfile.read(8)
        if buf[:2] == 'II' and buf[2:4] == '*\x00':
            self.E = '<'
            self.sz = 4
        elif buf[:2] == 'MM' and buf[2:4] == '\x00*':
            self.E = '>'
            self.sz = 4
        else:
            raise
        A = self.get32int(buf[4:8])
        vfile.seek(A)
        buf = vfile.read(10*1024)
        B = self.get16int(buf[:2])
        if 2 + B*12 + 4 > self.bufferSize:
            self.bufferSize = 2*(2 + B*12 + 4)
        A, md = self.getNextOffset(A, vfile)
        Ny = md['Ny']
        Nx = md['Nx']
        imgMD = {'Ny': Ny, 'Nx': Nx}
        imgMD['fileSize'] = self.fileSize
        imgMD['pixelSizeBytes'] = md['pixelSizeBytes']
        mdOffset, mdSize = md['mdOffset']
        vfile.seek(mdOffset)
        metaDataRaw = vfile.read(mdSize - 1)
        imgMD['raw'] = metaDataRaw
        if '<?xml' in metaDataRaw and '</OME>' in metaDataRaw:
            root = ET.fromstring(metaDataRaw)
            urlPrefixThing = root.tag[:-3]
            for p in root.iter(urlPrefixThing + 'Pixels'):
                pixelMD = p.attrib
            imgMD['Nt'] = int(pixelMD['SizeT'])
            imgMD['Nz'] = int(pixelMD['SizeZ'])
            imgMD['dxy'] = float(pixelMD['PhysicalSizeY'])
            assert pixelMD['PhysicalSizeX'] == pixelMD['PhysicalSizeY']
            imgMD['dz'] = float(pixelMD['PhysicalSizeZ'])
            assert int(pixelMD['SizeC']) == 1
            imgMD['dxy units'] = pixelMD['PhysicalSizeYUnit']
            assert pixelMD['PhysicalSizeXUnit'] == pixelMD['PhysicalSizeYUnit']
            imgMD['dt'] = float(pixelMD['TimeIncrement'])
        elif '<MetaData' in metaDataRaw and '</MetaData>' in metaDataRaw:
            imgMD['Nt'] = int(self.fileSize/(imgMD['pixelSizeBytes']*Nx*Ny))
            imgMD['Nz'] = 1
            imgMD['dz'] = 1.0
            imgMD['dxy'] = 1.0
        elif 'ImageJ' in metaDataRaw:
            metaData = metaDataRaw.split()[:20]
            metaData = [m for m in metaData if '=' in m]
            metaData = dict(tuple(m.split('=')) for m in metaData)
            Nt = int(metaData['frames'])
            imgMD['Nt'] = Nt
            Nz = int(metaData['slices'])
            assert Nt == Nimages and Nz == 1 ## 3D not supported for this type
            imgMD['Nz'] = Nz
            # imgMD['Nz'] = 1
            # imgMD['Nt'] = Nimages
            imgMD['dz'] = 1.0
            imgMD['dxy'] = 1.0
        return A, imgMD
class _TIFSource(filebasedsource.FileBasedSource):
    """Read tif video into chunks."""
    DEFAULT_READ_BUFFER_SIZE = 8192

    class ReadBuffer(object):
    # A buffer that gives the buffered data and next position in the
    # buffer that should be read.
        def __init__(self, data, position):
            self._data = data
            self._position = position

        @property
        def data(self):
            return self._data

        @data.setter
        def data(self, value):
            assert isinstance(value, bytes)
            self._data = value

        @property
        def position(self):
            return self._position

        @position.setter
        def position(self, value):
            assert isinstance(value, (int, long))
            if value > len(self._data):
                raise ValueError('Cannot set position to %d since it\'s larger than '
                                 'size of data %d.', value, len(self._data))
            self._position = value
        def reset(self):
            self.data = ''
            self.position = 0
            
    def __init__(self, file_pattern, chunkShape=None, Overlap=None, downSample=1):
        super(_TIFSource, self).__init__(file_pattern, splittable=False, min_bundle_size=0, validate=False)
        self.chunkShape = chunkShape
        self.Overlap = Overlap
        self.downSample = downSample
    def _getSize(self, vfile):
        try:
            self.fileSize = vfile.size
        except:
            vfile.seek(0, 2)
            self.fileSize = vfile.tell()
    def _getSplitter(self):
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
        splitter = self._getSplitter()


        with self.open_file(fileName) as vfile:
            self._getSize(vfile)
            TU = _TIFutils()
            A, imgMetadata = TU.readMetadata(vfile)
            imgMetadata['fileSize'] = self.fileSize
            imgMetadata['fileName'] = fileName
            bytesPerPixel = imgMetadata['pixelSizeBytes']
            assert bytesPerPixel == 1 or bytesPerPixel == 2
            shape = tuple(int(imgMetadata[k]) for k in ['Nt', 'Ny', 'Nx', 'Nz'])
            assert all(shape > 0)
            Nt, Ny, Nx, Nz = shape
            frameSizeBytes = bytesPerPixel*Ny*Nx
            if startOffset is None:
                startOffset = A
            vfile.seek(startOffset)
            while range_tracker.try_claim(vfile.tell()):
                if vfile.tell() == self.fileSize:
                    break
                A, imgMD = self.getNextOffset(A, vfile)
                record = vfile.read(frameSizeBytes)
                if bytesPerPixel == 1:
                    dt = dtype('uint8').newbyteorder(TU.E)
                elif bytesPerPixel == 2:
                    dt = dtype('uint16').newbyteorder(TU.E)
                frame = frombuffer(record, dt).reshape(Ny, Nx)
                assert all(isfinite(frame))
                if A > 0:
                    nextBlockStart = A
                    startOffset = A
                    vfile.seek(A)
                else:
                    vfile.seek(self.fileSize)
                for chunk in splitter.iterChunks(n, frame):
                    yield chunk

        # with self.open_file(fileName) as vfile:
        #     self._getSize(vfile)
        #     TU = _TIFutils()
        #     offsets, imgMetadata = TU.readMetadata(vfile)
        #     imgMetadata['fileSize'] = self.fileSize
        #     imgMetadata['fileName'] = fileName
        #     bytesPerPixel = imgMetadata['pixelSizeBytes']
        #     assert bytesPerPixel == 1 or bytesPerPixel == 2
        #     shape = tuple(int(imgMetadata[k]) for k in ['Nt', 'Ny', 'Nx', 'Nz'])
        #     assert all(shape > 0)
        #     Nt, Ny, Nx, Nz = shape
        #     frameSizeBytes = bytesPerPixel*Ny*Nx
        #     assert offsets[0] > 0
        #     assert all(diff(offsets) >= frameSizeBytes)
        #     assert offsets[-1] <= self.fileSize - frameSizeBytes
        #
        #     if startOffset is None:
        #         startOffset = offsets[0]
        #     if not startOffset in offsets:
        #         n = searchsorted(offsets, startOffset)
        #         if n < offsets.size:
        #             startOffset = offsets[n]
        #     vfile.seek(startOffset)
        #     while range_tracker.try_claim(vfile.tell()):
        #         n = searchsorted(offsets, vfile.tell()) + 1
        #         if n > offsets.size:
        #             break
        #         record = vfile.read(frameSizeBytes)
        #         if bytesPerPixel == 1:
        #             dt = dtype('uint8').newbyteorder(TU.E)
        #         elif bytesPerPixel == 2:
        #             dt = dtype('uint16').newbyteorder(TU.E)
        #         frame = frombuffer(record, dt).reshape(Ny, Nx)
        #         assert all(isfinite(frame))
        #         if n < offsets.size:
        #             nextBlockStart = offsets[n]
        #             startOffset = offsets[n]
        #             vfile.seek(startOffset)
        #         else:
        #             vfile.seek(self.fileSize)
        #         for chunk in splitter.iterChunks(n, frame):
        #             yield chunk
