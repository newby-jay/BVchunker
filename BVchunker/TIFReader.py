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
import BVchunker.tifffile as tif

import xml.etree.ElementTree as ET
from cStringIO import StringIO

import apache_beam as beam
from apache_beam.transforms import PTransform
from apache_beam.io import filebasedsource, ReadFromText, WriteToText, iobase
from apache_beam.io.iobase import Read
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.options.value_provider import ValueProvider
from apache_beam.options.value_provider import check_accessible


# class tiffBlob(RuntimeValueProvider):
#     def __init__(self, rvp):
#         super(tiffBlob, self).__init__(rvp.option_name, rvp.value_type, rvp.default_value)
#     def get(self):
#         if RuntimeValueProvider.runtime_options is None:
#             raise error.RuntimeValueProviderError('%s.get() not called from a runtime context' % self)
#         candidate = RuntimeValueProvider.runtime_options.get(self.option_name)
#         if candidate:
#             value = os.path.join(self.value_type(candidate), '**.tif')
#         else:
#             value = self.default_value
#         return value

class ReadFromTIFVid(PTransform):
    """A PTransform for reading TIF video files."""

    def __init__(self, path, chunkShape=None, Overlap=None, downSample=1):
        """Initializes ``ReadFromTIFVid``."""
        super(ReadFromTIFVid, self).__init__()
        if isinstance(path, basestring):
            path = StaticValueProvider(str, path)
        blob = StaticValueProvider(str, '**.tif')
        if path.is_accessible() and blob.is_accessible():
            pattern = os.path.join(path.get(), blob.get())
        else:
            pattern = ''
        self._source = _TIFSource(pattern, chunkShape, Overlap, downSample)
    def expand(self, pvalue):
        frames = pvalue.pipeline | Read(self._source) | beam.Partition(splitBadFiles, 2)
        goodFiles = frames[1] | beam.FlatMap(lambda e: [e]) | beam.CombinePerKey(combineTZ())
        return goodFiles, frames[0]

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
            assert 0 < start + 12 < len(buf)
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
            else: # in this case, we have an offset to the actual offset
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
        buf = vfile.read(self.bufferSize)
        A, entries = self.getIFD(buf, 0)
        frameMD = self.parseEntries(entries)
        numStrips, stripOffset = frameMD['stripOffsets']
        if numStrips == 1: # in this case, we live in a sane universe
            frameOffset = stripOffset
        else: # in this case, we have an offset to the actual offset
            ind = stripOffset - A
            if not 0 < ind + 4 <= len(buf):
                vfile.seek(stripOffset)
                buf = vfile.read(4)
                ind = 0
            frameOffset = self.get32int(buf[ind: ind + 4])
        return A, frameOffset, frameMD
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
        A0 = self.get32int(buf[4:8])
        vfile.seek(A0)
        buf = vfile.read(10*1024)
        B = self.get16int(buf[:2])
        if 2 + B*12 + 4 > self.bufferSize:
            self.bufferSize = 2*(2 + B*12 + 4)
        A, imgOffset, md = self.getNextOffset(A0, vfile)
        blockSize = absolute(A-A0)
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
            if imgMD['Nt'] == 1 and imgMD['Nz'] > imgMD['Nt']:
                imgMD['Nt'] = imgMD['Nz']
                imgMD['Nz'] = 1
            imgMD['dxy'] = float(pixelMD['PhysicalSizeY'])
            assert pixelMD['PhysicalSizeX'] == pixelMD['PhysicalSizeY']
            imgMD['dz'] = float(pixelMD['PhysicalSizeZ'])
            assert int(pixelMD['SizeC']) == 1
            imgMD['dxy units'] = pixelMD['PhysicalSizeYUnit']
            assert pixelMD['PhysicalSizeXUnit'] == pixelMD['PhysicalSizeYUnit']
            imgMD['dt'] = float(pixelMD['TimeIncrement'])
        elif '<MetaData' in metaDataRaw and '</MetaData>' in metaDataRaw:
            imgMD['Nt'] = int(ceil(self.fileSize/blockSize))
            #print(imgMD['Nt'], self.fileSize, imgMD['pixelSizeBytes']*Nx*Ny)
            imgMD['Nz'] = 1
            imgMD['dz'] = 1.0
            imgMD['dxy'] = 1.0
        elif 'ImageJ' in metaDataRaw:
            metaData = metaDataRaw.split()[:20]
            metaData = [m for m in metaData if '=' in m]
            metaData = dict(tuple(m.split('=')) for m in metaData)
            if 'frames' in metaData.keys():
                Nt = int(metaData['frames'])
            elif 'images' in metaData.keys():
                Nt = int(metaData['images'])
            imgMD['Nt'] = Nt
            # Nz = int(metaData['slices'])
            imgMD['Nz'] = 1
            imgMD['dz'] = 1.0
            imgMD['dxy'] = 1.0
        return A, imgOffset, imgMD
class _TIFSource(filebasedsource.FileBasedSource):
    """Read tif video into chunks."""

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
                return 0
            return iobase.RangeTracker.SPLIT_POINTS_UNKNOWN
        range_tracker.set_split_points_unclaimed_callback(split_points_unclaimed)
        ###########
        basePath, vidName = os.path.split(fileName)
        _, ext = os.path.splitext(vidName)
        startOffset = range_tracker.start_position()
        with self.open_file(fileName) as vfile:
            try:
                self._getSize(vfile)
                if self.fileSize < 1.5*1024**3:
                    vfile.seek(0)
                    mfile = StringIO()
                    mfile.write(vfile.read(self.fileSize))
                    mfile.seek(0)
                    with tif.TiffFile(mfile) as tfile:
                        vid = tfile.asarray().squeeze()
                        dim = len(vid.shape)
                        assert 3 <= dim <= 4
                        if dim == 3:
                            Nt, Ny, Nx = vid.shape
                            Nz = 1
                            imgMetadata = {'Nt': Nt, 'Ny': Ny, 'Nx': Nx, 'Nz': 1,
                                     'dxy': 1.0, 'dz': 1.0,
                                     'fileName': fileName, 'fileSize': self.fileSize}
                        else:
                            assert tfile.is_ome
                            Nt, Nz, Ny, Nx = vid.shape
                            imgMetadata = {'Nt': Nt, 'Ny': Ny, 'Nx': Nx, 'Nz': Nz,
                                     'fileName': fileName, 'fileSize': self.fileSize}
                            vid = vid.transpose(1, 0, 2, 3).reshape(-1, Ny, Nx)
                        if tfile.is_ome:
                            metadataRaw = tfile.ome_metadata
                            imgMetadata['metadataRaw'] = metadataRaw
                            pixelMD = metadataRaw['OME']['Image']['Pixels']
                            # imgMetadata['Nt'] = int(pixelMD['SizeT'])
                            # imgMetadata['Nz'] = int(pixelMD['SizeZ'])
                            if Nz == 1:
                                assert Nt == int(pixelMD['SizeT']) or Nt == int(pixelMD['SizeZ'])
                            else:
                                assert  Nt == int(pixelMD['SizeT']) or Nz == int(pixelMD['SizeZ'])
                            imgMetadata['dxy'] = float(pixelMD['PhysicalSizeY'])
                            assert pixelMD['PhysicalSizeX'] == pixelMD['PhysicalSizeY']
                            imgMetadata['dz'] = float(pixelMD['PhysicalSizeZ'])
                            assert int(pixelMD['SizeC']) == 1
                            imgMetadata['dxy units'] = pixelMD['PhysicalSizeYUnit']
                            assert pixelMD['PhysicalSizeXUnit'] == pixelMD['PhysicalSizeYUnit']
                            imgMetadata['dt'] = float(pixelMD['TimeIncrement'])
                    mfile.truncate(0)
                    splitter = self._getSplitter(imgMetadata)
                    for n, frame in enumerate(vid):
                        for chunk in splitter.iterChunks(n + 1, frame):
                            yield chunk
                else:
                    TU = _TIFutils()
                    ## check if we have an imagej file, process like nd2 if so
                    ## use a variant of the text reader otherwise
                    TU.fileSize = self.fileSize
                    A, imgOffset, imgMetadata = TU.readMetadata(vfile)
                    bytesPerPixel = imgMetadata['pixelSizeBytes']
                    assert bytesPerPixel == 1 or bytesPerPixel == 2
                    if bytesPerPixel == 1:
                        dt = dtype('uint8').newbyteorder(TU.E)
                    elif bytesPerPixel == 2:
                        dt = dtype('uint16').newbyteorder(TU.E)
                    imgMetadata['fileSize'] = self.fileSize
                    imgMetadata['fileName'] = fileName
                    shape = tuple(int(imgMetadata[k]) for k in ['Nt', 'Ny', 'Nx', 'Nz'])
                    assert all(shape > 0)
                    Nt, Ny, Nx, Nz = shape
                    frameSizeBytes = bytesPerPixel*Ny*Nx
                    splitter = self._getSplitter(imgMetadata)
                    if startOffset is None:
                        startOffset = imgOffset
                    vfile.seek(imgOffset)
                    for n in arange(imgMetadata['Nt']*imgMetadata['Nz']):
                        record = vfile.read(frameSizeBytes)
                        frame = frombuffer(record, dt).reshape(Ny, Nx)
                        assert all(isfinite(frame))
                        if n < imgMetadata['Nt']*imgMetadata['Nz'] - 1:
                            A, frameOffset, imgMD = TU.getNextOffset(A, vfile)
                            nextBlockStart = frameOffset
                            startOffset = frameOffset
                            vfile.seek(frameOffset)
                        for chunk in splitter.iterChunks(n + 1, frame):
                            yield chunk
            except:
                yield ('File Not Processed', fileName)
