from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import os
import sys
import time
from cStringIO import StringIO
import re

from numpy import *
from BVchunker import VideoSplitter, combineTZ
from google.cloud import error_reporting, logging

import apache_beam as beam
from apache_beam.transforms import PTransform
from apache_beam.io import filebasedsource, iobase
from apache_beam.io.iobase import Read

class TagFrames(beam.DoFn):
    """Attach output tags for frames, offset data, and read errors."""

    def __init__(self):
        pass
    def process(self, KVelement):
        key, element = KVelement
        if key == 'offset':
            yield beam.pvalue.TaggedOutput('offsets', element)
        elif key == 'frame':
            yield beam.pvalue.TaggedOutput('frames', element)
        elif key == 'readError':
            yield beam.pvalue.TaggedOutput('readError', element)
        else:
            raise

class FramesToChunks(beam.DoFn):
    """Break frames into chunks."""

    def __init__(self, chunkShape=None, Overlap=None, downSample=1):
        self.chunkShape = chunkShape
        self.Overlap = Overlap
        self.downSample = downSample
    def _getSplitter(self, imgMetadata):
        if all(self.chunkShape == None) and all(self.Overlap == None):
            splitter = VideoSplitter(imgMetadata, downSample=self.downSample)
        elif all(self.chunkShape != None) and all(self.Overlap == None):
            splitter = VideoSplitter(
                imgMetadata,
                chunkShape=self.chunkShape,
                downSample=self.downSample)
        elif all(self.chunkShape == None) and all(self.Overlap != None):
            splitter = VideoSplitter(
                imgMetadata,
                Overlap=self.Overlap,
                downSample=self.downSample)
        else:
            splitter = VideoSplitter(
                imgMetadata,
                Overlap=self.Overlap,
                chunkShape=self.chunkShape,
                downSample=self.downSample)
        return splitter
    def process(self, KVelement, offsetData):
        try:
            key, element = KVelement
            imgMetadata = element['imgMetadata']
            fileName = imgMetadata['fileName']
            if not 'Nt' in imgMetadata:
                assert imgMetadata['Nz'] == 1
                assert offsetData[key].size > 0
                imgMetadata['Nt'] = offsetData[key].size
            else:
                assert 'Nz' in imgMetadata
                Nframes = imgMetadata['Nt']*imgMetadata['Nz']
                assert Nframes == offsetData[key].size
            splitter = self._getSplitter(imgMetadata)
            IFD = element['IFD']
            sortedIFDs = offsetData[key]
            assert IFD in sortedIFDs
            n = searchsorted(sortedIFDs, IFD) + 1
            for chunk in splitter.iterChunks(n, element['frame']):
                yield beam.pvalue.TaggedOutput('chunks', chunk)
        except:
            client = error_reporting.Client()
            client.report('File Not Processed: ' + fileName)
            client.report_exception()

            logging_client = logging.Client()
            log_name = 'aits-pipeline-leen'
            logger = logging_client.logger(log_name)
            logmessage = {
                'Error': 'File cannot be read',
                'Filename': fileName
            }
            logger.log_struct(logmessage)
            yield beam.pvalue.TaggedOutput(
                'readError',
                ('File Not Processed', fileName))

class CombineOffsets(beam.CombineFn):
    """Combine and process offset data."""

    def __init__(self):
        pass
    def create_accumulator(self):
        return {}
    def add_input(self, A, KVelement):
        key, element = KVelement
        if key in A:
            A[key].append(element)
        else:
            A[key] = [element]
        return A
    def merge_accumulators(self, accumulators):
        Aout = self.create_accumulator()
        for A in accumulators:
            for key in A:
                if key in Aout:
                    Aout[key].extend(A[key])
                else:
                    Aout[key] = A[key]
        return Aout
    def extract_output(self, A):
        out = {}
        for key in A:
            oData = int64(A[key])
            if oData.size == 0:
                out[key] = int64([])
            inds = oData[:, 0].argsort()
            IFDs = oData[:, 0][inds]
            nextIFDs = oData[:, 1][inds]
            if all(diff(IFDs) > 0) and all(IFDs[1:] == nextIFDs[:-1]):
                out[key] = IFDs
            else:
                out[key] = int64([])
        return out

class ReadFromTIFBase(PTransform):
    """Base class for reading TIF video files."""

    def __init__(self,
        pattern,
        metadata_decoder,
        chunkShape=None,
        Overlap=None,
        downSample=1):
        """Initializes ``ReadFromTIFBase``."""
        super(ReadFromTIFBase, self).__init__()
        self._source = _TIFSource(pattern, metadata_decoder)
        self.chunkShape = chunkShape
        self.Overlap = Overlap
        self.downSample = downSample

    def expand(self, pvalue):
        frames = (
            pvalue.pipeline
                | Read(self._source)
                | beam.ParDo(TagFrames()).with_outputs()
           )
        offsetMap = (
            frames.offsets
                | beam.CombineGlobally(CombineOffsets())
        )
        chunks = (
            frames.frames
                | beam.ParDo(
                    FramesToChunks(
                        chunkShape=self.chunkShape,
                        Overlap=self.Overlap,
                        downSample=self.downSample),
                    beam.pvalue.AsSingleton(offsetMap)
                ).with_outputs()
        )
        chunksCombined = (chunks.chunks | beam.CombinePerKey(combineTZ()))
        return chunksCombined
    def display_data(self):
        return {'source_dd': self._source}

class _TIFutils:
    def __init__(self, fileSize):
        self.TAGS = {
            256: 'ImageWidth',
            256: 'ImageHeight',
            258: 'BitsPerSample',
            259: 'Compression',
            270: 'ImageDescription',
            273: 'StripOffsets',
            278: 'RowsPerStrip',
            279: 'StripByteCounts',
            324: 'TileOffsets'}
        self.TIFtypes = {
            #2: dtype('string'),
            3: dtype('uint16'),
            4: dtype('uint32'),
            #5: dtype('float32'),
            16: dtype('uint64'),
            17: dtype('int64'),
            18: dtype('uint64')}
        self.fileSize = fileSize
        self.markerSize = 0
        self.IFD_RE = ''
        self.BO = None
        self.intTypeB = None
        self.intTypeC = None
        self.rs = None
        self.tagSize = None# 12, 20
        self.tagHead = None# 2, 8
    def _set_byte_order(self, BO, size):
        self.BO = BO
        self.rs = size
        self.TIFtypes = dict(
            (k, self.TIFtypes[k].newbyteorder(self.BO))
            for k in self.TIFtypes)
        if size == 4:
            self.intTypeB = dtype('uint16').newbyteorder(self.BO)
            self.intTypeC = dtype('uint32').newbyteorder(self.BO)
            self.tagHead = 2
            self.tagSize = 12
        else: # BIGTIF
            self.intTypeB = dtype('uint64').newbyteorder(self.BO)
            self.intTypeC = dtype('uint64').newbyteorder(self.BO)
            self.tagHead = 8
            self.tagSize = 20
    def decodeEntry(self, b, dType):
        s = dType.itemsize
        return int(frombuffer(b[:s], dType))
    def get_uint16(self, b):
        return self.decodeEntry(b, self.TIFtypes[3])
    def get_intB(self, b):
        return self.decodeEntry(b, self.intTypeB)
    def get_intC(self, b):
        return self.decodeEntry(b, self.intTypeC)
    def getIFD(self, buf, A):
        assert 0 < A + self.tagHead <= len(buf)
        Ntags = self.get_intB(buf[A:])
        entries = []
        for n in arange(Ntags):
            start = A + self.tagHead + n*self.tagSize
            assert 0 < start + self.tagSize <= len(buf)
            entries.append(buf[start:start + self.tagSize])
        Q = A + self.tagHead + Ntags*self.tagSize
        assert 0 < Q + self.rs <= len(buf)
        Anext = self.get_intC(buf[Q:])
        return Anext, entries
    def parseEntries(self, entries):
        output = {}
        tifMD = {}
        for e in entries:
            TAG = self.get_uint16(e)
            tifType = self.get_uint16(e[2:])
            N = self.get_intC(e[4:])
            x = 4 + self.rs
            if tifType in [3, 4, 16, 18, 17]:
                TAGvalue = self.decodeEntry(e[x:], self.TIFtypes[tifType])
            elif self.rs == 4:
                TAGvalue = self.decodeEntry(e[x:], self.TIFtypes[4])
            else:
                TAGvalue = self.decodeEntry(e[x:], self.TIFtypes[16])
            tifMD[TAG] = (tifType, N, TAGvalue)
        output['Ny'] = tifMD[257][2]
        output['Nx'] = tifMD[256][2]
        output['mdOffset'] = (tifMD[270][2], tifMD[270][1])
        output['pixelSizeBytes'] = int(tifMD[258][2]/8)
        output['stripOffsets'] = tifMD[273][1:]
        if 259 in tifMD.keys():
            assert tifMD[259][2] == 1, 'Compression is not supported'
        return output
    def getNextOffset(self, vfile):
        A0 = vfile.tell()
        assert 0 < A0 <= self.fileSize - self.markerSize
        buf = vfile.read(self.markerSize)
        A, entries = self.getIFD(buf, 0)
        if A == 0:
            A = self.fileSize
        assert A0 < A
        frameMD = self.parseEntries(entries)
        numStrips, stripOffset = frameMD['stripOffsets']
        if numStrips == 1:
            frameOffset = stripOffset
        else:
            ind = stripOffset - A
            if not 0 < ind + self.rs <= len(buf):
                assert 0 < stripOffset <= self.fileSize - self.rs
                vfile.seek(stripOffset)
                buf = vfile.read(self.rs)
                vfile.seek(A0 + self.markerSize)
                ind = 0
            frameOffset = self.get_intC(buf[ind:])
        if not vfile.tell() == A0 + self.markerSize:
            vfile.seek(A0 + self.markerSize)
        return A0, A, frameOffset, frameMD
    def _formatBytes(self, bytes):
        return '({0})'.format(re.escape(bytes))
    def _makeIFDre(self, IFDbytes, Ntags):
        assert self.markerSize > 0
        fixedTags = [256, 257, 258, 259]
        Wcard = r'(.{'+str(self.rs)+'})'
        RElist = []
        RElist.append('({0})'.format(re.escape(IFDbytes[:self.tagHead])))
        for n in arange(Ntags):
            start = self.tagHead + n*self.tagSize
            tagBytes = IFDbytes[start:start + 2]
            tifTypeBytes = IFDbytes[start + 2:start + 4]
            tagSizeBytes = IFDbytes[start + 4:start + 4 + self.rs]
            tagValueBytes = IFDbytes[start + 4 + self.rs:start + 4 + 2*self.rs]
            TAGlist = []
            TAGlist.append(self._formatBytes(tagBytes))
            TAGlist.append(self._formatBytes(tifTypeBytes))
            tag = self.get_uint16(tagBytes)
            if tag in fixedTags:
                TAGlist.append(self._formatBytes(tagSizeBytes))
                TAGlist.append(self._formatBytes(tagValueBytes))
            else:
                TAGlist.append(Wcard)
                TAGlist.append(Wcard)
            RElist.append(TAGlist)
        RElist.append(Wcard)
        self.IFD_RE = ''.join([''.join(el) for el in RElist])
    def findNextIFD(self, vfile):
        bufferSize = 16 * 1024 * 1024
        assert bufferSize > self.markerSize
        assert len(self.IFD_RE) > 0
        startOffset = vfile.tell()
        if startOffset >= self.fileSize:
            return
        offset = startOffset
        buf = vfile.read(bufferSize)
        match = re.search(self.IFD_RE, buf, flags=re.DOTALL)
        overlapCorrection = 0
        while match is None:
            buf = buf[-self.markerSize - 1:] + vfile.read(bufferSize)
            offset += bufferSize
            overlapCorrection = self.markerSize + 1
            if vfile.tell() >= self.fileSize:
                return
        groups = match.groups()
        IFD = offset + match.start() - overlapCorrection
        nextIFD = self.get_intC(groups[-1])
        # if nextIFD <= IFD: # False positive for IFD
        #     vfile.seek(IFD + self.markerSize)
        #     self.findNextIFD(vfile)
        #     return
        # assert startOffset <= IFD < nextIFD - self.markerSize
        # vfile.seek(nextIFD)
        # buf = vfile.read(self.markerSize)
        # match = re.search(self.IFD_RE, buf)
        # if match is None: # False positive for IFD
        #     vfile.seek(IFD + self.markerSize)
        #     self.findNextIFD(vfile)
        #     return
        # else:
        vfile.seek(IFD)
        return
    def _initHeader(self, buf):
        if buf[:2] == 'II' and buf[2:4] == '*\x00':
            self._set_byte_order('<', 4)
        elif buf[:2] == 'MM' and buf[2:4] == '\x00*':
            self._set_byte_order('>', 4)
        elif buf[:2] == 'II' and buf[2:4] == '+\x00':
            self._set_byte_order('<', 8)
        elif buf[:2] == 'MM' and buf[2:4] == '\x00+':
            self._set_byte_order('>', 8)
        else:
            raise
    def readHeader(self, vfile):
        vfile.seek(0)
        assert self.fileSize > 16
        buf = vfile.read(16)
        self._initHeader(buf)
        A0 = self.get_intC(buf[self.rs:])
        assert 0 < A0 <= self.fileSize - 10*1024
        vfile.seek(A0)
        buf = vfile.read(10*1024)
        Ntags = self.get_intB(buf)
        self.markerSize = self.tagHead + Ntags*self.tagSize + self.rs
        vfile.seek(A0)
        IFDbytes = vfile.read(self.markerSize)
        self._makeIFDre(IFDbytes, Ntags)
        vfile.seek(A0)
        IFD, nextIFD, imgOffset, md = self.getNextOffset(vfile)
        assert A0 < nextIFD
        Ny = md['Ny']
        Nx = md['Nx']
        imgMD = {'Ny': Ny, 'Nx': Nx}
        imgMD['fileSize'] = self.fileSize
        imgMD['pixelSizeBytes'] = md['pixelSizeBytes']
        mdOffset, mdSize = md['mdOffset']
        assert 0 < mdOffset <= self.fileSize - mdSize
        vfile.seek(mdOffset)
        assert mdSize < 100*1024**2
        imgMD['raw'] = vfile.read(mdSize - 1)
        vfile.seek(0)
        return A0, nextIFD, imgOffset, imgMD

class _TIFSource(filebasedsource.FileBasedSource):
    """Read TIF video into chunks."""

    def __init__(self, file_pattern, metadata_decoder):
        super(_TIFSource, self).__init__(
            file_pattern,
            splittable=True,
            min_bundle_size=0,
            validate=False)
        self.decode_metadata = metadata_decoder
    def _getSize(self, vfile):
        try:
            return vfile.size
        except:
            vfile.seek(0, 2)
            return vfile.tell()
    def read_records(self, fileName, range_tracker):
        next_block_start = -1
        def split_points_unclaimed(stopPosition):
            if stopPosition <= next_block_start:
                return 0
            return iobase.RangeTracker.SPLIT_POINTS_UNKNOWN
        range_tracker.set_split_points_unclaimed_callback(split_points_unclaimed)
        ###########
        basePath, vidName = os.path.split(fileName)
        _, ext = os.path.splitext(vidName)
        startOffset = range_tracker.start_position()
        if startOffset is None:
            startOffset = 0
        with self.open_file(fileName) as vfile:
            try:
                fileSize = self._getSize(vfile)
                TU = _TIFutils(fileSize)
                firstIFD, nextIFD, imgOffset, imgMetadata = TU.readHeader(vfile) # leave vfile at 0
                imgMetadata = self.decode_metadata(imgMetadata)
                IFD = firstIFD
                markerSize = TU.markerSize
                bytesPerPixel = imgMetadata['pixelSizeBytes']
                assert bytesPerPixel == 1 or bytesPerPixel == 2
                if bytesPerPixel == 1:
                    dType = dtype('uint8').newbyteorder(TU.BO)
                elif bytesPerPixel == 2:
                    dType = dtype('uint16').newbyteorder(TU.BO)
                imgMetadata['fileSize'] = fileSize
                imgMetadata['fileName'] = fileName
                shape = tuple(int(imgMetadata[k]) for k in ['Ny', 'Nx', 'Nz'])
                assert all(shape > 0)
                Ny, Nx, Nz = shape
                assert Nx < 2048*10 and Ny < 2048*10
                Nc = imgMetadata['Nc']
                frameSizeBytes = bytesPerPixel*Ny*Nx*Nc
                if startOffset > firstIFD + markerSize:
                    vfile.seek(startOffset - markerSize)
                    TU.findNextIFD(vfile) # leave vfile at begining of IFD
                    sIFD = vfile.tell()
                    if vfile.tell() <= fileSize - markerSize:
                        IFD, nextIFD, imgOffset, imgMD = TU.getNextOffset(vfile) # leave vfile at end of IFD
                        assert sIFD == IFD
                        assert vfile.tell() == IFD + markerSize
                    else:
                        vfile.seek(fileSize)
                next_block_start = nextIFD + markerSize
                while range_tracker.try_claim(vfile.tell()):
                    if vfile.tell() >= fileSize:
                        break
                    assert imgOffset < fileSize - frameSizeBytes
                    vfile.seek(imgOffset)
                    record = vfile.read(frameSizeBytes)
                    if Nc > 1:
                        assert bytesPerPixel == 1, 'Color images must be 8 bit'
                        cframe = frombuffer(record, dType).reshape(Ny, Nx, Nc)
                        cframe = float64(cframe).sum(axis=2)
                        cframe -= data.min()
                        frame = uint16(cframe)
                    else:
                        frame = frombuffer(record, dType).reshape(Ny, Nx)
                    assert all(isfinite(frame))
                    yield ('offset', (fileName, (IFD, nextIFD)))
                    frameData = {
                        'frame': frame,
                        'IFD': IFD,
                        'imgMetadata': imgMetadata}
                    yield ('frame', (fileName, frameData))
                    if 0 < nextIFD <= fileSize - markerSize:
                        vfile.seek(nextIFD)
                        IFD, nextIFD, imgOffset, imgMD = TU.getNextOffset(vfile) # leave vfile at end of IFD
                        next_block_start = nextIFD + markerSize
                        assert imgMD['Nx'] == Nx
                        assert imgMD['Ny'] == Ny
                        assert imgMD['pixelSizeBytes'] == bytesPerPixel
                    else:
                        vfile.seek(fileSize)
                        break
            except:
                client = error_reporting.Client()
                client.report('File Not Processed: ' + fileName)
                client.report_exception()

                logging_client = logging.Client()
                log_name = 'aits-pipeline-leen'
                logger = logging_client.logger(log_name)
                logmessage = {
                    'Error': 'File cannot be read',
                    'Filename': fileName
                }
                logger.log_struct(logmessage)
                yield ('readError', ('File Not Processed', fileName))
