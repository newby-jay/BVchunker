from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import os
import sys
import time
import pandas as pd
from itertools import product
import xml.etree.ElementTree as ET
from cStringIO import StringIO

from numpy import *
from numpy.random import rand
from BVchunker import VideoSplitter, combineTZ, splitBadFiles
import BVchunker.tifffile_local as tif

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
        frames = pvalue.pipeline | Read(self._source) | beam.Partition(splitBadFiles, 2)
        goodFiles = frames[1] | beam.FlatMap(lambda e: [e]) | beam.CombinePerKey(combineTZ())
        return goodFiles, frames[0]

    def display_data(self):
        return {'source_dd': self._source}
class _TIFutils:
    def __init__(self):
        self.TAGS = {
            256: 'ImageWidth',
            256: 'ImageHeight',
            258: 'BitsPerSample',
            259: 'Compression',
            270: 'ImageDescription',
            273: 'StripOffsets',
            278: 'RowsPerStrip',
            279: 'StripByteCounts',
            324: 'TileOffsets'
            }
        self.TIFtypes = {
            2: 'ascii',
            3: 'short',
            4: 'long',
            5: 'rational'
            }
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
        IFD = buf[A: Q + 4]
        return Anext, entries, IFD
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
    def getNextOffset(self, A0, vfile):
        vfile.seek(A0)
        buf = vfile.read(self.bufferSize)
        A, entries, IFD = self.getIFD(buf, 0)
        frameMD = self.parseEntries(entries)
        frameMD['IFD'] = IFD
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
        imgMD['IFD'] = md['IFD']
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
            Nt = int(metaData['frames'])
            imgMD['Nt'] = Nt
            Nz = int(metaData['slices'])
            assert Nt == Nimages and Nz == 1 ## 3D not supported for this type
            imgMD['Nz'] = Nz
            # imgMD['Nz'] = 1
            # imgMD['Nt'] = Nimages
            imgMD['dz'] = 1.0
            imgMD['dxy'] = 1.0
        return A, imgOffset, imgMD
class _TIFSource(filebasedsource.FileBasedSource):
    """Read tif video into chunks."""
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
        #DEFAULT_READ_BUFFER_SIZE = 8192
        self.bufferSize = 8192
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
    def read_records(self, file_name, range_tracker):
        start_offset = range_tracker.start_position()
        read_buffer = _TIFSource.ReadBuffer('', 0)
        next_record_start_position = -1
        def split_points_unclaimed(stop_position):
            return (0 if stop_position <= next_record_start_position
                    else iobase.RangeTracker.SPLIT_POINTS_UNKNOWN)
        range_tracker.set_split_points_unclaimed_callback(split_points_unclaimed)
        with self.open_file(file_name) as file_to_read:
            self._getSize()
            TU = _TIFutils()
            TU.fileSize = self.fileSize
            position_after_processing_header_lines, _, imgMetadata = \
                      TU.readMetadata(file_to_read, read_buffer)
            self._processIFDpattern(imgMetadata['IFD'])
            start_offset = max(start_offset, position_after_processing_header_lines)
            splitter = self._getSplitter(imgMetadata)
            # if start_offset > position_after_processing_header_lines:
            #   # Seeking to one position before the start index and ignoring the
            #   # current line. If start_position is at beginning if the line, that line
            #   # belongs to the current bundle, hence ignoring that is incorrect.
            #   # Seeking to one byte before prevents that.
            #
            #   file_to_read.seek(start_offset - 1)
            #   read_buffer.reset()
            #   sep_bounds = self._find_separator_bounds(file_to_read, read_buffer)
            #   if not sep_bounds:
            #     # Could not find a separator after (start_offset - 1). This means that
            #     # none of the records within the file belongs to the current source.
            #     return
            #
            #   _, sep_end = sep_bounds
            #   read_buffer.data = read_buffer.data[sep_end:]
            #   next_record_start_position = start_offset - 1 + sep_end
            # else:
            next_record_start_position = position_after_processing_header_lines

            while range_tracker.try_claim(next_record_start_position):
                record, num_bytes_to_next_record = self._read_record(file_to_read,
                                                                     read_buffer)
                ########
                ########
                ## This needs to know the absolute position in the file
                frame = self._getFrameFromRecord(next_record_start_position, record)
                ########
                ########

                # Record separator must be larger than zero bytes.
                assert num_bytes_to_next_record != 0
                if num_bytes_to_next_record > 0:
                    next_record_start_position += num_bytes_to_next_record
                #############
                #############
                ### Major issue: how do we figure out the frame number "n"
                for chunk in splitter.iterChunks(n + 1, frame):
                    yield chunk
                #############
                #############
                if num_bytes_to_next_record < 0:
                    break
    def _processIFDpattern(self, IFD):
        self.pattern = pattern
    def _getFrameFromRecord(self, startOffset, IFDoffset, record):
        return frame
    def _checkIFDpattern(self, read_buffer, current_pos):
        """Checks for a pattern indicating a valid IFD. Returns the offset bounds."""
        next_lf = read_buffer.data.find('\n', current_pos)
        #next_rf needs to be the right bound of the IFD
        return isFound, (next_lf, next_rf)
    def _find_separator_bounds(self, file_to_read, read_buffer):
        # Determines the start and end positions within 'read_buffer.data' of the
        # next separator starting from position 'read_buffer.position'.
        # This method may increase the size of buffer but it will not decrease the
        # size of it.
        current_pos = read_buffer.position
        while True:
            if current_pos >= len(read_buffer.data):
                # Ensuring that there are enough bytes to determine if there is a '\n'
                # at current_pos.
                if not self._try_to_ensure_num_bytes_in_buffer(
                                    file_to_read, read_buffer, current_pos + 1):
                    return
            # Using find() here is more efficient than a linear scan of the byte
            # array.
            isFound, nextRecordBounds = self._checkIFDpattern(read_buffer, current_pos)
            if isFound:
                return nextRecordBounds
            current_pos = len(read_buffer.data)
    def _try_to_ensure_num_bytes_in_buffer(self, file_to_read, read_buffer, num_bytes):
        # Tries to ensure that there are at least num_bytes bytes in the buffer.
        # Returns True if this can be fulfilled, returned False if this cannot be
        # fulfilled due to reaching EOF.
        while len(read_buffer.data) < num_bytes:
            read_data = file_to_read.read(self.bufferSize)
            if not read_data:
                return False

            read_buffer.data += read_data
        return True
    def _read_record(self, file_to_read, read_buffer):
        # Returns a tuple containing the current_record and number of bytes to the
        # next record starting from 'read_buffer.position'. If EOF is
        # reached, returns a tuple containing the current record and -1.
        if read_buffer.position > self.bufferSize:
            # read_buffer is too large. Truncating and adjusting it.
            read_buffer.data = read_buffer.data[read_buffer.position:]
            read_buffer.position = 0
        record_start_position_in_buffer = read_buffer.position
        sep_bounds = self._find_separator_bounds(file_to_read, read_buffer)
        read_buffer.position = sep_bounds[1] if sep_bounds else len(read_buffer.data)
        if not sep_bounds:
            # Reached EOF. Bytes up to the EOF is the next record. Returning '-1' for
            # the starting position of the next record.
            return (read_buffer.data[record_start_position_in_buffer:], -1)
        if self._strip_trailing_newlines:
            # Current record should not contain the separator.
            return (read_buffer.data[record_start_position_in_buffer:sep_bounds[0]],
                    sep_bounds[1] - record_start_position_in_buffer)
        else:
            # Current record should contain the separator.
            return (read_buffer.data[record_start_position_in_buffer:sep_bounds[1]],
                    sep_bounds[1] - record_start_position_in_buffer)






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
