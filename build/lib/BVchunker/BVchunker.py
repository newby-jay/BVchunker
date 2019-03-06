from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
import os
import sys
import time
import tempfile
from cStringIO import StringIO
import json

from numpy import *
from itertools import product
import pandas as pd
# import google.cloud
# from google.cloud import logging

import apache_beam as beam
from apache_beam.transforms import PTransform
from apache_beam.io import filebasedsource, ReadFromText, WriteToText, iobase
from apache_beam.io.iobase import Read

class VideoSplitter:
    """Break a multidimensional array into chunks of
    size `chunkShape`, with overlap size of size
    `Overlap`. Default output is to split all
    image frames, individually."""

    def __init__(self, imgMetadata,
                 chunkShape=[1, inf, inf, 1],
                 Overlap=[0, 0, 0, 0],
                 downSample=1):
        # downSample = 2
        chunkShape = array(chunkShape)
        Overlap = int64(Overlap)
        self.downSample = int(downSample)
        assert downSample > 0
        self.imgMetadata = imgMetadata.copy()
        assert all([key in self.imgMetadata.keys() for key in
            ['Nt', 'Ny', 'Nx', 'Nz', 'fileName', 'fileSize', 'dxy', 'dz']])
        assert self.imgMetadata['dxy'] > 0
        assert self.imgMetadata['dz'] > 0
        assert self.imgMetadata['fileSize'] > 0
        assert len(self.imgMetadata['fileName']) > 0
        self.imgMetadata['Ny'] = int(self.imgMetadata['Ny']/self.downSample)
        self.imgMetadata['Nx'] = int(self.imgMetadata['Nx']/self.downSample)
        self.imgMetadata['dxy'] *= float(self.downSample)
        vidShape = int64([
            self.imgMetadata['Nt'],
            self.imgMetadata['Ny'],
            self.imgMetadata['Nx'],
            self.imgMetadata['Nz']])
        for n in arange(4):
            if vidShape[n] <= chunkShape[n]:
                chunkShape[n] = vidShape[n]
                Overlap[n] = 0
        chunkShape = int64(chunkShape)
        assert vidShape.size == 4
        assert chunkShape.size == 4
        assert Overlap.size == 4
        assert all(vidShape > 0)
        assert all(chunkShape > 0)
        assert all(Overlap >= 0)
        self.vidShape = int64(vidShape)
        self.chunkShape = int64(chunkShape)
        self.Overlap = int64(Overlap)
        self.Nchunks = int64(vidShape/chunkShape) + (vidShape % chunkShape > 0)
        padTotal = (chunkShape - (vidShape % chunkShape)) % chunkShape
        self.Lpad = int64(padTotal/2) + int64(padTotal % 2)
        self.Rpad = int64(padTotal/2)
    def getRange(self, nChunk):
        GlobalRange = []
        for n in [0, 3]:
            ol = self.Overlap[n]
            Lpad = self.Lpad[n]
            Rpad = self.Rpad[n]
            t = arange(self.vidShape[n])
            padLeft = t[1:Lpad+ol+1][::-1]
            padRight = t[-Rpad-ol-1:-1][::-1]
            tpadded = concatenate([padLeft, t, padRight])
            low = nChunk[n]*self.chunkShape[n]
            high = (nChunk[n] + 1)*self.chunkShape[n]
            tsec = tpadded[low : high + 2*ol]
            GlobalRange.append((tsec.min(), tsec.max()))
        return array(GlobalRange).ravel()
    def getGuide(self, nChunk):
        validLocal = zeros((4, 2), 'int')
        validGlobal = zeros((4, 2), 'int')
        inds = []
        for n in arange(4):
            ol = self.Overlap[n]
            Lpad = self.Lpad[n]
            Rpad = self.Rpad[n]
            t = arange(self.vidShape[n])
            padLeft = t[1:Lpad+ol+1][::-1]
            padRight = t[-Rpad-ol-1:-1][::-1]
            tpadded = concatenate([padLeft, t, padRight])
            tForGuides = concatenate([-ones(Lpad + ol), t, -ones(Rpad+ ol)])
            low = nChunk[n]*self.chunkShape[n]
            high = (nChunk[n] + 1)*self.chunkShape[n]
            tsec = tpadded[low : high + 2*ol]
            inds.append(tsec)
            tg = tForGuides[low : high + 2*ol]
            if ol > 0:
                tg[:ol] = -1
                tg[-ol:] = -1
            local = arange(tsec.size)[tg >= 0]
            validLocal[n] = (local[0], local[-1] + 1)
            validGlobal[n] = (tsec[local[0]], tsec[local[-1]] + 1)
        guide = {'t': inds[0], 'y': inds[1], 'x': inds[2], 'z': inds[3]}
        valid = {'validLocal': validLocal, 'validGlobal': validGlobal}
        return guide, valid
    def iterChunks(self, n, frame):
        if self.downSample > 1:
            frame = frame[::self.downSample, ::self.downSample]
        Nz = self.vidShape[3]
        z = (n-1) % Nz
        t = int((n-1)/Nz)
        frameMean = mean(frame)
        frameVar = mean(float64(frame)**2)
        for ny, nx in product(arange(self.Nchunks[1]), arange(self.Nchunks[2])):
            guide, _ = self.getGuide((0, ny, nx, 0))
            X, Y = meshgrid(guide['x'], guide['y'])
            frameChunk = frame[Y, X]
            for nt, nz in product(
                arange(self.Nchunks[0]),
                arange(self.Nchunks[3])):
                tA, tB, zA, zB = self.getRange((nt, ny, nx, nz))
                assert tA <= tB and zA <= zB
                if not tA <= t <= tB or not zA <= z <= zB:
                    continue
                guide, valid = self.getGuide((nt, ny, nx, nz))
                metadata = {
                    'validLocal': valid['validLocal'],
                    'validGlobal': valid['validGlobal'],
                    'chunkShape': self.chunkShape,
                    'Overlap': self.Overlap,
                    'vidShape': self.vidShape,
                    'chunkIndex': (nt, ny, nx, nz)}
                for k in self.imgMetadata:
                    metadata[k] = self.imgMetadata[k]
                assert guide['t'].size == self.chunkShape[0] + 2*self.Overlap[0]
                indst = arange(guide['t'].size)[t == guide['t']]
                indsz = arange(guide['z'].size)[z == guide['z']]
                key = metadata['fileName']
                key += '-{0}-{1}-{2}-{3}'.format(nt, ny, nx, nz)
                for localt, localz in product(indst, indsz):
                    output = {
                        't': localt,
                        'z': localz,
                        'frame': frameChunk,
                        'stats': (frameMean, frameVar),
                        'metadata': metadata}
                    yield (key, output)

class combineTZ(beam.CombineFn):
    """Combines a collection of 2D image frames into 4D TYXZ arrays."""

    def __init__(self):
        pass
    def create_accumulator(self):
        vid = zeros((0, 0, 0, 0), 'uint16')
        metadata = {}
        t = zeros((0), 'int')
        z = zeros((0), 'int')
        stats = zeros((0, 2), 'float64')
        return t, z, metadata, stats, vid
    def add_input(self, A, element):
        At, Az, Ametadata, Astats, Avid = A
        metadata = element['metadata']
        if len(Ametadata) == 0:
            Ametadata = metadata
        # if metadata['stats'] != None:
        #     Ametadata = metadata
        #     return At, Az, Ametadata, Avid
        Ny, Nx = element['frame'].squeeze().shape
        frame = element['frame'].reshape(1, Ny, Nx)
        At = concatenate([At, [element['t']]], 0)
        Az = concatenate([Az, [element['z']]], 0)
        stats = array([element['stats']])
        Astats = concatenate([Astats, stats], 0)
        if Avid.size == 0:
            Avid = frame
        else:
            Avid = concatenate([Avid, frame], 0)
        return At, Az, Ametadata, Astats, Avid
    def merge_accumulators(self, accumulators):
        t, z, metadata, stats, vid = self.create_accumulator()
        for At, Az, Ametadata, Astats, Avid in accumulators:
            t = concatenate([t, At], 0)
            z = concatenate([z, Az], 0)
            stats = concatenate([stats, Astats], 0)
            if len(Ametadata) > 0:
                metadata = Ametadata
            if vid.size == 0:
                vid = Avid
            else:
                vid = concatenate([vid, Avid], 0)
        return t, z, metadata, stats, vid
    def extract_output(self, A):
        t, z, metadata, stats, vid = A
        shape = array(metadata['chunkShape']) + 2*array(metadata['Overlap'])
        assert t.size == z.size
        Nt, Ny, Nx, Nz = shape
        assert all((Ny, Nx) == vid[0].shape)
        n = t*Nz + z
        inds = n.argsort()
        assert all(diff(n[inds]) <= 1) # check if missing frames
        assert unique(n).size == n.size # check if duplicates
        assert n.size == Nt*Nz
        outVid = vid[inds].reshape(Nt, Nz, Ny, Nx).transpose(0, 2, 3, 1)
        stats = stats[inds].reshape(Nt, Nz, 2)
        return {
            'videoData': outVid,
            'stats': stats,
            'metadata': metadata}

class remapPoints(beam.DoFn):
    """Remap points in a DataFrame from local chunk
    coordinates to global video coordinates."""

    def __init__(self):
        pass
    def process(self, KVelement):
        key, element = KVelement
        md = element['metadata']
        PS = element['pointSet']
        tA, tB = md['validLocal'][0]
        yA, yB = md['validLocal'][1]
        xA, xB = md['validLocal'][2]
        zA, zB = md['validLocal'][3]
        newPS = PS[(PS.t >= tA)&(PS.t < tB)
                  &(PS.y >= yA)&(PS.y < yB)
                  &(PS.x >= xA)&(PS.x < xB)
                  &(PS.z >= zA)&(PS.z < zB)]
        dt, dy, dx, dz = md['validGlobal'][:, 0]
        newPS.t += dt - tA
        newPS.y += dy - yA
        newPS.x += dx - xA
        newPS.z += dz - zA
        Nt, Ny, Nx, Nz = md['vidShape']
        outPS = newPS[(newPS.t >= 0)&(newPS.t <= Nt - 1)
                     &(newPS.y >  0)&(newPS.y <  Ny - 1)
                     &(newPS.x >  0)&(newPS.x <  Nx - 1)
                     &(newPS.z >= 0)&(newPS.z <= Nz - 1)]
        mdout = dict((k, v) for k, v in md.iteritems()
                    if k not in ['validLocal', 'validGlobal', 'chunkIndex'])
        output = {'pointSet': outPS,
                  'metadata': mdout}
        newKey = md['fileName']
        yield (newKey, output)

class combinePointset(beam.CombineFn):
    """Combine a collection of Pandas DataFrames
    containing points into a single DataFrame."""

    def __init__(self, columns=['x', 'y', 'z', 't']):
        self.columns = columns
    def create_accumulator(self):
        PS = zeros((0, len(self.columns)))
        return PS, {}
    def add_input(self, A, element):
        APS, Amd = A
        md = element['metadata']
        newPSdata = array(element['pointSet'][self.columns])
        APScombined = concatenate([APS, newPSdata], 0)
        return APScombined, md
    def merge_accumulators(self, accumulators):
        PS, md = self.create_accumulator()
        for APS, Amd in accumulators:
            PS = concatenate([PS, APS], 0)
            if len(md.keys()) == 0:
                md = Amd
        return PS, md
    def extract_output(self, A):
        PS, md = A
        mdout = dict((k, md[k]) for k in md if k != 'slice')
        return {'pointSet': pd.DataFrame(PS, columns=self.columns),
                'metadata': mdout}

class stripChunks(beam.DoFn):

    def __init__(self):
        pass
    def process(self, KVelement):
        key, element = KVelement
        yield (element['metadata']['fileName'], element)

class combineStats(beam.CombineFn):
    """Reduce video chunks and extract mean and standard deviation.
    Intended for testing purposes."""

    def __init__(self):
        pass
    def create_accumulator(self):
        return 0.0, 0.0, 0, 0
    def add_input(self, A, element):
        Am, As, Ac, Af = A
        frame = float64(element['videoData'])
        shape = frame.shape
        Am += frame.sum()
        As += (frame**2).sum()
        Ac += frame.size
        Af += shape[0]*shape[3]
        return Am, As, Ac, Af
    def merge_accumulators(self, accumulators):
        m, s, c, f = self.create_accumulator()
        for Am, As, Ac, Af in accumulators:
            m += Am
            s += As
            c += Ac
            f += Af
        return m, s, c, f
    def extract_output(self, A):
        m, s, c, f = A
        out = {'mean': m/c,
               'std': sqrt(s/c - (m/c)**2),
               'count': c,
               'Nframes': f}
        return out

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ndarray):
            return obj.tolist()
        if isinstance(obj, pd.DataFrame):
            return obj.to_dict()
        return json.JSONEncoder.default(self, obj)

class toJSON(beam.DoFn):

    def __init__(self):
        pass
    def process(self, element):
        # yield json.dumps(
        #     {element[0]: element[1]},
        #     cls=NumpyEncoder)
        yield pd.io.json.dumps({element[0]: element[1]})

def splitBadFiles(KVelement, N):
    key, element = KVelement
    return int(key != 'File Not Processed')
