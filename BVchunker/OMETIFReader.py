import os
import sys
import time
import xml.etree.ElementTree as ET

from numpy import *

from . import TIFReaderBase as tifbase

class ReadFromOMETIFVid(tifbase.ReadFromTIFBase):
    """A PTransform for reading 2D or 3D OME TIF video files."""

    def __init__(self, path, chunkShape=None, Overlap=None, downSample=1):
        """Initializes ``ReadFromOMETIFVid``."""
        super(ReadFromOMETIFVid, self).__init__(
            path,
            self.read_metadata,
            chunkShape=chunkShape,
            Overlap=Overlap,
            downSample=downSample)
    @staticmethod
    def read_metadata(imgMD):
        metaDataRaw = imgMD['raw']
        assert '<?xml' in metaDataRaw and '</OME>' in metaDataRaw
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
        imgMD['Nc'] = int(pixelMD['SizeC'])
        # assert imgMD['Nc'] == 1, 'Color not supported yet'
        imgMD['dxy units'] = pixelMD['PhysicalSizeYUnit']
        assert pixelMD['PhysicalSizeXUnit'] == pixelMD['PhysicalSizeYUnit']
        imgMD['dt'] = float(pixelMD['TimeIncrement'])
        return imgMD
