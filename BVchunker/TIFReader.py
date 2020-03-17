import os
import sys
import time

from numpy import *

from . import TIFReaderBase as tifbase

class ReadFrom2DTIFVid(tifbase.ReadFromTIFBase):
    """A PTransform for reading generic 2D TIF video files."""

    def __init__(self, path, chunkShape=None, Overlap=None, downSample=1):
        """Initializes ``ReadFrom2DTIFVid``."""
        super(ReadFrom2DTIFVid, self).__init__(
            path,
            self.read_metadata,
            chunkShape=chunkShape,
            Overlap=Overlap,
            downSample=downSample)
    @staticmethod
    def read_metadata(imgMD):
        imgMD['Nz'] = 1
        imgMD['dxy'] = 1.
        imgMD['dz'] = 1.
        imgMD['Nc'] = 1
        imgMD['dt'] = 1.
        return imgMD
