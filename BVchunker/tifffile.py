#! /usr/bin/python3
# -*- coding: utf-8 -*-
# tifffile.py

# Copyright (c) 2008-2017, Christoph Gohlke
# Copyright (c) 2008-2017, The Regents of the University of California
# Produced at the Laboratory for Fluorescence Dynamics
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright
#   notice, this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright
#   notice, this list of conditions and the following disclaimer in the
#   documentation and/or other materials provided with the distribution.
# * Neither the name of the copyright holders nor the names of any
#   contributors may be used to endorse or promote products derived
#   from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

"""Read image and meta data from (bio) TIFF® files. Save numpy arrays as TIFF.

Image and metadata can be read from TIFF, BigTIFF, OME-TIFF, STK, LSM, NIH,
SGI, ImageJ, MicroManager, FluoView, SEQ and GEL files.

Tifffile is not a general purpose TIFF library. Only a subset of the TIFF
specification is supported, mainly uncompressed and losslessly compressed
2**(0 to 6) bit integer, 16, 32 and 64-bit float, grayscale and RGB(A) images,
which are commonly used in bio-scientific imaging. Specifically, reading image
trees defined via SubIFDs, JPEG and CCITT compression, chroma subsampling,
or IPTC and XMP metadata are not implemented.

TIFF®, the tagged Image File Format, is a trademark and under control of
Adobe Systems Incorporated. BigTIFF allows for files greater than 4 GB.
STK, LSM, FluoView, SGI, SEQ, GEL, and OME-TIFF, are custom extensions
defined by Molecular Devices (Universal Imaging Corporation), Carl Zeiss
MicroImaging, Olympus, Silicon Graphics International, Media Cybernetics,
Molecular Dynamics, and the Open Microscopy Environment consortium
respectively.

For command line usage run C{python -m tifffile --help}

:Author:
  `Christoph Gohlke <http://www.lfd.uci.edu/~gohlke/>`_

:Organization:
  Laboratory for Fluorescence Dynamics, University of California, Irvine

:Version: 2017.09.29

Requirements
------------
* `CPython 3.6 64-bit <http://www.python.org>`_
* `Numpy 1.13 <http://www.numpy.org>`_
* `Matplotlib 2.0 <http://www.matplotlib.org>`_ (optional for plotting)
* `Tifffile.c 2017.01.10 <http://www.lfd.uci.edu/~gohlke/>`_
  (recommended for faster decoding of PackBits and LZW encoded strings)

Revisions
---------
2017.09.29 (tentative)
    Many backwards incompatible changes improving speed and resource usage:
    Pass 2268 tests.
    Add detail argument to __str__ function. Remove info functions.
    Fix potential issue correcting offsets of large LSM files with positions.
    Remove TiffFile iterator interface; use TiffFile.pages instead.
    Do not make tag values available as TiffPage attributes.
    Use str (not bytes) type for tag and metadata strings (WIP).
    Use documented standard tag and value names (WIP).
    Use enums for some documented TIFF tag values.
    Remove 'memmap' and 'tmpfile' options; use out='memmap' instead.
    Add option to specify output in asarray functions.
    Add option to concurrently decode image strips or tiles using threads.
    Add TiffPage.asrgb function (WIP).
    Do not apply colormap in asarray.
    Remove 'colormapped', 'rgbonly', and 'scale_mdgel' options from asarray.
    Consolidate metadata in TiffFile _metadata functions.
    Remove non-tag metadata properties from TiffPage.
    Add function to convert LSM to tiled BIN files.
    Align image data in file.
    Make TiffPage.dtype a numpy.dtype.
    Add 'ndim' and 'size' properties to TiffPage and TiffPageSeries.
    Allow imsave to write non-BigTIFF files up to ~4 GB.
    Only read one page for shaped series if possible.
    Add memmap function to create memory-mapped array stored in TIFF file.
    Add option to save empty arrays to TIFF files.
    Add option to save truncated TIFF files.
    Allow single tile images to be saved contiguously.
    Add optional movie mode for files with uniform pages.
    Lazy load pages.
    Use lightweight TiffFrame for IFDs sharing properties with key TiffPage.
    Move module constants to 'TIFF' namespace (speed up module import).
    Remove 'fastij' option from TiffFile.
    Remove 'pages' parameter from TiffFile.
    Remove TIFFfile alias.
    Deprecate Python 2.
    Require enum34 and futures packages on Python 2.7.
    Remove Record class and return all metadata as dict instead.
    Add functions to parse STK, MetaSeries, ScanImage, SVS, Pilatus metadata.
    Read tags from EXIF and GPS IFDs.
    Use pformat for tag and metadata values.
    Fix reading some UIC tags (bug fix).
    Do not modify input array in imshow (bug fix).
    Fix Python implementation of unpack_ints.
2017.05.23
    Pass 1961 tests.
    Write correct number of sample_format values (bug fix).
    Use Adobe deflate code to write ZIP compressed files.
    Add option to pass tag values as packed binary data for writing.
    Defer tag validation to attribute access.
    Use property instead of lazyattr decorator for simple expressions.
2017.03.17
    Write IFDs and tag values on word boundaries.
    Read ScanImage metadata.
    Remove is_rgb and is_indexed attributes from TiffFile.
    Create files used by doctests.
2017.01.12
    Read Zeiss SEM metadata.
    Read OME-TIFF with invalid references to external files.
    Rewrite C LZW decoder (5x faster).
    Read corrupted LSM files missing EOI code in LZW stream.
2017.01.01
    Add option to append images to existing TIFF files.
    Read files without pages.
    Read S-FEG and Helios NanoLab tags created by FEI software.
    Allow saving Color Filter Array (CFA) images.
    Add info functions returning more information about TiffFile and TiffPage.
    Add option to read specific pages only.
    Remove maxpages argument (backwards incompatible).
    Remove test_tifffile function.
2016.10.28
    Pass 1944 tests.
    Improve detection of ImageJ hyperstacks.
    Read TVIPS metadata created by EM-MENU (by Marco Oster).
    Add option to disable using OME-XML metadata.
    Allow non-integer range attributes in modulo tags (by Stuart Berg).
2016.06.21
    Do not always memmap contiguous data in page series.
2016.05.13
    Add option to specify resolution unit.
    Write grayscale images with extra samples when planarconfig is specified.
    Do not write RGB color images with 2 samples.
    Reorder TiffWriter.save keyword arguments (backwards incompatible).
2016.04.18
    Pass 1932 tests.
    TiffWriter, imread, and imsave accept open binary file streams.
2016.04.13
    Correctly handle reversed fill order in 2 and 4 bps images (bug fix).
    Implement reverse_bitorder in C.
2016.03.18
    Fix saving additional ImageJ metadata.
2016.02.22
    Pass 1920 tests.
    Write 8 bytes double tag values using offset if necessary (bug fix).
    Add option to disable writing second image description tag.
    Detect tags with incorrect counts.
    Disable color mapping for LSM.
2015.11.13
    Read LSM 6 mosaics.
    Add option to specify directory of memory-mapped files.
    Add command line options to specify vmin and vmax values for colormapping.
2015.10.06
    New helper function to apply colormaps.
    Renamed is_palette attributes to is_indexed (backwards incompatible).
    Color-mapped samples are now contiguous (backwards incompatible).
    Do not color-map ImageJ hyperstacks (backwards incompatible).
    Towards supporting Leica SCN.
2015.09.25
    Read images with reversed bit order (FillOrder is LSB2MSB).
2015.09.21
    Read RGB OME-TIFF.
    Warn about malformed OME-XML.
2015.09.16
    Detect some corrupted ImageJ metadata.
    Better axes labels for 'shaped' files.
    Do not create TiffTag for default values.
    Chroma subsampling is not supported.
    Memory-map data in TiffPageSeries if possible (optional).
2015.08.17
    Pass 1906 tests.
    Write ImageJ hyperstacks (optional).
    Read and write LZMA compressed data.
    Specify datetime when saving (optional).
    Save tiled and color-mapped images (optional).
    Ignore void bytecounts and offsets if possible.
    Ignore bogus image_depth tag created by ISS Vista software.
    Decode floating point horizontal differencing (not tiled).
    Save image data contiguously if possible.
    Only read first IFD from ImageJ files if possible.
    Read ImageJ 'raw' format (files larger than 4 GB).
    TiffPageSeries class for pages with compatible shape and data type.
    Try to read incomplete tiles.
    Open file dialog if no filename is passed on command line.
    Ignore errors when decoding OME-XML.
    Rename decoder functions (backwards incompatible).
2014.08.24
    TiffWriter class for incremental writing images.
    Simplify examples.
2014.08.19
    Add memmap function to FileHandle.
    Add function to determine if image data in TiffPage is memory-mappable.
    Do not close files if multifile_close parameter is False.
2014.08.10
    Pass 1730 tests.
    Return all extrasamples by default (backwards incompatible).
    Read data from series of pages into memory-mapped array (optional).
    Squeeze OME dimensions (backwards incompatible).
    Workaround missing EOI code in strips.
    Support image and tile depth tags (SGI extension).
    Better handling of STK/UIC tags (backwards incompatible).
    Disable color mapping for STK.
    Julian to datetime converter.
    TIFF ASCII type may be NULL separated.
    Unwrap strip offsets for LSM files greater than 4 GB.
    Correct strip byte counts in compressed LSM files.
    Skip missing files in OME series.
    Read embedded TIFF files.
2014.02.05
    Save rational numbers as type 5 (bug fix).
2013.12.20
    Keep other files in OME multi-file series closed.
    FileHandle class to abstract binary file handle.
    Disable color mapping for bad OME-TIFF produced by bio-formats.
    Read bad OME-XML produced by ImageJ when cropping.
2013.11.03
    Allow zlib compress data in imsave function (optional).
    Memory-map contiguous image data (optional).
2013.10.28
    Read MicroManager metadata and little endian ImageJ tag.
    Save extra tags in imsave function.
    Save tags in ascending order by code (bug fix).
2012.10.18
    Accept file like objects (read from OIB files).
2012.08.21
    Rename TIFFfile to TiffFile and TIFFpage to TiffPage.
    TiffSequence class for reading sequence of TIFF files.
    Read UltraQuant tags.
    Allow float numbers as resolution in imsave function.
2012.08.03
    Read MD GEL tags and NIH Image header.
2012.07.25
    Read ImageJ tags.
    ...

Notes
-----
The API is not stable yet and might change between revisions.

Tested on little-endian platforms only.

Other Python packages and modules for reading bio-scientific TIFF files:

*  `python-bioformats <https://github.com/CellProfiler/python-bioformats>`_
*  `Imread <https://github.com/luispedro/imread>`_
*  `PyLibTiff <https://github.com/pearu/pylibtiff>`_
*  `SimpleITK <http://www.simpleitk.org>`_
*  `PyLSM <https://launchpad.net/pylsm>`_
*  `PyMca.TiffIO.py <https://github.com/vasole/pymca>`_ (same as fabio.TiffIO)
*  `BioImageXD.Readers <http://www.bioimagexd.net/>`_
*  `Cellcognition.io <http://cellcognition.org/>`_
*  `pymimage <https://github.com/ardoi/pymimage>`_

Acknowledgements
----------------
*   Egor Zindy, University of Manchester, for lsm_scan_info specifics.
*   Wim Lewis for a bug fix and some LSM functions.
*   Hadrien Mary for help on reading MicroManager files.
*   Christian Kliche for help writing tiled and color-mapped files.

References
----------
1)  TIFF 6.0 Specification and Supplements. Adobe Systems Incorporated.
    http://partners.adobe.com/public/developer/tiff/
2)  TIFF File Format FAQ. http://www.awaresystems.be/imaging/tiff/faq.html
3)  MetaMorph Stack (STK) Image File Format.
    http://support.meta.moleculardevices.com/docs/t10243.pdf
4)  Image File Format Description LSM 5/7 Release 6.0 (ZEN 2010).
    Carl Zeiss MicroImaging GmbH. BioSciences. May 10, 2011
5)  The OME-TIFF format.
    http://www.openmicroscopy.org/site/support/file-formats/ome-tiff
6)  UltraQuant(r) Version 6.0 for Windows Start-Up Guide.
    http://www.ultralum.com/images%20ultralum/pdf/UQStart%20Up%20Guide.pdf
7)  Micro-Manager File Formats.
    http://www.micro-manager.org/wiki/Micro-Manager_File_Formats
8)  Tags for TIFF and Related Specifications. Digital Preservation.
    http://www.digitalpreservation.gov/formats/content/tiff_tags.shtml
9)  ScanImage BigTiff Specification - ScanImage 2016.
    http://scanimage.vidriotechnologies.com/display/SI2016/
    ScanImage+BigTiff+Specification
10) CIPA DC-008-2016: Exchangeable image file format for digital still cameras:
    Exif Version 2.31.
    http://www.cipa.jp/std/documents/e/DC-008-Translation-2016-E.pdf

Examples
--------
>>> # write and read numpy array
>>> data = numpy.random.rand(5, 301, 219)
>>> imsave('temp.tif', data)
>>> image = imread('temp.tif')
>>> numpy.testing.assert_array_equal(image, data)

>>> # iterate over pages and tags
>>> with TiffFile('temp.tif') as tif:
...     images = tif.asarray()
...     for page in tif.pages:
...         for tag in page.tags.values():
...             _ = tag.name, tag.value
...         image = page.asarray()

"""

from __future__ import division, print_function

import sys
import os
import io
import re
import glob
import math
import zlib
import time
import json
import enum
import struct
import warnings
import tempfile
import datetime
import threading
import collections
import multiprocessing
import concurrent.futures
# from fractions import Fraction  # delay import
# from xml.etree import cElementTree as etree  # delay import

import numpy

try:
    import lzma
except ImportError:
    try:
        import backports.lzma as lzma
    except ImportError:
        lzma = None

__version__ = '2017.09.29'
__docformat__ = 'restructuredtext en'
__all__ = (
    'imsave', 'imread', 'imshow', 'memmap',
    'TiffFile', 'TiffWriter', 'TiffSequence',
    # utility functions used by oiffile or czifile
    'FileHandle', 'lazyattr', 'natural_sorted', 'decode_lzw', 'stripnull',
    'create_output', 'repeat_nd', 'format_size', 'product')


def imread(files, **kwargs):
    """Return image data from TIFF file(s) as numpy array.

    Refer to the TiffFile class and member functions for documentation.

    Parameters
    ----------
    files : str, binary stream, or sequence
        File name, seekable binary stream, glob pattern, or sequence of
        file names.
    kwargs : dict
        Parameters 'multifile' and 'is_ome' are passed to the TiffFile class.
        The 'pattern' parameter is passed to the TiffSequence class.
        Other parameters are passed to the asarray functions.
        The first image series is returned if no arguments are provided.

    Examples
    --------
    >>> # get image from first page
    >>> imsave('temp.tif', numpy.random.rand(3, 4, 301, 219))
    >>> im = imread('temp.tif', key=0)
    >>> im.shape
    (4, 301, 219)

    >>> # get images from sequence of files
    >>> ims = imread(['temp.tif', 'temp.tif'])
    >>> ims.shape
    (2, 3, 4, 301, 219)

    """
    kwargs_file = parse_kwargs(kwargs, 'multifile', 'is_ome')
    kwargs_seq = parse_kwargs(kwargs, 'pattern')

    if isinstance(files, basestring) and any(i in files for i in '?*'):
        files = glob.glob(files)
    if not files:
        raise ValueError('no files found')
    if not hasattr(files, 'seek') and len(files) == 1:
        files = files[0]

    if isinstance(files, basestring) or hasattr(files, 'seek'):
        with TiffFile(files, **kwargs_file) as tif:
            return tif.asarray(**kwargs)
    else:
        with TiffSequence(files, **kwargs_seq) as imseq:
            return imseq.asarray(**kwargs)


def imsave(file, data=None, shape=None, dtype=None, bigsize=2**32-2**25,
           **kwargs):
    """Write numpy array to TIFF file.

    Refer to the TiffWriter class and member functions for documentation.

    Parameters
    ----------
    file : str or binary stream
        File name or writable binary stream, such as a open file or BytesIO.
    data : array_like
        Input image. The last dimensions are assumed to be image depth,
        height, width, and samples.
        If data is None, an empty array of the specified shape and dtype is
        saved to file.
    shape : tuple
        If data is None, shape of an empty array to save to the file.
    dtype : numpy.dtype
        If data is None, data-type of an empty array to save to the file.
    bigsize : int
        Create a BigTIFF file if the size of data in bytes is larger than
        this threshold and 'imagej' or 'truncate' are not enabled.
        By default, the threshold is 4 GB minus 32 MB reserved for metadata.
        Use the 'bigtiff' parameter to explicitly specify the type of
        file created.
    kwargs : dict
        Parameters 'append', 'byteorder', 'bigtiff', 'software', and 'imagej',
        are passed to TiffWriter().
        Other parameters are passed to TiffWriter.save().

    Returns
    -------
    If the image data are written contiguously, return offset and bytecount
    of image data in the file.

    Examples
    --------
    >>> # save a RGB image
    >>> data = numpy.random.randint(0, 255, (256, 256, 3), 'uint8')
    >>> imsave('temp.tif', data, photometric='rgb')

    >>> # save a random array and metadata, using compression
    >>> data = numpy.random.rand(2, 5, 3, 301, 219)
    >>> imsave('temp.tif', data, compress=6, metadata={'axes': 'TZCYX'})

    """
    tifargs = parse_kwargs(kwargs, 'append', 'bigtiff', 'byteorder',
                           'software', 'imagej')
    if data is None:
        size = product(shape) * numpy.dtype(dtype).itemsize
    else:
        try:
            size = data.nbytes
        except Exception:
            size = 0
    if size > bigsize and 'bigtiff' not in tifargs and not (
            tifargs.get('imagej', False) or tifargs.get('truncate', False)):
        tifargs['bigtiff'] = True

    with TiffWriter(file, **tifargs) as tif:
        return tif.save(data, shape, dtype, **kwargs)


def memmap(filename, shape=None, dtype=None, page=None, series=0, mode='r+',
           **kwargs):
    """Return memory-mapped numpy array stored in TIFF file.

    Memory-mapping requires data stored in native byte order, without tiling,
    compression, predictors, etc.
    If shape and dtype are provided, existing files will be overwritten or
    appended to depending on the 'append' parameter.
    Otherwise the image data of a specified page or series in an existing
    file will be memory-mapped. By default, the image data of the first page
    series is memory-mapped.
    Call flush() to write any changes in the array to the file.
    Raise ValueError if the image data in the file is not memory-mappable

    Parameters
    ----------
    filename : str
        Name of the TIFF file which stores the array.
    shape : tuple
        Shape of the empty array.
    dtype : numpy.dtype
        Data-type of the empty array.
    page : int
        Index of the page which image data to memory-map.
    series : int
        Index of the page series which image data to memory-map.
    mode : {'r+', 'r', 'c'}, optional
        The file open mode. Default is to open existing file for reading and
        writing ('r+').
    kwargs : dict
        Additional parameters passed to imsave() or TiffFile().

    Examples
    --------
    >>> # create an empty TIFF file and write to memory-mapped image
    >>> im = memmap('temp.tif', shape=(256, 256), dtype='float32')
    >>> im[255, 255] = 1.0
    >>> im.flush()
    >>> im.shape, im.dtype
    ((256, 256), dtype('float32'))
    >>> del im

    >>> # memory-map image data in a TIFF file
    >>> im = memmap('temp.tif', page=0)
    >>> im[255, 255]
    1.0

    """
    if shape is not None and dtype is not None:
        # create a new, empty array
        kwargs.update(data=None, shape=shape, dtype=dtype, returnoffset=True,
                      align=TIFF.ALLOCATIONGRANULARITY)
        result = imsave(filename, **kwargs)
        if result is None:
            # TODO: fail before creating file or writing data
            raise ValueError("image data is not memory-mappable")
        offset = result[0]
    else:
        # use existing file
        with TiffFile(filename, **kwargs) as tif:
            if page is not None:
                page = tif.pages[page]
                if not page.is_memmappable:
                    raise ValueError("image data is not memory-mappable")
                offset, _ = page.is_contiguous
                shape = page.shape
                dtype = page.dtype
            else:
                series = tif.series[series]
                if series.offset is None:
                    raise ValueError("image data is not memory-mappable")
                shape = series.shape
                dtype = series.dtype
                offset = series.offset
    return numpy.memmap(filename, dtype, mode, offset, shape, 'C')


class lazyattr(object):
    """Attribute whose value is computed on first access."""
    # TODO: help() doesn't work
    __slots__ = ('func',)

    def __init__(self, func):
        self.func = func
        # self.__name__ = func.__name__
        # self.__doc__ = func.__doc__
        # self.lock = threading.RLock()

    def __get__(self, instance, owner):
        # with self.lock:
        if instance is None:
            return self
        try:
            value = self.func(instance)
        except AttributeError as e:
            raise RuntimeError(e)
        if value is NotImplemented:
            return getattr(super(owner, instance), self.func.__name__)
        setattr(instance, self.func.__name__, value)
        return value


class TiffFile(object):
    """Read image and metadata from TIFF file.

    TiffFile instances must be closed using the 'close' method, which is
    automatically called when using the 'with' context manager.

    Attributes
    ----------
    pages : TiffPages
        Sequence of TIFF pages in file.
    series : list of TiffPageSeries
        Sequences of closely related TIFF pages. These are computed
        from OME, LSM, ImageJ, etc. metadata or based on similarity
        of page properties such as shape, dtype, compression, etc.
    byteorder : '>', '<'
        The endianness of data in the file.
        '>': big-endian (Motorola).
        '>': little-endian (Intel).
    is_flag : bool
        If True, file is of a certain format.
        Flags are: bigtiff, movie, shaped, ome, imagej, stk, lsm, fluoview,
        nih, vista, 'micromanager, metaseries, mdgel, mediacy, tvips, fei,
        sem, scn, svs, scanimage, andor, epics, pilatus.

    All attributes are read-only.

    Examples
    --------
    >>> # read image array from TIFF file
    >>> imsave('temp.tif', numpy.random.rand(5, 301, 219))
    >>> with TiffFile('temp.tif') as tif:
    ...     data = tif.asarray()
    >>> data.shape
    (5, 301, 219)

    """
    def __init__(self, arg, name=None, offset=None, size=None,
                 multifile=True, movie=None, **kwargs):
        """Initialize instance from file.

        Parameters
        ----------
        arg : str or open file
            Name of file or open file object.
            The file objects are closed in TiffFile.close().
        name : str
            Optional name of file in case 'arg' is a file handle.
        offset : int
            Optional start position of embedded file. By default, this is
            the current file position.
        size : int
            Optional size of embedded file. By default, this is the number
            of bytes from the 'offset' to the end of the file.
        multifile : bool
            If True (default), series may include pages from multiple files.
            Currently applies to OME-TIFF only.
        movie : bool
            If True, assume that later pages differ from first page only by
            data offsets and bytecounts. Significantly increases speed and
            reduces memory usage when reading movies with thousands of pages.
            Enabling this for non-movie files will result in data corruption
            or crashes. Python 3 only.
        kwargs : bool
            'is_ome': If False, disable processing of OME-XML metadata.

        """
        if 'fastij' in kwargs:
            del kwargs['fastij']
            raise DeprecationWarning("The fastij option will be removed.")
        for key, value in kwargs.items():
            if key[:3] == 'is_' and key[3:] in TIFF.FILE_FLAGS:
                if value is not None and not value:
                    setattr(self, key, bool(value))
            else:
                raise TypeError(
                    "got an unexpected keyword argument '%s'" % key)

        fh = FileHandle(arg, mode='rb', name=name, offset=offset, size=size)
        self._fh = fh
        self._multifile = bool(multifile)
        self._files = {fh.name: self}  # cache of TiffFiles
        try:
            fh.seek(0)
            try:
                byteorder = {b'II': '<', b'MM': '>'}[fh.read(2)]
            except KeyError:
                raise ValueError("invalid TIFF file")
            sys_byteorder = {'big': '>', 'little': '<'}[sys.byteorder]
            self.is_native = byteorder == sys_byteorder

            version = struct.unpack(byteorder+'H', fh.read(2))[0]
            if version == 43:
                # BigTiff
                self.is_bigtiff = True
                offsetsize, zero = struct.unpack(byteorder+'HH', fh.read(4))
                if zero or offsetsize != 8:
                    raise ValueError("invalid BigTIFF file")
                self.byteorder = byteorder
                self.offsetsize = 8
                self.offsetformat = byteorder+'Q'
                self.tagnosize = 8
                self.tagnoformat = byteorder+'Q'
                self.tagsize = 20
                self.tagformat1 = byteorder+'HH'
                self.tagformat2 = byteorder+'Q8s'
            elif version == 42:
                self.is_bigtiff = False
                self.byteorder = byteorder
                self.offsetsize = 4
                self.offsetformat = byteorder+'I'
                self.tagnosize = 2
                self.tagnoformat = byteorder+'H'
                self.tagsize = 12
                self.tagformat1 = byteorder+'HH'
                self.tagformat2 = byteorder+'I4s'
            else:
                raise ValueError("not a TIFF file")

            # file handle is at offset to offset to first page
            self.pages = TiffPages(self)

            if self.is_lsm and (self.filehandle.size >= 2**32 or
                                self.pages[0].compression != 1 or
                                self.pages[1].compression != 1):
                self._lsm_load_pages()
                self._lsm_fix_strip_offsets()
                self._lsm_fix_strip_bytecounts()
            elif movie:
                self.pages.useframes = True

        except Exception:
            fh.close()
            raise

    @property
    def filehandle(self):
        """Return file handle."""
        return self._fh

    @property
    def filename(self):
        """Return name of file handle."""
        return self._fh.name

    @lazyattr
    def fstat(self):
        """Return status of file handle as stat_result object."""
        try:
            return os.fstat(self._fh.fileno())
        except Exception:  # io.UnsupportedOperation
            return None

    def close(self):
        """Close open file handle(s)."""
        for tif in self._files.values():
            tif.filehandle.close()
        self._files = {}

    def asarray(self, key=None, series=None, out=None, maxworkers=1):
        """Return image data from multiple TIFF pages as numpy array.

        By default, the data from the first series is returned.

        Parameters
        ----------
        key : int, slice, or sequence of page indices
            Defines which pages to return as array.
        series : int or TiffPageSeries
            Defines which series of pages to return as array.
        out : numpy.ndarray, str, or file-like object; optional
            Buffer where image data will be saved.
            If numpy.ndarray, a writable array of compatible dtype and shape.
            If str or open file, the file name or file object used to
            create a memory-map to an array stored in a binary file on disk.
        maxworkers : int
            Maximum number of threads to concurrently get data from pages.
            Default is 1. If None, up to half the CPU cores are used.
            Reading data from file is limited to a single thread.
            Using multiple threads can significantly speed up this function
            if the bottleneck is decoding compressed data.
            If the bottleneck is I/O or pure Python code, using multiple
            threads might be detrimental.

        """
        if not self.pages:
            return numpy.array([])
        if key is None and series is None:
            series = 0
        if series is not None:
            try:
                series = self.series[series]
            except (KeyError, TypeError):
                pass
            pages = series.pages
        else:
            pages = self.pages

        if key is None:
            pass
        elif isinstance(key, inttypes):
            pages = [pages[key]]
        elif isinstance(key, slice):
            pages = pages[key]
        elif isinstance(key, collections.Iterable):
            pages = [pages[k] for k in key]
        else:
            raise TypeError("key must be an int, slice, or sequence")

        if not pages:
            raise ValueError("no pages selected")

        if self.is_nih:
            result = stack_pages(pages, out=out, maxworkers=maxworkers,
                                 squeeze=False)
        elif key is None and series and series.offset:
            if out == 'memmap' and pages[0].is_memmappable:
                result = self.filehandle.memmap_array(
                    series.dtype, series.shape, series.offset)
            else:
                if out is not None:
                    out = create_output(out, series.shape, series.dtype)
                self.filehandle.seek(series.offset)
                i = product(series.shape)
                result = self.filehandle.read_array(series.dtype, i, out=out)
                if not self.is_native:
                    result.byteswap(True)
        elif len(pages) == 1:
            result = pages[0].asarray(out=out)
        else:
            result = stack_pages(pages, out=out, maxworkers=maxworkers)

        if result is None:
            return

        if key is None:
            try:
                result.shape = series.shape
            except ValueError:
                try:
                    warnings.warn("failed to reshape %s to %s" % (
                        result.shape, series.shape))
                    # try series of expected shapes
                    result.shape = (-1,) + series.shape
                except ValueError:
                    # revert to generic shape
                    result.shape = (-1,) + pages[0].shape
        elif len(pages) == 1:
            result.shape = pages[0].shape
        else:
            result.shape = (-1,) + pages[0].shape
        return result

    @lazyattr
    def series(self):
        """Return related pages as TiffPageSeries.

        Side effect: after calling this function, TiffFile.pages might contain
        TiffPage and TiffFrame instances.

        """
        if not self.pages:
            return []

        useframes = self.pages.useframes
        keyframe = self.pages.keyframe
        series = []
        for name in 'ome imagej lsm fluoview nih mdgel shaped'.split():
            if getattr(self, 'is_' + name, False):
                series = getattr(self, '_%s_series' % name)()
                break
        if not series:
            self.pages.useframes = useframes
            self.pages.keyframe = keyframe
            series = self._generic_series()

        # remove empty series, e.g. in MD Gel files
        series = [s for s in series if sum(s.shape) > 0]

        for i, s in enumerate(series):
            s.index = i
        return series

    def _generic_series(self):
        """Return image series in file."""
        if self.pages.useframes:
            # movie mode
            page = self.pages[0]
            shape = page.shape
            axes = page.axes
            if len(self.pages) > 1:
                shape = (len(self.pages),) + shape
                axes = 'I' + axes
            return [TiffPageSeries(self.pages[:], shape, page.dtype, axes,
                                   stype='movie')]

        self.pages.clear(False)
        self.pages.load()
        result = []
        keys = []
        series = {}
        compressions = TIFF.DECOMPESSORS
        for page in self.pages:
            if not page.shape:
                continue
            key = page.shape + (page.axes, page.compression in compressions)
            if key in series:
                series[key].append(page)
            else:
                keys.append(key)
                series[key] = [page]
        for key in keys:
            pages = series[key]
            page = pages[0]
            shape = page.shape
            axes = page.axes
            if len(pages) > 1:
                shape = (len(pages),) + shape
                axes = 'I' + axes
            result.append(TiffPageSeries(pages, shape, page.dtype, axes,
                                         stype='Generic'))

        return result

    def _shaped_series(self):
        """Return image series in "shaped" file."""
        pages = self.pages
        pages.useframes = True
        lenpages = len(pages)

        def append_series(series, pages, axes, shape, reshape, name):
            page = pages[0]
            if not axes:
                shape = page.shape
                axes = page.axes
                if len(pages) > 1:
                    shape = (len(pages),) + shape
                    axes = 'Q' + axes
            size = product(shape)
            resize = product(reshape)
            if page.is_contiguous and resize > size and resize % size == 0:
                # truncated file
                axes = 'Q' + axes
                shape = (resize // size,) + shape
            try:
                axes = reshape_axes(axes, shape, reshape)
                shape = reshape
            except ValueError as e:
                warnings.warn(str(e))
            series.append(TiffPageSeries(pages, shape, page.dtype, axes,
                                         name=name, stype='Shaped'))

        keyframe = axes = shape = reshape = name = None
        series = []
        index = 0
        while True:
            if index >= lenpages:
                break
            # new keyframe; start of new series
            pages.keyframe = index
            keyframe = pages[index]
            if not keyframe.is_shaped:
                warnings.warn("invalid shape metadata or corrupted file")
                return
            # read metadata
            axes = None
            shape = None
            metadata = json_description_metadata(keyframe.is_shaped)
            name = metadata.get('name', '')
            reshape = metadata['shape']
            truncated = metadata.get('truncated', False)
            if 'axes' in metadata:
                axes = metadata['axes']
                if len(axes) == len(reshape):
                    shape = reshape
                else:
                    axes = ''
                    warnings.warn("axes do not match shape")
            # skip pages if possible
            spages = [keyframe]
            size = product(reshape)
            npages, mod = divmod(size, product(keyframe.shape))
            if mod:
                warnings.warn("series shape not matching page shape")
                return
            if 1 < npages <= lenpages - index:
                size *= keyframe._dtype.itemsize
                if truncated:
                    npages = 1
                elif not (keyframe.is_final and
                          keyframe.offset + size < pages[index+1].offset):
                    # need to read all pages for series
                    for j in range(index+1, index+npages):
                        page = pages[j]
                        page.keyframe = keyframe
                        spages.append(page)
            append_series(series, spages, axes, shape, reshape, name)
            index += npages

        return series

    def _imagej_series(self):
        """Return image series in ImageJ file."""
        # ImageJ's dimension order is always TZCYXS
        # TODO: fix loading of color, composite or palette images
        self.pages.useframes = True
        self.pages.keyframe = 0

        ij = self.imagej_metadata
        pages = self.pages
        page = pages[0]

        def is_hyperstack():
            # ImageJ hyperstack store all image metadata in the first page and
            # image data is stored contiguously before the second page, if any.
            if not page.is_final:
                return False
            images = ij.get('images', 0)
            if images <= 1:
                return False
            offset, count = page.is_contiguous
            if (count != product(page.shape) * page.bitspersample // 8
                    or offset + count*images > self.filehandle.size):
                raise ValueError()
            # check that next page is stored after data
            if len(pages) > 1 and offset + count*images > pages[1].offset:
                return False
            return True

        try:
            hyperstack = is_hyperstack()
        except ValueError:
            warnings.warn("invalid ImageJ metadata or corrupted file")
            return
        if hyperstack:
            # no need to read other pages
            pages = [page]
        else:
            self.pages.load()

        shape = []
        axes = []
        if 'frames' in ij:
            shape.append(ij['frames'])
            axes.append('T')
        if 'slices' in ij:
            shape.append(ij['slices'])
            axes.append('Z')
        if 'channels' in ij and not (page.photometric == 2 and not
                                     ij.get('hyperstack', False)):
            shape.append(ij['channels'])
            axes.append('C')
        remain = ij.get('images', len(pages))//(product(shape) if shape else 1)
        if remain > 1:
            shape.append(remain)
            axes.append('I')
        if page.axes[0] == 'I':
            # contiguous multiple images
            shape.extend(page.shape[1:])
            axes.extend(page.axes[1:])
        elif page.axes[:2] == 'SI':
            # color-mapped contiguous multiple images
            shape = page.shape[0:1] + tuple(shape) + page.shape[2:]
            axes = list(page.axes[0]) + axes + list(page.axes[2:])
        else:
            shape.extend(page.shape)
            axes.extend(page.axes)
        return [TiffPageSeries(pages, shape, page.dtype, axes, stype='ImageJ')]

    def _fluoview_series(self):
        """Return image series in FluoView file."""
        self.pages.useframes = True
        self.pages.keyframe = 0
        self.pages.load()
        mm = self.fluoview_metadata
        mmhd = list(reversed(mm['Dimensions']))
        axes = ''.join(TIFF.MM_DIMENSIONS.get(i[0].upper(), 'Q')
                       for i in mmhd if i[1] > 1)
        shape = tuple(int(i[1]) for i in mmhd if i[1] > 1)
        return [TiffPageSeries(self.pages, shape, self.pages[0].dtype, axes,
                               name=mm['ImageName'], stype='FluoView')]

    def _mdgel_series(self):
        """Return image series in MD Gel file."""
        # only a single page, scaled according to metadata in second page
        self.pages.useframes = False
        self.pages.keyframe = 0
        self.pages.load()
        md = self.mdgel_metadata
        if md['FileTag'] in (2, 128):
            dtype = numpy.dtype('float32')
            scale = md['ScalePixel']
            scale = scale[0] / scale[1]  # rational
            if md['FileTag'] == 2:
                # squary root data format
                def transform(a):
                    return a.astype('float32')**2 * scale
            else:
                def transform(a):
                    return a.astype('float32') * scale
        else:
            transform = None
        page = self.pages[0]
        return [TiffPageSeries([page], page.shape, dtype, page.axes,
                               transform=transform, stype='MDGel')]

    def _nih_series(self):
        """Return image series in NIH file."""
        self.pages.useframes = True
        self.pages.keyframe = 0
        self.pages.load()
        page0 = self.pages[0]
        if len(self.pages) == 1:
            shape = page0.shape
            axes = page0.axes
        else:
            shape = (len(self.pages),) + page0.shape
            axes = 'I' + page0.axes
        return [
            TiffPageSeries(self.pages, shape, page0.dtype, axes, stype='NIH')]

    def _ome_series(self):
        """Return image series in OME-TIFF file(s)."""
        from xml.etree import cElementTree as etree  # delayed import
        omexml = self.pages[0].description
        try:
            root = etree.fromstring(omexml)
        except etree.ParseError as e:
            # TODO: test badly encoded ome-xml
            warnings.warn("ome-xml: %s" % e)
            try:
                # might work on Python 2
                omexml = omexml.decode('utf-8', 'ignore').encode('utf-8')
                root = etree.fromstring(omexml)
            except Exception:
                return

        self.pages.useframes = True
        self.pages.keyframe = 0
        self.pages.load()

        uuid = root.attrib.get('UUID', None)
        self._files = {uuid: self}
        dirname = self._fh.dirname
        modulo = {}
        series = []
        for element in root:
            if element.tag.endswith('BinaryOnly'):
                warnings.warn("ome-xml: not an ome-tiff master file")
                break
            if element.tag.endswith('StructuredAnnotations'):
                for annot in element:
                    if not annot.attrib.get('Namespace',
                                            '').endswith('modulo'):
                        continue
                    for value in annot:
                        for modul in value:
                            for along in modul:
                                if not along.tag[:-1].endswith('Along'):
                                    continue
                                axis = along.tag[-1]
                                newaxis = along.attrib.get('Type', 'other')
                                newaxis = TIFF.AXES_LABELS[newaxis]
                                if 'Start' in along.attrib:
                                    step = float(along.attrib.get('Step', 1))
                                    start = float(along.attrib['Start'])
                                    stop = float(along.attrib['End']) + step
                                    labels = numpy.arange(start, stop, step)
                                else:
                                    labels = [label.text for label in along
                                              if label.tag.endswith('Label')]
                                modulo[axis] = (newaxis, labels)

            if not element.tag.endswith('Image'):
                continue

            attr = element.attrib
            name = attr.get('Name', None)

            for pixels in element:
                if not pixels.tag.endswith('Pixels'):
                    continue
                attr = pixels.attrib
                dtype = attr.get('PixelType', None)
                axes = ''.join(reversed(attr['DimensionOrder']))
                shape = list(int(attr['Size'+ax]) for ax in axes)
                size = product(shape[:-2])
                ifds = None
                spp = 1  # samples per pixel
                for data in pixels:
                    if data.tag.endswith('Channel'):
                        attr = data.attrib
                        if ifds is None:
                            spp = int(attr.get('SamplesPerPixel', spp))
                            ifds = [None] * (size // spp)
                        elif int(attr.get('SamplesPerPixel', 1)) != spp:
                            raise ValueError(
                                "Can't handle differing SamplesPerPixel")
                        continue
                    if ifds is None:
                        ifds = [None] * (size // spp)
                    if not data.tag.endswith('TiffData'):
                        continue
                    attr = data.attrib
                    ifd = int(attr.get('IFD', 0))
                    num = int(attr.get('NumPlanes', 1 if 'IFD' in attr else 0))
                    num = int(attr.get('PlaneCount', num))
                    idx = [int(attr.get('First'+ax, 0)) for ax in axes[:-2]]
                    try:
                        idx = numpy.ravel_multi_index(idx, shape[:-2])
                    except ValueError:
                        # ImageJ produces invalid ome-xml when cropping
                        warnings.warn("ome-xml: invalid TiffData index")
                        continue
                    for uuid in data:
                        if not uuid.tag.endswith('UUID'):
                            continue
                        if uuid.text not in self._files:
                            if not self._multifile:
                                # abort reading multifile OME series
                                # and fall back to generic series
                                return []
                            fname = uuid.attrib['FileName']
                            try:
                                tif = TiffFile(os.path.join(dirname, fname))
                                tif.pages.useframes = True
                                tif.pages.keyframe = 0
                                tif.pages.load()
                            except (IOError, FileNotFoundError, ValueError):
                                warnings.warn(
                                    "ome-xml: failed to read '%s'" % fname)
                                break
                            self._files[uuid.text] = tif
                            tif.close()
                        pages = self._files[uuid.text].pages
                        try:
                            for i in range(num if num else len(pages)):
                                ifds[idx + i] = pages[ifd + i]
                        except IndexError:
                            warnings.warn("ome-xml: index out of range")
                        # only process first uuid
                        break
                    else:
                        pages = self.pages
                        try:
                            for i in range(num if num else len(pages)):
                                ifds[idx + i] = pages[ifd + i]
                        except IndexError:
                            warnings.warn("ome-xml: index out of range")

                if all(i is None for i in ifds):
                    # skip images without data
                    continue

                # set a keyframe on all ifds
                keyframe = None
                for i in ifds:
                    # try find a TiffPage
                    if i and i == i.keyframe:
                        keyframe = i
                        break
                if not keyframe:
                    # reload a TiffPage from file
                    for i, keyframe in enumerate(ifds):
                        if keyframe:
                            keyframe.parent.pages.keyframe = keyframe.index
                            keyframe = keyframe.parent.pages[keyframe.index]
                            ifds[i] = keyframe
                            break
                for i in ifds:
                    if i is not None:
                        i.keyframe = keyframe

                dtype = keyframe.dtype
                series.append(
                    TiffPageSeries(ifds, shape, dtype, axes, parent=self,
                                   name=name, stype='OME'))
        for serie in series:
            shape = list(serie.shape)
            for axis, (newaxis, labels) in modulo.items():
                i = serie.axes.index(axis)
                size = len(labels)
                if shape[i] == size:
                    serie.axes = serie.axes.replace(axis, newaxis, 1)
                else:
                    shape[i] //= size
                    shape.insert(i+1, size)
                    serie.axes = serie.axes.replace(axis, axis+newaxis, 1)
            serie.shape = tuple(shape)
        # squeeze dimensions
        for serie in series:
            serie.shape, serie.axes = squeeze_axes(serie.shape, serie.axes)
        return series

    def _lsm_series(self):
        """Return main image series in LSM file. Skip thumbnails."""
        lsmi = self.lsm_metadata
        axes = TIFF.CZ_LSMINFO_SCANTYPE[lsmi['ScanType']]
        if self.pages[0].photometric == 2:  # RGB; more than one channel
            axes = axes.replace('C', '').replace('XY', 'XYC')
        if lsmi.get('DimensionP', 0) > 1:
            axes += 'P'
        if lsmi.get('DimensionM', 0) > 1:
            axes += 'M'
        axes = axes[::-1]
        shape = tuple(int(lsmi[TIFF.CZ_LSMINFO_DIMENSIONS[i]]) for i in axes)
        name = lsmi.get('Name', '')
        self.pages.keyframe = 0
        pages = self.pages[::2]
        dtype = pages[0].dtype
        series = [TiffPageSeries(pages, shape, dtype, axes, name=name,
                                 stype='LSM')]

        if self.pages[1].is_reduced:
            self.pages.keyframe = 1
            pages = self.pages[1::2]
            dtype = pages[0].dtype
            cp, i = 1, 0
            while cp < len(pages) and i < len(shape)-2:
                cp *= shape[i]
                i += 1
            shape = shape[:i] + pages[0].shape
            axes = axes[:i] + 'CYX'
            series.append(TiffPageSeries(pages, shape, dtype, axes, name=name,
                                         stype='LSMreduced'))

        return series

    def _lsm_load_pages(self):
        """Load all pages from LSM file."""
        self.pages.cache = True
        self.pages.useframes = True
        # second series: thumbnails
        self.pages.keyframe = 1
        keyframe = self.pages[1]
        for page in self.pages[1::2]:
            page.keyframe = keyframe
        # first series: data
        self.pages.keyframe = 0
        keyframe = self.pages[0]
        for page in self.pages[::2]:
            page.keyframe = keyframe

    def _lsm_fix_strip_offsets(self):
        """Unwrap strip offsets for LSM files greater than 4 GB.

        Each series and position require separate unwrapping (undocumented).

        """
        if self.filehandle.size < 2**32:
            return

        pages = self.pages
        npages = len(pages)
        series = self.series[0]
        axes = series.axes

        # find positions
        positions = 1
        for i in 0, 1:
            if series.axes[i] in 'PM':
                positions *= series.shape[i]

        # make time axis first
        if positions > 1:
            ntimes = 0
            for i in 1, 2:
                if axes[i] == 'T':
                    ntimes = series.shape[i]
                    break
            if ntimes:
                div, mod = divmod(npages, 2*positions*ntimes)
                assert mod == 0
                shape = (positions, ntimes, div, 2)
                indices = numpy.arange(product(shape)).reshape(shape)
                indices = numpy.moveaxis(indices, 1, 0)
        else:
            indices = numpy.arange(npages).reshape(-1, 2)

        # images of reduced page might be stored first
        if pages[0].dataoffsets[0] > pages[1].dataoffsets[0]:
            indices = indices[..., ::-1]

        # unwrap offsets
        wrap = 0
        previousoffset = 0
        for i in indices.flat:
            page = pages[i]
            dataoffsets = []
            for currentoffset in page.dataoffsets:
                if currentoffset < previousoffset:
                    wrap += 2**32
                dataoffsets.append(currentoffset + wrap)
                previousoffset = currentoffset
            page.dataoffsets = tuple(dataoffsets)

    def _lsm_fix_strip_bytecounts(self):
        """Set databytecounts to size of compressed data.

        The StripByteCounts tag in LSM files contains the number of bytes
        for the uncompressed data.

        """
        pages = self.pages
        if pages[0].compression == 1:
            return
        # sort pages by first strip offset
        pages = sorted(pages, key=lambda p: p.dataoffsets[0])
        npages = len(pages) - 1
        for i, page in enumerate(pages):
            if page.index % 2:
                continue
            offsets = page.dataoffsets
            bytecounts = page.databytecounts
            if i < npages:
                lastoffset = pages[i+1].dataoffsets[0]
            else:
                # LZW compressed strips might be longer than uncompressed
                lastoffset = min(offsets[-1] + 2*bytecounts[-1], self._fh.size)
            offsets = offsets + (lastoffset,)
            page.databytecounts = tuple(offsets[j+1] - offsets[j]
                                        for j in range(len(bytecounts)))

    def __getattr__(self, name):
        """Return 'is_flag' attributes from first page."""
        if name[3:] in TIFF.FILE_FLAGS:
            if not self.pages:
                return False
            value = bool(getattr(self.pages[0], name))
            setattr(self, name, value)
            return value
        raise AttributeError("'%s' object has no attribute '%s'" %
                             (self.__class__.__name__, name))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __str__(self, detail=0):
        """Return string containing information about file.

        The detail parameter specifies the level of detail returned:

        0: file only.
        1: all series, first page of series and its tags.
        2: large tag values and file metadata.
        3: all pages.

        """
        info = [
            "TiffFile '%s'" % snipstr(self._fh.name, 32),
            format_size(self._fh.size),
            {'<': 'LittleEndian', '>': 'BigEndian'}[self.byteorder]]
        if self.is_bigtiff:
            info.append('BigTiff')
        info.append('|'.join(f.upper() for f in self.flags))
        if len(self.pages) > 1:
            info.append('%i Pages' % len(self.pages))
        if len(self.series) > 1:
            info.append('%i Series' % len(self.series))
        if len(self._files) > 1:
            info.append('%i Files' % (len(self._files)))
        info = '  '.join(info)
        if detail <= 0:
            return info
        info = [info]
        info.append('\n'.join(str(s) for s in self.series))
        if detail >= 3:
            info.extend((TiffPage.__str__(p, detail=detail)
                         for p in self.pages
                         if p is not None))
        else:
            info.extend((TiffPage.__str__(s.pages[0], detail=detail)
                         for s in self.series
                         if s.pages[0] is not None))
        if detail >= 2:
            for name in sorted(self.flags):
                if hasattr(self, name + '_metadata'):
                    m = getattr(self, name + '_metadata')
                    if m:
                        info.append(
                            "%s_METADATA\n%s" % (name.upper(), pformat(m)))
        return '\n\n'.join(info).replace('\n\n\n', '\n\n')

    @lazyattr
    def flags(self):
        """Return set of file flags."""
        return set(name.lower() for name in sorted(TIFF.FILE_FLAGS)
                   if getattr(self, 'is_' + name))

    @lazyattr
    def is_mdgel(self):
        """File has MD Gel format."""
        try:
            return self.pages[0].is_mdgel or self.pages[1].is_mdgel
        except IndexError:
            return False

    @property
    def is_movie(self):
        """Return if file is a movie."""
        return self.pages.useframes

    @lazyattr
    def shaped_metadata(self):
        """Return Tifffile metadata from JSON descriptions as dicts."""
        if not self.is_shaped:
            return
        return tuple(json_description_metadata(s.pages[0].is_shaped)
                     for s in self.series if s.stype.lower() == 'shaped')

    @lazyattr
    def ome_metadata(self):
        """Return OME XML as dict."""
        if not self.is_ome:
            return
        return xml2dict(self.pages[0].description)

    @lazyattr
    def lsm_metadata(self):
        """Return LSM metadata from CZ_LSMINFO tag as dict."""
        if not self.is_lsm:
            return
        return self.pages[0].tags['CZ_LSMINFO'].value

    @lazyattr
    def stk_metadata(self):
        """Return STK metadata from UIC tags as dict."""
        if not self.is_stk:
            return
        page = self.pages[0]
        tags = page.tags
        result = {}
        result['NumberPlanes'] = tags['UIC2tag'].count
        if page.description:
            result['PlaneDescriptions'] = page.description.split('\0')
            # result['plane_descriptions'] = stk_description_metadata(
            #    page.image_description)
        if 'UIC1tag' in tags:
            result.update(tags['UIC1tag'].value)
        if 'UIC3tag' in tags:
            result.update(tags['UIC3tag'].value)  # wavelengths
        if 'UIC4tag' in tags:
            result.update(tags['UIC4tag'].value)  # override uic1 tags
        uic2tag = tags['UIC2tag'].value
        result['ZDistance'] = uic2tag['ZDistance']
        result['TimeCreated'] = uic2tag['TimeCreated']
        result['TimeModified'] = uic2tag['TimeModified']
        try:
            result['DatetimeCreated'] = numpy.array(
                [julian_datetime(*dt) for dt in
                 zip(uic2tag['DateCreated'], uic2tag['TimeCreated'])],
                dtype='datetime64[ns]')
            result['DatetimeModified'] = numpy.array(
                [julian_datetime(*dt) for dt in
                 zip(uic2tag['DateModified'], uic2tag['TimeModified'])],
                dtype='datetime64[ns]')
        except ValueError as e:
            warnings.warn("stk_metadata: %s" % e)
        return result

    @lazyattr
    def imagej_metadata(self):
        """Return consolidated ImageJ metadata as dict."""
        if not self.is_imagej:
            return
        page = self.pages[0]
        result = imagej_description_metadata(page.is_imagej)
        if 'IJMetadata' in page.tags:
            try:
                result.update(page.tags['IJMetadata'].value)
            except Exception:
                pass
        return result

    @lazyattr
    def fluoview_metadata(self):
        """Return consolidated FluoView metadata as dict."""
        if not self.is_fluoview:
            return
        result = {}
        page = self.pages[0]
        result.update(page.tags['MM_Header'].value)
        # TODO: read stamps from all pages
        result['Stamp'] = page.tags['MM_Stamp'].value
        # skip parsing image description; not reliable
        # try:
        #     t = fluoview_description_metadata(page.image_description)
        #     if t is not None:
        #         result['ImageDescription'] = t
        # except Exception as e:
        #     warnings.warn(
        #         "failed to read FluoView image description: %s" % e)
        return result

    @lazyattr
    def nih_metadata(self):
        """Return NIH Image metadata from NIHImageHeader tag as dict."""
        if not self.is_nih:
            return
        return self.pages[0].tags['NIHImageHeader'].value

    @lazyattr
    def fei_metadata(self):
        """Return FEI metadata from SFEG or HELIOS tags as dict."""
        if not self.is_fei:
            return
        tags = self.pages[0].tags
        if 'FEI_SFEG' in tags:
            return tags['FEI_SFEG'].value
        if 'FEI_HELIOS' in tags:
            return tags['FEI_HELIOS'].value

    @lazyattr
    def sem_metadata(self):
        """Return SEM metadata from CZ_SEM tag as dict."""
        if not self.is_sem:
            return
        return self.pages[0].tags['CZ_SEM'].value

    @lazyattr
    def mdgel_metadata(self):
        """Return consolidated metadata from MD GEL tags as dict."""
        for page in self.pages[:2]:
            if 'MDFileTag' in page.tags:
                tags = page.tags
                break
        else:
            return
        result = {}
        for code in range(33445, 33453):
            name = TIFF.TAGS[code]
            if name not in tags:
                continue
            result[name[2:]] = tags[name].value
        return result

    @lazyattr
    def andor_metadata(self):
        """Return Andor tags as dict."""
        return self.pages[0].andor_tags

    @lazyattr
    def epics_metadata(self):
        """Return EPICS areaDetector tags as dict."""
        return self.pages[0].epics_tags

    @lazyattr
    def tvips_metadata(self):
        """Return TVIPS tag as dict."""
        if not self.is_tvips:
            return
        return self.pages[0].tags['TVIPS'].value

    @lazyattr
    def metaseries_metadata(self):
        """Return MetaSeries metadata from image description as dict."""
        if not self.is_metaseries:
            return
        return metaseries_description_metadata(self.pages[0].description)

    @lazyattr
    def pilatus_metadata(self):
        """Return Pilatus metadata from image description as dict."""
        if not self.is_pilatus:
            return
        return pilatus_description_metadata(self.pages[0].description)

    @lazyattr
    def micromanager_metadata(self):
        """Return consolidated MicroManager metadata as dict."""
        if not self.is_micromanager:
            return
        # from file header
        result = read_micromanager_metadata(self._fh)
        # from tag
        result.update(self.pages[0].tags['MicroManagerMetadata'].value)
        return result

    @lazyattr
    def scanimage_metadata(self):
        """Return ScanImage non-varying frame and ROI metadata as dict."""
        if not self.is_scanimage:
            return
        result = {}
        try:
            framedata, roidata = read_scanimage_metadata(self._fh)
            result['FrameData'] = framedata
            result.update(roidata)
        except ValueError:
            pass
        # TODO: scanimage_artist_metadata
        try:
            result['Description'] = scanimage_description_metadata(
                self.pages[0].description)
        except Exception as e:
            warnings.warn("scanimage_description_metadata failed: %s" % e)
        return result


class TiffPages(object):
    """Sequence of TIFF image file directories."""
    def __init__(self, parent):
        """Initialize instance from file. Read first TiffPage from file.

        The file position must be at an offset to an offset to a TiffPage.

        """
        self.parent = parent
        self.pages = []  # cache of TiffPages, TiffFrames, or their offsets
        self.complete = False  # True if offsets to all pages were read
        self._tiffpage = TiffPage  # class for reading tiff pages
        self._keyframe = None
        self._cache = True

        # read offset to first page
        fh = parent.filehandle
        self._nextpageoffset = fh.tell()
        offset = struct.unpack(parent.offsetformat,
                               fh.read(parent.offsetsize))[0]

        if offset == 0:
            # warnings.warn("file contains no pages")
            self.complete = True
            return
        if offset >= fh.size:
            warnings.warn("invalid page offset (%i)" % offset)
            self.complete = True
            return

        # always read and cache first page
        fh.seek(offset)
        page = TiffPage(parent, index=0)
        self.pages.append(page)
        self._keyframe = page

    @property
    def cache(self):
        """Return if pages/frames are currenly being cached."""
        return self._cache

    @cache.setter
    def cache(self, value):
        """Enable or disable caching of pages/frames. Clear cache if False."""
        value = bool(value)
        if self._cache and not value:
            self.clear()
        self._cache = value

    @property
    def useframes(self):
        """Return if currently using TiffFrame (True) or TiffPage (False)."""
        return self._tiffpage == TiffFrame and TiffFrame is not TiffPage

    @useframes.setter
    def useframes(self, value):
        """Set to use TiffFrame (True) or TiffPage (False)."""
        self._tiffpage = TiffFrame if value else TiffPage

    @property
    def keyframe(self):
        """Return index of current keyframe."""
        return self._keyframe.index

    @keyframe.setter
    def keyframe(self, index):
        """Set current keyframe. Load TiffPage from file if necessary."""
        if self.complete or 0 <= index < len(self.pages):
            page = self.pages[index]
            if isinstance(page, TiffPage):
                self._keyframe = page
                return
            elif isinstance(page, TiffFrame):
                # remove existing frame
                self.pages[index] = page.offset
        # load TiffPage from file
        useframes = self.useframes
        self._tiffpage = TiffPage
        self._keyframe = self[index]
        self.useframes = useframes

    @property
    def next_page_offset(self):
        """Return offset where offset to a new page can be stored."""
        if not self.complete:
            self._seek(-1)
        return self._nextpageoffset

    def load(self):
        """Read all remaining pages from file."""
        fh = self.parent.filehandle
        keyframe = self._keyframe
        pages = self.pages
        if not self.complete:
            self._seek(-1)
        for i, page in enumerate(pages):
            if isinstance(page, inttypes):
                fh.seek(page)
                page = self._tiffpage(self.parent, index=i, keyframe=keyframe)
                pages[i] = page

    def clear(self, fully=True):
        """Delete all but first page from cache. Set keyframe to first page."""
        pages = self.pages
        if not self._cache or len(pages) < 1:
            return
        self._keyframe = pages[0]
        if fully:
            # delete all but first TiffPage/TiffFrame
            for i, page in enumerate(pages[1:]):
                if not isinstance(page, inttypes):
                    pages[i+1] = page.offset
        elif TiffFrame is not TiffPage:
            # delete only TiffFrames
            for i, page in enumerate(pages):
                if isinstance(page, TiffFrame):
                    pages[i] = page.offset

    def _seek(self, index):
        """Seek file to offset of specified page."""
        pages = self.pages
        if not pages:
            return

        fh = self.parent.filehandle
        if fh.closed:
            raise RuntimeError("FileHandle is closed")

        if self.complete or 0 <= index < len(pages):
            page = pages[index]
            offset = page if isinstance(page, inttypes) else page.offset
            fh.seek(offset)
            return

        offsetformat = self.parent.offsetformat
        offsetsize = self.parent.offsetsize
        tagnoformat = self.parent.tagnoformat
        tagnosize = self.parent.tagnosize
        tagsize = self.parent.tagsize
        unpack = struct.unpack

        page = pages[-1]
        offset = page if isinstance(page, inttypes) else page.offset

        while True:
            # read offsets to pages from file until index is reached
            fh.seek(offset)
            # skip tags
            try:
                tagno = unpack(tagnoformat, fh.read(tagnosize))[0]
                if tagno > 4096:
                    raise ValueError("suspicious number of tags")
            except Exception:
                warnings.warn("corrupted tag list at offset %i" % offset)
                del pages[-1]
                self.complete = True
                break
            self._nextpageoffset = offset + tagnosize + tagno * tagsize
            fh.seek(self._nextpageoffset)

            # read offset to next page
            offset = unpack(offsetformat, fh.read(offsetsize))[0]
            if offset == 0:
                self.complete = True
                break
            if offset >= fh.size:
                warnings.warn("invalid page offset (%i)" % offset)
                self.complete = True
                break

            pages.append(offset)
            if 0 <= index < len(pages):
                break

        if index >= len(pages):
            raise IndexError('list index out of range')

        page = pages[index]
        fh.seek(page if isinstance(page, inttypes) else page.offset)

    def __bool__(self):
        """Return True if file contains any pages."""
        return len(self.pages) > 0

    def __len__(self):
        """Return number of pages in file."""
        if not self.complete:
            self._seek(-1)
        return len(self.pages)

    def __getitem__(self, key):
        """Return specified page(s) from cache or file."""
        pages = self.pages
        if not pages:
            raise IndexError('list index out of range')
        if key is 0:
            return pages[key]

        if isinstance(key, slice):
            start, stop, _ = key.indices(2**31)
            if not self.complete and max(stop, start) > len(pages):
                self._seek(-1)
            return [self[i] for i in range(*key.indices(len(pages)))]

        if self.complete and key >= len(pages):
            raise IndexError('list index out of range')

        try:
            page = pages[key]
        except IndexError:
            page = 0
        if not isinstance(page, inttypes):
            return page

        self._seek(key)
        page = self._tiffpage(self.parent, index=key, keyframe=self._keyframe)
        if self._cache:
            pages[key] = page
        return page

    def __iter__(self):
        """Return iterator over all pages."""
        i = 0
        while True:
            try:
                yield self[i]
                i += 1
            except IndexError:
                break


class TiffPage(object):
    """TIFF image file directory (IFD).

    Attributes
    ----------
    index : int
        Index of page in file.
    dtype : numpy.dtype or None
        Data type of the image in IFD.
    shape : tuple
        Dimensions of the image in IFD.
    axes : str
        Axes label codes:
        'X' width, 'Y' height, 'S' sample, 'I' image series|page|plane,
        'Z' depth, 'C' color|em-wavelength|channel, 'E' ex-wavelength|lambda,
        'T' time, 'R' region|tile, 'A' angle, 'P' phase, 'H' lifetime,
        'L' exposure, 'V' event, 'Q' unknown, '_' missing
    tags : dict
        Dictionary of tags in IFD. {tag.name: TiffTag}
    colormap : numpy.ndarray
        Color look up table, if exists.

    All attributes are read-only.

    Notes
    -----
    The internal, normalized '_shape' attribute is 6 dimensional:

    0 : number planes/images  (stk, ij).
    1 : planar samplesperpixel.
    2 : imagedepth Z  (sgi).
    3 : imagelength Y.
    4 : imagewidth X.
    5 : contig samplesperpixel.

    """
    # default properties; will be updated from tags
    imagewidth = 0
    imagelength = 0
    imagedepth = 1
    tilewidth = 0
    tilelength = 0
    tiledepth = 1
    bitspersample = 1
    samplesperpixel = 1
    sampleformat = 1
    rowsperstrip = 2**32-1
    compression = 1
    planarconfig = 1
    fillorder = 1
    photometric = 0
    predictor = 1
    extrasamples = 1
    colormap = None
    software = ''
    description = ''
    description1 = ''

    def __init__(self, parent, index, keyframe=None):
        """Initialize instance from file.

        The file handle position must be at offset to a valid IFD.

        """
        self.parent = parent
        self.index = index
        self.shape = ()
        self._shape = ()
        self.dtype = None
        self._dtype = None
        self.axes = ""
        self.tags = {}

        self.dataoffsets = ()
        self.databytecounts = ()

        # read TIFF IFD structure and its tags from file
        fh = parent.filehandle
        self.offset = fh.tell()  # offset to this IDF
        try:
            tagno = struct.unpack(parent.tagnoformat,
                                  fh.read(parent.tagnosize))[0]
            if tagno > 4096:
                raise ValueError("suspicious number of tags")
        except Exception:
            raise ValueError("corrupted tag list at offset %i" % self.offset)

        tagsize = parent.tagsize
        data = fh.read(tagsize * tagno)
        tags = self.tags
        index = -tagsize
        for _ in range(tagno):
            index += tagsize
            try:
                tag = TiffTag(self.parent, data[index:index+tagsize])
            except TiffTag.Error as e:
                warnings.warn(str(e))
                continue
            tagname = tag.name
            if tagname not in tags:
                name = tagname
                tags[name] = tag
            else:
                # some files contain multiple tags with same code
                # e.g. MicroManager files contain two ImageDescription tags
                i = 1
                while True:
                    name = "%s%i" % (tagname, i)
                    if name not in tags:
                        tags[name] = tag
                        break
            name = TIFF.TAG_ATTRIBUTES.get(name, '')
            if name:
                setattr(self, name, tag.value)

        if not tags:
            return  # found in FIBICS

        # consolidate private tags; remove them from self.tags
        if self.is_andor:
            self.andor_tags
        elif self.is_epics:
            self.epics_tags

        if self.is_lsm or (self.index and self.parent.is_lsm):
            # correct non standard LSM bitspersample tags
            self.tags['BitsPerSample']._fix_lsm_bitspersample(self)

        if self.is_vista or (self.index and self.parent.is_vista):
            # ISS Vista writes wrong ImageDepth tag
            self.imagedepth = 1

        if self.is_stk and 'UIC1tag' in tags and not tags['UIC1tag'].value:
            # read UIC1tag now that plane count is known
            uic1tag = tags['UIC1tag']
            fh.seek(uic1tag.valueoffset)
            tags['UIC1tag'].value = read_uic1tag(
                fh, self.parent.byteorder, uic1tag.dtype,
                uic1tag.count, None, tags['UIC2tag'].count)

        if 'IJMetadata' in tags:
            # decode IJMetadata tag
            try:
                tags['IJMetadata'].value = imagej_metadata(
                    tags['IJMetadata'].value,
                    tags['IJMetadataByteCounts'].value,
                    self.parent.byteorder)
            except Exception as e:
                warnings.warn(str(e))

        if 'BitsPerSample' in tags:
            tag = tags['BitsPerSample']
            if tag.count == 1:
                self.bitspersample = tag.value
            else:
                # LSM might list more items than samples_per_pixel
                value = tag.value[:self.samplesperpixel]
                if any((v-value[0] for v in value)):
                    self.bitspersample = value
                else:
                    self.bitspersample = value[0]

        if 'SampleFormat' in tags:
            tag = tags['SampleFormat']
            if tag.count == 1:
                self.sampleformat = tag.value
            else:
                value = tag.value[:self.samplesperpixel]
                if any((v-value[0] for v in value)):
                    self.sampleformat = value
                else:
                    self.sampleformat = value[0]

        if 'ImageLength' in tags:
            if 'RowsPerStrip' not in tags or tags['RowsPerStrip'].count > 1:
                self.rowsperstrip = self.imagelength
            # self.stripsperimage = int(math.floor(
            #    float(self.imagelength + self.rowsperstrip - 1) /
            #    self.rowsperstrip))

        # determine dtype
        dtype = self.sampleformat, self.bitspersample
        dtype = TIFF.SAMPLE_DTYPES.get(dtype, None)
        if dtype is not None:
            dtype = numpy.dtype(dtype)
        self.dtype = self._dtype = dtype

        # determine shape of data
        imagelength = self.imagelength
        imagewidth = self.imagewidth
        imagedepth = self.imagedepth
        samplesperpixel = self.samplesperpixel

        if self.is_stk:
            assert self.imagedepth == 1
            uictag = tags['UIC2tag'].value
            planes = tags['UIC2tag'].count
            if self.planarconfig == 1:
                self._shape = (
                    planes, 1, 1, imagelength, imagewidth, samplesperpixel)
                if samplesperpixel == 1:
                    self.shape = (planes, imagelength, imagewidth)
                    self.axes = 'YX'
                else:
                    self.shape = (
                        planes, imagelength, imagewidth, samplesperpixel)
                    self.axes = 'YXS'
            else:
                self._shape = (
                    planes, samplesperpixel, 1, imagelength, imagewidth, 1)
                if samplesperpixel == 1:
                    self.shape = (planes, imagelength, imagewidth)
                    self.axes = 'YX'
                else:
                    self.shape = (
                        planes, samplesperpixel, imagelength, imagewidth)
                    self.axes = 'SYX'
            # detect type of series
            if planes == 1:
                self.shape = self.shape[1:]
            elif numpy.all(uictag['ZDistance'] != 0):
                self.axes = 'Z' + self.axes
            elif numpy.all(numpy.diff(uictag['TimeCreated']) != 0):
                self.axes = 'T' + self.axes
            else:
                self.axes = 'I' + self.axes
        elif self.photometric == 2 or samplesperpixel > 1:  # PHOTOMETRIC.RGB
            if self.planarconfig == 1:
                self._shape = (
                    1, 1, imagedepth, imagelength, imagewidth, samplesperpixel)
                if imagedepth == 1:
                    self.shape = (imagelength, imagewidth, samplesperpixel)
                    self.axes = 'YXS'
                else:
                    self.shape = (
                        imagedepth, imagelength, imagewidth, samplesperpixel)
                    self.axes = 'ZYXS'
            else:
                self._shape = (1, samplesperpixel, imagedepth,
                               imagelength, imagewidth, 1)
                if imagedepth == 1:
                    self.shape = (samplesperpixel, imagelength, imagewidth)
                    self.axes = 'SYX'
                else:
                    self.shape = (
                        samplesperpixel, imagedepth, imagelength, imagewidth)
                    self.axes = 'SZYX'
        else:
            self._shape = (1, 1, imagedepth, imagelength, imagewidth, 1)
            if imagedepth == 1:
                self.shape = (imagelength, imagewidth)
                self.axes = 'YX'
            else:
                self.shape = (imagedepth, imagelength, imagewidth)
                self.axes = 'ZYX'

        # dataoffsets and databytecounts
        if 'TileOffsets' in tags:
            self.dataoffsets = tags['TileOffsets'].value
        elif 'StripOffsets' in tags:
            self.dataoffsets = tags['StripOffsets'].value
        else:
            self.dataoffsets = (0,)

        if 'TileByteCounts' in tags:
            self.databytecounts = tags['TileByteCounts'].value
        elif 'StripByteCounts' in tags:
            self.databytecounts = tags['StripByteCounts'].value
        elif self.compression == 1:
            self.databytecounts = (
                product(self.shape) * (self.bitspersample // 8),)
        else:
            raise ValueError("ByteCounts not found")
        assert len(self.shape) == len(self.axes)

    def asarray(self, out=None, squeeze=True, lock=None, reopen=True,
                maxsize=64*2**30, validate=True):
        """Read image data from file and return as numpy array.

        Raise ValueError if format is unsupported.

        Parameters
        ----------
        out : numpy.ndarray, str, or file-like object; optional
            Buffer where image data will be saved.
            If numpy.ndarray, a writable array of compatible dtype and shape.
            If str or open file, the file name or file object used to
            create a memory-map to an array stored in a binary file on disk.
        squeeze : bool
            If True, all length-1 dimensions (except X and Y) are
            squeezed out from the array.
            If False, the shape of the returned array might be different from
            the page.shape.
        lock : {RLock, NullContext}
            A reentrant lock used to syncronize reads from file.
            If None (default), the lock of the parent's filehandle is used.
        reopen : bool
            If True (default) and the parent file handle is closed, the file
            is temporarily re-opened and closed if no exception occurs.
        maxsize: int or None
            Maximum size of data before a ValueError is raised.
            Can be used to catch DOS. Default: 64 GB.
        validate : bool
            If True (default), validate various parameters.
            If None, only validate parameters and return None.

        """
        self_ = self
        self = self.keyframe  # self or keyframe

        if not self._shape or product(self._shape) == 0:
            return

        tags = self.tags

        if validate or validate is None:
            if maxsize and product(self._shape) > maxsize:
                raise ValueError("data is too large %s" % str(self._shape))
            if self.dtype is None:
                raise ValueError("data type not supported: %s%i" % (
                    self.sampleformat, self.bitspersample))
            if self.compression not in TIFF.DECOMPESSORS:
                raise ValueError(
                    "can not decompress %s" % self.compression.name)
            if 'SampleFormat' in tags:
                tag = tags['SampleFormat']
                if tag.count != 1 and any((i-tag.value[0] for i in tag.value)):
                    raise ValueError(
                        "sample formats do not match %s" % tag.value)
            if self.is_chroma_subsampled:
                # TODO: implement chroma subsampling
                raise NotImplementedError("chroma subsampling not supported")
            if validate is None:
                return

        fh = self_.parent.filehandle
        lock = fh.lock if lock is None else lock
        with lock:
            closed = fh.closed
            if closed:
                if reopen:
                    fh.open()
                else:
                    raise IOError("file handle is closed")

        dtype = self._dtype
        shape = self._shape
        imagewidth = self.imagewidth
        imagelength = self.imagelength
        imagedepth = self.imagedepth
        bitspersample = self.bitspersample
        typecode = self.parent.byteorder + dtype.char
        lsb2msb = self.fillorder == 2
        offsets, bytecounts = self_.offsets_bytecounts
        istiled = self.is_tiled

        if istiled:
            tilewidth = self.tilewidth
            tilelength = self.tilelength
            tiledepth = self.tiledepth
            tw = (imagewidth + tilewidth - 1) // tilewidth
            tl = (imagelength + tilelength - 1) // tilelength
            td = (imagedepth + tiledepth - 1) // tiledepth
            shape = (shape[0], shape[1],
                     td*tiledepth, tl*tilelength, tw*tilewidth, shape[-1])
            tileshape = (tiledepth, tilelength, tilewidth, shape[-1])
            runlen = tilewidth
        else:
            runlen = imagewidth

        if out == 'memmap' and self.is_memmappable:
            with lock:
                result = fh.memmap_array(typecode, shape, offset=offsets[0])
        elif self.is_contiguous:
            isnative = self.parent.is_native
            if out is not None:
                isnative = True
                out = create_output(out, shape, dtype)
            with lock:
                fh.seek(offsets[0])
                result = fh.read_array(typecode, product(shape), out=out)
            if not isnative:
                result = result.astype('=' + dtype.char)
            if lsb2msb:
                reverse_bitorder(result)
        else:
            result = create_output(out, shape, dtype)
            if self.planarconfig == 1:
                runlen *= self.samplesperpixel
            if bitspersample in (8, 16, 32, 64, 128):
                if (bitspersample * runlen) % 8:
                    raise ValueError("data and sample size mismatch")

                def unpack(x, typecode=typecode):
                    if self.predictor == 3:  # PREDICTOR.FLOATINGPOINT
                        # the floating point horizontal differencing decoder
                        # needs the raw byte order
                        typecode = dtype.char
                    try:
                        return numpy.fromstring(x, typecode)
                    except ValueError as e:
                        # strips may be missing EOI
                        # warnings.warn("unpack: %s" % e)
                        xlen = ((len(x) // (bitspersample // 8)) *
                                (bitspersample // 8))
                        return numpy.fromstring(x[:xlen], typecode)

            elif isinstance(bitspersample, tuple):
                def unpack(x):
                    return unpack_rgb(x, typecode, bitspersample)
            else:
                def unpack(x):
                    return unpack_ints(x, typecode, bitspersample, runlen)

            decompress = TIFF.DECOMPESSORS[self.compression]
            if self.compression == 7:  # COMPRESSION.JPEG
                if 'JPEGTables' in tags:
                    table = tags['JPEGTables'].value
                else:
                    table = b''

                def decompress(x):
                    return decode_jpeg(x, table, self.photometric)

            if istiled:
                tw, tl, td, pl = 0, 0, 0, 0
                for tile in buffered_read(fh, lock, offsets, bytecounts):
                    if lsb2msb:
                        tile = reverse_bitorder(tile)
                    tile = decompress(tile)
                    tile = unpack(tile)
                    try:
                        tile.shape = tileshape
                    except ValueError:
                        # incomplete tiles; see gdal issue #1179
                        warnings.warn("invalid tile data")
                        t = numpy.zeros(tileshape, dtype).reshape(-1)
                        s = min(tile.size, t.size)
                        t[:s] = tile[:s]
                        tile = t.reshape(tileshape)
                    if self.predictor == 2:  # PREDICTOR.HORIZONTAL
                        numpy.cumsum(tile, axis=-2, dtype=dtype, out=tile)
                    elif self.predictor == 3:  # PREDICTOR.FLOATINGPOINT
                        raise NotImplementedError()
                    result[0, pl, td:td+tiledepth,
                           tl:tl+tilelength, tw:tw+tilewidth, :] = tile
                    del tile
                    tw += tilewidth
                    if tw >= shape[4]:
                        tw, tl = 0, tl + tilelength
                        if tl >= shape[3]:
                            tl, td = 0, td + tiledepth
                            if td >= shape[2]:
                                td, pl = 0, pl + 1
                result = result[...,
                                :imagedepth, :imagelength, :imagewidth, :]
            else:
                strip_size = self.rowsperstrip * self.imagewidth
                if self.planarconfig == 1:
                    strip_size *= self.samplesperpixel
                result = result.reshape(-1)
                index = 0
                for strip in buffered_read(fh, lock, offsets, bytecounts):
                    if lsb2msb:
                        strip = reverse_bitorder(strip)
                    strip = decompress(strip)
                    strip = unpack(strip)
                    size = min(result.size, strip.size, strip_size,
                               result.size - index)
                    result[index:index+size] = strip[:size]
                    del strip
                    index += size

        result.shape = self._shape

        if self.predictor != 1 and not (istiled and not self.is_contiguous):
            if self.parent.is_lsm and self.compression == 1:
                pass  # work around bug in LSM510 software
            elif self.predictor == 2:  # PREDICTOR.HORIZONTAL
                numpy.cumsum(result, axis=-2, dtype=dtype, out=result)
            elif self.predictor == 3:  # PREDICTOR.FLOATINGPOINT
                result = decode_floats(result)

        if squeeze:
            try:
                result.shape = self.shape
            except ValueError:
                warnings.warn("failed to reshape from %s to %s" % (
                    str(result.shape), str(self.shape)))

        if closed:
            # TODO: file should remain open if an exception occurred above
            fh.close()
        return result

    def asrgb(self, uint8=False, alpha=None, colormap=None,
              dmin=None, dmax=None, *args, **kwargs):
        """Return image data as RGB(A).

        Work in progress.

        """
        data = self.asarray(*args, **kwargs)
        self = self.keyframe  # self or keyframe
        photometric = self.photometric
        PHOTOMETRIC = TIFF.PHOTOMETRIC

        if photometric == PHOTOMETRIC.PALETTE:
            colormap = self.colormap
            if (colormap.shape[1] < 2**self.bitspersample or
                    self.dtype.char not in 'BH'):
                raise ValueError("can not apply colormap")
            if uint8:
                if colormap.max() > 255:
                    colormap >>= 8
                colormap = colormap.astype('uint8')
            if 'S' in self.axes:
                data = data[..., 0] if self.planarconfig == 1 else data[0]
            data = apply_colormap(data, colormap)

        elif photometric == PHOTOMETRIC.RGB:
            if 'ExtraSamples' in self.tags:
                if alpha is None:
                    alpha = TIFF.EXTRASAMPLE
                extrasamples = self.extrasamples
                if self.tags['ExtraSamples'].count == 1:
                    extrasamples = (extrasamples,)
                for i, exs in enumerate(extrasamples):
                    if exs in alpha:
                        if self.planarconfig == 1:
                            data = data[..., [0, 1, 2, 3+i]]
                        else:
                            data = data[:, [0, 1, 2, 3+i]]
                        break
            else:
                if self.planarconfig == 1:
                    data = data[..., :3]
                else:
                    data = data[:, :3]
            # TODO: convert to uint8

        elif photometric == PHOTOMETRIC.MINISBLACK:
            raise NotImplementedError()
        elif photometric == PHOTOMETRIC.MINISWHITE:
            raise NotImplementedError()
        elif photometric == PHOTOMETRIC.SEPARATED:
            raise NotImplementedError()
        else:
            raise NotImplementedError()
        return data

    def aspage(self):
        return self

    @property
    def keyframe(self):
        return self

    @keyframe.setter
    def keyframe(self, index):
        return

    @lazyattr
    def offsets_bytecounts(self):
        """Return simplified offsets and bytecounts."""
        if self.is_contiguous:
            offset, byte_count = self.is_contiguous
            return [offset], [byte_count]
        return clean_offsets_counts(self.dataoffsets, self.databytecounts)

    @lazyattr
    def is_contiguous(self):
        """Return offset and size of contiguous data, else None.

        Excludes prediction and fill_order.

        """
        if (self.compression != 1
                or self.bitspersample not in (8, 16, 32, 64)):
            return
        if 'TileWidth' in self.tags:
            if (self.imagewidth != self.tilewidth or
                    self.imagelength % self.tilelength or
                    self.tilewidth % 16 or self.tilelength % 16):
                return
            if ('ImageDepth' in self.tags and 'TileDepth' in self.tags and
                    (self.imagelength != self.tilelength or
                     self.imagedepth % self.tiledepth)):
                return

        offsets = self.dataoffsets
        bytecounts = self.databytecounts
        if len(offsets) == 1:
            return offsets[0], bytecounts[0]
        if self.is_stk or all((offsets[i] + bytecounts[i] == offsets[i+1] or
                               bytecounts[i+1] == 0)  # no data/ignore offset
                              for i in range(len(offsets)-1)):
            return offsets[0], sum(bytecounts)

    @lazyattr
    def is_final(self):
        """Return if page's image data is stored in final form.

        Excludes byte-swapping.

        """
        return (self.is_contiguous and self.fillorder == 1 and
                self.predictor == 1 and not self.is_chroma_subsampled)

    @lazyattr
    def is_memmappable(self):
        """Return if page's image data in file can be memory-mapped."""
        return (self.parent.filehandle.is_file and self.is_final and
                (self.bitspersample == 8 or self.parent.is_native) and
                self.is_contiguous[0] % self.dtype.itemsize == 0)

    def __str__(self, detail=0):
        """Return string containing information about page."""
        if self.keyframe != self:
            return TiffFrame.__str__(self, detail)
        attr = ''
        for name in ('memmappable', 'final', 'contiguous'):
            attr = getattr(self, 'is_'+name)
            if attr:
                attr = name.upper()
                break
        info = '  '.join(s for s in (
            'x'.join(str(i) for i in self.shape),
            '%s%s' % (TIFF.SAMPLEFORMAT(self.sampleformat).name,
                      self.bitspersample),
            '|'.join(i for i in (
                TIFF.PHOTOMETRIC(self.photometric).name,
                'TILED' if self.is_tiled else '',
                self.compression.name if self.compression != 1 else '',
                self.planarconfig.name if self.planarconfig != 1 else '',
                self.predictor.name if self.predictor != 1 else '',
                self.fillorder.name if self.fillorder != 1 else '')
                     if i),
            attr,
            '|'.join((f.upper() for f in self.flags))
            ) if s)
        info = "TiffPage %i @%i  %s" % (self.index, self.offset, info)
        if detail <= 0:
            return info
        info = [info]
        tags = self.tags
        tlines = []
        vlines = []
        for tag in sorted(tags.values(), key=lambda x: x.code):
            value = tag.__str__()
            tlines.append(value[:TIFF.PRINT_LINE_WIDTH].lstrip())
            if detail > 1 and len(value) > TIFF.PRINT_LINE_WIDTH:
                vlines.append("%s\n%s" % (tag.name.upper(),
                                          pformat(tag.value)))
        info.append('\n'.join(tlines))
        if detail > 1:
            info.append('\n\n'.join(vlines))
        return '\n\n'.join(info)

    @lazyattr
    def flags(self):
        """Return set of flags."""
        return set((name.lower() for name in sorted(TIFF.FILE_FLAGS)
                    if getattr(self, 'is_' + name)))

    @property
    def ndim(self):
        """Return number of array dimensions."""
        return len(self.shape)

    @property
    def size(self):
        """Return number of elements in array."""
        return product(self.shape)

    @lazyattr
    def andor_tags(self):
        """Return consolidated metadata from Andor tags as dict.

        Remove Andor tags from self.tags.

        """
        if not self.is_andor:
            return
        tags = self.tags
        result = {'Id': tags['AndorId'].value}
        for tag in list(self.tags.values()):
            code = tag.code
            if not 4864 < code < 5031:
                continue
            value = tag.value
            name = tag.name[5:] if len(tag.name) > 5 else tag.name
            result[name] = value
            del tags[tag.name]
        return result

    @lazyattr
    def epics_tags(self):
        """Return consolidated metadata from EPICS areaDetector tags as dict.

        Remove areaDetector tags from self.tags.

        """
        # TODO: obtain test file
        if not self.is_epics:
            return
        result = {}
        tags = self.tags
        for tag in list(self.tags.values()):
            code = tag.code
            if not 65000 < code < 65500:
                continue
            value = tag.value
            if code == 65000:
                result['timeStamp'] = float(value)
            elif code == 65001:
                result['uniqueID'] = int(value)
            elif code == 65002:
                result['epicsTS'] = int(value)
            elif code == 65003:
                result['epicsTS'] = int(value)
            else:
                key, value = value.split(':')
                result[key] = astype(value)
            del tags[tag.name]
        return result

    @property
    def is_tiled(self):
        """Page contains tiled image."""
        return 'TileWidth' in self.tags

    @property
    def is_reduced(self):
        """Page is reduced image of another image."""
        return ('NewSubfileType' in self.tags and
                self.tags['NewSubfileType'].value & 1)

    @property
    def is_chroma_subsampled(self):
        """Page contains chroma subsampled image."""
        return ('YCbCrSubSampling' in self.tags and
                self.tags['YCbCrSubSampling'].value != (1, 1))

    @lazyattr
    def is_imagej(self):
        """Return ImageJ description if exists, else None."""
        for description in (self.description, self.description1):
            if not description:
                return
            if description[:7] == 'ImageJ=':
                return description

    @lazyattr
    def is_shaped(self):
        """Return description containing array shape if exists, else None."""
        for description in (self.description, self.description1):
            if not description:
                return
            if description[:1] == '{' and '"shape":' in description:
                return description
            if description[:6] == 'shape=':
                return description

    @property
    def is_mdgel(self):
        """Page contains MDFileTag tag."""
        return 'MDFileTag' in self.tags

    @property
    def is_mediacy(self):
        """Page contains Media Cybernetics Id tag."""
        return ('MC_Id' in self.tags and
                self.tags['MC_Id'].value[:7] == b'MC TIFF')

    @property
    def is_stk(self):
        """Page contains UIC2Tag tag."""
        return 'UIC2tag' in self.tags

    @property
    def is_lsm(self):
        """Page contains CZ_LSMINFO tag."""
        return 'CZ_LSMINFO' in self.tags

    @property
    def is_fluoview(self):
        """Page contains FluoView MM_STAMP tag."""
        return 'MM_Stamp' in self.tags

    @property
    def is_nih(self):
        """Page contains NIH image header."""
        return 'NIHImageHeader' in self.tags

    @property
    def is_sgi(self):
        """Page contains SGI image and tile depth tags."""
        return 'ImageDepth' in self.tags and 'TileDepth' in self.tags

    @property
    def is_vista(self):
        """Software tag is 'ISS Vista'."""
        return self.software == 'ISS Vista'

    @property
    def is_metaseries(self):
        """Page contains MDS MetaSeries metadata in ImageDescription tag."""
        if self.index > 1 or self.software != 'MetaSeries':
            return False
        d = self.description
        return d.startswith('<MetaData>') and d.endswith('</MetaData>')

    @property
    def is_ome(self):
        """Page contains OME-XML in ImageDescription tag."""
        if self.index > 1 or not self.description:
            return False
        d = self.description
        return d[:14] == '<?xml version=' and d[-6:] == '</OME>'

    @property
    def is_scn(self):
        """Page contains Leica SCN XML in ImageDescription tag."""
        if self.index > 1 or not self.description:
            return False
        d = self.description
        return d[:14] == '<?xml version=' and d[-6:] == '</scn>'

    @property
    def is_micromanager(self):
        """Page contains Micro-Manager metadata."""
        return 'MicroManagerMetadata' in self.tags

    @property
    def is_andor(self):
        """Page contains Andor Technology tags."""
        return 'AndorId' in self.tags

    @property
    def is_pilatus(self):
        """Page contains Pilatus tags."""
        return (self.software[:8] == 'TVX TIFF' and
                self.description[:2] == '# ')

    @property
    def is_epics(self):
        """Page contains EPICS areaDetector tags."""
        return self.description == 'EPICS areaDetector'

    @property
    def is_tvips(self):
        """Page contains TVIPS metadata."""
        return 'TVIPS' in self.tags

    @property
    def is_fei(self):
        """Page contains SFEG or HELIOS metadata."""
        return 'FEI_SFEG' in self.tags or 'FEI_HELIOS' in self.tags

    @property
    def is_sem(self):
        """Page contains Zeiss SEM metadata."""
        return 'CZ_SEM' in self.tags

    @property
    def is_svs(self):
        """Page contains Aperio metadata."""
        return self.description[:20] == 'Aperio Image Library'

    @property
    def is_scanimage(self):
        """Page contains ScanImage metadata."""
        return (self.description[:12] == 'state.config' or
                self.software[:22] == 'SI.LINE_FORMAT_VERSION')


class TiffFrame(object):
    """Lightweight TIFF image file directory (IFD).

    Only a limited number of tag values are read from file, e.g. StripOffsets,
    and StripByteCounts. Other tag values are assumed to be identical with a
    specified TiffPage instance, the keyframe.

    This is intended to reduce resource usage and speed up reading data from
    file, not for introspection of metadata.

    Not compatible with Python 2.

    """
    __slots__ = ('keyframe', 'parent', 'index', 'offset',
                 'dataoffsets', 'databytecounts')

    is_mdgel = False
    tags = {}

    def __init__(self, parent, index, keyframe):
        """Read specified tags from file.

        The file handle position must be at the offset to a valid IFD.

        """
        self.keyframe = keyframe
        self.parent = parent
        self.index = index

        unpack = struct.unpack
        fh = parent.filehandle
        self.offset = fh.tell()
        try:
            tagno = unpack(parent.tagnoformat, fh.read(parent.tagnosize))[0]
            if tagno > 4096:
                raise ValueError("suspicious number of tags")
        except Exception:
            raise ValueError("corrupted page list at offset %i" % self.offset)

        # tags = {}
        tagcodes = {273, 279, 324, 325}  # TIFF.FRAME_TAGS
        tagsize = parent.tagsize
        codeformat = parent.tagformat1[:2]

        data = fh.read(tagsize * tagno)
        index = -tagsize
        for _ in range(tagno):
            index += tagsize
            code = unpack(codeformat, data[index:index+2])[0]
            if code not in tagcodes:
                continue
            try:
                tag = TiffTag(parent, data[index:index+tagsize])
            except TiffTag.Error as e:
                warnings.warn(str(e))
                continue
            if code == 273 or code == 324:
                setattr(self, 'dataoffsets', tag.value)
            elif code == 279 or code == 325:
                setattr(self, 'databytecounts', tag.value)
            # elif code == 270:
            #     tagname = tag.name
            #     if tagname not in tags:
            #         tags[tagname] = bytes2str(tag.value)
            #     elif 'ImageDescription1' not in tags:
            #         tags['ImageDescription1'] = bytes2str(tag.value)
            # else:
            #     tags[tag.name] = tag.value

    def aspage(self):
        """Return TiffPage from file."""
        self.parent.filehandle.seek(self.offset)
        return TiffPage(self.parent, index=self.index, keyframe=None)

    def asarray(self, *args, **kwargs):
        """Read image data from file and return as numpy array."""
        # TODO: fix TypeError on Python 2
        #   "TypeError: unbound method asarray() must be called with TiffPage
        #   instance as first argument (got TiffFrame instance instead)"
        kwargs['validate'] = False
        return TiffPage.asarray(self, *args, **kwargs)

    def asrgb(self, *args, **kwargs):
        """Read image data from file and return RGB image as numpy array."""
        kwargs['validate'] = False
        return TiffPage.asrgb(self, *args, **kwargs)

    @property
    def offsets_bytecounts(self):
        """Return simplified offsets and bytecounts."""
        if self.keyframe.is_contiguous:
            return self.dataoffsets[:1], self.keyframe.is_contiguous[1:]
        return clean_offsets_counts(self.dataoffsets, self.databytecounts)

    @property
    def is_contiguous(self):
        """Return offset and size of contiguous data, else None."""
        if self.keyframe.is_contiguous:
            return self.dataoffsets[0], self.keyframe.is_contiguous[1]

    @property
    def is_memmappable(self):
        """Return if page's image data in file can be memory-mapped."""
        return self.keyframe.is_memmappable

    def __getattr__(self, name):
        """Return attribute from keyframe."""
        if name in TIFF.FRAME_ATTRS:
            return getattr(self.keyframe, name)
        raise AttributeError("'%s' object has no attribute '%s'" %
                             (self.__class__.__name__, name))

    def __str__(self, detail=0):
        """Return string containing information about frame."""
        info = '  '.join(s for s in (
            'x'.join(str(i) for i in self.shape),
            str(self.dtype)))
        return "TiffFrame %i @%i  %s" % (self.index, self.offset, info)


class TiffTag(object):
    """TIFF tag structure.

    Attributes
    ----------
    name : string
        Name of tag.
    code : int
        Decimal code of tag.
    dtype : str
        Datatype of tag data. One of TIFF DATA_FORMATS.
    count : int
        Number of values.
    value : various types
        Tag data as Python object.
    valueoffset : int
        Location of value in file.

    All attributes are read-only.

    """
    __slots__ = ('code', 'count', 'dtype', 'value', 'valueoffset')

    class Error(Exception):
        pass

    def __init__(self, parent, tagheader, **kwargs):
        """Initialize instance from tag header."""
        fh = parent.filehandle
        byteorder = parent.byteorder
        unpack = struct.unpack
        offsetsize = parent.offsetsize

        self.valueoffset = fh.tell() + offsetsize + 4
        code, dtype = unpack(parent.tagformat1, tagheader[:4])
        count, value = unpack(parent.tagformat2, tagheader[4:])

        try:
            dtype = TIFF.DATA_FORMATS[dtype]
        except KeyError:
            raise TiffTag.Error("unknown tag data type %i" % dtype)

        fmt = '%s%i%s' % (byteorder, count * int(dtype[0]), dtype[1])
        size = struct.calcsize(fmt)
        if size > offsetsize or code in TIFF.TAG_READERS:
            self.valueoffset = offset = unpack(parent.offsetformat, value)[0]
            if offset < 8 or offset > fh.size - size:
                raise TiffTag.Error("invalid tag value offset")
            # if offset % 2:
            #     warnings.warn("tag value does not begin on word boundary")
            fh.seek(offset)
            if code in TIFF.TAG_READERS:
                readfunc = TIFF.TAG_READERS[code]
                value = readfunc(fh, byteorder, dtype, count, offsetsize)
            elif code in TIFF.TAGS or dtype[-1] == 's':
                value = unpack(fmt, fh.read(size))
            else:
                value = read_numpy(fh, byteorder, dtype, count, offsetsize)
        else:
            value = unpack(fmt, value[:size])

        process = code not in TIFF.TAG_READERS and code not in TIFF.TAG_TUPLE
        if process and dtype[-1] == 's' and isinstance(value[0], bytes):
            # TIFF ASCII fields can contain multiple strings,
            #   each terminated with a NUL
            value = bytes2str(stripascii(value[0]).strip())
        else:
            if code in TIFF.TAG_ENUM:
                t = TIFF.TAG_ENUM[code]
                try:
                    value = tuple(t(v) for v in value)
                except ValueError as e:
                    warnings.warn(str(e))
            if process:
                if len(value) == 1:
                    value = value[0]

        self.code = code
        self.dtype = dtype
        self.count = count
        self.value = value

    @property
    def name(self):
        return TIFF.TAGS.get(self.code, str(self.code))

    def _fix_lsm_bitspersample(self, parent):
        """Correct LSM bitspersample tag.

        Old LSM writers may use a separate region for two 16-bit values,
        although they fit into the tag value element of the tag.

        """
        if self.code == 258 and self.count == 2:
            # TODO: test this case; need example file
            warnings.warn("correcting LSM bitspersample tag")
            tof = parent.offsetformat[parent.offsetsize]
            self.valueoffset = struct.unpack(tof, self._value)[0]
            parent.filehandle.seek(self.valueoffset)
            self.value = struct.unpack("<HH", parent.filehandle.read(4))

    def __str__(self):
        """Return string containing information about tag."""
        if self.code in TIFF.TAG_ENUM:
            if self.count == 1:
                value = TIFF.TAG_ENUM[self.code](self.value).name
            else:
                value = tuple(v.name for v in self.value)
        elif isinstance(self.value, unicode):
                value = pformat(self.value)
                value = value.replace(u'\n', u'\\n').replace(u'\r', u'')
                value = u'"%s"' % value
        else:
            value = pformat(self.value, linewidth=False, maxlines=2)
            value = str(value).split('\n', 1)[0]

        tcode = "%i%s" % (self.count * int(self.dtype[0]), self.dtype[1])
        line = "TiffTag %i %s  %s @%i  %s" % (
            self.code, self.name, tcode, self.valueoffset, value)
        return line


class TiffPageSeries(object):
    """Series of TIFF pages with compatible shape and data type.

    Attributes
    ----------
    pages : list of TiffPage
        Sequence of TiffPages in series.
    dtype : numpy.dtype or str
        Data type of the image array in series.
    shape : tuple
        Dimensions of the image array in series.
    axes : str
        Labels of axes in shape. See TiffPage.axes.
    offset : int or None
        Position of image data in file if memory-mappable, else None.

    """
    def __init__(self, pages, shape, dtype, axes,
                 parent=None, name=None, transform=None, stype=None):
        """Initialize instance."""
        self.index = 0
        self.pages = pages
        self.shape = tuple(shape)
        self.axes = ''.join(axes)
        self.dtype = numpy.dtype(dtype)
        self.stype = stype if stype else ''
        self.name = name if name else ''
        self.transform = transform
        if parent:
            self.parent = parent
        elif pages:
            self.parent = pages[0].parent
        else:
            self.parent = None

    def asarray(self, out=None):
        """Return image data from series of TIFF pages as numpy array."""
        if self.parent:
            result = self.parent.asarray(series=self, out=out)
            if self.transform is not None:
                result = self.transform(result)
            return result

    @lazyattr
    def offset(self):
        """Return offset to series data in file, if any."""
        if not self.pages:
            return

        pos = 0
        for page in self.pages:
            if page is None:
                return
            if not page.is_final:
                return
            if not pos:
                pos = page.is_contiguous[0] + page.is_contiguous[1]
                continue
            if pos != page.is_contiguous[0]:
                return
            pos += page.is_contiguous[1]

        page = self.pages[0]
        offset = page.is_contiguous[0]
        if (page.is_imagej or page.is_shaped) and len(self.pages) == 1:
            # truncated files
            return offset
        if pos == offset + product(self.shape) * self.dtype.itemsize:
            return offset

    @property
    def ndim(self):
        """Return number of array dimensions."""
        return len(self.shape)

    @property
    def size(self):
        """Return number of elements in array."""
        return int(product(self.shape))

    def __len__(self):
        """Return number of TiffPages in series."""
        return len(self.pages)

    def __getitem__(self, key):
        """Return specified TiffPage."""
        return self.pages[key]

    def __iter__(self):
        """Return iterator over TiffPages in series."""
        return iter(self.pages)

    def __str__(self):
        """Return string with information about series."""
        s = '  '.join(s for s in (
            snipstr("'%s'" % self.name, 20) if self.name else '',
            'x'.join(str(i) for i in self.shape),
            str(self.dtype),
            self.axes,
            self.stype,
            '%i Pages' % len(self.pages),
            ('Offset=%i' % self.offset) if self.offset else '') if s)
        return 'TiffPageSeries %i  %s' % (self.index, s)


class TiffSequence(object):
    """Sequence of TIFF files.

    The image data in all files must match shape, dtype, etc.

    Attributes
    ----------
    files : list
        List of file names.
    shape : tuple
        Shape of image sequence. Excludes shape of image array.
    axes : str
        Labels of axes in shape.

    Examples
    --------
    >>> # read image stack from sequence of TIFF files
    >>> imsave('temp_C001T001.tif', numpy.random.rand(64, 64))
    >>> imsave('temp_C001T002.tif', numpy.random.rand(64, 64))
    >>> tifs = TiffSequence("temp_C001*.tif")
    >>> tifs.shape
    (1, 2)
    >>> tifs.axes
    'CT'
    >>> data = tifs.asarray()
    >>> data.shape
    (1, 2, 64, 64)

    """
    _patterns = {
        'axes': r"""
            # matches Olympus OIF and Leica TIFF series
            _?(?:(q|l|p|a|c|t|x|y|z|ch|tp)(\d{1,4}))
            _?(?:(q|l|p|a|c|t|x|y|z|ch|tp)(\d{1,4}))?
            _?(?:(q|l|p|a|c|t|x|y|z|ch|tp)(\d{1,4}))?
            _?(?:(q|l|p|a|c|t|x|y|z|ch|tp)(\d{1,4}))?
            _?(?:(q|l|p|a|c|t|x|y|z|ch|tp)(\d{1,4}))?
            _?(?:(q|l|p|a|c|t|x|y|z|ch|tp)(\d{1,4}))?
            _?(?:(q|l|p|a|c|t|x|y|z|ch|tp)(\d{1,4}))?
            """}

    class ParseError(Exception):
        pass

    def __init__(self, files, imread=TiffFile, pattern='axes',
                 *args, **kwargs):
        """Initialize instance from multiple files.

        Parameters
        ----------
        files : str, or sequence of str
            Glob pattern or sequence of file names.
            Binary streams are not supported.
        imread : function or class
            Image read function or class with asarray function returning numpy
            array from single file.
        pattern : str
            Regular expression pattern that matches axes names and sequence
            indices in file names.
            By default, this matches Olympus OIF and Leica TIFF series.

        """
        if isinstance(files, basestring):
            files = natural_sorted(glob.glob(files))
        files = list(files)
        if not files:
            raise ValueError("no files found")
        if not isinstance(files[0], basestring):
            raise ValueError("not a file name")
        self.files = files

        if hasattr(imread, 'asarray'):
            # redefine imread
            _imread = imread

            def imread(fname, *args, **kwargs):
                with _imread(fname) as im:
                    return im.asarray(*args, **kwargs)

        self.imread = imread

        self.pattern = self._patterns.get(pattern, pattern)
        try:
            self._parse()
            if not self.axes:
                self.axes = 'I'
        except self.ParseError:
            self.axes = 'I'
            self.shape = (len(files),)
            self._startindex = (0,)
            self._indices = tuple((i,) for i in range(len(files)))

    def __str__(self):
        """Return string with information about image sequence."""
        return "\n".join([
            self.files[0],
            ' size: %i' % len(self.files),
            ' axes: %s' % self.axes,
            ' shape: %s' % str(self.shape)])

    def __len__(self):
        return len(self.files)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        pass

    def asarray(self, out=None, *args, **kwargs):
        """Read image data from all files and return as numpy array.

        The args and kwargs parameters are passed to the imread function.

        Raise IndexError or ValueError if image shapes do not match.

        """
        im = self.imread(self.files[0], *args, **kwargs)
        shape = self.shape + im.shape
        result = create_output(out, shape, dtype=im.dtype)
        result = result.reshape(-1, *im.shape)
        for index, fname in zip(self._indices, self.files):
            index = [i-j for i, j in zip(index, self._startindex)]
            index = numpy.ravel_multi_index(index, self.shape)
            im = self.imread(fname, *args, **kwargs)
            result[index] = im
        result.shape = shape
        return result

    def _parse(self):
        """Get axes and shape from file names."""
        if not self.pattern:
            raise self.ParseError("invalid pattern")
        pattern = re.compile(self.pattern, re.IGNORECASE | re.VERBOSE)
        matches = pattern.findall(self.files[0])
        if not matches:
            raise self.ParseError("pattern does not match file names")
        matches = matches[-1]
        if len(matches) % 2:
            raise self.ParseError("pattern does not match axis name and index")
        axes = ''.join(m for m in matches[::2] if m)
        if not axes:
            raise self.ParseError("pattern does not match file names")

        indices = []
        for fname in self.files:
            matches = pattern.findall(fname)[-1]
            if axes != ''.join(m for m in matches[::2] if m):
                raise ValueError("axes do not match within the image sequence")
            indices.append([int(m) for m in matches[1::2] if m])
        shape = tuple(numpy.max(indices, axis=0))
        startindex = tuple(numpy.min(indices, axis=0))
        shape = tuple(i-j+1 for i, j in zip(shape, startindex))
        if product(shape) != len(self.files):
            warnings.warn("files are missing. Missing data are zeroed")

        self.axes = axes.upper()
        self.shape = shape
        self._indices = indices
        self._startindex = startindex


class FileHandle(object):
    """Binary file handle.

    A limited, special purpose file handler that can:

    * handle embedded files (for CZI within CZI files)
    * re-open closed files (for multi file formats, such as OME-TIFF)
    * read and write numpy arrays and records from file like objects

    Only 'rb' and 'wb' modes are supported. Concurrently reading and writing
    of the same stream is untested.

    When initialized from another file handle, do not use it unless this
    FileHandle is closed.

    Attributes
    ----------
    name : str
        Name of the file.
    path : str
        Absolute path to file.
    size : int
        Size of file in bytes.
    is_file : bool
        If True, file has a filno and can be memory-mapped.

    All attributes are read-only.

    """
    __slots__ = ('_fh', '_file', '_mode', '_name', '_dir', '_lock',
                 '_offset', '_size', '_close', 'is_file')

    def __init__(self, file, mode='rb', name=None, offset=None, size=None):
        """Initialize file handle from file name or another file handle.

        Parameters
        ----------
        file : str, binary stream, or FileHandle
            File name or seekable binary stream, such as a open file
            or BytesIO.
        mode : str
            File open mode in case 'file' is a file name. Must be 'rb' or 'wb'.
        name : str
            Optional name of file in case 'file' is a binary stream.
        offset : int
            Optional start position of embedded file. By default, this is
            the current file position.
        size : int
            Optional size of embedded file. By default, this is the number
            of bytes from the 'offset' to the end of the file.

        """
        self._fh = None
        self._file = file
        self._mode = mode
        self._name = name
        self._dir = ''
        self._offset = offset
        self._size = size
        self._close = True
        self.is_file = False
        self._lock = NullContext()
        self.open()

    def open(self):
        """Open or re-open file."""
        if self._fh:
            return  # file is open

        if isinstance(self._file, basestring):
            # file name
            self._file = os.path.realpath(self._file)
            self._dir, self._name = os.path.split(self._file)
            self._fh = open(self._file, self._mode)
            self._close = True
            if self._offset is None:
                self._offset = 0
        elif isinstance(self._file, FileHandle):
            # FileHandle
            self._fh = self._file._fh
            if self._offset is None:
                self._offset = 0
            self._offset += self._file._offset
            self._close = False
            if not self._name:
                if self._offset:
                    name, ext = os.path.splitext(self._file._name)
                    self._name = "%s@%i%s" % (name, self._offset, ext)
                else:
                    self._name = self._file._name
            if self._mode and self._mode != self._file._mode:
                raise ValueError('FileHandle has wrong mode')
            self._mode = self._file._mode
            self._dir = self._file._dir
        elif hasattr(self._file, 'seek'):
            # binary stream: open file, BytesIO
            try:
                self._file.tell()
            except Exception:
                raise ValueError("binary stream is not seekable")
            self._fh = self._file
            if self._offset is None:
                self._offset = self._file.tell()
            self._close = False
            if not self._name:
                try:
                    self._dir, self._name = os.path.split(self._fh.name)
                except AttributeError:
                    self._name = "Unnamed binary stream"
            try:
                self._mode = self._fh.mode
            except AttributeError:
                pass
        else:
            raise ValueError("The first parameter must be a file name, "
                             "seekable binary stream, or FileHandle")

        if self._offset:
            self._fh.seek(self._offset)

        if self._size is None:
            pos = self._fh.tell()
            self._fh.seek(self._offset, 2)
            self._size = self._fh.tell()
            self._fh.seek(pos)

        try:
            self._fh.fileno()
            self.is_file = True
        except Exception:
            self.is_file = False

    def read(self, size=-1):
        """Read 'size' bytes from file, or until EOF is reached."""
        if size < 0 and self._offset:
            size = self._size
        return self._fh.read(size)

    def write(self, bytestring):
        """Write bytestring to file."""
        return self._fh.write(bytestring)

    def flush(self):
        """Flush write buffers if applicable."""
        return self._fh.flush()

    def memmap_array(self, dtype, shape, offset=0, mode='r', order='C'):
        """Return numpy.memmap of data stored in file."""
        if not self.is_file:
            raise ValueError("Can not memory-map file without fileno")
        return numpy.memmap(self._fh, dtype=dtype, mode=mode,
                            offset=self._offset + offset,
                            shape=shape, order=order)

    def read_array(self, dtype, count=-1, sep="", chunksize=2**25, out=None):
        """Return numpy array from file.

        Work around numpy issue #2230, "numpy.fromfile does not accept
        StringIO object" https://github.com/numpy/numpy/issues/2230.

        """
        fh = self._fh
        dtype = numpy.dtype(dtype)
        size = self._size if count < 0 else count * dtype.itemsize

        if out is None:
            # try:
            #     return numpy.fromfile(fh, dtype, count, sep)
            # except IOError:
            #     # ByteIO
            data = fh.read(size)
            return numpy.fromstring(data, dtype, count, sep)

        # Read data from file in chunks and copy to output array
        shape = out.shape
        size = min(out.nbytes, size)
        out = out.reshape(-1)
        index = 0
        while size > 0:
            data = fh.read(min(chunksize, size))
            datasize = len(data)
            if datasize == 0:
                break
            size -= datasize
            data = numpy.fromstring(data, dtype)
            out[index:index+data.size] = data
            index += data.size

        if hasattr(out, 'flush'):
            out.flush()
        return out.reshape(shape)

    def read_record(self, dtype, shape=1, byteorder=None):
        """Return numpy record from file."""
        rec = numpy.rec
        try:
            record = rec.fromfile(self._fh, dtype, shape, byteorder=byteorder)
        except Exception:
            dtype = numpy.dtype(dtype)
            if shape is None:
                shape = self._size // dtype.itemsize
            size = product(sequence(shape)) * dtype.itemsize
            data = self._fh.read(size)
            record = rec.fromstring(data, dtype, shape, byteorder=byteorder)
        return record[0] if shape == 1 else record

    def write_empty(self, size):
        """Append size bytes to file. Position must be at end of file."""
        if size < 1:
            return
        self._fh.seek(size-1, 1)
        self._fh.write(b'\x00')

    def write_array(self, data):
        """Write numpy array to binary file."""
        try:
            data.tofile(self._fh)
        except Exception:
            # BytesIO
            self._fh.write(data.tostring())

    def tell(self):
        """Return file's current position."""
        return self._fh.tell() - self._offset

    def seek(self, offset, whence=0):
        """Set file's current position."""
        if self._offset:
            if whence == 0:
                self._fh.seek(self._offset + offset, whence)
                return
            elif whence == 2 and self._size > 0:
                self._fh.seek(self._offset + self._size + offset, 0)
                return
        self._fh.seek(offset, whence)

    def close(self):
        """Close file."""
        if self._close and self._fh:
            self._fh.close()
            self._fh = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __getattr__(self, name):
        """Return attribute from underlying file object."""
        if self._offset:
            warnings.warn(
                "FileHandle: '%s' not implemented for embedded files" % name)
        return getattr(self._fh, name)

    @property
    def name(self):
        return self._name

    @property
    def dirname(self):
        return self._dir

    @property
    def path(self):
        return os.path.join(self._dir, self._name)

    @property
    def size(self):
        return self._size

    @property
    def closed(self):
        return self._fh is None

    @property
    def lock(self):
        return self._lock

    @lock.setter
    def lock(self, value):
        self._lock = threading.RLock() if value else NullContext()


class NullContext(object):
    """Null context manager.

    >>> with NullContext():
    ...     pass

    """
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class OpenFileCache(object):
    """Keep files open."""

    __slots__ = ('files', 'past', 'lock', 'size')

    def __init__(self, size, lock=None):
        """Initialize open file cache."""
        self.past = []  # FIFO of opened files
        self.files = {}  # refcounts of opened files
        self.lock = NullContext() if lock is None else lock
        self.size = int(size)

    def open(self, filehandle):
        """Re-open file if necessary."""
        with self.lock:
            if filehandle in self.files:
                self.files[filehandle] += 1
            elif filehandle.closed:
                filehandle.open()
                self.files[filehandle] = 1
                self.past.append(filehandle)

    def close(self, filehandle):
        """Close openend file if no longer used."""
        with self.lock:
            if filehandle in self.files:
                self.files[filehandle] -= 1
                # trim the file cache
                index = 0
                size = len(self.past)
                while size > self.size and index < size:
                    filehandle = self.past[index]
                    if self.files[filehandle] == 0:
                        filehandle.close()
                        del self.files[filehandle]
                        del self.past[index]
                        size -= 1
                    else:
                        index += 1

    def clear(self):
        """Close all opened files if not in use."""
        with self.lock:
            for filehandle, refcount in list(self.files.items()):
                if refcount == 0:
                    filehandle.close()
                    del self.files[filehandle]
                    del self.past[self.past.index(filehandle)]


class LazyConst(object):
    """Class whose attributes are computed on first access from its methods."""
    def __init__(self, cls):
        self._cls = cls
        self.__doc__ = getattr(cls, '__doc__')

    def __getattr__(self, name):
        func = getattr(self._cls, name)
        if not callable(func):
            return func
        try:
            value = func()
        except TypeError:
            # Python 2 unbound method
            value = func.__func__()
        setattr(self, name, value)
        return value


@LazyConst
class TIFF(object):
    """Namespace for module constants."""

    def TAGS():
        # TIFF tag codes and names
        return {
            254: 'NewSubfileType',
            255: 'SubfileType',
            256: 'ImageWidth',
            257: 'ImageLength',
            258: 'BitsPerSample',
            259: 'Compression',
            262: 'PhotometricInterpretation',
            263: 'Threshholding',
            264: 'CellWidth',
            265: 'CellLength',
            266: 'FillOrder',
            269: 'DocumentName',
            270: 'ImageDescription',
            271: 'Make',
            272: 'Model',
            273: 'StripOffsets',
            274: 'Orientation',
            277: 'SamplesPerPixel',
            278: 'RowsPerStrip',
            279: 'StripByteCounts',
            280: 'MinSampleValue',
            281: 'MaxSampleValue',
            282: 'XResolution',
            283: 'YResolution',
            284: 'PlanarConfiguration',
            285: 'PageName',
            286: 'XPosition',
            287: 'YPosition',
            288: 'FreeOffsets',
            289: 'FreeByteCounts',
            290: 'GrayResponseUnit',
            291: 'GrayResponseCurve',
            292: 'T4Options',
            293: 'T6Options',
            296: 'ResolutionUnit',
            297: 'PageNumber',
            300: 'ColorResponseUnit',
            301: 'TransferFunction',
            305: 'Software',
            306: 'DateTime',
            315: 'Artist',
            316: 'HostComputer',
            317: 'Predictor',
            318: 'WhitePoint',
            319: 'PrimaryChromaticities',
            320: 'ColorMap',
            321: 'HalftoneHints',
            322: 'TileWidth',
            323: 'TileLength',
            324: 'TileOffsets',
            325: 'TileByteCounts',
            326: 'BadFaxLines',
            327: 'CleanFaxData',
            328: 'ConsecutiveBadFaxLines',
            330: 'SubIFDs',
            332: 'InkSet',
            333: 'InkNames',
            334: 'NumberOfInks',
            336: 'DotRange',
            337: 'TargetPrinter',
            338: 'ExtraSamples',
            339: 'SampleFormat',
            340: 'SMinSampleValue',
            341: 'SMaxSampleValue',
            342: 'TransferRange',
            343: 'ClipPath',
            344: 'XClipPathUnits',
            345: 'YClipPathUnits',
            346: 'Indexed',
            347: 'JPEGTables',
            351: 'OPIProxy',
            400: 'GlobalParametersIFD',
            401: 'ProfileType',
            402: 'FaxProfile',
            403: 'CodingMethods',
            404: 'VersionYear',
            405: 'ModeNumber',
            433: 'Decode',
            434: 'DefaultImageColor',
            435: 'T82Options',
            512: 'JPEGProc',
            513: 'JPEGInterchangeFormat',
            514: 'JPEGInterchangeFormatLength',
            515: 'JPEGRestartInterval',
            517: 'JPEGLosslessPredictors',
            518: 'JPEGPointTransforms',
            519: 'JPEGQTables',
            520: 'JPEGDCTables',
            521: 'JPEGACTables',
            529: 'YCbCrCoefficients',
            530: 'YCbCrSubSampling',
            531: 'YCbCrPositioning',
            532: 'ReferenceBlackWhite',
            559: 'StripRowCounts',
            700: 'XMP',
            4864: 'AndorId',  # TODO: Andor Technology 4864 - 5030
            4869: 'AndorTemperature',
            4876: 'AndorExposureTime',
            4878: 'AndorKineticCycleTime',
            4879: 'AndorAccumulations',
            4881: 'AndorAcquisitionCycleTime',
            4882: 'AndorReadoutTime',
            4884: 'AndorPhotonCounting',
            4885: 'AndorEmDacLevel',
            4890: 'AndorFrames',
            4896: 'AndorHorizontalFlip',
            4897: 'AndorVerticalFlip',
            4898: 'AndorClockwise',
            4899: 'AndorCounterClockwise',
            4904: 'AndorVerticalClockVoltage',
            4905: 'AndorVerticalShiftSpeed',
            4907: 'AndorPreAmpSetting',
            4908: 'AndorCameraSerial',
            4911: 'AndorActualTemperature',
            4912: 'AndorBaselineClamp',
            4913: 'AndorPrescans',
            4914: 'AndorModel',
            4915: 'AndorChipSizeX',
            4916: 'AndorChipSizeY',
            4944: 'AndorBaselineOffset',
            4966: 'AndorSoftwareVersion',
            # Private tags
            32781: 'ImageID',
            32932: 'WangAnnotation',
            32995: 'Matteing',
            32996: 'DataType',
            32997: 'ImageDepth',
            32998: 'TileDepth',
            33300: 'ImageFullWidth',
            33301: 'ImageFullLength',
            33302: 'TextureFormat',
            33303: 'TextureWrapModes',
            33304: 'FieldOfViewCotangent',
            33305: 'MatrixWorldToScreen',
            33306: 'MatrixWorldToCamera',
            33421: 'CFARepeatPatternDim',
            33422: 'CFAPattern',
            33432: 'Copyright',
            33445: 'MDFileTag',
            33446: 'MDScalePixel',
            33447: 'MDColorTable',
            33448: 'MDLabName',
            33449: 'MDSampleInfo',
            33450: 'MDPrepDate',
            33451: 'MDPrepTime',
            33452: 'MDFileUnits',
            33550: 'ModelPixelScaleTag',
            33628: 'UIC1tag',  # Metamorph  Universal Imaging Corp STK
            33629: 'UIC2tag',
            33630: 'UIC3tag',
            33631: 'UIC4tag',
            33723: 'IPTC',
            33918: 'INGRPacketDataTag',
            33919: 'INGRFlagRegisters',
            33920: 'IrasBTransformationMatrix',
            33922: 'ModelTiepointTag',
            34118: 'CZ_SEM',  # Zeiss SEM
            34122: 'IPLAB',  # number of images
            34264: 'ModelTransformationTag',
            34361: 'MM_Header',
            34362: 'MM_Stamp',
            34363: 'MM_Unknown',
            34377: 'Photoshop',
            34386: 'MM_UserBlock',
            34412: 'CZ_LSMINFO',
            34665: 'ExifIFD',
            34675: 'ICCProfile',
            34680: 'FEI_SFEG',  #
            34682: 'FEI_HELIOS',  #
            34683: 'FEI_TITAN',  #
            34732: 'ImageLayer',
            34735: 'GeoKeyDirectoryTag',
            34736: 'GeoDoubleParamsTag',
            34737: 'GeoAsciiParamsTag',
            34853: 'GPSIFD',
            34908: 'HylaFAXFaxRecvParams',
            34909: 'HylaFAXFaxSubAddress',
            34910: 'HylaFAXFaxRecvTime',
            34911: 'FaxDcs',
            37439: 'StoNits',
            37679: 'MODI_TXT',  # Microsoft Office Document Imaging
            37681: 'MODI_POS',
            37680: 'MODI_OLE',
            37706: 'TVIPS',  # offset to TemData structure
            37707: 'TVIPS1',
            37708: 'TVIPS2',  # same TemData structure as undefined
            37724: 'ImageSourceData',
            40001: 'MC_IpWinScal',  # Media Cybernetics
            40100: 'MC_IdOld',
            40965: 'InteroperabilityIFD',
            42112: 'GDAL_METADATA',
            42113: 'GDAL_NODATA',
            43314: 'NIHImageHeader',
            50215: 'OceScanjobDescription',
            50216: 'OceApplicationSelector',
            50217: 'OceIdentificationNumber',
            50218: 'OceImageLogicCharacteristics',
            50288: 'MC_Id',  # Media Cybernetics
            50289: 'MC_XYPosition',
            50290: 'MC_ZPosition',
            50291: 'MC_XYCalibration',
            50292: 'MC_LensCharacteristics',
            50293: 'MC_ChannelName',
            50294: 'MC_ExcitationWavelength',
            50295: 'MC_TimeStamp',
            50296: 'MC_FrameProperties',
            50706: 'DNGVersion',
            50707: 'DNGBackwardVersion',
            50708: 'UniqueCameraModel',
            50709: 'LocalizedCameraModel',
            50710: 'CFAPlaneColor',
            50711: 'CFALayout',
            50712: 'LinearizationTable',
            50713: 'BlackLevelRepeatDim',
            50714: 'BlackLevel',
            50715: 'BlackLevelDeltaH',
            50716: 'BlackLevelDeltaV',
            50717: 'WhiteLevel',
            50718: 'DefaultScale',
            50719: 'DefaultCropOrigin',
            50720: 'DefaultCropSize',
            50721: 'ColorMatrix1',
            50722: 'ColorMatrix2',
            50723: 'CameraCalibration1',
            50724: 'CameraCalibration2',
            50725: 'ReductionMatrix1',
            50726: 'ReductionMatrix2',
            50727: 'AnalogBalance',
            50728: 'AsShotNeutral',
            50729: 'AsShotWhiteXY',
            50730: 'BaselineExposure',
            50731: 'BaselineNoise',
            50732: 'BaselineSharpness',
            50733: 'BayerGreenSplit',
            50734: 'LinearResponseLimit',
            50735: 'CameraSerialNumber',
            50736: 'LensInfo',
            50737: 'ChromaBlurRadius',
            50738: 'AntiAliasStrength',
            50739: 'ShadowScale',
            50740: 'DNGPrivateData',
            50741: 'MakerNoteSafety',
            50778: 'CalibrationIlluminant1',
            50779: 'CalibrationIlluminant2',
            50780: 'BestQualityScale',
            50781: 'RawDataUniqueID',
            50784: 'AliasLayerMetadata',
            50827: 'OriginalRawFileName',
            50828: 'OriginalRawFileData',
            50829: 'ActiveArea',
            50830: 'MaskedAreas',
            50831: 'AsShotICCProfile',
            50832: 'AsShotPreProfileMatrix',
            50833: 'CurrentICCProfile',
            50834: 'CurrentPreProfileMatrix',
            50838: 'IJMetadataByteCounts',
            50839: 'IJMetadata',
            51023: 'FibicsXML',  #
            51123: 'MicroManagerMetadata',
            65200: 'FlexXML',  #
            65563: 'PerSample',
        }

    def TAG_NAMES():
        return {v: c for c, v in TIFF.TAGS.items()}

    def TAG_READERS():
        # Map TIFF tag codes to import functions
        return {
            320: read_colormap,
            700: read_bytes,  # read_utf8,
            34377: read_numpy,
            33723: read_bytes,
            34675: read_bytes,
            33628: read_uic1tag,  # Universal Imaging Corp STK
            33629: read_uic2tag,
            33630: read_uic3tag,
            33631: read_uic4tag,
            34118: read_cz_sem,  # Carl Zeiss SEM
            34361: read_mm_header,  # Olympus FluoView
            34362: read_mm_stamp,
            34363: read_numpy,  # MM_Unknown
            34386: read_numpy,  # MM_UserBlock
            34412: read_cz_lsminfo,  # Carl Zeiss LSM
            34680: read_fei_metadata,  # S-FEG
            34682: read_fei_metadata,  # Helios NanoLab
            37706: read_tvips_header,  # TVIPS EMMENU
            43314: read_nih_image_header,
            # 40001: read_bytes,
            40100: read_bytes,
            50288: read_bytes,
            50296: read_bytes,
            50839: read_bytes,
            51123: read_json,
            34665: read_exif_ifd,
            34853: read_gps_ifd,
            40965: read_interoperability_ifd
        }

    def TAG_TUPLE():
        # Tags whose values must be stored as tuples
        return frozenset((273, 279, 324, 325, 530, 531))

    def TAG_ATTRIBUTES():
        #  Map tag codes to TiffPage attribute names
        return {
            'ImageWidth': 'imagewidth',
            'ImageLength': 'imagelength',
            'BitsPerSample': 'bitspersample',
            'Compression': 'compression',
            'PlanarConfiguration': 'planarconfig',
            'FillOrder': 'fillorder',
            'PhotometricInterpretation': 'photometric',
            'ColorMap': 'colormap',
            'ImageDescription': 'description',
            'ImageDescription1': 'description1',
            'SamplesPerPixel': 'samplesperpixel',
            'RowsPerStrip': 'rowsperstrip',
            'Software': 'software',
            'Predictor': 'predictor',
            'TileWidth': 'tilewidth',
            'TileLength': 'tilelength',
            'ExtraSamples': 'extrasamples',
            'SampleFormat': 'sampleformat',
            'ImageDepth': 'imagedepth',
            'TileDepth': 'tiledepth',
        }

    def TAG_ENUM():
        return {
            # 254: TIFF.FILETYPE,
            255: TIFF.OFILETYPE,
            259: TIFF.COMPRESSION,
            262: TIFF.PHOTOMETRIC,
            263: TIFF.THRESHHOLD,
            266: TIFF.FILLORDER,
            274: TIFF.ORIENTATION,
            284: TIFF.PLANARCONFIG,
            290: TIFF.GRAYRESPONSEUNIT,
            # 292: TIFF.GROUP3OPT,
            # 293: TIFF.GROUP4OPT,
            296: TIFF.RESUNIT,
            300: TIFF.COLORRESPONSEUNIT,
            317: TIFF.PREDICTOR,
            338: TIFF.EXTRASAMPLE,
            339: TIFF.SAMPLEFORMAT,
            # 512: TIFF.JPEGPROC,
            # 531: TIFF.YCBCRPOSITION,
        }

    def FILETYPE():
        class FILETYPE(enum.IntFlag):
            # Python 3.6 only
            UNDEFINED = 0
            REDUCEDIMAGE = 1
            PAGE = 2
            MASK = 4
        return FILETYPE

    def OFILETYPE():
        class OFILETYPE(enum.IntEnum):
            UNDEFINED = 0
            IMAGE = 1
            REDUCEDIMAGE = 2
            PAGE = 3
        return OFILETYPE

    def COMPRESSION():
        class COMPRESSION(enum.IntEnum):
            NONE = 1  # Uncompressed
            CCITTRLE = 2  # CCITT 1D
            CCITT_T4 = 3  # 'T4/Group 3 Fax',
            CCITT_T6 = 4  # 'T6/Group 4 Fax',
            LZW = 5
            OJPEG = 6  # old-style JPEG
            JPEG = 7
            ADOBE_DEFLATE = 8
            JBIG_BW = 9
            JBIG_COLOR = 10
            JPEG_99 = 99
            KODAK_262 = 262
            NEXT = 32766
            SONY_ARW = 32767
            PACKED_RAW = 32769
            SAMSUNG_SRW = 32770
            CCIRLEW = 32771
            SAMSUNG_SRW2 = 32772
            PACKBITS = 32773
            THUNDERSCAN = 32809
            IT8CTPAD = 32895
            IT8LW = 32896
            IT8MP = 32897
            IT8BL = 32898
            PIXARFILM = 32908
            PIXARLOG = 32909
            DEFLATE = 32946
            DCS = 32947
            APERIO_JP2000_YCBC = 33003  # Leica Aperio
            APERIO_JP2000_RGB = 33005  # Leica Aperio
            JBIG = 34661
            SGILOG = 34676
            SGILOG24 = 34677
            JPEG2000 = 34712
            NIKON_NEF = 34713
            JBIG2 = 34715
            MDI_BINARY = 34718  # 'Microsoft Document Imaging
            MDI_PROGRESSIVE = 34719  # 'Microsoft Document Imaging
            MDI_VECTOR = 34720  # 'Microsoft Document Imaging
            JPEG_LOSSY = 34892
            LZMA = 34925
            OPS_PNG = 34933  # Objective Pathology Services
            OPS_JPEGXR = 34934  # Objective Pathology Services
            KODAK_DCR = 65000
            PENTAX_PEF = 65535
            # def __bool__(self): return self != 1  # Python 3.6 only
        return COMPRESSION

    def PHOTOMETRIC():
        class PHOTOMETRIC(enum.IntEnum):
            MINISWHITE = 0
            MINISBLACK = 1
            RGB = 2
            PALETTE = 3
            MASK = 4
            SEPARATED = 5  # CMYK
            YCBCR = 6
            CIELAB = 8
            ICCLAB = 9
            ITULAB = 10
            CFA = 32803  # Color Filter Array
            LOGL = 32844
            LOGLUV = 32845
            LINEAR_RAW = 34892
        return PHOTOMETRIC

    def THRESHHOLD():
        class THRESHHOLD(enum.IntEnum):
            BILEVEL = 1
            HALFTONE = 2
            ERRORDIFFUSE = 3
        return THRESHHOLD

    def FILLORDER():
        class FILLORDER(enum.IntEnum):
            MSB2LSB = 1
            LSB2MSB = 2
        return FILLORDER

    def ORIENTATION():
        class ORIENTATION(enum.IntEnum):
            TOPLEFT = 1
            TOPRIGHT = 2
            BOTRIGHT = 3
            BOTLEFT = 4
            LEFTTOP = 5
            RIGHTTOP = 6
            RIGHTBOT = 7
            LEFTBOT = 8
        return ORIENTATION

    def PLANARCONFIG():
        class PLANARCONFIG(enum.IntEnum):
            CONTIG = 1
            SEPARATE = 2
        return PLANARCONFIG

    def GRAYRESPONSEUNIT():
        class GRAYRESPONSEUNIT(enum.IntEnum):
            _10S = 1
            _100S = 2
            _1000S = 3
            _10000S = 4
            _100000S = 5
        return GRAYRESPONSEUNIT

    def GROUP4OPT():
        class GROUP4OPT(enum.IntEnum):
            UNCOMPRESSED = 2
        return GROUP4OPT

    def RESUNIT():
        class RESUNIT(enum.IntEnum):
            NONE = 1
            INCH = 2
            CENTIMETER = 3
            # def __bool__(self): return self != 1  # Python 3.6 only
        return RESUNIT

    def COLORRESPONSEUNIT():
        class COLORRESPONSEUNIT(enum.IntEnum):
            _10S = 1
            _100S = 2
            _1000S = 3
            _10000S = 4
            _100000S = 5
        return COLORRESPONSEUNIT

    def PREDICTOR():
        class PREDICTOR(enum.IntEnum):
            NONE = 1
            HORIZONTAL = 2
            FLOATINGPOINT = 3
            # def __bool__(self): return self != 1  # Python 3.6 only
        return PREDICTOR

    def EXTRASAMPLE():
        class EXTRASAMPLE(enum.IntEnum):
            UNSPECIFIED = 0
            ASSOCALPHA = 1
            UNASSALPHA = 2
        return EXTRASAMPLE

    def SAMPLEFORMAT():
        class SAMPLEFORMAT(enum.IntEnum):
            UINT = 1
            INT = 2
            IEEEFP = 3
            VOID = 4
            COMPLEXINT = 5
            COMPLEXIEEEFP = 6
        return SAMPLEFORMAT

    def DATATYPES():
        class DATATYPES(enum.IntEnum):
            NOTYPE = 0
            BYTE = 1
            ASCII = 2
            SHORT = 3
            LONG = 4
            RATIONAL = 5
            SBYTE = 6
            UNDEFINED = 7
            SSHORT = 8
            SLONG = 9
            SRATIONAL = 10
            FLOAT = 11
            DOUBLE = 12
            IFD = 13
            UNICODE = 14
            COMPLEX = 15
            LONG8 = 16
            SLONG8 = 17
            IFD8 = 18
        return DATATYPES

    def DATA_FORMATS():
        # Map TIFF DATATYPES to Python struct formats
        return {
            1: '1B',   # BYTE 8-bit unsigned integer.
            2: '1s',   # ASCII 8-bit byte that contains a 7-bit ASCII code;
                       #   the last byte must be NULL (binary zero).
            3: '1H',   # SHORT 16-bit (2-byte) unsigned integer
            4: '1I',   # LONG 32-bit (4-byte) unsigned integer.
            5: '2I',   # RATIONAL Two LONGs: the first represents the numerator
                       #   of a fraction; the second, the denominator.
            6: '1b',   # SBYTE An 8-bit signed (twos-complement) integer.
            7: '1p',   # UNDEFINED An 8-bit byte that may contain anything,
                       #   depending on the definition of the field.
            8: '1h',   # SSHORT A 16-bit (2-byte) signed (twos-complement)
                       #   integer.
            9: '1i',   # SLONG A 32-bit (4-byte) signed (twos-complement)
                       #   integer.
            10: '2i',  # SRATIONAL Two SLONGs: the first represents the
                       #   numerator of a fraction, the second the denominator.
            11: '1f',  # FLOAT Single precision (4-byte) IEEE format.
            12: '1d',  # DOUBLE Double precision (8-byte) IEEE format.
            13: '1I',  # IFD unsigned 4 byte IFD offset.
            # 14: '',  # UNICODE
            # 15: '',  # COMPLEX
            16: '1Q',  # LONG8 unsigned 8 byte integer (BigTiff)
            17: '1q',  # SLONG8 signed 8 byte integer (BigTiff)
            18: '1Q',  # IFD8 unsigned 8 byte IFD offset (BigTiff)
        }

    def DATA_DTYPES():
        # Map numpy dtypes to TIFF DATATYPES
        return {'B': 1, 's': 2, 'H': 3, 'I': 4, '2I': 5, 'b': 6,
                'h': 8, 'i': 9, '2i': 10, 'f': 11, 'd': 12, 'Q': 16, 'q': 17}

    def SAMPLE_DTYPES():
        # Map TIFF SampleFormats and BitsPerSample to numpy dtype
        return {
            (1, 1): '?',  # bitmap
            (1, 2): 'B',
            (1, 3): 'B',
            (1, 4): 'B',
            (1, 5): 'B',
            (1, 6): 'B',
            (1, 7): 'B',
            (1, 8): 'B',
            (1, 9): 'H',
            (1, 10): 'H',
            (1, 11): 'H',
            (1, 12): 'H',
            (1, 13): 'H',
            (1, 14): 'H',
            (1, 15): 'H',
            (1, 16): 'H',
            (1, 17): 'I',
            (1, 18): 'I',
            (1, 19): 'I',
            (1, 20): 'I',
            (1, 21): 'I',
            (1, 22): 'I',
            (1, 23): 'I',
            (1, 24): 'I',
            (1, 25): 'I',
            (1, 26): 'I',
            (1, 27): 'I',
            (1, 28): 'I',
            (1, 29): 'I',
            (1, 30): 'I',
            (1, 31): 'I',
            (1, 32): 'I',
            (1, 64): 'Q',
            (2, 8): 'b',
            (2, 16): 'h',
            (2, 32): 'i',
            (2, 64): 'q',
            (3, 16): 'e',
            (3, 32): 'f',
            (3, 64): 'd',
            (6, 64): 'F',
            (6, 128): 'D',
            (1, (5, 6, 5)): 'B',
        }

    def DECOMPESSORS():
        decompressors = {
            None: identityfunc,
            1: identityfunc,
            5: decode_lzw,
            # 7: decode_jpeg,
            8: zlib.decompress,
            32946: zlib.decompress,
            32773: decode_packbits,
        }
        if lzma:
            decompressors[34925] = lzma.decompress
        return decompressors

    def FRAME_ATTRS():
        # Attributes that a TiffFrame shares with its keyframe
        return set('shape ndim size dtype axes is_final'.split())

    def FILE_FLAGS():
        # TiffFile and TiffPage 'is_\*' attributes
        exclude = set('reduced final memmappable contiguous '
                      'chroma_subsampled'.split())
        return set(a[3:] for a in dir(TiffPage)
                   if a[:3] == 'is_' and a[3:] not in exclude)

    def FILE_EXTENSIONS():
        # TIFF file extensions
        return tuple('tif tiff ome.tif lsm stk '
                     'gel seq svs bif tf8 tf2 btf'.split())

    def FILEOPEN_FILTER():
        # String for use in Windows File Open box
        return [("%s files" % ext.upper(), "*.%s" % ext)
                for ext in TIFF.FILE_EXTENSIONS] + [("allfiles", "*")]

    def AXES_LABELS():
        # TODO: is there a standard for character axes labels?
        axes = {
            'X': 'width',
            'Y': 'height',
            'Z': 'depth',
            'S': 'sample',  # rgb(a)
            'I': 'series',  # general sequence, plane, page, IFD
            'T': 'time',
            'C': 'channel',  # color, emission wavelength
            'A': 'angle',
            'P': 'phase',  # formerly F    # P is Position in LSM!
            'R': 'tile',  # region, point, mosaic
            'H': 'lifetime',  # histogram
            'E': 'lambda',  # excitation wavelength
            'L': 'exposure',  # lux
            'V': 'event',
            'Q': 'other',
            'M': 'mosaic',  # LSM 6
        }
        axes.update(dict((v, k) for k, v in axes.items()))
        return axes

    def ANDOR_TAGS():
        # Andor Technology tags #4864 - 5030
        return set(range(4864, 5030))

    def EXIF_TAGS():
        return {
            33434: 'ExposureTime',
            33437: 'FNumber',
            34850: 'ExposureProgram',
            34852: 'SpectralSensitivity',
            34855: 'ISOSpeedRatings',
            34856: 'OECF',
            34858: 'TimeZoneOffset',
            34859: 'SelfTimerMode',
            34864: 'SensitivityType',
            34865: 'StandardOutputSensitivity',
            34866: 'RecommendedExposureIndex',
            34867: 'ISOSpeed',
            34868: 'ISOSpeedLatitudeyyy',
            34869: 'ISOSpeedLatitudezzz',
            36864: 'ExifVersion',
            36867: 'DateTimeOriginal',
            36868: 'DateTimeDigitized',
            36873: 'GooglePlusUploadCode',
            36880: 'OffsetTime',
            36881: 'OffsetTimeOriginal',
            36882: 'OffsetTimeDigitized',
            37121: 'ComponentsConfiguration',
            37122: 'CompressedBitsPerPixel',
            37377: 'ShutterSpeedValue',
            37378: 'ApertureValue',
            37379: 'BrightnessValue',
            37380: 'ExposureBiasValue',
            37381: 'MaxApertureValue',
            37382: 'SubjectDistance',
            37383: 'MeteringMode',
            37384: 'LightSource',
            37385: 'Flash',
            37386: 'FocalLength',
            37393: 'ImageNumber',
            37394: 'SecurityClassification',
            37395: 'ImageHistory',
            37396: 'SubjectArea',
            37500: 'MakerNote',
            37510: 'UserComment',
            37520: 'SubsecTime',
            37521: 'SubsecTimeOriginal',
            37522: 'SubsecTimeDigitized',
            37888: 'Temperature',
            37889: 'Humidity',
            37890: 'Pressure',
            37891: 'WaterDepth',
            37892: 'Acceleration',
            37893: 'CameraElevationAngle',
            40960: 'FlashpixVersion',
            40961: 'ColorSpace',
            40962: 'PixelXDimension',
            40963: 'PixelYDimension',
            40964: 'RelatedSoundFile',
            41483: 'FlashEnergy',
            41484: 'SpatialFrequencyResponse',
            41486: 'FocalPlaneXResolution',
            41487: 'FocalPlaneYResolution',
            41488: 'FocalPlaneResolutionUnit',
            41492: 'SubjectLocation',
            41493: 'ExposureIndex',
            41495: 'SensingMethod',
            41728: 'FileSource',
            41729: 'SceneType',
            41730: 'CFAPattern',
            41985: 'CustomRendered',
            41986: 'ExposureMode',
            41987: 'WhiteBalance',
            41988: 'DigitalZoomRatio',
            41989: 'FocalLengthIn35mmFilm',
            41990: 'SceneCaptureType',
            41991: 'GainControl',
            41992: 'Contrast',
            41993: 'Saturation',
            41994: 'Sharpness',
            41995: 'DeviceSettingDescription',
            41996: 'SubjectDistanceRange',
            42016: 'ImageUniqueID',
            42032: 'CameraOwnerName',
            42033: 'BodySerialNumber',
            42034: 'LensSpecification',
            42035: 'LensMake',
            42036: 'LensModel',
            42037: 'LensSerialNumber',
            42240: 'Gamma',
            59932: 'Padding',
            59933: 'OffsetSchema',
            65000: 'OwnerName',
            65001: 'SerialNumber',
            65002: 'Lens',
            65100: 'RawFile',
            65101: 'Converter',
            65102: 'WhiteBalance',
            65105: 'Exposure',
            65106: 'Shadows',
            65107: 'Brightness',
            65108: 'Contrast',
            65109: 'Saturation',
            65110: 'Sharpness',
            65111: 'Smoothness',
            65112: 'MoireFilter',
        }

    def GPS_TAGS():
        return {
            0: 'GPSVersionID',
            1: 'GPSLatitudeRef',
            2: 'GPSLatitude',
            3: 'GPSLongitudeRef',
            4: 'GPSLongitude',
            5: 'GPSAltitudeRef',
            6: 'GPSAltitude',
            7: 'GPSTimeStamp',
            8: 'GPSSatellites',
            9: 'GPSStatus',
            10: 'GPSMeasureMode',
            11: 'GPSDOP',
            12: 'GPSSpeedRef',
            13: 'GPSSpeed',
            14: 'GPSTrackRef',
            15: 'GPSTrack',
            16: 'GPSImgDirectionRef',
            17: 'GPSImgDirection',
            18: 'GPSMapDatum',
            19: 'GPSDestLatitudeRef',
            20: 'GPSDestLatitude',
            21: 'GPSDestLongitudeRef',
            22: 'GPSDestLongitude',
            23: 'GPSDestBearingRef',
            24: 'GPSDestBearing',
            25: 'GPSDestDistanceRef',
            26: 'GPSDestDistance',
            27: 'GPSProcessingMethod',
            28: 'GPSAreaInformation',
            29: 'GPSDateStamp',
            30: 'GPSDifferential',
            31: 'GPSHPositioningError',
        }

    def IOP_TAGS():
        return {
            1: 'InteroperabilityIndex',
            2: 'InteroperabilityVersion',
            4096: 'RelatedImageFileFormat',
            4097: 'RelatedImageWidth',
            4098: 'RelatedImageLength',
        }

    def CZ_LSMINFO():
        return [
            ('MagicNumber', 'u4'),
            ('StructureSize', 'i4'),
            ('DimensionX', 'i4'),
            ('DimensionY', 'i4'),
            ('DimensionZ', 'i4'),
            ('DimensionChannels', 'i4'),
            ('DimensionTime', 'i4'),
            ('DataType', 'i4'),  # DATATYPES
            ('ThumbnailX', 'i4'),
            ('ThumbnailY', 'i4'),
            ('VoxelSizeX', 'f8'),
            ('VoxelSizeY', 'f8'),
            ('VoxelSizeZ', 'f8'),
            ('OriginX', 'f8'),
            ('OriginY', 'f8'),
            ('OriginZ', 'f8'),
            ('ScanType', 'u2'),
            ('SpectralScan', 'u2'),
            ('TypeOfData', 'u4'),  # TYPEOFDATA
            ('OffsetVectorOverlay', 'u4'),
            ('OffsetInputLut', 'u4'),
            ('OffsetOutputLut', 'u4'),
            ('OffsetChannelColors', 'u4'),
            ('TimeIntervall', 'f8'),
            ('OffsetChannelDataTypes', 'u4'),
            ('OffsetScanInformation', 'u4'),  # SCANINFO
            ('OffsetKsData', 'u4'),
            ('OffsetTimeStamps', 'u4'),
            ('OffsetEventList', 'u4'),
            ('OffsetRoi', 'u4'),
            ('OffsetBleachRoi', 'u4'),
            ('OffsetNextRecording', 'u4'),
            # LSM 2.0 ends here
            ('DisplayAspectX', 'f8'),
            ('DisplayAspectY', 'f8'),
            ('DisplayAspectZ', 'f8'),
            ('DisplayAspectTime', 'f8'),
            ('OffsetMeanOfRoisOverlay', 'u4'),
            ('OffsetTopoIsolineOverlay', 'u4'),
            ('OffsetTopoProfileOverlay', 'u4'),
            ('OffsetLinescanOverlay', 'u4'),
            ('ToolbarFlags', 'u4'),
            ('OffsetChannelWavelength', 'u4'),
            ('OffsetChannelFactors', 'u4'),
            ('ObjectiveSphereCorrection', 'f8'),
            ('OffsetUnmixParameters', 'u4'),
            # LSM 3.2, 4.0 end here
            ('OffsetAcquisitionParameters', 'u4'),
            ('OffsetCharacteristics', 'u4'),
            ('OffsetPalette', 'u4'),
            ('TimeDifferenceX', 'f8'),
            ('TimeDifferenceY', 'f8'),
            ('TimeDifferenceZ', 'f8'),
            ('InternalUse1', 'u4'),
            ('DimensionP', 'i4'),
            ('DimensionM', 'i4'),
            ('DimensionsReserved', '16i4'),
            ('OffsetTilePositions', 'u4'),
            ('', '9u4'),  # Reserved
            ('OffsetPositions', 'u4'),
            # ('', '21u4'),  # must be 0
        ]

    def CZ_LSMINFO_READERS():
        # Import functions for CZ_LSMINFO sub-records
        # TODO: read more CZ_LSMINFO sub-records
        return {
            'ScanInformation': read_lsm_scaninfo,
            'TimeStamps': read_lsm_timestamps,
            'EventList': read_lsm_eventlist,
            'ChannelColors': read_lsm_channelcolors,
            'Positions': read_lsm_floatpairs,
            'TilePositions': read_lsm_floatpairs,
            'VectorOverlay': None,
            'InputLut': None,
            'OutputLut': None,
            'TimeIntervall': None,
            'ChannelDataTypes': None,
            'KsData': None,
            'Roi': None,
            'BleachRoi': None,
            'NextRecording': None,
            'MeanOfRoisOverlay': None,
            'TopoIsolineOverlay': None,
            'TopoProfileOverlay': None,
            'ChannelWavelength': None,
            'SphereCorrection': None,
            'ChannelFactors': None,
            'UnmixParameters': None,
            'AcquisitionParameters': None,
            'Characteristics': None,
        }

    def CZ_LSMINFO_SCANTYPE():
        # Map CZ_LSMINFO.ScanType to dimension order
        return {
            0: 'XYZCT',  # 'Stack' normal x-y-z-scan
            1: 'XYZCT',  # 'Z-Scan' x-z-plane Y=1
            2: 'XYZCT',  # 'Line'
            3: 'XYTCZ',  # 'Time Series Plane' time series x-y  XYCTZ ? Z=1
            4: 'XYZTC',  # 'Time Series z-Scan' time series x-z
            5: 'XYTCZ',  # 'Time Series Mean-of-ROIs'
            6: 'XYZTC',  # 'Time Series Stack' time series x-y-z
            7: 'XYCTZ',  # Spline Scan
            8: 'XYCZT',  # Spline Plane x-z
            9: 'XYTCZ',  # Time Series Spline Plane x-z
            10: 'XYZCT',  # 'Time Series Point' point mode
        }

    def CZ_LSMINFO_DIMENSIONS():
        # Map dimension codes to CZ_LSMINFO attribute
        return {
            'X': 'DimensionX',
            'Y': 'DimensionY',
            'Z': 'DimensionZ',
            'C': 'DimensionChannels',
            'T': 'DimensionTime',
            'P': 'DimensionP',
            'M': 'DimensionM',
        }

    def CZ_LSMINFO_DATATYPES():
        # Description of CZ_LSMINFO.DataType
        return {
            0: 'varying data types',
            1: '8 bit unsigned integer',
            2: '12 bit unsigned integer',
            5: '32 bit float',
        }

    def CZ_LSMINFO_TYPEOFDATA():
        # Description of CZ_LSMINFO.TypeOfData
        return {
            0: 'Original scan data',
            1: 'Calculated data',
            2: '3D reconstruction',
            3: 'Topography height map',
        }

    def CZ_LSMINFO_SCANINFO_ARRAYS():
        return {
            0x20000000: 'Tracks',
            0x30000000: 'Lasers',
            0x60000000: 'DetectionChannels',
            0x80000000: 'IlluminationChannels',
            0xa0000000: 'BeamSplitters',
            0xc0000000: 'DataChannels',
            0x11000000: 'Timers',
            0x13000000: 'Markers',
        }

    def CZ_LSMINFO_SCANINFO_STRUCTS():
        return {
            # 0x10000000: "Recording",
            0x40000000: 'Track',
            0x50000000: 'Laser',
            0x70000000: 'DetectionChannel',
            0x90000000: 'IlluminationChannel',
            0xb0000000: 'BeamSplitter',
            0xd0000000: 'DataChannel',
            0x12000000: 'Timer',
            0x14000000: 'Marker',
        }

    def CZ_LSMINFO_SCANINFO_ATTRIBUTES():
        return {
            # Recording
            0x10000001: 'Name',
            0x10000002: 'Description',
            0x10000003: 'Notes',
            0x10000004: 'Objective',
            0x10000005: 'ProcessingSummary',
            0x10000006: 'SpecialScanMode',
            0x10000007: 'ScanType',
            0x10000008: 'ScanMode',
            0x10000009: 'NumberOfStacks',
            0x1000000a: 'LinesPerPlane',
            0x1000000b: 'SamplesPerLine',
            0x1000000c: 'PlanesPerVolume',
            0x1000000d: 'ImagesWidth',
            0x1000000e: 'ImagesHeight',
            0x1000000f: 'ImagesNumberPlanes',
            0x10000010: 'ImagesNumberStacks',
            0x10000011: 'ImagesNumberChannels',
            0x10000012: 'LinscanXySize',
            0x10000013: 'ScanDirection',
            0x10000014: 'TimeSeries',
            0x10000015: 'OriginalScanData',
            0x10000016: 'ZoomX',
            0x10000017: 'ZoomY',
            0x10000018: 'ZoomZ',
            0x10000019: 'Sample0X',
            0x1000001a: 'Sample0Y',
            0x1000001b: 'Sample0Z',
            0x1000001c: 'SampleSpacing',
            0x1000001d: 'LineSpacing',
            0x1000001e: 'PlaneSpacing',
            0x1000001f: 'PlaneWidth',
            0x10000020: 'PlaneHeight',
            0x10000021: 'VolumeDepth',
            0x10000023: 'Nutation',
            0x10000034: 'Rotation',
            0x10000035: 'Precession',
            0x10000036: 'Sample0time',
            0x10000037: 'StartScanTriggerIn',
            0x10000038: 'StartScanTriggerOut',
            0x10000039: 'StartScanEvent',
            0x10000040: 'StartScanTime',
            0x10000041: 'StopScanTriggerIn',
            0x10000042: 'StopScanTriggerOut',
            0x10000043: 'StopScanEvent',
            0x10000044: 'StopScanTime',
            0x10000045: 'UseRois',
            0x10000046: 'UseReducedMemoryRois',
            0x10000047: 'User',
            0x10000048: 'UseBcCorrection',
            0x10000049: 'PositionBcCorrection1',
            0x10000050: 'PositionBcCorrection2',
            0x10000051: 'InterpolationY',
            0x10000052: 'CameraBinning',
            0x10000053: 'CameraSupersampling',
            0x10000054: 'CameraFrameWidth',
            0x10000055: 'CameraFrameHeight',
            0x10000056: 'CameraOffsetX',
            0x10000057: 'CameraOffsetY',
            0x10000059: 'RtBinning',
            0x1000005a: 'RtFrameWidth',
            0x1000005b: 'RtFrameHeight',
            0x1000005c: 'RtRegionWidth',
            0x1000005d: 'RtRegionHeight',
            0x1000005e: 'RtOffsetX',
            0x1000005f: 'RtOffsetY',
            0x10000060: 'RtZoom',
            0x10000061: 'RtLinePeriod',
            0x10000062: 'Prescan',
            0x10000063: 'ScanDirectionZ',
            # Track
            0x40000001: 'MultiplexType',  # 0 After Line; 1 After Frame
            0x40000002: 'MultiplexOrder',
            0x40000003: 'SamplingMode',  # 0 Sample; 1 Line Avg; 2 Frame Avg
            0x40000004: 'SamplingMethod',  # 1 Mean; 2 Sum
            0x40000005: 'SamplingNumber',
            0x40000006: 'Acquire',
            0x40000007: 'SampleObservationTime',
            0x4000000b: 'TimeBetweenStacks',
            0x4000000c: 'Name',
            0x4000000d: 'Collimator1Name',
            0x4000000e: 'Collimator1Position',
            0x4000000f: 'Collimator2Name',
            0x40000010: 'Collimator2Position',
            0x40000011: 'IsBleachTrack',
            0x40000012: 'IsBleachAfterScanNumber',
            0x40000013: 'BleachScanNumber',
            0x40000014: 'TriggerIn',
            0x40000015: 'TriggerOut',
            0x40000016: 'IsRatioTrack',
            0x40000017: 'BleachCount',
            0x40000018: 'SpiCenterWavelength',
            0x40000019: 'PixelTime',
            0x40000021: 'CondensorFrontlens',
            0x40000023: 'FieldStopValue',
            0x40000024: 'IdCondensorAperture',
            0x40000025: 'CondensorAperture',
            0x40000026: 'IdCondensorRevolver',
            0x40000027: 'CondensorFilter',
            0x40000028: 'IdTransmissionFilter1',
            0x40000029: 'IdTransmission1',
            0x40000030: 'IdTransmissionFilter2',
            0x40000031: 'IdTransmission2',
            0x40000032: 'RepeatBleach',
            0x40000033: 'EnableSpotBleachPos',
            0x40000034: 'SpotBleachPosx',
            0x40000035: 'SpotBleachPosy',
            0x40000036: 'SpotBleachPosz',
            0x40000037: 'IdTubelens',
            0x40000038: 'IdTubelensPosition',
            0x40000039: 'TransmittedLight',
            0x4000003a: 'ReflectedLight',
            0x4000003b: 'SimultanGrabAndBleach',
            0x4000003c: 'BleachPixelTime',
            # Laser
            0x50000001: 'Name',
            0x50000002: 'Acquire',
            0x50000003: 'Power',
            # DetectionChannel
            0x70000001: 'IntegrationMode',
            0x70000002: 'SpecialMode',
            0x70000003: 'DetectorGainFirst',
            0x70000004: 'DetectorGainLast',
            0x70000005: 'AmplifierGainFirst',
            0x70000006: 'AmplifierGainLast',
            0x70000007: 'AmplifierOffsFirst',
            0x70000008: 'AmplifierOffsLast',
            0x70000009: 'PinholeDiameter',
            0x7000000a: 'CountingTrigger',
            0x7000000b: 'Acquire',
            0x7000000c: 'PointDetectorName',
            0x7000000d: 'AmplifierName',
            0x7000000e: 'PinholeName',
            0x7000000f: 'FilterSetName',
            0x70000010: 'FilterName',
            0x70000013: 'IntegratorName',
            0x70000014: 'ChannelName',
            0x70000015: 'DetectorGainBc1',
            0x70000016: 'DetectorGainBc2',
            0x70000017: 'AmplifierGainBc1',
            0x70000018: 'AmplifierGainBc2',
            0x70000019: 'AmplifierOffsetBc1',
            0x70000020: 'AmplifierOffsetBc2',
            0x70000021: 'SpectralScanChannels',
            0x70000022: 'SpiWavelengthStart',
            0x70000023: 'SpiWavelengthStop',
            0x70000026: 'DyeName',
            0x70000027: 'DyeFolder',
            # IlluminationChannel
            0x90000001: 'Name',
            0x90000002: 'Power',
            0x90000003: 'Wavelength',
            0x90000004: 'Aquire',
            0x90000005: 'DetchannelName',
            0x90000006: 'PowerBc1',
            0x90000007: 'PowerBc2',
            # BeamSplitter
            0xb0000001: 'FilterSet',
            0xb0000002: 'Filter',
            0xb0000003: 'Name',
            # DataChannel
            0xd0000001: 'Name',
            0xd0000003: 'Acquire',
            0xd0000004: 'Color',
            0xd0000005: 'SampleType',
            0xd0000006: 'BitsPerSample',
            0xd0000007: 'RatioType',
            0xd0000008: 'RatioTrack1',
            0xd0000009: 'RatioTrack2',
            0xd000000a: 'RatioChannel1',
            0xd000000b: 'RatioChannel2',
            0xd000000c: 'RatioConst1',
            0xd000000d: 'RatioConst2',
            0xd000000e: 'RatioConst3',
            0xd000000f: 'RatioConst4',
            0xd0000010: 'RatioConst5',
            0xd0000011: 'RatioConst6',
            0xd0000012: 'RatioFirstImages1',
            0xd0000013: 'RatioFirstImages2',
            0xd0000014: 'DyeName',
            0xd0000015: 'DyeFolder',
            0xd0000016: 'Spectrum',
            0xd0000017: 'Acquire',
            # Timer
            0x12000001: 'Name',
            0x12000002: 'Description',
            0x12000003: 'Interval',
            0x12000004: 'TriggerIn',
            0x12000005: 'TriggerOut',
            0x12000006: 'ActivationTime',
            0x12000007: 'ActivationNumber',
            # Marker
            0x14000001: 'Name',
            0x14000002: 'Description',
            0x14000003: 'TriggerIn',
            0x14000004: 'TriggerOut',
        }

    def NIH_IMAGE_HEADER():
        return [
            ('FileID', 'a8'),
            ('nLines', 'i2'),
            ('PixelsPerLine', 'i2'),
            ('Version', 'i2'),
            ('OldLutMode', 'i2'),
            ('OldnColors', 'i2'),
            ('Colors', 'u1', (3, 32)),
            ('OldColorStart', 'i2'),
            ('ColorWidth', 'i2'),
            ('ExtraColors', 'u2', (6, 3)),
            ('nExtraColors', 'i2'),
            ('ForegroundIndex', 'i2'),
            ('BackgroundIndex', 'i2'),
            ('XScale', 'f8'),
            ('Unused2', 'i2'),
            ('Unused3', 'i2'),
            ('UnitsID', 'i2'),  # NIH_UNITS_TYPE
            ('p1', [('x', 'i2'), ('y', 'i2')]),
            ('p2', [('x', 'i2'), ('y', 'i2')]),
            ('CurveFitType', 'i2'),  # NIH_CURVEFIT_TYPE
            ('nCoefficients', 'i2'),
            ('Coeff', 'f8', 6),
            ('UMsize', 'u1'),
            ('UM', 'a15'),
            ('UnusedBoolean', 'u1'),
            ('BinaryPic', 'b1'),
            ('SliceStart', 'i2'),
            ('SliceEnd', 'i2'),
            ('ScaleMagnification', 'f4'),
            ('nSlices', 'i2'),
            ('SliceSpacing', 'f4'),
            ('CurrentSlice', 'i2'),
            ('FrameInterval', 'f4'),
            ('PixelAspectRatio', 'f4'),
            ('ColorStart', 'i2'),
            ('ColorEnd', 'i2'),
            ('nColors', 'i2'),
            ('Fill1', '3u2'),
            ('Fill2', '3u2'),
            ('Table', 'u1'),  # NIH_COLORTABLE_TYPE
            ('LutMode', 'u1'),  # NIH_LUTMODE_TYPE
            ('InvertedTable', 'b1'),
            ('ZeroClip', 'b1'),
            ('XUnitSize', 'u1'),
            ('XUnit', 'a11'),
            ('StackType', 'i2'),  # NIH_STACKTYPE_TYPE
            # ('UnusedBytes', 'u1', 200)
        ]

    def NIH_COLORTABLE_TYPE():
        return ('CustomTable', 'AppleDefault', 'Pseudo20', 'Pseudo32',
                'Rainbow', 'Fire1', 'Fire2', 'Ice', 'Grays', 'Spectrum')

    def NIH_LUTMODE_TYPE():
        return ('PseudoColor', 'OldAppleDefault', 'OldSpectrum', 'GrayScale',
                'ColorLut', 'CustomGrayscale')

    def NIH_CURVEFIT_TYPE():
        return ('StraightLine', 'Poly2', 'Poly3', 'Poly4', 'Poly5', 'ExpoFit',
                'PowerFit', 'LogFit', 'RodbardFit', 'SpareFit1',
                'Uncalibrated', 'UncalibratedOD')

    def NIH_UNITS_TYPE():
        return ('Nanometers', 'Micrometers', 'Millimeters', 'Centimeters',
                'Meters', 'Kilometers', 'Inches', 'Feet', 'Miles', 'Pixels',
                'OtherUnits')

    def NIH_STACKTYPE_TYPE():
        return ('VolumeStack', 'RGBStack', 'MovieStack', 'HSVStack')

    def TVIPS_HEADER_V1():
        # TVIPS TemData structure from EMMENU Help file
        return [
            ('Version', 'i4'),
            ('CommentV1', 'a80'),
            ('HighTension', 'i4'),
            ('SphericalAberration', 'i4'),
            ('IlluminationAperture', 'i4'),
            ('Magnification', 'i4'),
            ('PostMagnification', 'i4'),
            ('FocalLength', 'i4'),
            ('Defocus', 'i4'),
            ('Astigmatism', 'i4'),
            ('AstigmatismDirection', 'i4'),
            ('BiprismVoltage', 'i4'),
            ('SpecimenTiltAngle', 'i4'),
            ('SpecimenTiltDirection', 'i4'),
            ('IlluminationTiltDirection', 'i4'),
            ('IlluminationTiltAngle', 'i4'),
            ('ImageMode', 'i4'),
            ('EnergySpread', 'i4'),
            ('ChromaticAberration', 'i4'),
            ('ShutterType', 'i4'),
            ('DefocusSpread', 'i4'),
            ('CcdNumber', 'i4'),
            ('CcdSize', 'i4'),
            ('OffsetXV1', 'i4'),
            ('OffsetYV1', 'i4'),
            ('PhysicalPixelSize', 'i4'),
            ('Binning', 'i4'),
            ('ReadoutSpeed', 'i4'),
            ('GainV1', 'i4'),
            ('SensitivityV1', 'i4'),
            ('ExposureTimeV1', 'i4'),
            ('FlatCorrected', 'i4'),
            ('DeadPxCorrected', 'i4'),
            ('ImageMean', 'i4'),
            ('ImageStd', 'i4'),
            ('DisplacementX', 'i4'),
            ('DisplacementY', 'i4'),
            ('DateV1', 'i4'),
            ('TimeV1', 'i4'),
            ('ImageMin', 'i4'),
            ('ImageMax', 'i4'),
            ('ImageStatisticsQuality', 'i4'),
        ]

    def TVIPS_HEADER_V2():
        return [
            ('ImageName', 'V160'),  # utf16
            ('ImageFolder', 'V160'),
            ('ImageSizeX', 'i4'),
            ('ImageSizeY', 'i4'),
            ('ImageSizeZ', 'i4'),
            ('ImageSizeE', 'i4'),
            ('ImageDataType', 'i4'),
            ('Date', 'i4'),
            ('Time', 'i4'),
            ('Comment', 'V1024'),
            ('ImageHistory', 'V1024'),
            ('Scaling', '16f4'),
            ('ImageStatistics', '16c16'),
            ('ImageType', 'i4'),
            ('ImageDisplaType', 'i4'),
            ('PixelSizeX', 'f4'),  # distance between two px in x, [nm]
            ('PixelSizeY', 'f4'),  # distance between two px in y, [nm]
            ('ImageDistanceZ', 'f4'),
            ('ImageDistanceE', 'f4'),
            ('ImageMisc', '32f4'),
            ('TemType', 'V160'),
            ('TemHighTension', 'f4'),
            ('TemAberrations', '32f4'),
            ('TemEnergy', '32f4'),
            ('TemMode', 'i4'),
            ('TemMagnification', 'f4'),
            ('TemMagnificationCorrection', 'f4'),
            ('PostMagnification', 'f4'),
            ('TemStageType', 'i4'),
            ('TemStagePosition', '5f4'),  # x, y, z, a, b
            ('TemImageShift', '2f4'),
            ('TemBeamShift', '2f4'),
            ('TemBeamTilt', '2f4'),
            ('TilingParameters', '7f4'),  # 0: tiling? 1:x 2:y 3: max x
                                          # 4: max y 5: overlap x 6: overlap y
            ('TemIllumination', '3f4'),  # 0: spotsize 1: intensity
            ('TemShutter', 'i4'),
            ('TemMisc', '32f4'),
            ('CameraType', 'V160'),
            ('PhysicalPixelSizeX', 'f4'),
            ('PhysicalPixelSizeY', 'f4'),
            ('OffsetX', 'i4'),
            ('OffsetY', 'i4'),
            ('BinningX', 'i4'),
            ('BinningY', 'i4'),
            ('ExposureTime', 'f4'),
            ('Gain', 'f4'),
            ('ReadoutRate', 'f4'),
            ('FlatfieldDescription', 'V160'),
            ('Sensitivity', 'f4'),
            ('Dose', 'f4'),
            ('CamMisc', '32f4'),
            ('FeiMicroscopeInformation', 'V1024'),
            ('FeiSpecimenInformation', 'V1024'),
            ('Magic', 'u4'),
        ]

    def MM_HEADER():
        # Olympus FluoView MM_Header
        MM_DIMENSION = [
            ('Name', 'a16'),
            ('Size', 'i4'),
            ('Origin', 'f8'),
            ('Resolution', 'f8'),
            ('Unit', 'a64')]
        return [
            ('HeaderFlag', 'i2'),
            ('ImageType', 'u1'),
            ('ImageName', 'a257'),
            ('OffsetData', 'u4'),
            ('PaletteSize', 'i4'),
            ('OffsetPalette0', 'u4'),
            ('OffsetPalette1', 'u4'),
            ('CommentSize', 'i4'),
            ('OffsetComment', 'u4'),
            ('Dimensions', MM_DIMENSION, 10),
            ('OffsetPosition', 'u4'),
            ('MapType', 'i2'),
            ('MapMin', 'f8'),
            ('MapMax', 'f8'),
            ('MinValue', 'f8'),
            ('MaxValue', 'f8'),
            ('OffsetMap', 'u4'),
            ('Gamma', 'f8'),
            ('Offset', 'f8'),
            ('GrayChannel', MM_DIMENSION),
            ('OffsetThumbnail', 'u4'),
            ('VoiceField', 'i4'),
            ('OffsetVoiceField', 'u4'),
        ]

    def MM_DIMENSIONS():
        # Map FluoView MM_Header.Dimensions to axes characters
        return {
            'X': 'X',
            'Y': 'Y',
            'Z': 'Z',
            'T': 'T',
            'CH': 'C',
            'WAVELENGTH': 'C',
            'TIME': 'T',
            'XY': 'R',
            'EVENT': 'V',
            'EXPOSURE': 'L',
        }

    def UIC_TAGS():
        # Map Universal Imaging Corporation MetaMorph internal tag ids to
        # name and type
        from fractions import Fraction

        return [
            ('AutoScale', int),
            ('MinScale', int),
            ('MaxScale', int),
            ('SpatialCalibration', int),
            ('XCalibration', Fraction),
            ('YCalibration', Fraction),
            ('CalibrationUnits', str),
            ('Name', str),
            ('ThreshState', int),
            ('ThreshStateRed', int),
            ('tagid_10', None),  # undefined
            ('ThreshStateGreen', int),
            ('ThreshStateBlue', int),
            ('ThreshStateLo', int),
            ('ThreshStateHi', int),
            ('Zoom', int),
            ('CreateTime', julian_datetime),
            ('LastSavedTime', julian_datetime),
            ('currentBuffer', int),
            ('grayFit', None),
            ('grayPointCount', None),
            ('grayX', Fraction),
            ('grayY', Fraction),
            ('grayMin', Fraction),
            ('grayMax', Fraction),
            ('grayUnitName', str),
            ('StandardLUT', int),
            ('wavelength', int),
            ('StagePosition', '(%i,2,2)u4'),  # N xy positions as fract
            ('CameraChipOffset', '(%i,2,2)u4'),  # N xy offsets as fract
            ('OverlayMask', None),
            ('OverlayCompress', None),
            ('Overlay', None),
            ('SpecialOverlayMask', None),
            ('SpecialOverlayCompress', None),
            ('SpecialOverlay', None),
            ('ImageProperty', read_uic_image_property),
            ('StageLabel', '%ip'),  # N str
            ('AutoScaleLoInfo', Fraction),
            ('AutoScaleHiInfo', Fraction),
            ('AbsoluteZ', '(%i,2)u4'),  # N fractions
            ('AbsoluteZValid', '(%i,)u4'),  # N long
            ('Gamma', 'I'),  # 'I' uses offset
            ('GammaRed', 'I'),
            ('GammaGreen', 'I'),
            ('GammaBlue', 'I'),
            ('CameraBin', '2I'),
            ('NewLUT', int),
            ('ImagePropertyEx', None),
            ('PlaneProperty', int),
            ('UserLutTable', '(256,3)u1'),
            ('RedAutoScaleInfo', int),
            ('RedAutoScaleLoInfo', Fraction),
            ('RedAutoScaleHiInfo', Fraction),
            ('RedMinScaleInfo', int),
            ('RedMaxScaleInfo', int),
            ('GreenAutoScaleInfo', int),
            ('GreenAutoScaleLoInfo', Fraction),
            ('GreenAutoScaleHiInfo', Fraction),
            ('GreenMinScaleInfo', int),
            ('GreenMaxScaleInfo', int),
            ('BlueAutoScaleInfo', int),
            ('BlueAutoScaleLoInfo', Fraction),
            ('BlueAutoScaleHiInfo', Fraction),
            ('BlueMinScaleInfo', int),
            ('BlueMaxScaleInfo', int),
            # ('OverlayPlaneColor', read_uic_overlay_plane_color),
        ]

    def PILATUS_HEADER():
        # PILATUS CBF Header Specification, Version 1.4
        # Map key to [value_indices], type
        return {
            'Detector': ([slice(1, None)], str),
            'Pixel_size': ([1, 4], float),
            'Silicon': ([3], float),
            'Exposure_time': ([1], float),
            'Exposure_period': ([1], float),
            'Tau': ([1], float),
            'Count_cutoff': ([1], int),
            'Threshold_setting': ([1], float),
            'Gain_setting': ([1, 2], str),
            'N_excluded_pixels': ([1], int),
            'Excluded_pixels': ([1], str),
            'Flat_field': ([1], str),
            'Trim_file': ([1], str),
            'Image_path': ([1], str),
            # optional
            'Wavelength': ([1], float),
            'Energy_range': ([1, 2], float),
            'Detector_distance': ([1], float),
            'Detector_Voffset': ([1], float),
            'Beam_xy': ([1, 2], float),
            'Flux': ([1], str),
            'Filter_transmission': ([1], float),
            'Start_angle': ([1], float),
            'Angle_increment': ([1], float),
            'Detector_2theta': ([1], float),
            'Polarization': ([1], float),
            'Alpha': ([1], float),
            'Kappa': ([1], float),
            'Phi': ([1], float),
            'Phi_increment': ([1], float),
            'Chi': ([1], float),
            'Chi_increment': ([1], float),
            'Oscillation_axis': ([slice(1, None)], str),
            'N_oscillations': ([1], int),
            'Start_position': ([1], float),
            'Position_increment': ([1], float),
            'Shutter_time': ([1], float),
            'Omega': ([1], float),
            'Omega_increment': ([1], float)
        }

    def REVERSE_BITORDER_BYTES():
        # Bytes with reversed bitorder
        return (
            b'\x00\x80@\xc0 \xa0`\xe0\x10\x90P\xd00\xb0p\xf0\x08\x88H\xc8('
            b'\xa8h\xe8\x18\x98X\xd88\xb8x\xf8\x04\x84D\xc4$\xa4d\xe4\x14'
            b'\x94T\xd44\xb4t\xf4\x0c\x8cL\xcc,\xacl\xec\x1c\x9c\\\xdc<\xbc|'
            b'\xfc\x02\x82B\xc2"\xa2b\xe2\x12\x92R\xd22\xb2r\xf2\n\x8aJ\xca*'
            b'\xaaj\xea\x1a\x9aZ\xda:\xbaz\xfa\x06\x86F\xc6&\xa6f\xe6\x16'
            b'\x96V\xd66\xb6v\xf6\x0e\x8eN\xce.\xaen\xee\x1e\x9e^\xde>\xbe~'
            b'\xfe\x01\x81A\xc1!\xa1a\xe1\x11\x91Q\xd11\xb1q\xf1\t\x89I\xc9)'
            b'\xa9i\xe9\x19\x99Y\xd99\xb9y\xf9\x05\x85E\xc5%\xa5e\xe5\x15'
            b'\x95U\xd55\xb5u\xf5\r\x8dM\xcd-\xadm\xed\x1d\x9d]\xdd=\xbd}'
            b'\xfd\x03\x83C\xc3#\xa3c\xe3\x13\x93S\xd33\xb3s\xf3\x0b\x8bK'
            b'\xcb+\xabk\xeb\x1b\x9b[\xdb;\xbb{\xfb\x07\x87G\xc7\'\xa7g\xe7'
            b'\x17\x97W\xd77\xb7w\xf7\x0f\x8fO\xcf/\xafo\xef\x1f\x9f_'
            b'\xdf?\xbf\x7f\xff')

    def REVERSE_BITORDER_ARRAY():
        # Numpy array of bytes with reversed bitorder
        return numpy.fromstring(TIFF.REVERSE_BITORDER_BYTES, dtype='uint8')

    def ALLOCATIONGRANULARITY():
        # alignment for writing contiguous data to TIFF
        import mmap  # delayed import
        return mmap.ALLOCATIONGRANULARITY

    # Max line length of printed output
    PRINT_LINE_WIDTH = 100

    # Max number of lines to print
    PRINT_MAX_LINES = 512


def read_tags(fh, byteorder, offsetsize, tagnames,
              customtags=None, maxifds=None):
    """Read tags from chain of IFDs and return as list of dicts.

    The file handle position must be at a valid IFD header.

    """
    if offsetsize == 4:
        offsetformat = byteorder+'I'
        tagnosize = 2
        tagnoformat = byteorder+'H'
        tagsize = 12
        tagformat1 = byteorder+'HH'
        tagformat2 = byteorder+'I4s'
    elif offsetsize == 8:
        offsetformat = byteorder+'Q'
        tagnosize = 8
        tagnoformat = byteorder+'Q'
        tagsize = 20
        tagformat1 = byteorder+'HH'
        tagformat2 = byteorder+'Q8s'
    else:
        raise ValueError("invalid offset size")

    if customtags is None:
        customtags = {}
    if maxifds is None:
        maxifds = 2**32

    result = []
    unpack = struct.unpack
    offset = fh.tell()
    while len(result) < maxifds:
        # loop over IFDs
        try:
            tagno = unpack(tagnoformat, fh.read(tagnosize))[0]
            if tagno > 4096:
                raise ValueError("suspicious number of tags")
        except Exception:
            warnings.warn("corrupted tag list at offset %i" % offset)
            break

        tags = {}
        data = fh.read(tagsize * tagno)
        pos = fh.tell()
        index = 0
        for _ in range(tagno):
            code, type_ = unpack(tagformat1, data[index:index+4])
            count, value = unpack(tagformat2, data[index+4:index+tagsize])
            index += tagsize
            name = tagnames.get(code, str(code))
            try:
                dtype = TIFF.DATA_FORMATS[type_]
            except KeyError:
                raise TiffTag.Error("unknown tag data type %i" % type_)

            fmt = '%s%i%s' % (byteorder, count * int(dtype[0]), dtype[1])
            size = struct.calcsize(fmt)
            if size > offsetsize or code in customtags:
                offset = unpack(offsetformat, value)[0]
                if offset < 8 or offset > fh.size - size:
                    raise TiffTag.Error("invalid tag value offset")
                fh.seek(offset)
                if code in customtags:
                    readfunc = customtags[code][1]
                    value = readfunc(fh, byteorder, dtype, count, offsetsize)
                elif code in tagnames or dtype[-1] == 's':
                    value = unpack(fmt, fh.read(size))
                else:
                    value = read_numpy(fh, byteorder, dtype, count, offsetsize)
            else:
                value = unpack(fmt, value[:size])

            if code not in customtags and code not in TIFF.TAG_TUPLE:
                if len(value) == 1:
                    value = value[0]
            if type_ != 7 and dtype[-1] == 's' and isinstance(value, bytes):
                # TIFF ASCII fields can contain multiple strings,
                #   each terminated with a NUL
                value = bytes2str(stripascii(value))

            tags[name] = value

        result.append(tags)
        # read offset to next page
        fh.seek(pos)
        offset = unpack(offsetformat, fh.read(offsetsize))[0]
        if offset == 0:
            break
        if offset >= fh.size:
            warnings.warn("invalid page offset (%i)" % offset)
            break
        fh.seek(offset)

    if maxifds == 1:
        result = result[0]
    return result


def read_exif_ifd(fh, byteorder, dtype, count, offsetsize):
    """Read EXIF tags from file and return as dict."""
    tags = read_tags(fh, byteorder, offsetsize, TIFF.EXIF_TAGS, maxifds=1)
    if 'ExifVersion' in tags:
        tags['ExifVersion'] = bytes2str(tags['ExifVersion'])
    return tags


def read_gps_ifd(fh, byteorder, dtype, count, offsetsize):
    """Read GPS tags from file and return as dict."""
    return read_tags(fh, byteorder, offsetsize, TIFF.GPS_TAGS, maxifds=1)


def read_interoperability_ifd(fh, byteorder, dtype, count, offsetsize):
    """Read Interoperability tags from file and return as dict."""
    tag_names = {1: 'InteroperabilityIndex'}
    return read_tags(fh, byteorder, offsetsize, tag_names, maxifds=1)


def read_bytes(fh, byteorder, dtype, count, offsetsize):
    """Read tag data from file and return as byte string."""
    dtype = 'b' if dtype[-1] == 's' else byteorder+dtype[-1]
    return fh.read_array(dtype, count).tostring()


def read_utf8(fh, byteorder, dtype, count, offsetsize):
    """Read tag data from file and return as unicode string."""
    return fh.read(count).decode('utf-8')


def read_numpy(fh, byteorder, dtype, count, offsetsize):
    """Read tag data from file and return as numpy array."""
    dtype = 'b' if dtype[-1] == 's' else byteorder+dtype[-1]
    return fh.read_array(dtype, count)


def read_colormap(fh, byteorder, dtype, count, offsetsize):
    """Read ColorMap data from file and return as numpy array."""
    cmap = fh.read_array(byteorder+dtype[-1], count)
    cmap.shape = (3, -1)
    return cmap


def read_json(fh, byteorder, dtype, count, offsetsize):
    """Read JSON tag data from file and return as object."""
    data = fh.read(count)
    try:
        return json.loads(unicode(stripnull(data), 'utf-8'))
    except ValueError:
        warnings.warn("invalid JSON '%s'" % data)


def read_mm_header(fh, byteorder, dtype, count, offsetsize):
    """Read FluoView mm_header tag from file and return as dict."""
    mmh = fh.read_record(TIFF.MM_HEADER, byteorder=byteorder)
    mmh = recarray2dict(mmh)
    mmh['Dimensions'] = [
        (bytes2str(d[0]).strip(), d[1], d[2], d[3], bytes2str(d[4]).strip())
        for d in mmh['Dimensions']]
    d = mmh['GrayChannel']
    mmh['GrayChannel'] = (
        bytes2str(d[0]).strip(), d[1], d[2], d[3], bytes2str(d[4]).strip())
    return mmh


def read_mm_stamp(fh, byteorder, dtype, count, offsetsize):
    """Read FluoView mm_stamp tag from file and return as numpy.ndarray."""
    return fh.read_array(byteorder+'f8', 8)


def read_uic1tag(fh, byteorder, dtype, count, offsetsize, planecount=None):
    """Read MetaMorph STK UIC1Tag from file and return as dict.

    Return empty dictionary if planecount is unknown.

    """
    assert dtype in ('2I', '1I') and byteorder == '<'
    result = {}
    if dtype == '2I':
        # pre MetaMorph 2.5 (not tested)
        values = fh.read_array('<u4', 2*count).reshape(count, 2)
        result = {'ZDistance': values[:, 0] / values[:, 1]}
    elif planecount:
        for _ in range(count):
            tagid = struct.unpack('<I', fh.read(4))[0]
            if tagid in (28, 29, 37, 40, 41):
                # silently skip unexpected tags
                fh.read(4)
                continue
            name, value = read_uic_tag(fh, tagid, planecount, offset=True)
            result[name] = value
    return result


def read_uic2tag(fh, byteorder, dtype, planecount, offsetsize):
    """Read MetaMorph STK UIC2Tag from file and return as dict."""
    assert dtype == '2I' and byteorder == '<'
    values = fh.read_array('<u4', 6*planecount).reshape(planecount, 6)
    return {
        'ZDistance': values[:, 0] / values[:, 1],
        'DateCreated': values[:, 2],  # julian days
        'TimeCreated': values[:, 3],  # milliseconds
        'DateModified': values[:, 4],  # julian days
        'TimeModified': values[:, 5]}  # milliseconds


def read_uic3tag(fh, byteorder, dtype, planecount, offsetsize):
    """Read MetaMorph STK UIC3Tag from file and return as dict."""
    assert dtype == '2I' and byteorder == '<'
    values = fh.read_array('<u4', 2*planecount).reshape(planecount, 2)
    return {'Wavelengths': values[:, 0] / values[:, 1]}


def read_uic4tag(fh, byteorder, dtype, planecount, offsetsize):
    """Read MetaMorph STK UIC4Tag from file and return as dict."""
    assert dtype == '1I' and byteorder == '<'
    result = {}
    while True:
        tagid = struct.unpack('<H', fh.read(2))[0]
        if tagid == 0:
            break
        name, value = read_uic_tag(fh, tagid, planecount, offset=False)
        result[name] = value
    return result


def read_uic_tag(fh, tagid, planecount, offset):
    """Read a single UIC tag value from file and return tag name and value.

    UIC1Tags use an offset.

    """
    def read_int(count=1):
        value = struct.unpack('<%iI' % count, fh.read(4*count))
        return value[0] if count == 1 else value

    try:
        name, dtype = TIFF.UIC_TAGS[tagid]
    except IndexError:
        # unknown tag
        return '_TagId%i' % tagid, read_int()

    Fraction = TIFF.UIC_TAGS[4][1]

    if offset:
        pos = fh.tell()
        if dtype not in (int, None):
            off = read_int()
            if off < 8:
                if dtype is str:
                    return name, ''
                warnings.warn("invalid offset for uic tag '%s': %i" %
                              (name, off))
                return name, off
            fh.seek(off)

    if dtype is None:
        # skip
        name = '_' + name
        value = read_int()
    elif dtype is int:
        # int
        value = read_int()
    elif dtype is Fraction:
        # fraction
        value = read_int(2)
        value = value[0] / value[1]
    elif dtype is julian_datetime:
        # datetime
        value = julian_datetime(*read_int(2))
    elif dtype is read_uic_image_property:
        # ImagePropertyEx
        value = read_uic_image_property(fh)
    elif dtype is str:
        # pascal string
        size = read_int()
        if 0 <= size < 2**10:
            value = struct.unpack('%is' % size, fh.read(size))[0][:-1]
            value = bytes2str(stripnull(value))
        elif offset:
            value = ''
            warnings.warn("corrupt string in uic tag '%s'" % name)
        else:
            raise ValueError("invalid string size %i" % size)
    elif dtype == '%ip':
        # sequence of pascal strings
        value = []
        for _ in range(planecount):
            size = read_int()
            if 0 <= size < 2**10:
                string = struct.unpack('%is' % size, fh.read(size))[0][:-1]
                string = bytes2str(stripnull(string))
                value.append(string)
            elif offset:
                warnings.warn("corrupt string in uic tag '%s'" % name)
            else:
                raise ValueError("invalid string size %i" % size)
    else:
        # struct or numpy type
        dtype = '<' + dtype
        if '%i' in dtype:
            dtype = dtype % planecount
        if '(' in dtype:
            # numpy type
            value = fh.read_array(dtype, 1)[0]
            if value.shape[-1] == 2:
                # assume fractions
                value = value[..., 0] / value[..., 1]
        else:
            # struct format
            value = struct.unpack(dtype, fh.read(struct.calcsize(dtype)))
            if len(value) == 1:
                value = value[0]

    if offset:
        fh.seek(pos + 4)

    return name, value


def read_uic_image_property(fh):
    """Read UIC ImagePropertyEx tag from file and return as dict."""
    # TODO: test this
    size = struct.unpack('B', fh.read(1))[0]
    name = struct.unpack('%is' % size, fh.read(size))[0][:-1]
    flags, prop = struct.unpack('<IB', fh.read(5))
    if prop == 1:
        value = struct.unpack('II', fh.read(8))
        value = value[0] / value[1]
    else:
        size = struct.unpack('B', fh.read(1))[0]
        value = struct.unpack('%is' % size, fh.read(size))[0]
    return dict(name=name, flags=flags, value=value)


def read_cz_lsminfo(fh, byteorder, dtype, count, offsetsize):
    """Read CZ_LSMINFO tag from file and return as dict."""
    assert byteorder == '<'
    magic_number, structure_size = struct.unpack('<II', fh.read(8))
    if magic_number not in (50350412, 67127628):
        raise ValueError("invalid CZ_LSMINFO structure")
    fh.seek(-8, 1)

    if structure_size < numpy.dtype(TIFF.CZ_LSMINFO).itemsize:
        # adjust structure according to structure_size
        lsminfo = []
        size = 0
        for name, dtype in TIFF.CZ_LSMINFO:
            size += numpy.dtype(dtype).itemsize
            if size > structure_size:
                break
            lsminfo.append((name, dtype))
    else:
        lsminfo = TIFF.CZ_LSMINFO

    lsminfo = fh.read_record(lsminfo, byteorder=byteorder)
    lsminfo = recarray2dict(lsminfo)

    # read LSM info subrecords at offsets
    for name, reader in TIFF.CZ_LSMINFO_READERS.items():
        if reader is None:
            continue
        offset = lsminfo.get('Offset' + name, 0)
        if offset < 8:
            continue
        fh.seek(offset)
        try:
            lsminfo[name] = reader(fh)
        except ValueError:
            pass
    return lsminfo


def read_lsm_floatpairs(fh):
    """Read LSM sequence of float pairs from file and return as list."""
    size = struct.unpack('<i', fh.read(4))[0]
    return fh.read_array('<2f8', count=size)


def read_lsm_positions(fh):
    """Read LSM positions from file and return as list."""
    size = struct.unpack('<I', fh.read(4))[0]
    return fh.read_array('<2f8', count=size)


def read_lsm_timestamps(fh):
    """Read LSM time stamps from file and return as list."""
    size, count = struct.unpack('<ii', fh.read(8))
    if size != (8 + 8 * count):
        warnings.warn("invalid LSM TimeStamps block")
        return []
    # return struct.unpack('<%dd' % count, fh.read(8*count))
    return fh.read_array('<f8', count=count)


def read_lsm_eventlist(fh):
    """Read LSM events from file and return as list of (time, type, text)."""
    count = struct.unpack('<II', fh.read(8))[1]
    events = []
    while count > 0:
        esize, etime, etype = struct.unpack('<IdI', fh.read(16))
        etext = bytes2str(stripnull(fh.read(esize - 16)))
        events.append((etime, etype, etext))
        count -= 1
    return events


def read_lsm_channelcolors(fh):
    """Read LSM ChannelColors structure from file and return as dict."""
    result = {'Mono': False, 'Colors': [], 'ColorNames': []}
    pos = fh.tell()
    (size, ncolors, nnames,
     coffset, noffset, mono) = struct.unpack('<IIIIII', fh.read(24))
    if ncolors != nnames:
        warnings.warn("invalid LSM ChannelColors structure")
        return result
    result['Mono'] = bool(mono)
    # Colors
    fh.seek(pos + coffset)
    colors = fh.read_array('uint8', count=ncolors*4).reshape((ncolors, 4))
    result['Colors'] = colors.tolist()
    # ColorNames
    fh.seek(pos + noffset)
    buffer = fh.read(size - noffset)
    names = []
    while len(buffer) > 4:
        size = struct.unpack('<I', buffer[:4])[0]
        names.append(bytes2str(buffer[4:3+size]))
        buffer = buffer[4+size:]
    result['ColorNames'] = names
    return result


def read_lsm_scaninfo(fh):
    """Read LSM ScanInfo structure from file and return as dict."""
    block = {}
    blocks = [block]
    unpack = struct.unpack
    if struct.unpack('<I', fh.read(4))[0] != 0x10000000:
        # not a Recording sub block
        warnings.warn("invalid LSM ScanInfo structure")
        return block
    fh.read(8)
    while True:
        entry, dtype, size = unpack('<III', fh.read(12))
        if dtype == 2:
            # ascii
            value = bytes2str(stripnull(fh.read(size)))
        elif dtype == 4:
            # long
            value = unpack('<i', fh.read(4))[0]
        elif dtype == 5:
            # rational
            value = unpack('<d', fh.read(8))[0]
        else:
            value = 0
        if entry in TIFF.CZ_LSMINFO_SCANINFO_ARRAYS:
            blocks.append(block)
            name = TIFF.CZ_LSMINFO_SCANINFO_ARRAYS[entry]
            newobj = []
            block[name] = newobj
            block = newobj
        elif entry in TIFF.CZ_LSMINFO_SCANINFO_STRUCTS:
            blocks.append(block)
            newobj = {}
            block.append(newobj)
            block = newobj
        elif entry in TIFF.CZ_LSMINFO_SCANINFO_ATTRIBUTES:
            name = TIFF.CZ_LSMINFO_SCANINFO_ATTRIBUTES[entry]
            block[name] = value
        elif entry == 0xffffffff:
            # end sub block
            block = blocks.pop()
        else:
            # unknown entry
            block["Entry0x%x" % entry] = value
        if not blocks:
            break
    return block


def read_tvips_header(fh, byteorder, dtype, count, offsetsize):
    """Read TVIPS EM-MENU headers and return as dict."""
    result = {}
    header = fh.read_record(TIFF.TVIPS_HEADER_V1, byteorder=byteorder)
    for name, typestr in TIFF.TVIPS_HEADER_V1:
        result[name] = header[name].tolist()
    if header['Version'] == 2:
        header = fh.read_record(TIFF.TVIPS_HEADER_V2, byteorder=byteorder)
        if header['Magic'] != int(0xaaaaaaaa):
            warnings.warn("invalid TVIPS v2 magic number")
            return {}
        # decode utf16 strings
        for name, typestr in TIFF.TVIPS_HEADER_V2:
            if typestr.startswith('V'):
                s = header[name].tostring().decode('utf16', errors='ignore')
                result[name] = stripnull(s, null='\0')
            else:
                result[name] = header[name].tolist()
        # convert nm to m
        for axis in 'XY':
            header['PhysicalPixelSize' + axis] /= 1e9
            header['PixelSize' + axis] /= 1e9
    elif header.version != 1:
        warnings.warn("unknown TVIPS header version")
        return {}
    return result


def read_fei_metadata(fh, byteorder, dtype, count, offsetsize):
    """Read FEI SFEG/HELIOS headers and return as dict."""
    result = {}
    section = {}
    data = bytes2str(fh.read(count))
    for line in data.splitlines():
        line = line.strip()
        if line.startswith('['):
            section = {}
            result[line[1:-1]] = section
            continue
        try:
            key, value = line.split('=')
        except ValueError:
            continue
        section[key] = astype(value)
    return result


def read_cz_sem(fh, byteorder, dtype, count, offsetsize):
    """Read Zeiss SEM tag and return as dict."""
    result = {'': ()}
    key = None
    data = bytes2str(fh.read(count))
    for line in data.splitlines():
        if line.isupper():
            key = line.lower()
        elif key:
            try:
                name, value = line.split('=')
            except ValueError:
                continue
            value = value.strip()
            unit = ''
            try:
                v, u = value.split()
                number = astype(v, (int, float))
                if number != v:
                    value = number
                    unit = u
            except Exception:
                number = astype(value, (int, float))
                if number != value:
                    value = number
                if value in ('No', 'Off'):
                    value = False
                elif value in ('Yes', 'On'):
                    value = True
            result[key] = (name.strip(), value)
            if unit:
                result[key] += (unit,)
            key = None
        else:
            result[''] += (astype(line, (int, float)),)
    return result


def read_nih_image_header(fh, byteorder, dtype, count, offsetsize):
    """Read NIH_IMAGE_HEADER tag from file and return as dict."""
    a = fh.read_record(TIFF.NIH_IMAGE_HEADER, byteorder=byteorder)
    a = a.newbyteorder(byteorder)
    a = recarray2dict(a)
    a['XUnit'] = a['XUnit'][:a['XUnitSize']]
    a['UM'] = a['UM'][:a['UMsize']]
    return a


def read_scanimage_metadata(fh):
    """Read ScanImage BigTIFF v3 static and ROI metadata from open file.

    Return non-varying frame data as dict and ROI group data as JSON.

    The settings can be used to read image data and metadata without parsing
    the TIFF file.

    Raise ValueError if file does not contain valid ScanImage v3 metadata.

    """
    fh.seek(0)
    try:
        byteorder, version = struct.unpack('<2sH', fh.read(4))
        if byteorder != b'II' or version != 43:
            raise Exception
        fh.seek(16)
        magic, version, size0, size1 = struct.unpack('<IIII', fh.read(16))
        if magic != 117637889 or version != 3:
            raise Exception
    except Exception:
        raise ValueError("not a ScanImage BigTIFF v3 file")

    frame_data = matlabstr2py(bytes2str(fh.read(size0)[:-1]))
    roi_data = read_json(fh, '<', None, size1, None)
    return frame_data, roi_data


def read_micromanager_metadata(fh):
    """Read MicroManager non-TIFF settings from open file and return as dict.

    The settings can be used to read image data without parsing the TIFF file.

    Raise ValueError if the file does not contain valid MicroManager metadata.

    """
    fh.seek(0)
    try:
        byteorder = {b'II': '<', b'MM': '>'}[fh.read(2)]
    except IndexError:
        raise ValueError("not a MicroManager TIFF file")

    result = {}
    fh.seek(8)
    (index_header, index_offset, display_header, display_offset,
     comments_header, comments_offset, summary_header, summary_length
     ) = struct.unpack(byteorder + "IIIIIIII", fh.read(32))

    if summary_header != 2355492:
        raise ValueError("invalid MicroManager summary header")
    result['Summary'] = read_json(fh, byteorder, None, summary_length, None)

    if index_header != 54773648:
        raise ValueError("invalid MicroManager index header")
    fh.seek(index_offset)
    header, count = struct.unpack(byteorder + "II", fh.read(8))
    if header != 3453623:
        raise ValueError("invalid MicroManager index header")
    data = struct.unpack(byteorder + "IIIII"*count, fh.read(20*count))
    result['IndexMap'] = {'Channel': data[::5],
                          'Slice': data[1::5],
                          'Frame': data[2::5],
                          'Position': data[3::5],
                          'Offset': data[4::5]}

    if display_header != 483765892:
        raise ValueError("invalid MicroManager display header")
    fh.seek(display_offset)
    header, count = struct.unpack(byteorder + "II", fh.read(8))
    if header != 347834724:
        raise ValueError("invalid MicroManager display header")
    result['DisplaySettings'] = read_json(fh, byteorder, None, count, None)

    if comments_header != 99384722:
        raise ValueError("invalid MicroManager comments header")
    fh.seek(comments_offset)
    header, count = struct.unpack(byteorder + "II", fh.read(8))
    if header != 84720485:
        raise ValueError("invalid MicroManager comments header")
    result['Comments'] = read_json(fh, byteorder, None, count, None)

    return result


def read_metaseries_catalog(fh):
    """Read MetaSeries non-TIFF hint catalog from file.

    Raise ValueError if the file does not contain a valid hint catalog.

    """
    # TODO: implement read_metaseries_catalog
    raise NotImplementedError()


def imagej_metadata(data, bytecounts, byteorder):
    """Return IJMetadata tag value as dict.

    The 'info' string can have multiple formats, e.g. OIF or ScanImage,
    that might be parsed into dicts using the matlabstr2py or
    oiffile.SettingsFile functions.

    """
    def readstring(data, byteorder):
        return data.decode('utf-16' + {'>': 'be', '<': 'le'}[byteorder])

    def readdouble(data, byteorder):
        return struct.unpack(byteorder+('d' * (len(data) // 8)), data)

    def readbytes(data, byteorder):
        return numpy.fromstring(data, 'uint8')

    metadata_types = {  # big endian
        b'info': ('Info', readstring),
        b'labl': ('Labels', readstring),
        b'rang': ('Ranges', readdouble),
        b'luts': ('LUTs', readbytes),
        b'roi ': ('ROI', readbytes),
        b'over': ('Overlays', readbytes)}
    metadata_types.update(  # little endian
        dict((k[::-1], v) for k, v in metadata_types.items()))

    if not bytecounts:
        raise ValueError("no ImageJ metadata")

    if not data[:4] in (b'IJIJ', b'JIJI'):
        raise ValueError("invalid ImageJ metadata")

    header_size = bytecounts[0]
    if header_size < 12 or header_size > 804:
        raise ValueError("invalid ImageJ metadata header size")

    ntypes = (header_size - 4) // 8
    header = struct.unpack(byteorder+'4sI'*ntypes, data[4:4+ntypes*8])
    pos = 4 + ntypes * 8
    counter = 0
    result = {}
    for mtype, count in zip(header[::2], header[1::2]):
        values = []
        name, func = metadata_types.get(mtype, (bytes2str(mtype), read_bytes))
        for _ in range(count):
            counter += 1
            pos1 = pos + bytecounts[counter]
            values.append(func(data[pos:pos1], byteorder))
            pos = pos1
        result[name.strip()] = values[0] if count == 1 else values
    return result


def imagej_description_metadata(description):
    """Return metatata from ImageJ image description as dict.

    Raise ValueError if not a valid ImageJ description.

    >>> description = 'ImageJ=1.11a\\nimages=510\\nhyperstack=true\\n'
    >>> imagej_description_metadata(description)  # doctest: +SKIP
    {'ImageJ': '1.11a', 'images': 510, 'hyperstack': True}

    """
    def _bool(val):
        return {'true': True, 'false': False}[val.lower()]

    result = {}
    for line in description.splitlines():
        try:
            key, val = line.split('=')
        except Exception:
            continue
        key = key.strip()
        val = val.strip()
        for dtype in (int, float, _bool):
            try:
                val = dtype(val)
                break
            except Exception:
                pass
        result[key] = val

    if 'ImageJ' not in result:
        raise ValueError("not a ImageJ image description")
    return result


def imagej_description(shape, rgb=None, colormaped=False, version='1.11a',
                       hyperstack=None, mode=None, loop=None, **kwargs):
    """Return ImageJ image description from data shape.

    ImageJ can handle up to 6 dimensions in order TZCYXS.

    >>> imagej_description((51, 5, 2, 196, 171))  # doctest: +SKIP
    ImageJ=1.11a
    images=510
    channels=2
    slices=5
    frames=51
    hyperstack=true
    mode=grayscale
    loop=false

    """
    if colormaped:
        raise NotImplementedError("ImageJ colormapping not supported")
    shape = imagej_shape(shape, rgb=rgb)
    rgb = shape[-1] in (3, 4)

    result = ['ImageJ=%s' % version]
    append = []
    result.append('images=%i' % product(shape[:-3]))
    if hyperstack is None:
        hyperstack = True
        append.append('hyperstack=true')
    else:
        append.append('hyperstack=%s' % bool(hyperstack))
    if shape[2] > 1:
        result.append('channels=%i' % shape[2])
    if mode is None and not rgb:
        mode = 'grayscale'
    if hyperstack and mode:
        append.append('mode=%s' % mode)
    if shape[1] > 1:
        result.append('slices=%i' % shape[1])
    if shape[0] > 1:
        result.append("frames=%i" % shape[0])
        if loop is None:
            append.append('loop=false')
    if loop is not None:
        append.append('loop=%s' % bool(loop))
    for key, value in kwargs.items():
        append.append('%s=%s' % (key.lower(), value))

    return '\n'.join(result + append + [''])


def imagej_shape(shape, rgb=None):
    """Return shape normalized to 6D ImageJ hyperstack TZCYXS.

    Raise ValueError if not a valid ImageJ hyperstack shape.

    >>> imagej_shape((2, 3, 4, 5, 3), False)
    (2, 3, 4, 5, 3, 1)

    """
    shape = tuple(int(i) for i in shape)
    ndim = len(shape)
    if 1 > ndim > 6:
        raise ValueError("invalid ImageJ hyperstack: not 2 to 6 dimensional")
    if rgb is None:
        rgb = shape[-1] in (3, 4) and ndim > 2
    if rgb and shape[-1] not in (3, 4):
        raise ValueError("invalid ImageJ hyperstack: not a RGB image")
    if not rgb and ndim == 6 and shape[-1] != 1:
        raise ValueError("invalid ImageJ hyperstack: not a non-RGB image")
    if rgb or shape[-1] == 1:
        return (1, ) * (6 - ndim) + shape
    return (1, ) * (5 - ndim) + shape + (1,)


def json_description(shape, **metadata):
    """Return JSON image description from data shape and other meta data.

    Return UTF-8 encoded JSON.

    >>> json_description((256, 256, 3), axes='YXS')  # doctest: +SKIP
    b'{"shape": [256, 256, 3], "axes": "YXS"}'

    """
    metadata.update(shape=shape)
    return json.dumps(metadata)  # .encode('utf-8')


def json_description_metadata(description):
    """Return metatata from JSON formated image description as dict.

    Raise ValuError if description is of unknown format.

    >>> description = '{"shape": [256, 256, 3], "axes": "YXS"}'
    >>> json_description_metadata(description)  # doctest: +SKIP
    {'shape': [256, 256, 3], 'axes': 'YXS'}
    >>> json_description_metadata('shape=(256, 256, 3)')
    {'shape': (256, 256, 3)}

    """
    if description[:6] == 'shape=':
        # old style 'shaped' description; not JSON
        shape = tuple(int(i) for i in description[7:-1].split(','))
        return dict(shape=shape)
    if description[:1] == '{' and description[-1:] == '}':
        # JSON description
        return json.loads(description)
    raise ValueError("invalid JSON image description", description)


def fluoview_description_metadata(description, ignoresections=None):
    """Return metatata from FluoView image description as dict.

    The FluoView image description format is unspecified. Expect failures.

    >>> descr = ('[Intensity Mapping]\\nMap Ch0: Range=00000 to 02047\\n'
    ...          '[Intensity Mapping End]')
    >>> fluoview_description_metadata(descr)
    {'Intensity Mapping': {'Map Ch0: Range': '00000 to 02047'}}

    """
    if not description.startswith('['):
        raise ValueError("invalid FluoView image description")
    if ignoresections is None:
        ignoresections = {'Region Info (Fields)', 'Protocol Description'}

    result = {}
    sections = [result]
    comment = False
    for line in description.splitlines():
        if not comment:
            line = line.strip()
        if not line:
            continue
        if line[0] == '[':
            if line[-5:] == ' End]':
                # close section
                del sections[-1]
                section = sections[-1]
                name = line[1:-5]
                if comment:
                    section[name] = '\n'.join(section[name])
                if name[:4] == 'LUT ':
                    a = numpy.array(section[name], dtype='uint8')
                    a.shape = -1, 3
                    section[name] = a
                continue
            # new section
            comment = False
            name = line[1:-1]
            if name[:4] == 'LUT ':
                section = []
            elif name in ignoresections:
                section = []
                comment = True
            else:
                section = {}
            sections.append(section)
            result[name] = section
            continue
        # add entry
        if comment:
            section.append(line)
            continue
        line = line.split('=', 1)
        if len(line) == 1:
            section[line[0].strip()] = None
            continue
        key, value = line
        if key[:4] == 'RGB ':
            section.extend(int(rgb) for rgb in value.split())
        else:
            section[key.strip()] = astype(value.strip())
    return result


def pilatus_description_metadata(description):
    """Return metatata from Pilatus image description as dict.

    Return metadata from Pilatus pixel array detectors by Dectris, created
    by camserver or TVX software.

    >>> pilatus_description_metadata('# Pixel_size 172e-6 m x 172e-6 m')
    {'Pixel_size': (0.000172, 0.000172)}

    """
    result = {}
    if not description.startswith('# '):
        return result
    for c in '#:=,()':
        description = description.replace(c, ' ')
    for line in description.split('\n'):
        if line[:2] != '  ':
            continue
        line = line.split()
        name = line[0]
        if line[0] not in TIFF.PILATUS_HEADER:
            try:
                result['DateTime'] = datetime.datetime.strptime(
                    ' '.join(line), '%Y-%m-%dT%H %M %S.%f')
            except Exception:
                result[name] = ' '.join(line[1:])
            continue
        indices, dtype = TIFF.PILATUS_HEADER[line[0]]
        if isinstance(indices[0], slice):
            # assumes one slice
            values = line[indices[0]]
        else:
            values = [line[i] for i in indices]
        if dtype is float and values[0] == 'not':
            values = ['NaN']
        values = tuple(dtype(v) for v in values)
        if dtype == str:
            values = ' '.join(values)
        elif len(values) == 1:
            values = values[0]
        result[name] = values
    return result


def svs_description_metadata(description):
    """Return metatata from Aperio image description as dict.

    The Aperio image description format is unspecified. Expect failures.

    >>> svs_description_metadata('Aperio Image Library v1.0')
    {'Aperio Image Library': 'v1.0'}

    """
    if not description.startswith('Aperio Image Library '):
        raise ValueError("invalid Aperio image description")
    result = {}
    lines = description.split('\n')
    key, value = lines[0].strip().rsplit(None, 1)  # 'Aperio Image Library'
    result[key.strip()] = value.strip()
    if len(lines) == 1:
        return result
    items = lines[1].split('|')
    result[''] = items[0].strip()  # TODO: parse this?
    for item in items[1:]:
        key, value = item.split(' = ')
        result[key.strip()] = astype(value.strip())
    return result


def stk_description_metadata(description):
    """Return metadata from MetaMorph image description as list of dict.

    The MetaMorph image description format is unspecified. Expect failures.

    """
    description = description.strip()
    if not description:
        return []
    try:
        description = bytes2str(description)
    except UnicodeDecodeError:
        warnings.warn("failed to parse MetaMorph image description")
        return []
    result = []
    for plane in description.split('\x00'):
        d = {}
        for line in plane.split('\r\n'):
            line = line.split(':', 1)
            if len(line) > 1:
                name, value = line
                d[name.strip()] = astype(value.strip())
            else:
                value = line[0].strip()
                if value:
                    if '' in d:
                        d[''].append(value)
                    else:
                        d[''] = [value]
        result.append(d)
    return result


def metaseries_description_metadata(description):
    """Return metatata from MetaSeries image description as dict."""
    if not description.startswith('<MetaData>'):
        raise ValueError("invalid MetaSeries image description")

    from xml.etree import cElementTree as etree  # delayed import
    root = etree.fromstring(description)
    types = {'float': float, 'int': int,
             'bool': lambda x: asbool(x, 'on', 'off')}

    def parse(root, result):
        # recursive
        for child in root:
            attrib = child.attrib
            if not attrib:
                result[child.tag] = parse(child, {})
                continue
            if 'id' in attrib:
                i = attrib['id']
                t = attrib['type']
                v = attrib['value']
                if t in types:
                    result[i] = types[t](v)
                else:
                    result[i] = v
        return result

    adict = parse(root, {})
    if 'Description' in adict:
        adict['Description'] = adict['Description'].replace('&#13;&#10;', '\n')
    return adict


def scanimage_description_metadata(description):
    """Return metatata from ScanImage image description as dict."""
    return matlabstr2py(description)


def scanimage_artist_metadata(artist):
    """Return metatata from ScanImage artist tag as dict."""
    try:
        return json.loads(artist)
    except ValueError:
        warnings.warn("invalid JSON '%s'" % artist)


def _replace_by(module_function, package=__package__, warn=None, prefix='_'):
    """Try replace decorated function by module.function."""
    def _warn(e, warn):
        if warn is None:
            warn = "\n  Functionality might be degraded or be slow.\n"
        elif warn is True:
            warn = ''
        elif not warn:
            return
        warnings.warn("%s%s" % (e, warn))

    try:
        from importlib import import_module
    except ImportError as e:
        _warn(e, warn)
        return identityfunc

    def decorate(func, module_function=module_function, warn=warn):
        module, function = module_function.split('.')
        try:
            if package:
                module = import_module('.' + module, package=package)
            else:
                module = import_module(module)
        except Exception as e:
            _warn(e, warn)
            return func
        try:
            func, oldfunc = getattr(module, function), func
        except Exception as e:
            _warn(e, warn)
            return func
        globals()[prefix + func.__name__] = oldfunc
        return func

    return decorate


def decode_floats(data):
    """Decode floating point horizontal differencing.

    The TIFF predictor type 3 reorders the bytes of the image values and
    applies horizontal byte differencing to improve compression of floating
    point images. The ordering of interleaved color channels is preserved.

    Parameters
    ----------
    data : numpy.ndarray
        The image to be decoded. The dtype must be a floating point.
        The shape must include the number of contiguous samples per pixel
        even if 1.

    """
    shape = data.shape
    dtype = data.dtype
    if len(shape) < 3:
        raise ValueError('invalid data shape')
    if dtype.char not in 'dfe':
        raise ValueError('not a floating point image')
    littleendian = data.dtype.byteorder == '<' or (
        sys.byteorder == 'little' and data.dtype.byteorder == '=')
    # undo horizontal byte differencing
    data = data.view('uint8')
    data.shape = shape[:-2] + (-1,) + shape[-1:]
    numpy.cumsum(data, axis=-2, dtype='uint8', out=data)
    # reorder bytes
    if littleendian:
        data.shape = shape[:-2] + (-1,) + shape[-2:]
    data = numpy.swapaxes(data, -3, -2)
    data = numpy.swapaxes(data, -2, -1)
    data = data[..., ::-1]
    # back to float
    data = numpy.ascontiguousarray(data)
    data = data.view(dtype)
    data.shape = shape
    return data


def decode_jpeg(encoded, tables=b'', photometric=None,
                ycbcrsubsampling=None, ycbcrpositioning=None):
    """Decode JPEG encoded byte string (using _czifile extension module)."""
    from czifile import _czifile
    image = _czifile.decode_jpeg(encoded, tables)
    if photometric == 2 and ycbcrsubsampling and ycbcrpositioning:
        # TODO: convert YCbCr to RGB
        pass
    return image.tostring()


@_replace_by('_tifffile.decode_packbits')
def decode_packbits(encoded):
    """Decompress PackBits encoded byte string.

    PackBits is a simple byte-oriented run-length compression scheme.

    """
    func = ord if sys.version[0] == '2' else identityfunc
    result = []
    result_extend = result.extend
    i = 0
    try:
        while True:
            n = func(encoded[i]) + 1
            i += 1
            if n < 129:
                result_extend(encoded[i:i+n])
                i += n
            elif n > 129:
                result_extend(encoded[i:i+1] * (258-n))
                i += 1
    except IndexError:
        pass
    return b''.join(result) if sys.version[0] == '2' else bytes(result)


@_replace_by('_tifffile.decode_lzw')
def decode_lzw(encoded):
    """Decompress LZW (Lempel-Ziv-Welch) encoded TIFF strip (byte string).

    The strip must begin with a CLEAR code and end with an EOI code.

    This is an implementation of the LZW decoding algorithm described in (1).
    It is not compatible with old style LZW compressed files like quad-lzw.tif.

    """
    len_encoded = len(encoded)
    bitcount_max = len_encoded * 8
    unpack = struct.unpack

    if sys.version[0] == '2':
        newtable = [chr(i) for i in range(256)]
    else:
        newtable = [bytes([i]) for i in range(256)]
    newtable.extend((0, 0))

    def next_code():
        """Return integer of 'bitw' bits at 'bitcount' position in encoded."""
        start = bitcount // 8
        s = encoded[start:start+4]
        try:
            code = unpack('>I', s)[0]
        except Exception:
            code = unpack('>I', s + b'\x00'*(4-len(s)))[0]
        code <<= bitcount % 8
        code &= mask
        return code >> shr

    switchbitch = {  # code: bit-width, shr-bits, bit-mask
        255: (9, 23, int(9*'1'+'0'*23, 2)),
        511: (10, 22, int(10*'1'+'0'*22, 2)),
        1023: (11, 21, int(11*'1'+'0'*21, 2)),
        2047: (12, 20, int(12*'1'+'0'*20, 2)), }
    bitw, shr, mask = switchbitch[255]
    bitcount = 0

    if len_encoded < 4:
        raise ValueError("strip must be at least 4 characters long")

    if next_code() != 256:
        raise ValueError("strip must begin with CLEAR code")

    code = 0
    oldcode = 0
    result = []
    result_append = result.append
    while True:
        code = next_code()  # ~5% faster when inlining this function
        bitcount += bitw
        if code == 257 or bitcount >= bitcount_max:  # EOI
            break
        if code == 256:  # CLEAR
            table = newtable[:]
            table_append = table.append
            lentable = 258
            bitw, shr, mask = switchbitch[255]
            code = next_code()
            bitcount += bitw
            if code == 257:  # EOI
                break
            result_append(table[code])
        else:
            if code < lentable:
                decoded = table[code]
                newcode = table[oldcode] + decoded[:1]
            else:
                newcode = table[oldcode]
                newcode += newcode[:1]
                decoded = newcode
            result_append(decoded)
            table_append(newcode)
            lentable += 1
        oldcode = code
        if lentable in switchbitch:
            bitw, shr, mask = switchbitch[lentable]

    if code != 257:
        warnings.warn("unexpected end of lzw stream (code %i)" % code)

    return b''.join(result)


@_replace_by('_tifffile.unpack_ints')
def unpack_ints(data, dtype, itemsize, runlen=0):
    """Decompress byte string to array of integers of any bit size <= 32.

    This Python implementation is slow and only handles itemsizes 1, 2, 4, 8,
    16, 32, and 64.

    Parameters
    ----------
    data : byte str
        Data to decompress.
    dtype : numpy.dtype or str
        A numpy boolean or integer type.
    itemsize : int
        Number of bits per integer.
    runlen : int
        Number of consecutive integers, after which to start at next byte.

    Examples
    --------
    >>> unpack_ints(b'a', 'B', 1)
    array([0, 1, 1, 0, 0, 0, 0, 1], dtype=uint8)
    >>> unpack_ints(b'ab', 'B', 2)
    array([1, 2, 0, 1, 1, 2, 0, 2], dtype=uint8)

    """
    if itemsize == 1:  # bitarray
        data = numpy.fromstring(data, '|B')
        data = numpy.unpackbits(data)
        if runlen % 8:
            data = data.reshape(-1, runlen + (8 - runlen % 8))
            data = data[:, :runlen].reshape(-1)
        return data.astype(dtype)

    dtype = numpy.dtype(dtype)
    if itemsize in (8, 16, 32, 64):
        return numpy.fromstring(data, dtype)
    if itemsize not in (1, 2, 4, 8, 16, 32):
        raise ValueError("itemsize not supported: %i" % itemsize)
    if dtype.kind not in "biu":
        raise ValueError("invalid dtype")

    itembytes = next(i for i in (1, 2, 4, 8) if 8 * i >= itemsize)
    if itembytes != dtype.itemsize:
        raise ValueError("dtype.itemsize too small")
    if runlen == 0:
        runlen = (8 * len(data)) // itemsize
    skipbits = runlen * itemsize % 8
    if skipbits:
        skipbits = 8 - skipbits
    shrbits = itembytes*8 - itemsize
    bitmask = int(itemsize*'1'+'0'*shrbits, 2)
    dtypestr = '>' + dtype.char  # dtype always big endian?

    unpack = struct.unpack
    l = runlen * (len(data)*8 // (runlen*itemsize + skipbits))
    result = numpy.empty((l,), dtype)
    bitcount = 0
    for i in range(l):
        start = bitcount // 8
        s = data[start:start+itembytes]
        try:
            code = unpack(dtypestr, s)[0]
        except Exception:
            code = unpack(dtypestr, s + b'\x00'*(itembytes-len(s)))[0]
        code <<= bitcount % 8
        code &= bitmask
        result[i] = code >> shrbits
        bitcount += itemsize
        if (i+1) % runlen == 0:
            bitcount += skipbits
    return result


def unpack_rgb(data, dtype='<B', bitspersample=(5, 6, 5), rescale=True):
    """Return array from byte string containing packed samples.

    Use to unpack RGB565 or RGB555 to RGB888 format.

    Parameters
    ----------
    data : byte str
        The data to be decoded. Samples in each pixel are stored consecutively.
        Pixels are aligned to 8, 16, or 32 bit boundaries.
    dtype : numpy.dtype
        The sample data type. The byteorder applies also to the data stream.
    bitspersample : tuple
        Number of bits for each sample in a pixel.
    rescale : bool
        Upscale samples to the number of bits in dtype.

    Returns
    -------
    result : ndarray
        Flattened array of unpacked samples of native dtype.

    Examples
    --------
    >>> data = struct.pack('BBBB', 0x21, 0x08, 0xff, 0xff)
    >>> print(unpack_rgb(data, '<B', (5, 6, 5), False))
    [ 1  1  1 31 63 31]
    >>> print(unpack_rgb(data, '<B', (5, 6, 5)))
    [  8   4   8 255 255 255]
    >>> print(unpack_rgb(data, '<B', (5, 5, 5)))
    [ 16   8   8 255 255 255]

    """
    dtype = numpy.dtype(dtype)
    bits = int(numpy.sum(bitspersample))
    if not (bits <= 32 and all(i <= dtype.itemsize*8 for i in bitspersample)):
        raise ValueError("sample size not supported %s" % str(bitspersample))
    dt = next(i for i in 'BHI' if numpy.dtype(i).itemsize*8 >= bits)
    data = numpy.fromstring(data, dtype.byteorder+dt)
    result = numpy.empty((data.size, len(bitspersample)), dtype.char)
    for i, bps in enumerate(bitspersample):
        t = data >> int(numpy.sum(bitspersample[i+1:]))
        t &= int('0b'+'1'*bps, 2)
        if rescale:
            o = ((dtype.itemsize * 8) // bps + 1) * bps
            if o > data.dtype.itemsize * 8:
                t = t.astype('I')
            t *= (2**o - 1) // (2**bps - 1)
            t //= 2**(o - (dtype.itemsize * 8))
        result[:, i] = t
    return result.reshape(-1)


@_replace_by('_tifffile.reverse_bitorder')
def reverse_bitorder(data):
    """Reverse bits in each byte of byte string or numpy array.

    Decode data where pixels with lower column values are stored in the
    lower-order bits of the bytes (FillOrder is LSB2MSB).

    Parameters
    ----------
    data : byte string or ndarray
        The data to be bit reversed. If byte string, a new bit-reversed byte
        string is returned. Numpy arrays are bit-reversed in-place.

    Examples
    --------
    >>> reverse_bitorder(b'\\x01\\x64')
    b'\\x80&'
    >>> data = numpy.array([1, 666], dtype='uint16')
    >>> reverse_bitorder(data)
    >>> data
    array([  128, 16473], dtype=uint16)

    """
    try:
        view = data.view('uint8')
        numpy.take(TIFF.REVERSE_BITORDER_ARRAY, view, out=view)
    except AttributeError:
        return data.translate(TIFF.REVERSE_BITORDER_BYTES)
    except ValueError:
        raise NotImplementedError("slices of arrays not supported")


def apply_colormap(image, colormap, contig=True):
    """Return palette-colored image.

    The image values are used to index the colormap on axis 1. The returned
    image is of shape image.shape+colormap.shape[0] and dtype colormap.dtype.

    Parameters
    ----------
    image : numpy.ndarray
        Indexes into the colormap.
    colormap : numpy.ndarray
        RGB lookup table aka palette of shape (3, 2**bits_per_sample).
    contig : bool
        If True, return a contiguous array.

    Examples
    --------
    >>> image = numpy.arange(256, dtype='uint8')
    >>> colormap = numpy.vstack([image, image, image]).astype('uint16') * 256
    >>> apply_colormap(image, colormap)[-1]
    array([65280, 65280, 65280], dtype=uint16)

    """
    image = numpy.take(colormap, image, axis=1)
    image = numpy.rollaxis(image, 0, image.ndim)
    if contig:
        image = numpy.ascontiguousarray(image)
    return image


def reorient(image, orientation):
    """Return reoriented view of image array.

    Parameters
    ----------
    image : numpy.ndarray
        Non-squeezed output of asarray() functions.
        Axes -3 and -2 must be image length and width respectively.
    orientation : int or str
        One of TIFF.ORIENTATION names or values.

    """
    ORIENTATION = TIFF.ORIENTATION
    orientation = enumarg(ORIENTATION, orientation)

    if orientation == ORIENTATION.TOPLEFT:
        return image
    elif orientation == ORIENTATION.TOPRIGHT:
        return image[..., ::-1, :]
    elif orientation == ORIENTATION.BOTLEFT:
        return image[..., ::-1, :, :]
    elif orientation == ORIENTATION.BOTRIGHT:
        return image[..., ::-1, ::-1, :]
    elif orientation == ORIENTATION.LEFTTOP:
        return numpy.swapaxes(image, -3, -2)
    elif orientation == ORIENTATION.RIGHTTOP:
        return numpy.swapaxes(image, -3, -2)[..., ::-1, :]
    elif orientation == ORIENTATION.RIGHTBOT:
        return numpy.swapaxes(image, -3, -2)[..., ::-1, :, :]
    elif orientation == ORIENTATION.LEFTBOT:
        return numpy.swapaxes(image, -3, -2)[..., ::-1, ::-1, :]


def repeat_nd(a, repeats):
    """Return read-only view into input array with elements repeated.

    Zoom nD image by integer factors using nearest neighbor interpolation
    (box filter).

    Parameters
    ----------
    a : array_like
        Input array.
    repeats : sequence of int
        The number of repetitions to apply along each dimension of input array.

    Example
    -------
    >>> repeat_nd([[1, 2], [3, 4]], (2, 2))
    array([[1, 1, 2, 2],
           [1, 1, 2, 2],
           [3, 3, 4, 4],
           [3, 3, 4, 4]])

    """
    a = numpy.asarray(a)
    reshape = []
    shape = []
    strides = []
    for i, j, k in zip(a.strides, a.shape, repeats):
        shape.extend((j, k))
        strides.extend((i, 0))
        reshape.append(j * k)
    return numpy.lib.stride_tricks.as_strided(
        a, shape, strides, writeable=False).reshape(reshape)


def reshape_nd(data_or_shape, ndim):
    """Return image array or shape with at least ndim dimensions.

    Prepend 1s to image shape as necessary.

    >>> reshape_nd(numpy.empty(0), 1).shape
    (0,)
    >>> reshape_nd(numpy.empty(1), 2).shape
    (1, 1)
    >>> reshape_nd(numpy.empty((2, 3)), 3).shape
    (1, 2, 3)
    >>> reshape_nd(numpy.empty((3, 4, 5)), 3).shape
    (3, 4, 5)
    >>> reshape_nd((2, 3), 3)
    (1, 2, 3)

    """
    is_shape = isinstance(data_or_shape, tuple)
    shape = data_or_shape if is_shape else data_or_shape.shape
    if len(shape) >= ndim:
        return data_or_shape
    shape = (1,) * (ndim - len(shape)) + shape
    return shape if is_shape else data_or_shape.reshape(shape)


def squeeze_axes(shape, axes, skip='XY'):
    """Return shape and axes with single-dimensional entries removed.

    Remove unused dimensions unless their axes are listed in 'skip'.

    >>> squeeze_axes((5, 1, 2, 1, 1), 'TZYXC')
    ((5, 2, 1), 'TYX')

    """
    if len(shape) != len(axes):
        raise ValueError("dimensions of axes and shape do not match")
    shape, axes = zip(*(i for i in zip(shape, axes)
                        if i[0] > 1 or i[1] in skip))
    return tuple(shape), ''.join(axes)


def transpose_axes(image, axes, asaxes='CTZYX'):
    """Return image with its axes permuted to match specified axes.

    A view is returned if possible.

    >>> transpose_axes(numpy.zeros((2, 3, 4, 5)), 'TYXC', asaxes='CTZYX').shape
    (5, 2, 1, 3, 4)

    """
    for ax in axes:
        if ax not in asaxes:
            raise ValueError("unknown axis %s" % ax)
    # add missing axes to image
    shape = image.shape
    for ax in reversed(asaxes):
        if ax not in axes:
            axes = ax + axes
            shape = (1,) + shape
    image = image.reshape(shape)
    # transpose axes
    image = image.transpose([axes.index(ax) for ax in asaxes])
    return image


def reshape_axes(axes, shape, newshape, unknown='Q'):
    """Return axes matching new shape.

    Unknown dimensions are labelled 'Q'.

    >>> reshape_axes('YXS', (219, 301, 1), (219, 301))
    'YX'
    >>> reshape_axes('IYX', (12, 219, 301), (3, 4, 219, 1, 301, 1))
    'QQYQXQ'

    """
    shape = tuple(shape)
    newshape = tuple(newshape)
    if len(axes) != len(shape):
        raise ValueError("axes do not match shape")

    size = product(shape)
    newsize = product(newshape)
    if size != newsize:
        raise ValueError("can not reshape %s to %s" % (shape, newshape))
    if not axes or not newshape:
        return ''

    lendiff = max(0, len(shape) - len(newshape))
    if lendiff:
        newshape = newshape + (1,) * lendiff

    i = len(shape)-1
    prodns = 1
    prods = 1
    result = []
    for ns in newshape[::-1]:
        prodns *= ns
        while i > 0 and shape[i] == 1 and ns != 1:
            i -= 1
        if ns == shape[i] and prodns == prods*shape[i]:
            prods *= shape[i]
            result.append(axes[i])
            i -= 1
        else:
            result.append(unknown)

    return ''.join(reversed(result[lendiff:]))


def stack_pages(pages, out=None, maxworkers=1, *args, **kwargs):
    """Read data from sequence of TiffPage and stack them vertically.

    Additional parameters are passsed to the TiffPage.asarray function.

    """
    npages = len(pages)
    if npages == 0:
        raise ValueError("no pages")

    if npages == 1:
        return pages[0].asarray(out=out, *args, **kwargs)

    page0 = next(p for p in pages if p is not None)
    page0.asarray(validate=None)  # ThreadPoolExecutor swallows exceptions
    shape = (npages,) + page0.keyframe.shape
    dtype = page0.keyframe.dtype
    out = create_output(out, shape, dtype)

    if maxworkers is None:
        maxworkers = multiprocessing.cpu_count() // 2
    page0.parent.filehandle.lock = maxworkers > 1

    filecache = OpenFileCache(size=max(4, maxworkers),
                              lock=page0.parent.filehandle.lock)

    def func(page, index, out=out, filecache=filecache,
             args=args, kwargs=kwargs):
        """Read, decode, and copy page data."""
        if page is not None:
            filecache.open(page.parent.filehandle)
            out[index] = page.asarray(lock=filecache.lock, reopen=False,
                                      validate=False, *args, **kwargs)
            filecache.close(page.parent.filehandle)

    if maxworkers < 2:
        for i, page in enumerate(pages):
            func(page, i)
    else:
        with concurrent.futures.ThreadPoolExecutor(maxworkers) as executor:
            executor.map(func, pages, range(npages))

    filecache.clear()
    page0.parent.filehandle.lock = None

    return out


def clean_offsets_counts(offsets, counts):
    """Return cleaned offsets and byte counts.

    Remove zero offsets and counts. Use to sanitize _offsets and _bytecounts
    tag values for strips or tiles.

    """
    offsets = list(offsets)
    counts = list(counts)
    assert len(offsets) == len(counts)
    j = 0
    for i, (o, b) in enumerate(zip(offsets, counts)):
        if o > 0 and b > 0:
            if i > j:
                offsets[j] = o
                counts[j] = b
            j += 1
        elif b > 0 and o <= 0:
            raise ValueError("invalid offset")
        else:
            warnings.warn("empty byte count")
    if j == 0:
        j = 1
    return offsets[:j], counts[:j]


def buffered_read(fh, lock, offsets, bytecounts, buffersize=2**26):
    """Return iterator over blocks read from file."""
    length = len(offsets)
    i = 0
    while i < length:
        data = []
        with lock:
            size = 0
            while size < buffersize and i < length:
                fh.seek(offsets[i])
                bytecount = bytecounts[i]
                data.append(fh.read(bytecount))
                size += bytecount
                i += 1
        for block in data:
            yield block


def create_output(out, shape, dtype, mode='w+', suffix='.memmap'):
    """Return numpy array where image data of shape and dtype can copied.

    The 'out' parameter may have the following values or types:

    None
        An empty array of shape and dtype is created and returned.
    numpy.ndarray
        An existing writable array of compatible dtype and shape. A view of
        the same array is returned after verification.
    'memmap' or 'memmap:tempdir'
        A memory-map to an array stored in a temporary binary file on disk
        is created and returned.
    str or open file
        The file name or file object used to create a memory-map to an array
        stored in a binary file on disk. The created memory-mapped array is
        returned.

    """
    if out is None:
        return numpy.zeros(shape, dtype)
    if isinstance(out, str) and out[:6] == 'memmap':
        tempdir = out[7:] if len(out) > 7 else None
        with tempfile.NamedTemporaryFile(dir=tempdir, suffix=suffix) as fh:
            return numpy.memmap(fh, shape=shape, dtype=dtype, mode=mode)
    if isinstance(out, numpy.ndarray):
        if product(shape) != product(out.shape):
            raise ValueError("incompatible output shape")
        if not numpy.can_cast(dtype, out.dtype):
            raise ValueError("incompatible output dtype")
        return out.reshape(shape)
    return numpy.memmap(out, shape=shape, dtype=dtype, mode=mode)


def matlabstr2py(s):
    """Return Python object from Matlab string representation.

    Return str, bool, int, float, list (Matlab arrays or cells), or
    dict (Matlab structures) types.

    Use to access ScanImage metadata.

    >>> matlabstr2py('1')
    1
    >>> matlabstr2py("['x y z' true false; 1 2.0 -3e4; NaN Inf @class]")
    [['x y z', True, False], [1, 2.0, -30000.0], [nan, inf, '@class']]
    >>> d = matlabstr2py("SI.hChannels.channelType = {'stripe' 'stripe'}\\n"
    ...                  "SI.hChannels.channelsActive = 2")
    >>> d['SI.hChannels.channelType']
    ['stripe', 'stripe']

    """
    # TODO: handle invalid input
    # TODO: review unboxing of multidimensional arrays

    def lex(s):
        # return sequence of tokens from matlab string representation
        tokens = ['[']
        while True:
            t, i = next_token(s)
            if t is None:
                break
            if t == ';':
                tokens.extend((']', '['))
            elif t == '[':
                tokens.extend(('[', '['))
            elif t == ']':
                tokens.extend((']', ']'))
            else:
                tokens.append(t)
            s = s[i:]
        tokens.append(']')
        return tokens

    def next_token(s):
        # return next token in matlab string
        length = len(s)
        if length == 0:
            return None, 0
        i = 0
        while i < length and s[i] == ' ':
            i += 1
        if i == length:
            return None, i
        if s[i] in '{[;]}':
            return s[i], i + 1
        if s[i] == "'":
            j = i + 1
            while j < length and s[j] != "'":
                j += 1
            return s[i: j+1], j + 1
        j = i
        while j < length and not s[j] in ' {[;]}':
            j += 1
        return s[i:j], j

    def value(s, fail=False):
        # return Python value of token
        s = s.strip()
        if not s:
            return s
        if len(s) == 1:
            try:
                return int(s)
            except Exception:
                if fail:
                    raise ValueError()
                return s
        if s[0] == "'":
            if fail and s[-1] != "'" or "'" in s[1:-1]:
                raise ValueError()
            return s[1:-1]
        if fail and any(i in s for i in " ';[]{}"):
            raise ValueError()
        if s[0] == '@':
            return s
        if s == 'true':
            return True
        if s == 'false':
            return False
        if '.' in s or 'e' in s:
            return float(s)
        try:
            return int(s)
        except Exception:
            pass
        try:
            return float(s)  # nan, inf
        except Exception:
            if fail:
                raise ValueError()
        return s

    def parse(s):
        # return Python value from string representation of Matlab value
        s = s.strip()
        try:
            return value(s, fail=True)
        except ValueError:
            pass
        result = add2 = []
        levels = [add2]
        for t in lex(s):
            if t in '[{':
                add2 = []
                levels.append(add2)
            elif t in ']}':
                x = levels.pop()
                if len(x) == 1 and isinstance(x[0], list):
                    x = x[0]
                add2 = levels[-1]
                add2.append(x)
            else:
                add2.append(value(t))
        if len(result) == 1 and isinstance(result[0], list):
            result = result[0]
        return result

    if '\r' in s or '\n' in s:
        # structure
        d = {}
        for line in s.splitlines():
            if not line.strip():
                continue
            k, v = line.split('=', 1)
            k = k.strip()
            if any(c in k for c in " ';[]{}"):
                continue
            d[k] = parse(v.strip())
        return d
    return parse(s)


def stripnull(string, null=b'\x00'):
    """Return string truncated at first null character.

    Clean NULL terminated C strings. For unicode strings use null='\\0'.

    >>> stripnull(b'string\\x00')
    b'string'
    >>> stripnull('string\\x00', null='\\0')
    'string'

    """
    i = string.find(null)
    return string if (i < 0) else string[:i]


def stripascii(string):
    """Return string truncated at last byte that is 7-bit ASCII.

    Clean NULL separated and terminated TIFF strings.

    >>> stripascii(b'string\\x00string\\n\\x01\\x00')
    b'string\\x00string\\n'
    >>> stripascii(b'\\x00')
    b''

    """
    # TODO: pythonize this
    i = len(string)
    while i:
        i -= 1
        if 8 < byte2int(string[i]) < 127:
            break
    else:
        i = -1
    return string[:i+1]


def asbool(value, true=(b'true', u'true'), false=(b'false', u'false')):
    """Return string as bool if possible, else raise TypeError.

    >>> asbool(b' False ')
    False

    """
    value = value.strip().lower()
    if value in true:  # might raise UnicodeWarning/BytesWarning
        return True
    if value in false:
        return False
    raise TypeError()


def astype(value, types=None):
    """Return argument as one of types if possible.

    >>> astype('42')
    42
    >>> astype('3.14')
    3.14
    >>> astype('True')
    True
    >>> astype(b'Neee-Wom')
    'Neee-Wom'

    """
    if types is None:
        types = int, float, asbool, bytes2str
    for typ in types:
        try:
            return typ(value)
        except (ValueError, AttributeError, TypeError, UnicodeEncodeError):
            pass
    return value


def format_size(size, threshold=1536):
    """Return file size as string from byte size.

    >>> format_size(1234)
    '1234 B'
    >>> format_size(12345678901)
    '11.50 GiB'

    """
    if size < threshold:
        return "%i B" % size
    for unit in ('KiB', 'MiB', 'GiB', 'TiB', 'PiB'):
        size /= 1024.0
        if size < threshold:
            return "%.2f %s" % (size, unit)


def identityfunc(arg):
    """Single argument identity function.

    >>> identityfunc('arg')
    'arg'

    """
    return arg


def nullfunc(*args, **kwargs):
    """Null function.

    >>> nullfunc('arg', kwarg='kwarg')

    """
    return


def sequence(value):
    """Return tuple containing value if value is not a sequence.

    >>> sequence(1)
    (1,)
    >>> sequence([1])
    [1]

    """
    try:
        len(value)
        return value
    except TypeError:
        return (value,)


def product(iterable):
    """Return product of sequence of numbers.

    Equivalent of functools.reduce(operator.mul, iterable, 1).
    Multiplying numpy integers might overflow.

    >>> product([2**8, 2**30])
    274877906944
    >>> product([])
    1

    """
    prod = 1
    for i in iterable:
        prod *= i
    return prod


def natural_sorted(iterable):
    """Return human sorted list of strings.

    E.g. for sorting file names.

    >>> natural_sorted(['f1', 'f2', 'f10'])
    ['f1', 'f2', 'f10']

    """
    def sortkey(x):
        return [(int(c) if c.isdigit() else c) for c in re.split(numbers, x)]

    numbers = re.compile(r'(\d+)')
    return sorted(iterable, key=sortkey)


def excel_datetime(timestamp, epoch=datetime.datetime.fromordinal(693594)):
    """Return datetime object from timestamp in Excel serial format.

    Convert LSM time stamps.

    >>> excel_datetime(40237.029999999795)
    datetime.datetime(2010, 2, 28, 0, 43, 11, 999982)

    """
    return epoch + datetime.timedelta(timestamp)


def julian_datetime(julianday, milisecond=0):
    """Return datetime from days since 1/1/4713 BC and ms since midnight.

    Convert Julian dates according to MetaMorph.

    >>> julian_datetime(2451576, 54362783)
    datetime.datetime(2000, 2, 2, 15, 6, 2, 783)

    """
    if julianday <= 1721423:
        # no datetime before year 1
        return None

    a = julianday + 1
    if a > 2299160:
        alpha = math.trunc((a - 1867216.25) / 36524.25)
        a += 1 + alpha - alpha // 4
    b = a + (1524 if a > 1721423 else 1158)
    c = math.trunc((b - 122.1) / 365.25)
    d = math.trunc(365.25 * c)
    e = math.trunc((b - d) / 30.6001)

    day = b - d - math.trunc(30.6001 * e)
    month = e - (1 if e < 13.5 else 13)
    year = c - (4716 if month > 2.5 else 4715)

    hour, milisecond = divmod(milisecond, 1000 * 60 * 60)
    minute, milisecond = divmod(milisecond, 1000 * 60)
    second, milisecond = divmod(milisecond, 1000)

    return datetime.datetime(year, month, day,
                             hour, minute, second, milisecond)


def byteorder_isnative(byteorder):
    """Return if byteorder matches the system's byteorder.

    >>> byteorder_isnative('=')
    True

    """
    if byteorder == '=' or byteorder == sys.byteorder:
        return True
    keys = {'big': '>', 'little': '<'}
    return keys.get(byteorder, byteorder) == keys[sys.byteorder]


def recarray2dict(recarray):
    """Return numpy.recarray as dict."""
    # TODO: subarrays
    result = {}
    for descr, value in zip(recarray.dtype.descr, recarray):
        name, dtype = descr[:2]
        if dtype[1] == 'S':
            value = bytes2str(stripnull(value))
        elif value.ndim < 2:
            value = value.tolist()
        result[name] = value
    return result


def xml2dict(xml, sanitize=True, prefix=None):
    """Return XML as dict.

    >>> xml2dict('<?xml version="1.0" ?><root attr="name"><key>1</key></root>')
    {'root': {'key': 1, 'attr': 'name'}}

    """
    from collections import defaultdict  # delayed import
    from xml.etree import cElementTree as etree  # delayed import

    at = tx = ''
    if prefix:
        at, tx = prefix

    def astype(value):
        # return value as int, float, bool, or str
        for t in (int, float, asbool):
            try:
                return t(value)
            except Exception:
                pass
        return value

    def etree2dict(t):
        # adapted from https://stackoverflow.com/a/10077069/453463
        key = t.tag
        if sanitize:
            key = key.rsplit('}', 1)[-1]
        d = {key: {} if t.attrib else None}
        children = list(t)
        if children:
            dd = defaultdict(list)
            for dc in map(etree2dict, children):
                for k, v in dc.items():
                    dd[k].append(astype(v))
            d = {key: {k: astype(v[0]) if len(v) == 1 else astype(v)
                       for k, v in dd.items()}}
        if t.attrib:
            d[key].update((at + k, astype(v)) for k, v in t.attrib.items())
        if t.text:
            text = t.text.strip()
            if children or t.attrib:
                if text:
                    d[key][tx + 'value'] = astype(text)
            else:
                d[key] = astype(text)
        return d

    return etree2dict(etree.fromstring(xml))


def pformat_xml(arg):
    """Return pretty formatted XML."""
    try:
        import lxml.etree as etree  # delayed import
        if not isinstance(arg, bytes):
            arg = arg.encode('utf-8')
        xml = etree.fromstring(arg)
        xml = etree.tostring(xml, pretty_print=True, encoding="unicode")
    except Exception:
        xml = bytes2str(arg).replace('><', '>\n<').replace('><', '>\n<')
    return xml.replace('  ', ' ').replace('\t', ' ')


def pformat(arg, maxlines=None, linewidth=None, compact=True):
    """Return pretty formatted representation of object as string."""
    if maxlines is None:
        maxlines = TIFF.PRINT_MAX_LINES
    elif not maxlines:
        maxlines = 2**32
    if linewidth is None:
        linewidth = TIFF.PRINT_LINE_WIDTH
    elif not linewidth:
        linewidth = 2**32

    numpy.set_printoptions(threshold=100, linewidth=linewidth)

    if isinstance(arg, basestring):
        if arg[:5].lower() in ('<?xml', b'<?xml'):
            arg = pformat_xml(arg)
        elif isinstance(arg, bytes):
            try:
                arg = bytes2str(arg)
                arg = arg.replace('\r', '\n').replace('\n\n', '\n')
            except Exception:
                import binascii  # delayed import
                import pprint  # delayed import
                arg = binascii.hexlify(arg)
                arg = pprint.pformat(arg, width=linewidth)
                maxlines = min(maxlines, 16)
        arg = arg.rstrip()
    elif isinstance(arg, numpy.record):
        arg = arg.pprint()
    else:
        from pprint import pformat  # delayed import
        compact = {} if sys.version_info[0] == 2 else dict(compact=compact)
        arg = pformat(arg, width=linewidth, **compact)

    argl = list(arg.splitlines())
    if len(argl) > maxlines:
        arg = '\n'.join(argl[:maxlines] +
                        ['...truncated to %i lines.' % maxlines])
    return arg


def snipstr(string, length=16, ellipse=None):
    """Return string cut in middle to specified length.

    >>> snipstr('abcdefghijklmnop', 8)
    'abcd…nop'

    """
    size = len(string)
    if size <= length:
        return string
    if ellipse is None:
        if isinstance(string, bytes):
            ellipse = b'...'
        else:
            ellipse = u'\u2026'
    esize = len(ellipse)
    if length < esize + 1:
        return string[:length]
    if length < esize + 4:
        return string[:length-esize] + ellipse
    half = (length - esize) // 2
    return string[:half + (length-esize) % 2] + ellipse + string[-half:]


def enumarg(enum, arg):
    """Return enum member from its name or value.

    >>> enumarg(TIFF.PHOTOMETRIC, 2)
    <PHOTOMETRIC.RGB: 2>
    >>> enumarg(TIFF.PHOTOMETRIC, 'RGB')
    <PHOTOMETRIC.RGB: 2>

    """
    try:
        return enum(arg)
    except Exception:
        try:
            return enum[arg.upper()]
        except Exception:
            raise ValueError("invalid argument %s" % arg)


def parse_kwargs(kwargs, *keys, **keyvalues):
    """Return dict with keys from keys|keyvals and values from kwargs|keyvals.

    Existing keys are deleted from kwargs.

    >>> kwargs = {'one': 1, 'two': 2, 'four': 4}
    >>> kwargs2 = parse_kwargs(kwargs, 'two', 'three', four=None, five=5)
    >>> kwargs == {'one': 1}
    True
    >>> kwargs2 == {'two': 2, 'four': 4, 'five': 5}
    True

    """
    result = {}
    for key in keys:
        if key in kwargs:
            result[key] = kwargs[key]
            del kwargs[key]
    for key, value in keyvalues.items():
        if key in kwargs:
            result[key] = kwargs[key]
            del kwargs[key]
        else:
            result[key] = value
    return result


def update_kwargs(kwargs, **keyvalues):
    """Update dict with keys and values if keys do not already exist.

    >>> kwargs = {'one': 1, }
    >>> update_kwargs(kwargs, one=None, two=2)
    >>> kwargs == {'one': 1, 'two': 2}
    True

    """
    for key, value in keyvalues.items():
        if key not in kwargs:
            kwargs[key] = value


def lsm2bin(lsmfile, binfile=None, tile=(256, 256), verbose=True):
    """Convert [MP]TZCYX LSM file to series of BIN files.

    One BIN file containing 'ZCYX' data is created for each position, time,
    and tile. The position, time, and tile indices are encoded at the end
    of the filenames.

    """
    verbose = print_ if verbose else nullfunc

    if binfile is None:
        binfile = lsmfile
    elif binfile.lower() == 'none':
        binfile = None
    if binfile:
        binfile += "_(z%ic%iy%ix%i)_m%%ip%%it%%03iy%%ix%%i.bin"

    verbose("\nOpening LSM file... ", end='', flush=True)
    start_time = time.time()

    with TiffFile(lsmfile) as lsm:
        if not lsm.is_lsm:
            verbose("\n", lsm, flush=True)
            raise ValueError("not a LSM file")
        series = lsm.series[0]  # first series contains the image data
        shape = series.shape
        axes = series.axes
        dtype = series.dtype
        size = product(shape) * dtype.itemsize

        verbose("%.3f s" % (time.time() - start_time))
        # verbose(lsm, flush=True)
        verbose("Image\n  axes:  %s\n  shape: %s\n  dtype: %s\n  size:  %s"
                % (axes, shape, dtype, format_size(size)), flush=True)
        if not series.axes.endswith('TZCYX'):
            raise ValueError("not a *TZCYX LSM file")

        verbose("Copying image from LSM to BIN files", end='', flush=True)
        start_time = time.time()
        tiles = shape[-2] // tile[-2], shape[-1] // tile[-1]
        if binfile:
            binfile = binfile % (shape[-4], shape[-3], tile[0], tile[1])
        shape = (1,) * (7-len(shape)) + shape
        # cache for ZCYX stacks and output files
        data = numpy.empty(shape[3:], dtype=dtype)
        out = numpy.empty((shape[-4], shape[-3], tile[0], tile[1]),
                          dtype=dtype)
        # iterate over Tiff pages containing data
        pages = iter(series.pages)
        for m in range(shape[0]):  # mosaic axis
            for p in range(shape[1]):  # position axis
                for t in range(shape[2]):  # time axis
                    for z in range(shape[3]):  # z slices
                        data[z] = next(pages).asarray()
                    for y in range(tiles[0]):  # tile y
                        for x in range(tiles[1]):  # tile x
                            out[:] = data[...,
                                          y*tile[0]:(y+1)*tile[0],
                                          x*tile[1]:(x+1)*tile[1]]
                            if binfile:
                                out.tofile(binfile % (m, p, t, y, x))
                            verbose('.', end='', flush=True)
        verbose(" %.3f s" % (time.time() - start_time))


def imshow(data, title=None, vmin=0, vmax=None, cmap=None,
           bitspersample=None, photometric='RGB',
           interpolation=None, dpi=96, figure=None, subplot=111, maxdim=32768,
           **kwargs):
    """Plot n-dimensional images using matplotlib.pyplot.

    Return figure, subplot and plot axis.
    Requires pyplot already imported C{from matplotlib import pyplot}.

    Parameters
    ----------
    bitspersample : int or None
        Number of bits per channel in integer RGB images.
    photometric : {'MINISWHITE', 'MINISBLACK', 'RGB', or 'PALETTE'}
        The color space of the image data.
    title : str
        Window and subplot title.
    figure : matplotlib.figure.Figure (optional).
        Matplotlib to use for plotting.
    subplot : int
        A matplotlib.pyplot.subplot axis.
    maxdim : int
        maximum image width and length.
    kwargs : optional
        Arguments for matplotlib.pyplot.imshow.

    """
    isrgb = photometric in ('RGB',)  # 'PALETTE'
    if isrgb and not (data.shape[-1] in (3, 4) or (
            data.ndim > 2 and data.shape[-3] in (3, 4))):
        isrgb = False
        photometric = 'MINISWHITE'

    data = data.squeeze()
    if photometric in ('MINISWHITE', 'MINISBLACK', None):
        data = reshape_nd(data, 2)
    else:
        data = reshape_nd(data, 3)

    dims = data.ndim
    if dims < 2:
        raise ValueError("not an image")
    elif dims == 2:
        dims = 0
        isrgb = False
    else:
        if isrgb and data.shape[-3] in (3, 4):
            data = numpy.swapaxes(data, -3, -2)
            data = numpy.swapaxes(data, -2, -1)
        elif not isrgb and (data.shape[-1] < data.shape[-2] // 8 and
                            data.shape[-1] < data.shape[-3] // 8 and
                            data.shape[-1] < 5):
            data = numpy.swapaxes(data, -3, -1)
            data = numpy.swapaxes(data, -2, -1)
        isrgb = isrgb and data.shape[-1] in (3, 4)
        dims -= 3 if isrgb else 2

    if isrgb:
        data = data[..., :maxdim, :maxdim, :maxdim]
    else:
        data = data[..., :maxdim, :maxdim]

    if photometric == 'PALETTE' and isrgb:
        datamax = data.max()
        if datamax > 255:
            data = data >> 8  # possible precision loss
        data = data.astype('B')
    elif data.dtype.kind in 'ui':
        if not (isrgb and data.dtype.itemsize <= 1) or bitspersample is None:
            try:
                bitspersample = int(math.ceil(math.log(data.max(), 2)))
            except Exception:
                bitspersample = data.dtype.itemsize * 8
        elif not isinstance(bitspersample, inttypes):
            # bitspersample can be tuple, e.g. (5, 6, 5)
            bitspersample = data.dtype.itemsize * 8
        datamax = 2**bitspersample
        if isrgb:
            if bitspersample < 8:
                data = data << (8 - bitspersample)
            elif bitspersample > 8:
                data = data >> (bitspersample - 8)  # precision loss
            data = data.astype('B')
    elif data.dtype.kind == 'f':
        datamax = data.max()
        if isrgb and datamax > 1.0:
            if data.dtype.char == 'd':
                data = data.astype('f')
                data /= datamax
            else:
                data = data / datamax
    elif data.dtype.kind == 'b':
        datamax = 1
    elif data.dtype.kind == 'c':
        data = numpy.absolute(data)
        datamax = data.max()

    if not isrgb:
        if vmax is None:
            vmax = datamax
        if vmin is None:
            if data.dtype.kind == 'i':
                dtmin = numpy.iinfo(data.dtype).min
                vmin = numpy.min(data)
                if vmin == dtmin:
                    vmin = numpy.min(data > dtmin)
            if data.dtype.kind == 'f':
                dtmin = numpy.finfo(data.dtype).min
                vmin = numpy.min(data)
                if vmin == dtmin:
                    vmin = numpy.min(data > dtmin)
            else:
                vmin = 0

    pyplot = sys.modules['matplotlib.pyplot']

    if figure is None:
        pyplot.rc('font', family='sans-serif', weight='normal', size=8)
        figure = pyplot.figure(dpi=dpi, figsize=(10.3, 6.3), frameon=True,
                               facecolor='1.0', edgecolor='w')
        try:
            figure.canvas.manager.window.title(title)
        except Exception:
            pass
        l = len(title.splitlines()) if title else 1
        pyplot.subplots_adjust(bottom=0.03*(dims+2), top=0.98-l*0.03,
                               left=0.1, right=0.95, hspace=0.05, wspace=0.0)
    subplot = pyplot.subplot(subplot)

    if title:
        try:
            title = unicode(title, 'Windows-1252')
        except TypeError:
            pass
        pyplot.title(title, size=11)

    if cmap is None:
        if data.dtype.kind in 'ubf' or vmin == 0:
            cmap = 'viridis'
        else:
            cmap = 'coolwarm'
        if photometric == 'MINISWHITE':
            cmap += '_r'

    image = pyplot.imshow(data[(0,) * dims].squeeze(), vmin=vmin, vmax=vmax,
                          cmap=cmap, interpolation=interpolation, **kwargs)

    if not isrgb:
        pyplot.colorbar()  # panchor=(0.55, 0.5), fraction=0.05

    def format_coord(x, y):
        # callback function to format coordinate display in toolbar
        x = int(x + 0.5)
        y = int(y + 0.5)
        try:
            if dims:
                return "%s @ %s [%4i, %4i]" % (
                    curaxdat[1][y, x], current, y, x)
            return "%s @ [%4i, %4i]" % (data[y, x], y, x)
        except IndexError:
            return ''

    def none(event):
        return ''

    subplot.format_coord = format_coord
    image.get_cursor_data = none
    image.format_cursor_data = none

    if dims:
        current = list((0,) * dims)
        curaxdat = [0, data[tuple(current)].squeeze()]
        sliders = [pyplot.Slider(
            pyplot.axes([0.125, 0.03*(axis+1), 0.725, 0.025]),
            'Dimension %i' % axis, 0, data.shape[axis]-1, 0, facecolor='0.5',
            valfmt='%%.0f [%i]' % data.shape[axis]) for axis in range(dims)]
        for slider in sliders:
            slider.drawon = False

        def set_image(current, sliders=sliders, data=data):
            # change image and redraw canvas
            curaxdat[1] = data[tuple(current)].squeeze()
            image.set_data(curaxdat[1])
            for ctrl, index in zip(sliders, current):
                ctrl.eventson = False
                ctrl.set_val(index)
                ctrl.eventson = True
            figure.canvas.draw()

        def on_changed(index, axis, data=data, current=current):
            # callback function for slider change event
            index = int(round(index))
            curaxdat[0] = axis
            if index == current[axis]:
                return
            if index >= data.shape[axis]:
                index = 0
            elif index < 0:
                index = data.shape[axis] - 1
            current[axis] = index
            set_image(current)

        def on_keypressed(event, data=data, current=current):
            # callback function for key press event
            key = event.key
            axis = curaxdat[0]
            if str(key) in '0123456789':
                on_changed(key, axis)
            elif key == 'right':
                on_changed(current[axis] + 1, axis)
            elif key == 'left':
                on_changed(current[axis] - 1, axis)
            elif key == 'up':
                curaxdat[0] = 0 if axis == len(data.shape)-1 else axis + 1
            elif key == 'down':
                curaxdat[0] = len(data.shape)-1 if axis == 0 else axis - 1
            elif key == 'end':
                on_changed(data.shape[axis] - 1, axis)
            elif key == 'home':
                on_changed(0, axis)

        figure.canvas.mpl_connect('key_press_event', on_keypressed)
        for axis, ctrl in enumerate(sliders):
            ctrl.on_changed(lambda k, a=axis: on_changed(k, a))

    return figure, subplot, image


def _app_show():
    """Block the GUI. For use as skimage plugin."""
    pyplot = sys.modules['matplotlib.pyplot']
    pyplot.show()


def askopenfilename(**kwargs):
    """Return file name(s) from Tkinter's file open dialog."""
    try:
        from Tkinter import Tk
        import tkFileDialog as filedialog
    except ImportError:
        from tkinter import Tk, filedialog
    root = Tk()
    root.withdraw()
    root.update()
    filenames = filedialog.askopenfilename(**kwargs)
    root.destroy()
    return filenames


def main(argv=None):
    """Command line usage main function."""
    if float(sys.version[0:3]) < 2.7:
        print("This script requires Python version 2.7 or better.")
        print("This is Python version %s" % sys.version)
        return 0
    if argv is None:
        argv = sys.argv

    import optparse  # TODO: use argparse

    parser = optparse.OptionParser(
        usage="usage: %prog [options] path",
        description="Display image data in TIFF files.",
        version="%%prog %s" % __version__)
    opt = parser.add_option
    opt('-p', '--page', dest='page', type='int', default=-1,
        help="display single page")
    opt('-s', '--series', dest='series', type='int', default=-1,
        help="display series of pages of same shape")
    opt('--nomultifile', dest='nomultifile', action='store_true',
        default=False, help="do not read OME series from multiple files")
    opt('--noplots', dest='noplots', type='int', default=8,
        help="maximum number of plots")
    opt('--interpol', dest='interpol', metavar='INTERPOL', default='bilinear',
        help="image interpolation method")
    opt('--dpi', dest='dpi', type='int', default=96,
        help="plot resolution")
    opt('--vmin', dest='vmin', type='int', default=None,
        help="minimum value for colormapping")
    opt('--vmax', dest='vmax', type='int', default=None,
        help="maximum value for colormapping")
    opt('--debug', dest='debug', action='store_true', default=False,
        help="raise exception on failures")
    opt('--doctest', dest='doctest', action='store_true', default=False,
        help="runs the docstring examples")
    opt('-v', '--detail', dest='detail', type='int', default=2)
    opt('-q', '--quiet', dest='quiet', action='store_true')

    settings, path = parser.parse_args()
    path = ' '.join(path)

    if settings.doctest:
        import doctest
        doctest.testmod(optionflags=doctest.ELLIPSIS)
        return 0
    if not path:
        path = askopenfilename(title="Select a TIFF file",
                               filetypes=TIFF.FILEOPEN_FILTER)
        if not path:
            parser.error("No file specified")

    if any(i in path for i in '?*'):
        path = glob.glob(path)
        if not path:
            print('no files match the pattern')
            return 0
        # TODO: handle image sequences
        path = path[0]

    if not settings.quiet:
        print("\nReading file structure...", end=' ')
    start = time.time()
    try:
        tif = TiffFile(path, multifile=not settings.nomultifile)
    except Exception as e:
        if settings.debug:
            raise
        else:
            print("\n", e)
            sys.exit(0)
    if not settings.quiet:
        print("%.3f ms" % ((time.time()-start) * 1e3))

    if tif.is_ome:
        settings.norgb = True

    images = []
    if settings.noplots > 0:
        if not settings.quiet:
            print("Reading image data... ", end=' ')

        def notnone(x):
            return next(i for i in x if i is not None)

        start = time.time()
        try:
            if settings.page >= 0:
                images = [(tif.asarray(key=settings.page),
                           tif[settings.page], None)]
            elif settings.series >= 0:
                images = [(tif.asarray(series=settings.series),
                           notnone(tif.series[settings.series].pages),
                           tif.series[settings.series])]
            else:
                images = []
                for i, s in enumerate(tif.series[:settings.noplots]):
                    try:
                        images.append((tif.asarray(series=i),
                                       notnone(s.pages),
                                       tif.series[i]))
                    except ValueError as e:
                        images.append((None, notnone(s.pages), None))
                        if settings.debug:
                            raise
                        else:
                            print("\nSeries %i failed: %s... " % (i, e),
                                  end='')
            if not settings.quiet:
                print("%.3f ms" % ((time.time()-start) * 1e3))
        except Exception as e:
            if settings.debug:
                raise
            else:
                print(e)

    if not settings.quiet:
        print()
        print(TiffFile.__str__(tif, detail=int(settings.detail)))
        print()
    tif.close()

    if images and settings.noplots > 0:
        try:
            import matplotlib
            matplotlib.use('TkAgg')
            from matplotlib import pyplot
        except ImportError as e:
            warnings.warn("failed to import matplotlib.\n%s" % e)
        else:
            for img, page, series in images:
                if img is None:
                    continue
                vmin, vmax = settings.vmin, settings.vmax
                if 'GDAL_NODATA' in page.tags:
                    try:
                        vmin = numpy.min(
                            img[img > float(page.tags['GDAL_NODATA'].value)])
                    except ValueError:
                        pass
                if tif.is_stk:
                    try:
                        vmin = tif.stk_metadata['MinScale']
                        vmax = tif.stk_metadata['MaxScale']
                    except KeyError:
                        pass
                    else:
                        if vmax <= vmin:
                            vmin, vmax = settings.vmin, settings.vmax
                if series:
                    title = "%s\n%s\n%s" % (str(tif), str(page), str(series))
                else:
                    title = "%s\n %s" % (str(tif), str(page))
                photometric = 'MINISBLACK'
                if page.photometric not in (3,):
                    photometric = TIFF.PHOTOMETRIC(page.photometric).name
                imshow(img, title=title, vmin=vmin, vmax=vmax,
                       bitspersample=page.bitspersample,
                       photometric=photometric,
                       interpolation=settings.interpol,
                       dpi=settings.dpi)
            pyplot.show()


if sys.version_info[0] == 2:
    inttypes = int, long  # noqa

    def print_(*args, **kwargs):
        """Print function with flush support."""
        flush = kwargs.pop('flush', False)
        print(*args, **kwargs)
        if flush:
            sys.stdout.flush()

    def bytes2str(b, encoding=None, errors=None):
        """Return string from bytes."""
        return b

    def str2bytes(s, encoding=None):
        """Return bytes from string."""
        return s

    def byte2int(b):
        """Return value of byte as int."""
        return ord(b)

    class FileNotFoundError(IOError):
        pass

    TiffFrame = TiffPage  # noqa
else:
    inttypes = int
    basestring = str, bytes
    unicode = str
    print_ = print

    def bytes2str(b, encoding=None, errors='strict'):
        """Return unicode string from encoded bytes."""
        if encoding is not None:
            return b.decode(encoding, errors)
        try:
            return b.decode('utf-8', errors)
        except UnicodeDecodeError:
            return b.decode('cp1252', errors)

    def str2bytes(s, encoding='cp1252'):
        """Return bytes from unicode string."""
        return s.encode(encoding)

    def byte2int(b):
        """Return value of byte as int."""
        return b

if __name__ == "__main__":
    sys.exit(main())
