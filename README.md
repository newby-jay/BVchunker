# Bio video data chunker
Apache Beam library (in Python) for reading and splitting bio video data (currently just Nikon ND2 files) into chunks of specified size, containing specified overlap. Overlapping regions are useful when, for example, convolutional neural networks are used that require local image patches. Without overlap, artifacts would be introduced at the chunk boundaries. Beam transforms are included for reassembling (remapping and combining) unorganized position-times.

## ND2 Reader
I have a working ND2 file reader, tested on several files generated from at least two different labs and multiple microscopes. I have not processed all of the metadata, but have enough to do 3D tracking. There is no support for 8bit images or compressed files.

## TIF reader - March 16-17
I have a basic TIF reader working, but it is extremely slow at best with the Dataflow runner. The issue is that it takes too many seek operations to collect all the frame offsets. TIF files are uniquely ill suited for the cloud. It's an ancient format that uses optimizations that are no longer necessary. (More on this later.) Most file types use some kind of marker to cut up the file into pieces, e.g., '\n' or '\r\n' in a text file. In the case of TIF, directory information is stored in a block called an IFD, which has everything you need to read one image frame. The IFDs are not preceded by a specific marker. Instead, the first IFD points to the second, which points to the third and so on. The IFDs and the data blocks they point to can be arranged in the file in many different ways. I have looked at a few variations.
  - imageJ: The IFDs are all at the back of the file. This is actually great because all of the offsets can be determined quickly with one large read. I think this might technically be against the "rules" for TIF files. The idea is that each IFD should only point to data in its own block of the file so that images can be edited without corrupting the rest of the file.
  - OME: The IFDs are stored sequentially, and the frame data block offset is stored in a block that is separate from the IFD. The IFD points to a block that points to the image data. This is because the block stores offsets to each row of the image data. The combined offsets for every row is too large to fit in the IFD, so it has to be moved elsewhere. When I made on OME TIF using the bioFormat plugin in imageJ (prepackaged with FIJI), all of the IFDs and image data blocks were equally spaced in the file, which means that only the first IFD needs to be read to get the first block offset and the block size. All of the block offsets are shifted by a multiple of the block size.
  - Metamorph: TIF files saved by the Metamorph microscope control software are complicated by several factors. Like OME, they also save image data offsets in a separate block. The data blocks appear before their IFD, i.e., the IFD is at the end of its block and points backward. The records are not equally spaced, so every IFD and offset block needs to be read prior to processing the file.

Out of those I've tested, only OME TIF files have sufficient metadata to do 3D tracking. I can get the number of z slices for imageJ, but I can't get the physical pixel spacing and z-axis slice spacing. I need the ratio to link localizations into tracks. The reader can process 8-bit and 16-bit files. There is no support for compression.

## Ideas:
  - IFD tags don't have explicit markers, but they should all partially match the first IFD. Specifically, the image dimension and pixel type should be the same. Technically, they don't have to be the same, in general, but must be the same for video files. This should serve as a marker to search for, which will enable a multi-threaded reader without pre scanning for all of the offsets.
    - The imageJ format is different and must be handled separately like the ND2 reader
    - Use TAGS 256, 257, 258, 259 and partial on 273 as a search pattern
    - March 19: The idea has a major problem. If multiple threads are pulling down the file, they wont know the position of the frame in the sequence. If this issue can be resolved. It should be possible to build on TIFReader-experimental.py.
  - A data buffer will probably help speed up the reader, but this poses a problem for Metamorph files that have their IFD at the end of the block and point backward.
    - I think I can fix this issue using TIFReader-experimental by setting a flag so that it is set to either the beginning or end of the block.
  - Expand to recognize and handle BIGTIF files (magic number 43 and 64 bit offset blocks)

## PIMS reader - April 1
I added a "PIMS" reader that can be used to process a wide range of file types. The downside is that it requires a large local hard drive for all workers. Videos are downloaded locally first and then processed into the pipeline. This adds to the cost of running the pipeline, particularly for sets with large videos. The PD size must be set to around 4-5 times the size of the largest video in the set.
   - Using a StringIO buffer does not work with the PIMS package because it does not accept a file object as input. If this ever changes, we can add a faster sub reader, like the one in the TIFReader, for small videos that can fit in memory.

## PIMS reader August 15th
I have a working TIF reader for 2D (generic TIF) and 3D (OME TIF only). I read the first IFD and generate a regular expression pattern for subsequent IFDs, based on matching the number of TAGs and up to four TAG types that should be consistent throughout the video (image size, pixel type, compression type). There is also support for regular TIF and BIG TIFF.

I solved the problem of detecting the number of frames, which is not consistently in the metadata. Offset data records are output in a separate tagged Pcollection. Offset data is organized (also counting the number of frames) and passed to the BVchunker node as a side input.

## Future file reader types
  - AVI: The format looks friendly enough. The frame offsets should be stored in the footer. I want to provide limited AVI support, particularly for those that are generated with the popular Matlab control software.
  - HDF5: an open format. I'm not sure how often it is used for microscopy videos.
