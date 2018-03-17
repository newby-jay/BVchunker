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

## Future file reader types
  - AVI: The format looks friendly enough. The frame offsets should be stored in the footer. I want to provide limited AVI support, particularly for those that are generated with the popular Matlab control software.
  - OME XML: This should be easy to provide, but might not have many users. Processing would be a simple extension of the text reader in the Beam package. Compressed files should be straightforward to handle.
