#include <acquire-zarr/acquire-zarr.hh>
#include <iostream>

int main() {
    // Create a new instance of the AcquireZarr class
    AcquireZarrWriter writer;

    writer.set_uri("simple.zarr");
    writer.set_use_v3(false);
    writer.set_dimensions({"x","y","t"});
    writer.set_dimension_sizes({64,64,0});
    writer.set_chunk_sizes({64,64,1});
    writer.set_shard_sizes({64,64,1});
    writer.set_enable_multiscale(false);
    writer.set_compression_codec(AcquireZarrCompressionCodec::COMPRESSION_NONE);
    writer.set_compression_level(0);
    writer.set_pixel_scale_x(1.0);
    writer.set_pixel_scale_y(1.0);
    writer.set_first_frame_id(0);
    //writer.setExternalMetadata("{metadata}");

    writer.start();

    writer.stop();

  return 0;
}
