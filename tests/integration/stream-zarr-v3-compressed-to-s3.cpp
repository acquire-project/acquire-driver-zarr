#include "acquire.zarr.h"
#include "test.macros.hh"

#include <nlohmann/json.hpp>
#include <miniocpp/client.h>

#include <vector>

namespace {
std::string s3_endpoint, s3_bucket_name, s3_access_key_id, s3_secret_access_key;

const unsigned int array_width = 64, array_height = 48, array_planes = 6,
                   array_channels = 8, array_timepoints = 10;

const unsigned int chunk_width = 16, chunk_height = 16, chunk_planes = 2,
                   chunk_channels = 4, chunk_timepoints = 5;

const unsigned int shard_width = 2, shard_height = 1, shard_planes = 1,
                   shard_channels = 2, shard_timepoints = 2;
const unsigned int chunks_per_shard =
  shard_width * shard_height * shard_planes * shard_channels * shard_timepoints;

const unsigned int chunks_in_x =
  (array_width + chunk_width - 1) / chunk_width; // 4 chunks
const unsigned int chunks_in_y =
  (array_height + chunk_height - 1) / chunk_height; // 3 chunks
const unsigned int chunks_in_z =
  (array_planes + chunk_planes - 1) / chunk_planes; // 3 chunks
const unsigned int chunks_in_c =
  (array_channels + chunk_channels - 1) / chunk_channels; // 2 chunks
const unsigned int chunks_in_t =
  (array_timepoints + chunk_timepoints - 1) / chunk_timepoints;

const unsigned int shards_in_x =
  (chunks_in_x + shard_width - 1) / shard_width; // 2 shards
const unsigned int shards_in_y =
  (chunks_in_y + shard_height - 1) / shard_height; // 3 shards
const unsigned int shards_in_z =
  (chunks_in_z + shard_planes - 1) / shard_planes; // 3 shards
const unsigned int shards_in_c =
  (chunks_in_c + shard_channels - 1) / shard_channels; // 1 shard
const unsigned int shards_in_t =
  (chunks_in_t + shard_timepoints - 1) / shard_timepoints; // 1 shard

const size_t nbytes_px = sizeof(uint16_t);
const uint32_t frames_to_acquire =
  array_planes * array_channels * array_timepoints;
const size_t bytes_of_frame = array_width * array_height * nbytes_px;

bool
get_credentials()
{
    char* env = nullptr;
    if (!(env = std::getenv("ZARR_S3_ENDPOINT"))) {
        LOG_ERROR("ZARR_S3_ENDPOINT not set.");
        return false;
    }
    s3_endpoint = env;

    if (!(env = std::getenv("ZARR_S3_BUCKET_NAME"))) {
        LOG_ERROR("ZARR_S3_BUCKET_NAME not set.");
        return false;
    }
    s3_bucket_name = env;

    if (!(env = std::getenv("ZARR_S3_ACCESS_KEY_ID"))) {
        LOG_ERROR("ZARR_S3_ACCESS_KEY_ID not set.");
        return false;
    }
    s3_access_key_id = env;

    if (!(env = std::getenv("ZARR_S3_SECRET_ACCESS_KEY"))) {
        LOG_ERROR("ZARR_S3_SECRET_ACCESS_KEY not set.");
        return false;
    }
    s3_secret_access_key = env;

    return true;
}

bool
object_exists(minio::s3::Client& client, const std::string& object_name)
{
    minio::s3::StatObjectArgs args;
    args.bucket = s3_bucket_name;
    args.object = object_name;

    minio::s3::StatObjectResponse response = client.StatObject(args);

    return (bool)response;
}

size_t
get_object_size(minio::s3::Client& client, const std::string& object_name)
{
    minio::s3::StatObjectArgs args;
    args.bucket = s3_bucket_name;
    args.object = object_name;

    minio::s3::StatObjectResponse response = client.StatObject(args);

    if (!response) {
        LOG_ERROR("Failed to get object size: %s", object_name.c_str());
        return 0;
    }

    return response.size;
}

std::string
get_object_contents(minio::s3::Client& client, const std::string& object_name)
{
    std::stringstream ss;

    minio::s3::GetObjectArgs args;
    args.bucket = s3_bucket_name;
    args.object = object_name;
    args.datafunc = [&ss](minio::http::DataFunctionArgs args) -> bool {
        ss << args.datachunk;
        return true;
    };

    // Call get object.
    minio::s3::GetObjectResponse resp = client.GetObject(args);

    return ss.str();
}

bool
remove_items(minio::s3::Client& client,
             const std::vector<std::string>& item_keys)
{
    std::list<minio::s3::DeleteObject> objects;
    for (const auto& key : item_keys) {
        minio::s3::DeleteObject object;
        object.name = key;
        objects.push_back(object);
    }

    minio::s3::RemoveObjectsArgs args;
    args.bucket = s3_bucket_name;

    auto it = objects.begin();

    args.func = [&objects = objects,
                 &i = it](minio::s3::DeleteObject& obj) -> bool {
        if (i == objects.end())
            return false;
        obj = *i;
        i++;
        return true;
    };

    minio::s3::RemoveObjectsResult result = client.RemoveObjects(args);
    for (; result; result++) {
        minio::s3::DeleteError err = *result;
        if (!err) {
            LOG_ERROR("Failed to delete object %s: %s",
                      err.object_name.c_str(),
                      err.message.c_str());
            return false;
        }
    }

    return true;
}
} // namespace

ZarrStream*
setup()
{
    auto* settings = ZarrStreamSettings_create();

    ZarrS3Settings s3_settings{
        .endpoint = s3_endpoint.c_str(),
        .bytes_of_endpoint = s3_endpoint.size() + 1,
        .bucket_name = s3_bucket_name.c_str(),
        .bytes_of_bucket_name = s3_bucket_name.size() + 1,
        .access_key_id = s3_access_key_id.c_str(),
        .bytes_of_access_key_id = s3_access_key_id.size() + 1,
        .secret_access_key = s3_secret_access_key.c_str(),
        .bytes_of_secret_access_key = s3_secret_access_key.size() + 1,
    };
    ZarrStreamSettings_set_store(settings, SIZED(TEST), &s3_settings);

    ZarrStreamSettings_set_data_type(settings, ZarrDataType_uint16);

    ZarrCompressionSettings compression_settings{
        .compressor = ZarrCompressor_Blosc1,
        .codec = ZarrCompressionCodec_BloscLZ4,
        .level = 3,
        .shuffle = 1,
    };
    ZarrStreamSettings_set_compression(settings, &compression_settings);

    ZarrStreamSettings_reserve_dimensions(settings, 5);
    ZarrDimensionProperties dimension;

    dimension = DIM("t",
                    ZarrDimensionType_Time,
                    array_timepoints,
                    chunk_timepoints,
                    shard_timepoints);
    ZarrStreamSettings_set_dimension(settings, 0, &dimension);

    dimension = DIM("c",
                    ZarrDimensionType_Channel,
                    array_channels,
                    chunk_channels,
                    shard_channels);
    ZarrStreamSettings_set_dimension(settings, 1, &dimension);

    dimension = DIM(
      "z", ZarrDimensionType_Space, array_planes, chunk_planes, shard_planes);
    ZarrStreamSettings_set_dimension(settings, 2, &dimension);

    dimension = DIM(
      "y", ZarrDimensionType_Space, array_height, chunk_height, shard_height);
    ZarrStreamSettings_set_dimension(settings, 3, &dimension);

    dimension =
      DIM("x", ZarrDimensionType_Space, array_width, chunk_width, shard_width);
    ZarrStreamSettings_set_dimension(settings, 4, &dimension);

    auto* stream = ZarrStream_create(settings, ZarrVersion_3);
    ZarrStreamSettings_destroy(settings);

    return stream;
}

void
verify_base_metadata(const nlohmann::json& meta)
{
    const auto extensions = meta["extensions"];
    EXPECT_EQ(size_t, "%zu", extensions.size(), 0);

    const auto encoding = meta["metadata_encoding"].get<std::string>();
    EXPECT(encoding == "https://purl.org/zarr/spec/protocol/core/3.0",
           "Expected encoding to be "
           "'https://purl.org/zarr/spec/protocol/core/3.0', but got '%s'",
           encoding.c_str());

    const auto suffix = meta["metadata_key_suffix"].get<std::string>();
    EXPECT(suffix == ".json",
           "Expected suffix to be '.json', but got '%s'",
           suffix.c_str());

    const auto zarr_format = meta["zarr_format"].get<std::string>();
    EXPECT(encoding == "https://purl.org/zarr/spec/protocol/core/3.0",
           "Expected encoding to be "
           "'https://purl.org/zarr/spec/protocol/core/3.0', but got '%s'",
           encoding.c_str());
}

void
verify_group_metadata(const nlohmann::json& meta)
{
    const auto multiscales = meta["attributes"]["multiscales"][0];

    const auto axes = multiscales["axes"];
    EXPECT_EQ(size_t, "%zu", axes.size(), 5);
    std::string name, type, unit;

    name = axes[0]["name"];
    type = axes[0]["type"];
    EXPECT(name == "t", "Expected name to be 't', but got '%s'", name.c_str());
    EXPECT(
      type == "time", "Expected type to be 'time', but got '%s'", type.c_str());

    name = axes[1]["name"];
    type = axes[1]["type"];
    EXPECT(name == "c", "Expected name to be 'c', but got '%s'", name.c_str());
    EXPECT(type == "channel",
           "Expected type to be 'channel', but got '%s'",
           type.c_str());

    name = axes[2]["name"];
    type = axes[2]["type"];
    EXPECT(name == "z", "Expected name to be 'z', but got '%s'", name.c_str());
    EXPECT(type == "space",
           "Expected type to be 'space', but got '%s'",
           type.c_str());

    name = axes[3]["name"];
    type = axes[3]["type"];
    unit = axes[3]["unit"];
    EXPECT(name == "y", "Expected name to be 'y', but got '%s'", name.c_str());
    EXPECT(type == "space",
           "Expected type to be 'space', but got '%s'",
           type.c_str());
    EXPECT(unit == "micrometer",
           "Expected unit to be 'micrometer', but got '%s'",
           unit.c_str());

    name = axes[4]["name"];
    type = axes[4]["type"];
    unit = axes[4]["unit"];
    EXPECT(name == "x", "Expected name to be 'x', but got '%s'", name.c_str());
    EXPECT(type == "space",
           "Expected type to be 'space', but got '%s'",
           type.c_str());
    EXPECT(unit == "micrometer",
           "Expected unit to be 'micrometer', but got '%s'",
           unit.c_str());

    const auto datasets = multiscales["datasets"][0];
    const std::string path = datasets["path"].get<std::string>();
    EXPECT(path == "0", "Expected path to be '0', but got '%s'", path.c_str());

    const auto coordinate_transformations =
      datasets["coordinateTransformations"][0];

    type = coordinate_transformations["type"].get<std::string>();
    EXPECT(type == "scale",
           "Expected type to be 'scale', but got '%s'",
           type.c_str());

    const auto scale = coordinate_transformations["scale"];
    EXPECT_EQ(size_t, "%zu", scale.size(), 5);
    EXPECT_EQ(int, "%f", scale[0].get<double>(), 1.0);
    EXPECT_EQ(int, "%f", scale[1].get<double>(), 1.0);
    EXPECT_EQ(int, "%f", scale[2].get<double>(), 1.0);
    EXPECT_EQ(int, "%f", scale[3].get<double>(), 1.0);
    EXPECT_EQ(int, "%f", scale[4].get<double>(), 1.0);
}

void
verify_array_metadata(const nlohmann::json& meta)
{
    const auto& shape = meta["shape"];
    EXPECT_EQ(size_t, "%zu", shape.size(), 5);
    EXPECT_EQ(int, "%d", shape[0].get<int>(), array_timepoints);
    EXPECT_EQ(int, "%d", shape[1].get<int>(), array_channels);
    EXPECT_EQ(int, "%d", shape[2].get<int>(), array_planes);
    EXPECT_EQ(int, "%d", shape[3].get<int>(), array_height);
    EXPECT_EQ(int, "%d", shape[4].get<int>(), array_width);

    const auto& chunks = meta["chunk_grid"]["chunk_shape"];
    EXPECT_EQ(size_t, "%zu", chunks.size(), 5);
    EXPECT_EQ(int, "%d", chunks[0].get<int>(), chunk_timepoints);
    EXPECT_EQ(int, "%d", chunks[1].get<int>(), chunk_channels);
    EXPECT_EQ(int, "%d", chunks[2].get<int>(), chunk_planes);
    EXPECT_EQ(int, "%d", chunks[3].get<int>(), chunk_height);
    EXPECT_EQ(int, "%d", chunks[4].get<int>(), chunk_width);

    const auto& shards =
      meta["storage_transformers"][0]["configuration"]["chunks_per_shard"];
    EXPECT_EQ(size_t, "%zu", shards.size(), 5);
    EXPECT_EQ(int, "%d", shards[0].get<int>(), shard_timepoints);
    EXPECT_EQ(int, "%d", shards[1].get<int>(), shard_channels);
    EXPECT_EQ(int, "%d", shards[2].get<int>(), shard_planes);
    EXPECT_EQ(int, "%d", shards[3].get<int>(), shard_height);
    EXPECT_EQ(int, "%d", shards[4].get<int>(), shard_width);

    const auto dtype = meta["data_type"].get<std::string>();
    EXPECT(dtype == "uint16",
           "Expected dtype to be 'uint16', but got '%s'",
           dtype.c_str());

    const auto& compressor = meta["compressor"];
    EXPECT(!compressor.is_null(), "Expected compressor to be non-null");

    const auto codec = compressor["codec"].get<std::string>();
    EXPECT(codec == "https://purl.org/zarr/spec/codec/blosc/1.0",
           "Expected codec to be 'https://purl.org/zarr/spec/codec/blosc/1.0', "
           "but got '%s'",
           codec.c_str());

    const auto& configuration = compressor["configuration"];
    EXPECT_EQ(int, "%d", configuration["blocksize"].get<int>(), 0);
    EXPECT_EQ(int, "%d", configuration["clevel"].get<int>(), 3);
    EXPECT_EQ(int, "%d", configuration["shuffle"].get<int>(), 1);

    const auto cname = configuration["cname"].get<std::string>();
    EXPECT(cname == "lz4",
           "Expected cname to be 'lz4', but got '%s'",
           cname.c_str());
}

void
verify_and_cleanup()
{
    minio::s3::BaseUrl url(s3_endpoint);
    url.https = s3_endpoint.starts_with("https://");

    minio::creds::StaticProvider provider(s3_access_key_id,
                                          s3_secret_access_key);
    minio::s3::Client client(url, &provider);

    std::string base_metadata_path = TEST "/zarr.json";
    std::string group_metadata_path = TEST "/meta/root.group.json";
    std::string array_metadata_path = TEST "/meta/root/0.array.json";

    {
        EXPECT(object_exists(client, base_metadata_path),
               "Object does not exist: %s",
               base_metadata_path.c_str());
        std::string contents = get_object_contents(client, base_metadata_path);
        nlohmann::json base_metadata = nlohmann::json::parse(contents);

        verify_base_metadata(base_metadata);
    }

    {
        EXPECT(object_exists(client, group_metadata_path),
               "Object does not exist: %s",
               group_metadata_path.c_str());
        std::string contents = get_object_contents(client, group_metadata_path);
        nlohmann::json group_metadata = nlohmann::json::parse(contents);

        verify_group_metadata(group_metadata);
    }

    {
        EXPECT(object_exists(client, array_metadata_path),
               "Object does not exist: %s",
               array_metadata_path.c_str());
        std::string contents = get_object_contents(client, array_metadata_path);
        nlohmann::json array_metadata = nlohmann::json::parse(contents);

        verify_array_metadata(array_metadata);
    }

    CHECK(remove_items(
      client,
      { base_metadata_path, group_metadata_path, array_metadata_path }));

    const auto chunk_size = chunk_width * chunk_height * chunk_planes *
                            chunk_channels * chunk_timepoints * nbytes_px;
    const auto index_size = chunks_per_shard *
                            sizeof(uint64_t) * // indices are 64 bits
                            2;                 // 2 indices per chunk
    const auto expected_file_size = shard_width * shard_height * shard_planes *
                                      shard_channels * shard_timepoints *
                                      chunk_size +
                                    index_size;

    // verify and clean up data files
    std::vector<std::string> data_files;
    std::string data_root = TEST "/data/root/0";

    for (auto t = 0; t < shards_in_t; ++t) {
        const auto t_dir = data_root + "/" + ("c" + std::to_string(t));

        for (auto c = 0; c < shards_in_c; ++c) {
            const auto c_dir = t_dir + "/" + std::to_string(c);

            for (auto z = 0; z < shards_in_z; ++z) {
                const auto z_dir = c_dir + "/" + std::to_string(z);

                for (auto y = 0; y < shards_in_y; ++y) {
                    const auto y_dir = z_dir + "/" + std::to_string(y);

                    for (auto x = 0; x < shards_in_x; ++x) {
                        const auto x_file = y_dir + "/" + std::to_string(x);
                        EXPECT(object_exists(client, x_file),
                               "Object does not exist: %s",
                               x_file.c_str());
                        const auto file_size = get_object_size(client, x_file);
                        EXPECT_LT(size_t, "%zu", file_size, expected_file_size);
                    }
                }
            }
        }
    }
}

int
main()
{
    if (!get_credentials()) {
        LOG_WARNING("Failed to get credentials. Skipping test.");
        return 0;
    }

    Zarr_set_log_level(ZarrLogLevel_Debug);

    auto* stream = setup();
    std::vector<uint16_t> frame(array_width * array_height, 0);

    int retval = 1;

    try {
        size_t bytes_out;
        for (auto i = 0; i < frames_to_acquire; ++i) {
            ZarrStatus err = ZarrStream_append(
              stream, frame.data(), bytes_of_frame, &bytes_out);
            EXPECT(err == ZarrStatus_Success,
                   "Failed to append frame %d: %s",
                   i,
                   Zarr_get_error_message(err));
            EXPECT_EQ(size_t, "%zu", bytes_out, bytes_of_frame);
        }

        ZarrStream_destroy(stream);

        verify_and_cleanup();

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: %s", e.what());
    }

    return retval;
}
