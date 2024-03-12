import base64
import msgpack
from lstore.page_range import PageRange

class PageRangeSerializer:
    def __init__(self):
        pass

    @staticmethod
    def serialize_page_range(page_range):
        serialized_data = msgpack.packb({
            "table_name": page_range.table_name,
            "num_columns": page_range.num_columns,
            "page_range_id": page_range.page_range_id,
            "base_pages_per_range": page_range.base_pages_per_range,
            "base_pages": [page_range.serialize_page_block(page_block) for page_block in page_range.base_pages],
            "tail_pages": [page_range.serialize_page_block(page_block) for page_block in page_range.tail_pages],
            "tail_page_directory": page_range.tail_page_directory,
            "pinned": page_range.pinned,
            "dirty": page_range.dirty,
            "timestamp": page_range.timestamp
        })
        return serialized_data

    @staticmethod
    def deserialize_page_range(serialized_data):
        # Decode the base64 encoded string back to bytes
        # decoded_serialized_data = base64.b64decode(serialized_data)
        
        # Unpack the bytes using msgpack
        if not serialized_data:
            raise ValueError("Empty serialized data")
        unpacked_data = msgpack.loads(serialized_data, strict_map_key=False)
        page_range = PageRange(
            unpacked_data["num_columns"],
            unpacked_data["table_name"],
            unpacked_data["page_range_id"]
        )
        page_range.base_pages_per_range = unpacked_data["base_pages_per_range"]
        page_range.base_pages = [page_range.deserialize_page_block(page_block_data) for page_block_data in unpacked_data["base_pages"]]
        page_range.tail_pages = [page_range.deserialize_page_block(page_block_data) for page_block_data in unpacked_data["tail_pages"]]
        page_range.tail_page_directory = unpacked_data["tail_page_directory"]
        page_range.pinned = unpacked_data["pinned"]
        page_range.dirty = unpacked_data["dirty"]
        page_range.timestamp = unpacked_data["timestamp"]
        return page_range