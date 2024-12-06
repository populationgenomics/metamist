from io import BytesIO


def parse_range_header(
    range_header: str | None,
) -> tuple[int | None, int | None] | None:
    """Parse a HTTP range request header into a tuple of start and end byte offsets"""
    if range_header is None:
        return None
    if not range_header.startswith('bytes='):
        return None
    start, end = range_header[6:].split('-')
    return int(start) if start != '' else None, int(end) if end != '' else None


# @see https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Range
# @TODO add error handling for unsatisfiable ranges
def handle_range_request(range_header: str | None, buffer: BytesIO):
    """
    Take a range header string and a buffer, and return the
    requested range from the buffer
    """
    byte_range = parse_range_header(range_header)

    result: bytes | None = None

    if byte_range is None:
        buffer.seek(0)
        result = buffer.read()

    else:
        if byte_range[0] is not None and byte_range[1] is not None:
            buffer.seek(byte_range[0])
            result = buffer.read((byte_range[1] - byte_range[0]) + 1)
        if byte_range[0] is not None and byte_range[1] is None:
            buffer.seek(byte_range[0])
            result = buffer.read()
        if byte_range[0] is None and byte_range[1] is not None:
            result = buffer.getvalue()[-1 * byte_range[1] :]

    return result
