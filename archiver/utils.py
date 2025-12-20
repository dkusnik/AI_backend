import hashlib


def parse_pipe_array(request, name):
    v = request.query_params.get(name)
    if not v:
        return None
    return [x for x in v.split("|") if x != ""]


def calculate_sha256(file_path: str, chunk_size: int = 1024 * 1024) -> str:
    """
    Calculate SHA-256 checksum of a file.

    :param file_path: Path to file
    :param chunk_size: Read size in bytes (default: 1 MB)
    :return: Hex-encoded SHA-256 hash
    """
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            sha256.update(chunk)

    return sha256.hexdigest()