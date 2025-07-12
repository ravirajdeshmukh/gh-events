def extract_nested_field(data, path):
    """
    Extracts a nested value from a dictionary using a dot-separated key path.

    Args:
        data (dict): The input dictionary (can be nested).
        path (str): Dot-separated path to the nested field (e.g., 'payload.pull_request.state').

    Returns:
        Any: The value found at the nested path, or None if the path doesn't exist.
    """
    keys = path.split(".")
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key)
        else:
            return None  # Exit early if path is broken
    return data


def trim_event_dynamic(event, field_paths):
    """
    Dynamically trims a raw GitHub event dictionary to only include specified fields.

    Fields are defined in a YAML config as dot-separated strings. This allows
    flexible schema extraction without hardcoding keys.

    Args:
        event (dict): The full GitHub event object (raw JSON parsed to dict).
        field_paths (List[str]): List of dot-separated paths to extract.

    Returns:
        dict: A trimmed dictionary containing only the requested fields.
    """
    return {path: extract_nested_field(event, path) for path in field_paths}
