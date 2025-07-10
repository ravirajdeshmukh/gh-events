def extract_nested_field(data, path):
    keys = path.split(".")
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key)
        else:
            return None
    return data

def trim_event_dynamic(event, field_paths):
    """Use YAML-defined paths to extract selected fields."""
    return {path: extract_nested_field(event, path) for path in field_paths}