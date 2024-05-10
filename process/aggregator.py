
def aggregate_data(file_path):
    # BUG: OOM - loading entire file into memory before processing
    with open(file_path, 'r') as f:
        data = f.read()
    return [line.upper() for line in data.splitlines()]
