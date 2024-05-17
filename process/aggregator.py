
def aggregate_data(file_path):
    # FIX: Stream file line by line to handle large datasets
    with open(file_path, 'r') as f:
        for line in f:
            yield line.strip().upper()
