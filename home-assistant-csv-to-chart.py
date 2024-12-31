import csv
from datetime import datetime, timedelta
import argparse
from bisect import bisect_right

# Set up argument parsing
parser = argparse.ArgumentParser(description='Process sensor data into minute-level granularity.')
parser.add_argument('input_file', type=str, help='Path to the input CSV file')
parser.add_argument('output_file', type=str, help='Path to the output CSV file')
args = parser.parse_args()

# Get input and output file paths from arguments
input_file = args.input_file
output_file = args.output_file

# Read the data from the CSV file
data = []
additional_columns = set()
entity_data = {}
additional_data = {}
with open(input_file, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        # Convert 'last_changed' to a datetime object
        last_changed = datetime.strptime(row['last_changed'], '%Y-%m-%dT%H:%M:%S.%fZ') - timedelta(hours=7)  # Convert to Mountain Time
        try:
            state = float(row['state'])  # Convert 'state' to a float if possible
        except ValueError:
            state = None  # Handle non-numeric states

        # Store data by entity_id
        entity_id = row['entity_id']
        if entity_id not in entity_data:
            entity_data[entity_id] = []
        entity_data[entity_id].append((last_changed, state))

        # Track additional columns
        for key in row.keys():
            if key not in ('entity_id', 'state', 'last_changed') and row[key]:
                col_name = f"{entity_id}_{key}"
                additional_columns.add(col_name)
                if col_name not in additional_data:
                    additional_data[col_name] = []
                additional_data[col_name].append((last_changed, row[key]))

# Sort all data by timestamp for efficient searching
for key in entity_data:
    entity_data[key].sort()
for key in additional_data:
    additional_data[key].sort()

# Generate a minute-by-minute time range
def generate_time_range(start, end):
    current_time = start
    while current_time <= end:
        yield current_time
        current_time += timedelta(minutes=1)

start_time = min(min(t[0] for t in values) for values in entity_data.values())
end_time = max(max(t[0] for t in values) for values in entity_data.values())

# Prepare the output data
header = ['timestamp'] + list(entity_data.keys()) + list(additional_data.keys())
output_rows = []
for time in generate_time_range(start_time, end_time):
    row = [time.strftime('%Y-%m-%dT%H:%M:%S')]  # Format time as string

    # Fetch closest values for entity_data
    for entity_id, values in entity_data.items():
        timestamps = [t[0] for t in values]
        idx = bisect_right(timestamps, time) - 1
        value = values[idx][1] if idx >= 0 else None
        row.append(value)

    # Fetch closest values for additional_data
    for col, values in additional_data.items():
        timestamps = [t[0] for t in values]
        idx = bisect_right(timestamps, time) - 1
        value = values[idx][1] if idx >= 0 else None
        row.append(value)

    output_rows.append(row)

# Write the output to a CSV file
with open(output_file, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(header)
    writer.writerows(output_rows)

print(f"Processed data saved to {output_file}")
