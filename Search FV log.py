import json
import os
import pprint

def main():
    # Read the config.json file
    with open('config.json', 'r') as file:
        config = json.load(file)
    # Search for the folder with 'parsing-output' in its name
    logs_folder = config['program_setup']['settings']['logs_folder']
    source = config['program_setup']['settings']['source']
    
    parsing_output_file = find_parsing_output_file(logs_folder, source)

    if parsing_output_file:
        with open(parsing_output_file) as file:
            alarm_text = config['program_setup']['settings']['alarm_text']
            results = extract_alarm_timestamps(file, alarm_text)
    else:
        print("Parsing output file not found.")

    if results:
        for result in results:
            # Find FV log file
            for filename, timestamps in result.items():
                for root, _, files in os.walk(logs_folder):
                    if filename in files:
                        fv_log_file_path = os.path.join(root, filename)
                        print(f"Found alarm in {fv_log_file_path} at {timestamps}")


def extract_alarm_timestamps(file, alarm_text):
    results = []
    for line in file:
        # Check if the line contains 'Results from'
        if "Results from" in line:
            # Extract the filename part from the line
            filename = line.split("\\")[-1].rstrip(":\n\r")
            # Check subsequent lines for alarm_text
            timestamps = []
            for subsequent_line in file:
                if "Results from" in subsequent_line:
                    break
                if alarm_text in subsequent_line:
                    # Extract the timestamp part from the line
                    timestamp = subsequent_line.split()[0].lstrip('*').rstrip(':')
                    timestamps.append(timestamp)
            if timestamps:
                results.append({filename: timestamps})
    return results

def find_parsing_output_file(logs_folder, source): 
    for root, dirs, files in os.walk(logs_folder):
        for dir_name in dirs:
            if 'parsing-output' in dir_name:
                parsing_output_folder = os.path.join(root, dir_name)
                print(f"Found folder: {parsing_output_folder}")
                # Search for the file that has 'source' in its name
                for file_name in os.listdir(parsing_output_folder):
                    if source in file_name:
                        print(f"Found file: {file_name}")
                        return os.path.join(parsing_output_folder, file_name)  # Return the full path to the file
                break


# Print the contents of the config file
if __name__ == '__main__':
    main()