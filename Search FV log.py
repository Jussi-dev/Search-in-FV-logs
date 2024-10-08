import json
import os
import re
import pprint

def main():
    # Read the config.json file
    with open('config.json', 'r') as file:
        config = json.load(file)

    # Search for the folder with 'parsing-output' in its name
    logs_folder = config['program_setup']['settings']['logs_folder']
    source = config['program_setup']['settings']['source']
    parsing_output_file = find_parsing_output_file(logs_folder, source)

    pattern = re.compile(rf'''
    Parsed:\s<proto\smessage="GUI\.JobOrder">\s*
    id\s*{{\s*
    id:\s"(?P<id>[^"]+)"\s*
    update_counter:\s(?P<update_counter>\d+)\s*
    }}\s*
    che_name:\s"(?P<che_name>{source})"\s*
    steps\s*{{\s*
    step_id:\s(?P<step_id>\d+)\s*
    type:\s(?P<type>[A-Z]+)\s*
    container_ids:\s"(?P<container_ids>[^"]+)"\s*
    completed:\s(?P<completed>\w+)\s*
    target\s*{{\s*
    target\s*{{\s*
    stack_position\s*{{\s*
    stack_name:\s"(?P<stack_name>[^"]+)"\s*
    }}\s*
    chassis_position\s*{{\s*
    lane\s*{{\s*
    stack_name:\s"(?P<lane_stack_name>[^"]+)"\s*
    }}\s*
    type:\s(?P<chassis_type>[A-Z_]+)\s*
    length:\s(?P<length>[A-Z_0-9]+)\s*
    location:\s(?P<location>[A-Z_]+)\s*
    end:\s(?P<end>[A-Z_]+)\s*
    combination\s*{{\s*
    front:\s(?P<front>[A-Z_0-9]+)\s*
    back:\s(?P<back>[A-Z_0-9]+)\s*
    }}\s*
    }}\s*
    }}\s*
    tier:\s"(?P<tier>\d+)"\s*
    }}\s*
    allowed_to_complete:\s(?P<allowed_to_complete>\w+)\s*
    complete_with_remote:\s(?P<complete_with_remote>\w+)\s*
    estimation_completion:\s(?P<estimation_completion>\d+)\s*
    pnr_passed:\s(?P<pnr_passed>\w+)\s*
''', re.VERBOSE)

    # Extract alarm timestamps from the parsing output file
    if parsing_output_file:
        alarm_text = config['program_setup']['settings']['alarm_text']   
        results = extract_alarm_timestamps(parsing_output_file, alarm_text)
    else:
        print("Parsing output file not found.")

    # Find the FV log files that correspond to the alarm timestamps
    if results:
        logs_with_alarms = (get_matching_log_files(logs_folder, results))
    else:
        print("No alarm timestamps found in the parsing output file.")

    # Search for the pattern in the FV log files
    if logs_with_alarms:
        # fv-log folder path is at program_setup.settings.logs_folder + (folder name containing)'fv-log' + 'logs'
        logs_folder_root = config['program_setup']['settings']['logs_folder']
        # Find the folder with 'fv-log' in its name and 'logs' in its name
        fv_log_folder = find_fv_log_folder(logs_folder_root, fv_log_folder_name="fv-log", fv_log_collection_name="logs")
        # Search for the pattern in the FV log files
        if fv_log_folder:
            print(f"Found FV log folder: {fv_log_folder}")
            matching_jobs_info = search_and_extract(logs_folder, pattern, logs_with_alarms)
        else:
            print(f"FV log folder ({fv_log_folder}) not found.")

    if matching_jobs_info:
        print("Matching jobs info:")
        pprint.pprint(matching_jobs_info)
    else:
        print("No matching jobs info found.")

def search_and_extract(logs_folder, pattern, logs_with_alarms):
    matches = []
    for log_file, timestamp in logs_with_alarms:
        with open(log_file) as file:
            log_text = file.read()
            # Search for the timestamp in the log file
            for line_number, line in enumerate(log_text.splitlines(), start=1):
                # Check if the line contains the timestamp
                if timestamp in line:
                    print(f"Found timestamp: {timestamp} in file: {log_file} at line number: {line_number}")
                    print(line)
                    # Search for the pattern in the log file
                    match = search_pattern_backwards(log_text, line_number, pattern)
                    if match:
                        print("Match found:")
                        pprint.pprint(match)
                        matches.append(match)
                    else:
                        print("No match found.")
                        print("Searching in previous log file...")
                        # Try to find a match in the previous log file
                        previous_log_file = get_previous_log_file(log_file, logs_folder)

                        if previous_log_file:
                            with open(previous_log_file) as prev_file:
                                prev_log_text = prev_file.read()
                            prev_match = search_pattern_backwards(prev_log_text, len(prev_log_text.splitlines()), pattern)
                            if prev_match:
                                print(f"Match found in previous log file: {previous_log_file}")
                                pprint.pprint(prev_match)
                                matches.append(prev_match)
                            else:
                                print("No match found in previous log file.")
                        else:
                            print("No previous log file found.")
    return matches

    
def find_fv_log_folder(logs_folder_root, fv_log_folder_name, fv_log_collection_name):
    for root, dirs, files in os.walk(logs_folder_root):
        for dir_name in dirs:
            if fv_log_folder_name in dir_name:
                fv_log_folder = os.path.join(root, dir_name, fv_log_collection_name)
                if os.path.exists(fv_log_folder):
                    return fv_log_folder
    return None
    


def get_matching_log_files(logs_folder, results):
    matching_files = []
    for result in results:
        # Find FV log files that correspond to the alarm timestamps
        for filename, timestamps in result.items():
            for root, _, files in os.walk(logs_folder):
                if filename in files:
                    fv_log_file_path = os.path.join(root, filename)
                    matching_files.append((fv_log_file_path, timestamps))
    return matching_files


def extract_alarm_timestamps(parsing_output_file, alarm_text):
    results = []
    with open(parsing_output_file) as file:
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
                        # timestamps.append(timestamp)
                        results.append({filename: timestamp})
                # if timestamps:
                #     results.append({filename: timestamps})
    return results

# Find the parsing output file that corresponds to the source
def find_parsing_output_file(logs_folder, source): 
    for root, dirs, files in os.walk(logs_folder):
        for dir_name in dirs:
            # Check if the folder name contains 'parsing-output'
            if 'parsing-output' in dir_name:
                parsing_output_folder = os.path.join(root, dir_name)
                print(f"Found folder: {parsing_output_folder}")
                # Search for the file that has 'source' in its name
                for file_name in os.listdir(parsing_output_folder):
                    if source in file_name:
                        print(f"Found file: {file_name}")
                        return os.path.join(parsing_output_folder, file_name)  # Return the full path to the file
                break
    return None

def search_pattern_backwards(log_content, start_line, pattern, window_size=37):
    lines = log_content.splitlines()
    # Iterate from the start line backwards
    for i in range(start_line, -1, -1):
        # Join a window of lines to search for multiline patterns
        chunk = '\n'.join(lines[max(0, i-window_size+1):i+1])
        match = pattern.search(chunk)
        if match:
            result = match.groupdict()
            result['timestamp'] = lines[start_line - 1].split()[0]  # Assuming the timestamp is at the start of the line
            return result
    return None

def get_previous_log_file(current_log_file, logs_folder):
    log_files = sorted(
        [os.path.join(root, file) for root, _, files in os.walk(logs_folder) for file in files if file.endswith('.log')],
        key=os.path.getmtime
    )
    current_index = log_files.index(current_log_file)
    if current_index > 0:
        return log_files[current_index - 1]
    return None

def init_measure_results_data():
    measure_results_data = {
        'filename' : None,
        'Timestamp' : None,
        'Date' : None,
        'Measurement_ID' : None,
        'Lane' : None,
        'Task_num' : None,
        'Task_str' : None,
        'Pos_num' : None,
        'Pos_str' : None,
        'Len_num' : None,
        'Len_str' : None,
        'Type_num' : None,
        'Type_str' : None,
        'Cont_Length' : None,
        'Cont_Width' : None,
        'Cont_Height' : None,
        'Init_lane_status' : None,
        'Init_meas_status' : None,
        'Last_lane_status' : None,
        'Last_meas_status' : None,
        'Assuming_trailer' : None,
        'Point_Center_X' : None,
        'Point_Center_Y' : None,
        'Point_Center_Z' : None,
        'Skew' : None,
        'Tilt' : None,
        'N_of_TWL_detected' : None, 
        'N_of_TWL_calculated' : None,
        'TLMS_success' : None

    }
    
    return measure_results_data


# Print the contents of the config file
if __name__ == '__main__':
    main()