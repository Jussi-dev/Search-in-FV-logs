import json
import os
import re
import pprint
import threading
import pandas as pd
from datetime import datetime
import tkinter as tk
from tkinter import messagebox

class MyApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("My Application")
        self.geometry("400x300")
        self.create_widgets()
        self.config = self.get_config()
        self.update_info(self.config)

    def create_widgets(self):
        self.label = tk.Label(self, text="Hello, Tkinter!")
        self.lbl_che_id = tk.Label(self, text="CHE ID:")
        self.lbl_alarm_text = tk.Label(self, text="Alarm Text:")
        self.button = tk.Button(self, text="Click Me", command=self.on_button_click)

        self.label.pack(pady=10)
        self.lbl_che_id.pack(pady=10)
        self.lbl_alarm_text.pack(pady=10)
        self.button.pack(pady=10)

    def on_button_click(self):
        messagebox.showinfo("Information", "Button clicked!")

    def get_config(self):
        config_path = os.path.join(os.path.dirname(__file__), 'Config.json')
        with open(config_path, 'r') as file:
            config = json.load(file)
        pprint.pprint(config)
        return config

    def update_info(self, config):
        self.lbl_che_id.config(text=f"CHE ID: {config['program_setup']['settings']['source']}")
        self.lbl_alarm_text.config(text=f"Alarm Text: {config['program_setup']['settings']['alarm_text']}")

def process_fv_logs(base_path) -> str:
    # This function can run outside of the Tkinter main loop
    config_path = os.path.join(base_path, 'Config.json')
    mathced_results_path = None # Initialize the variable to store the matched results file path
    matching_jobs_info_file_path = os.path.join(base_path, 'Output/matching_jobs_info.xlsx') # Initialize the variable to store the matching jobs info file path
  
    # Read the config.json file
    with open(config_path, 'r') as file:
        config = json.load(file)

    # Search for the folder with 'parsing-output' in its name
    logs_folder, source = get_logs_folder_and_source(config)
    # Get logs folder absolute path
    logs_folder = os.path.join(os.path.dirname(__file__), logs_folder)
    
    results = None # Initialize the variable to store the alarm timestamps
    logs_with_alarms = [] # Initialize the variable to store the FV log files with alarms
    matching_jobs_list = [] # Initialize the variable to store the matching jobs
    df_matching_jobs = None  # Initialize the variable to store the matching jobs
    measureresult_df = None

    # Search for the folder with 'parsing-output' in its name
    parsing_output_folder, parsing_output_file = find_parsing_output_file(logs_folder, source)

    # Define the pattern for the job order
    pattern_job_order = re.compile(rf'{source}m/{source}b:\d+')

    # Define the pattern for the event log
    pattern_event_log = re.compile(rf'''
    (?P<timestamp>\d{{4}}-\d{{2}}-\d{{2}}_\d{{2}}\.\d{{2}}\.\d{{2}}\.\d{{3}}):\s*\(\d+\):\s*EVENT:\[(?P<source>{source}m/{source}b:\d+)\]\s*\(\d+\s*bytes,\s*\d+\s*tags\)\s*
    Parsed:\s*<proto\s*message="GUI\.JobOrder">\s*
    id\s*{{\s*
    id:\s*"(?P<id>[^"]+)"\s*
    update_counter:\s*(?P<update_counter>\d+)\s*
    }}\s*
    che_name:\s*"(?P<che_name>[^"]+)"\s*
    ''', re.VERBOSE)
    
    # Define the pattern for the LS job
    pattern_ls_job = re.compile(r'''
    # id\s*{\s*
    # id:\s*"(?P<id>[^"]+)"\s*
    # update_counter:\s*(?P<update_counter>\d+)\s*
    # }\s*
    # che_name:\s*"(?P<che_name>[^"]+)"\s*
    (steps\s*{\s*
    step_id:\s(?P<step_id>\d+)\s*
    type:\s(?P<type>[A-Z]+)\s*
    container_ids:\s"(?P<container_ids>[^"]+)"\s*
    completed:\s(?P<completed>\w+)\s*
    target\s*{\s*
    target\s*{\s*
    stack_position\s*{\s*
    stack_name:\s"(?P<stack_name>[^"]+)"\s*
    }\s*
    }\s*
    tier:\s"(?P<tier>\d+)"\s*
    }\s*
    allowed_to_complete:\s(?P<allowed_to_complete>\w+)\s*
    complete_with_remote:\s(?P<complete_with_remote>\w+)\s*
    pnr_passed:\s(?P<pnr_passed>\w+)\s*
    }\s*)*
    steps\s*{\s*
    step_id:\s(?P<step_id_2>\d+)\s*
    type:\s(?P<type_2>[A-Z]+)\s*
    container_ids:\s"(?P<container_ids_2>[^"]+)"\s*
    completed:\s(?P<completed_2>\w+)\s*
    target\s*{\s*
    target\s*{\s*
    stack_position\s*{\s*
    stack_name:\s"(?P<stack_name_2>[^"]+)"\s*
    }\s*
    chassis_position\s*{\s*
    lane\s*{\s*
    stack_name:\s"(?P<lane_stack_name>[^"]+)"\s*
    }\s*
    type:\s(?P<chassis_type>[A-Z_]+)\s*
    length:\s(?P<length>[A-Z_0-9]+)\s*
    location:\s(?P<location>[A-Z_]+)\s*
    end:\s(?P<end>[A-Z_]+)\s*
    combination\s*{\s*
    front:\s(?P<front>[A-Z_0-9]+)\s*
    back:\s(?P<back>[A-Z_0-9]+)\s*
    }\s*
    }\s*
    }\s*
    tier:\s"(?P<tier_2>\d+)"\s*
    }\s*
    allowed_to_complete:\s(?P<allowed_to_complete_2>\w+)\s*
    complete_with_remote:\s(?P<complete_with_remote_2>\w+)\s*
    (estimation_completion:\s(?P<estimation_completion_2>\d+)\s*)?
    pnr_passed:\s(?P<pnr_passed_2>\w+)\s*
    }\s*
''', re.VERBOSE)

    # Define the pattern for the stack job
    pattern_stack_job = re.compile(r'''
    # Step 1 
    steps\s*{\s*
    step_id:\s(?P<step_id>\d+)\s*
    type:\s(?P<type>[A-Z]+)\s*
    container_ids:\s"(?P<container_ids>[^"]+)"\s*
    completed:\s(?P<completed>\w+)\s*
    target\s*{\s*
    target\s*{\s*
    stack_position\s*{\s*
    stack_name:\s"(?P<stack_name>[^"]+)"\s*
    }\s*
    ( # Chassis and lane position are optional
    chassis_position\s*{\s*
    lane\s*{\s*
    stack_name:\s"(?P<lane_stack_name>[^"]+)"\s*
    }\s*
    type:\s(?P<chassis_type>[A-Z_]+)\s*
    length:\s(?P<length>[A-Z_0-9]+)\s*
    location:\s(?P<location>[A-Z_]+)\s*
    end:\s(?P<end>[A-Z_]+)\s*
    combination\s*{\s*
    front:\s(?P<front>[A-Z_0-9]+)\s*
    back:\s(?P<back>[A-Z_0-9]+)\s*
    }\s*
    }\s*
    )?
    }\s*
    tier:\s"(?P<tier>\d+)"\s*
    }\s*
    (?: # DO NOT capture the following section
    allowed_to_complete:\s(\w+)\s*
    complete_with_remote:\s(\w+)\s*
    (estimation_completion:\s(\d+)\s*)?
    pnr_passed:\s(\w+)\s*
    )
    }\s*
    # Step 2
    steps\s*{\s*
    step_id:\s(?P<step_id_2>\d+)\s*
    type:\s(?P<type_2>[A-Z]+)\s*
    container_ids:\s"(?P<container_ids_2>[^"]+)"\s*
    completed:\s(?P<completed_2>\w+)\s*
    target\s*{\s*
    target\s*{\s*
    stack_position\s*{\s*
    stack_name:\s"(?P<stack_name_2>[^"]+)"\s*
    }\s*
    ( # Chassis and lane position are optional
    chassis_position\s*{\s*
    lane\s*{\s*
    stack_name:\s"(?P<lane_stack_name_2>[^"]+)"\s*
    }\s*
    type:\s(?P<chassis_type_2>[A-Z_]+)\s*
    length:\s(?P<length_2>[A-Z_0-9]+)\s*
    location:\s(?P<location_2>[A-Z_]+)\s*
    end:\s(?P<end_2>[A-Z_]+)\s*
    combination\s*{\s*
    front:\s(?P<front_2>[A-Z_0-9]+)\s*
    back:\s(?P<back_2>[A-Z_0-9]+)\s*
    }\s*
    }\s*
    )?
    }\s*
    tier:\s"(?P<tier_2>\d+)"\s*
    }\s*
    (?: # DO NOT capture the following section
    allowed_to_complete:\s(\w+)\s*
    complete_with_remote:\s(\w+)\s*
    (estimation_completion:\s(\d+)\s*)?
    pnr_passed:\s(\w+)\s*
    )
    }\s*
    ''', re.VERBOSE | re.DOTALL)

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

    # Initialize the Measureresult list
    matching_jobs_info = None

    # Check if "matching_job_info.xlsx" exists in the "Output" folder
    output_folder = os.path.join(os.path.dirname(__file__), 'Output')
    matching_job_info_file = os.path.join(output_folder, 'matching_jobs_info.xlsx')
    if os.path.exists(matching_job_info_file):
        print(f"\nFile {matching_job_info_file} exists.\n")
        if prompt_existing_file("Existing matching jobs Info"):
            # Read the matching job info file
            print(f"Reading file: {matching_job_info_file}")
            matching_jobs_info = pd.read_excel(matching_job_info_file).to_dict(orient='records')
            print(f"Matching jobs info read from file: {matching_job_info_file}, length: {len(matching_jobs_info)}")

    if matching_jobs_info is None:
        print(f"\nParsing matching jobs\n")
        # Search for the pattern in the FV log files
        if logs_with_alarms:
            search_depth = config['program_setup']['settings']['search_depth']
            # Find the folder with 'fv-log' in its name and 'logs' in its name
            fv_log_folder = find_fv_log_folder(logs_folder, fv_log_folder_name="fv-log", fv_log_collection_name="logs")
            # Search for the pattern in the FV log files
            if fv_log_folder:
                print(f"Found FV log folder: {fv_log_folder}")
                job_area = config['program_setup']['settings']['job_area']
                if job_area == 'landside':
                    print("Searching for land-side jobs.")
                    matching_jobs_info = search_and_extract(logs_folder, pattern_event_log, pattern_ls_job, logs_with_alarms, search_depth)
                elif job_area == 'stack':
                    print("Searching for stack jobs.")
                    matching_jobs_info = search_and_extract(logs_folder, pattern_event_log, pattern_stack_job, logs_with_alarms, search_depth)        
                else:
                    print("No job area recognized.")
            else:
                print(f"FV log folder ({fv_log_folder}) not found.")
        else:
            print("No logs with alarms found.")

    if matching_jobs_info:
        print("Matching jobs info found.")
        df_matching_jobs_info = pd.DataFrame(matching_jobs_info)
        print(df_matching_jobs_info)

        # Save the matching jobs info to an Excel file
        df_matching_jobs_info.to_excel(matching_jobs_info_file_path, index=False)

        # Prompt the user to proceed with matching the jobs with parsed Measureresults
        if match_jobs_prompt():
            matching_jobs_list = [] # Initialize the variable to store the matching jobs
            for job_info in matching_jobs_info:
                matching_job = init_measure_results_data()
                matching_job['Timestamp'] = job_info['timestamp'].strftime('%Y-%m-%d %H:%M:%S.%f')
                matching_job['Task_str'] = extract_ls_job_type(job_info)

                # Extract the lane and position from the job_info
                if 'lane_stack_name' in job_info or 'lane_stack_name_2' in job_info:
                    if isinstance(job_info.get('lane_stack_name'), str):
                        matching_job['Lane'] = int(job_info['lane_stack_name'].split('.')[1])
                        matching_job['Pos_str'] = job_info['lane_stack_name'].split('.')[2]
                    elif isinstance(job_info.get('lane_stack_name_2'), str):
                        matching_job['Lane'] = int(job_info['lane_stack_name_2'].split('.')[1])
                        matching_job['Pos_str'] = job_info['lane_stack_name_2'].split('.')[2]
                    else:
                        matching_job['Lane'] = None
                        matching_job['Pos_str'] = None            
                else:
                    matching_job['Lane'] = None
                    matching_job['Pos_str'] = None
                matching_jobs_list.append(matching_job)

            # Convert the matching jobs list to a DataFrame
            if matching_jobs_list: # Check if the list is not empty
                df_matching_jobs = pd.DataFrame(matching_jobs_list)
                matched_jobs_output_file = os.path.join(base_path, 'Output/matching_jobs.xlsx')
                df_matching_jobs.to_excel(matched_jobs_output_file, index=False)
            else:
                print("No matching jobs found.")
            
            # Load the Measureresult.xlsx file
            # Search for the .xlsx file that has 'MeasureResult' in the name
            measureresult_file_path = None
            for root, _, files in os.walk(os.path.join(logs_folder, 'MeasureResult-parsed')):
                for file in files:
                    if 'Measureresult' in file and file.endswith('.xlsx'):
                        measureresult_file_path = os.path.join(root, file)
                        break
                if measureresult_file_path:
                    break

            if measureresult_file_path:
                measureresult_df = pd.read_excel(measureresult_file_path)
            else:
                print("MeasureResult file not found.")

            if measureresult_df is not None:
                # Filter the Measureresult dataframe to find matching entries
                mathced_results_path = filter_measure_results(df_matching_jobs, measureresult_df, config['program_setup']['settings']['match_time_window_sec'])
            else:
                print("Measureresult dataframe is empty.")
        else:
            print("User chose not to proceed with matching the jobs with parsed Measureresults.")
    else:
        print("No matching jobs info found.")
    print("Processing completed.")
    return mathced_results_path, matching_jobs_info_file_path

def get_logs_folder_and_source(config):
    logs_folder = config['program_setup']['settings']['logs_folder']
    source = config['program_setup']['settings']['source']
    return logs_folder,source


def filter_measure_results(df_matching_jobs, measureresult_df, match_time_window_sec=180):
    if df_matching_jobs is None:
        print("No matching jobs to filter.")
        return
    # Prompt the user to proceed with marking ATH success
    if mark_ath_success():
        if ath_success_value():
            ath_success = 1
        else:
            ath_success = 0
    else:
        ath_success = None

    # Initialize a list to store the matched results
    matched_results = []
    # Initialize a DataFrame to store the ATH measure results
    try:
        output_folder = ensure_output_folder()
        
        ath_measure_results_file = os.path.join(output_folder, 'Measureresults_ATH.xlsx')
        if os.path.exists(ath_measure_results_file):
            # Prompt the user to append to the existing ATH measure results
            if prompt_existing_file("Existing ATH measure results", "Do you want to append to the existing ATH measure results?"):
                df_ath_measure_results = pd.read_excel(ath_measure_results_file) # Read the existing DataFrame
            else: # Create a new DataFrame
                df_ath_measure_results = measureresult_df.copy()
        else: # Create a new DataFrame
            df_ath_measure_results = measureresult_df.copy()
    except Exception as e:
        print(f"Error reading or creating ATH measure results DataFrame: {e}")
        df_ath_measure_results = measureresult_df.copy()

    # Iterate over each row in df_ath_measure_results and compare df_matching_jobs
    for _, job in df_matching_jobs.iterrows():
        filtered_df = df_ath_measure_results[
            (df_ath_measure_results['Lane'] == job['Lane']) &
            (df_ath_measure_results['Pos_str'] == job['Pos_str']) &
            (df_ath_measure_results['Task_str'] == job['Task_str'])
        ]

        if not filtered_df.empty:
            # Convert the 'Timestamp' columns to datetime
            filtered_df.loc[:, 'Timestamp'] = pd.to_datetime(filtered_df['Timestamp'])
            job_timestamp = pd.to_datetime(job['Timestamp'])

            # Find the entry with the nearest timestamp
            filtered_df = filtered_df.copy()
            filtered_df['Time_Diff'] = (filtered_df['Timestamp'] - job_timestamp).abs()
            nearest_entry = filtered_df.loc[filtered_df['Time_Diff'].idxmin()]

            # Filter out nearest_entry if 'Time_Diff' is greater than match_time_window_sec in seconds
            if nearest_entry['Time_Diff'].total_seconds() <= match_time_window_sec:
                # Add the nearest entry to the matched results
                matched_results.append(nearest_entry)

                # Update the 'ATH_success' column in df_ath_measure_results
                df_ath_measure_results.loc[nearest_entry.name, 'ATH_success'] = ath_success

    # Convert the matched results to a DataFrame
    if matched_results:
        df_matched_results = pd.DataFrame(matched_results)

        output_folder = ensure_output_folder()
        
        # Save the matched results to an Excel file
        df_matched_results.to_excel(os.path.join(output_folder, 'matched_results.xlsx'), index=False)
        df_ath_measure_results.to_excel(os.path.join(output_folder, 'Measureresults_ATH.xlsx'), index=False)
        print(df_matched_results)
    else:
        print("No matching entries found in Measureresult.xlsx.")

    print("Filtering completed.")

    # Return the 'mathced_results.xlsx' file path
    return os.path.join(output_folder, 'matched_results.xlsx')

def ensure_output_folder():
    root_folder = os.path.dirname(__file__) # Get the root folder
    output_folder = os.path.join(root_folder, 'Output') # Define the output folder
    if not os.path.exists(output_folder): # Check if the 'Output' folder exists
        print("No 'Output' folder found. Creating 'Output' folder.")
        os.makedirs(output_folder)
    return output_folder

def extract_ls_job_type(job_info):
    # Define the mapping for the job types
    job_type_mapping = {
        'GROUND': 'Place',
        'PICK': 'Pick',
        # Add other mappings as needed
    }
    # Extract the job type from the job_info
    # Check if the job type is present in the job_info
    if 'lane_stack_name' in job_info or 'lane_stack_name_2' in job_info:
        # Select the job type based on the presence of 'type' or 'type_2'
        if job_info.get('lane_stack_name') is not None and pd.notna(job_info['lane_stack_name']): # Check if 'lane_stack_name' is not None or NaN
            job_type = job_info['type']
        else:
            job_type = job_info['type_2']
        # Transform the job type using the mapping
        transformed_job = job_type_mapping.get(job_type, 'Unknown')
    else:
        transformed_job = 'No_job_type' # Default to 'Unknown' if job type is not found
    return transformed_job # Return the transformed job type

def search_pattern_backwards(log_content, start_line, pattern, window_size=37):
    lines = log_content.splitlines()
    timestamp_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}_\d{2}\.\d{2}\.\d{2}\.\d{3})')

    for i in range(start_line, -1, -1):
        chunk = '\n'.join(lines[max(0, i-window_size+1):i+1])
        match = pattern.search(chunk)
        if match:
            result = match.groupdict()
            if 'timestamp' in result:
                timestamp_str = result['timestamp']
                timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d_%H.%M.%S.%f')
                result['timestamp'] = timestamp
            # Correct the line number to reflect where the matched pattern starts
            result['line_number'] = max(0, i-window_size+1) + chunk[:match.start()].count('\n') + 1

            return result
    return None

def search_pattern_forwards(log_content, start_line, pattern, window_size=60):
    lines = log_content.splitlines()
    print(f"Searching job description from line: {start_line}")
    for i in range(start_line, len(lines)):
        chunk = '\n'.join(lines[i:min(len(lines), i+window_size)])
        match = pattern.search(chunk)
        if match:
            print(f"Match found at line number: {i + 1}")
            result = match.groupdict()
            result['distance_from_start'] = i - start_line
            return result

def search_and_extract(logs_folder, pattern_job_order, pattern_job, logs_with_alarms, search_depth=0):
    matches = []
    for log_file, timestamp in logs_with_alarms:
        with open(log_file) as file:
            log_content = file.read()
        
        for line_number, line in enumerate(log_content.splitlines(), start=1):
            if timestamp in line:
                print(f"Found timestamp: {timestamp} in log file at line number: {line_number}")
                print(line)
                
                # Combine logs up to the search depth
                combined_log_text, combined_log_lines = generate_combined_log(logs_folder, search_depth, log_file, log_content)
                
                # Adjust the starting line number for the combined logs
                adjusted_line_number = len(combined_log_lines) - len(log_content.splitlines()) + line_number
                
                match_job_order = search_pattern_backwards(combined_log_text, adjusted_line_number, pattern_job_order, window_size=8)
                if match_job_order:
                    print(f"Match found for pattern_job_order line: {match_job_order['line_number']}, {match_job_order['timestamp']}")
                    job_order_line_number = match_job_order['line_number']
                    
                    match_job = search_pattern_forwards(combined_log_text, job_order_line_number, pattern_job)
                    if match_job:
                        print("Match found for pattern_job:\n")
                        combined_match = {'filename': os.path.basename(log_file), **match_job_order, **match_job}
                        
                        # Calculate the elapsed time from job order to alarm
                        job_order_timestamp = match_job_order.get('timestamp')
                        alarm_timestamp = datetime.strptime(timestamp, '%Y-%m-%d_%H.%M.%S.%f')
                        if job_order_timestamp:
                            elapsed_time = alarm_timestamp - job_order_timestamp
                            combined_match['elapsed_time'] = elapsed_time
                        
                        matches.append(combined_match)
                        break  # Return only one match per log_file
                    else:
                        print("No match found for pattern_job.")
                else:
                    print("No match found for pattern_job_order.\n")
    return matches

def generate_combined_log(logs_folder, search_depth, log_file, log_content):
    combined_log_text = log_content
    current_log_file = log_file
    combined_log_lines = log_content.splitlines()
    for _ in range(search_depth):
        current_log_file = get_previous_log_file(current_log_file, logs_folder)
        if current_log_file:
            with open(current_log_file) as prev_file:
                prev_log_content = prev_file.read()
                combined_log_text = prev_log_content + "\n" + combined_log_text
                combined_log_lines = prev_log_content.splitlines() + combined_log_lines
        else:
            break
    return combined_log_text,combined_log_lines

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
    pattern_results_from = re.compile(r'Results from\s*.*\\(.*):')
    pattern_alarm_text = re.compile(re.escape(alarm_text))

    with open(parsing_output_file) as file:
        filename = None
        for line in file:
            # Check if the line matches 'Results from'
            match_results_from = pattern_results_from.search(line)
            if match_results_from:
                # Extract the filename part from the line
                filename = match_results_from.group(1)
            elif filename and pattern_alarm_text.search(line):
                # Extract the timestamp part from the line
                timestamp = line.split()[0].lstrip('*').rstrip(':')
                results.append({filename: timestamp})
                print(f"Found alarm timestamp: {timestamp} in file: {filename}")
    print(f"Total number of alarm timestamps found: {len(results)}")
    return results

# Find the parsing output file that corresponds to the source
def find_parsing_output_file(logs_folder, source):
    parsing_output_folder = find_parsing_output_folder(logs_folder)
    if parsing_output_folder:
        for file_name in os.listdir(parsing_output_folder):
            if source in file_name:
                parsing_output_file = os.path.join(parsing_output_folder, file_name)
                print(f"Found file: {file_name}")
                return parsing_output_folder, parsing_output_file  # Return the full path to the file
    else:
        return None

def find_parsing_output_folder(logs_folder):
    for root, dirs, files in os.walk(logs_folder):
        for dir_name in dirs:
            # Check if the folder name contains 'parsing-output'
            if 'parsing-output' in dir_name:
                parsing_output_folder = os.path.join(root, dir_name)
                print(f"Found folder: {parsing_output_folder}")
                return parsing_output_folder
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

def list_alarm_names(source_alarm_file):
    alarm_names = []
    pattern_alarm_stats = re.compile(r'Statistics for alarms requesting assistance:')
    pattern_alarm_header = re.compile(r'Count\s*Alarm\s*ID\s*Alarm\s*Text')


    with open(source_alarm_file) as file:
        found_alarm_stats = False
        for line in file:
            if not found_alarm_stats:
                if pattern_alarm_stats.search(line):
                    found_alarm_stats = True
            elif pattern_alarm_header.search(line):
                continue
            elif line.strip() == '':
                break
            else:
                parts = line.strip().split('\t')
                if len(parts) > 1:
                    alarm_names.append(parts[2])

    return alarm_names

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

# Prompt the user to proceed with matching the jobs with parsed Measureresults
def match_jobs_prompt():
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    user_response = messagebox.askyesno("Proceed", "Do you want to proceed to match the found jobs with parsed Measureresults?")
    root.destroy()
    return user_response

# Prompt the user to proceed with existing matching jobs info
def prompt_existing_file(title="Existing file", message="Do you want to use the existing file?"):
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    user_response = messagebox.askyesno(title, message)
    root.destroy()
    return user_response

# Prompt the user with a GUI to mark ATH success, yes or no
def mark_ath_success():
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    user_response = messagebox.askyesno(title="ATH Success", message="Mark ATH success for the matched results?")
    root.destroy()
    return user_response

# Prompt the user with a GUI to mark ATH success, 1 or 0
def ath_success_value():
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    user_response = messagebox.askyesno(title="ATH Success", message="Mark ATH success as 1?")
    root.destroy()
    return user_response


    
# Print the contents of the config file
if __name__ == '__main__':
    App = MyApp() # Create an instance of the MyApp class
    base_path = os.path.dirname(__file__) # Get the base path

    threading.Thread(target=process_fv_logs, args=(base_path,)).start() # Start the process_fv_logs function in a new thread
    
    App.mainloop() # Start the Tkinter main loop