from prefect import task, Flow
import subprocess
import os
import smtplib
from email.message import EmailMessage
import prefect
import requests
import hashlib
from urllib.parse import urljoin, quote_plus
import multiprocessing

# Replace with your actual GitLab API URL
GITLAB_API_URL = 'https://gitlab.example.com/api/v4/'
# Replace with your actual project ID
PROJECT_ID = 'YOUR_PROJECT_ID'
# Replace with your actual private token
PRIVATE_TOKEN = 'YOUR_PRIVATE_TOKEN'
checksum_file = '/opt/etl/cicd.txt'
all_files_path = '/opt/etl/git/dwh'
prefect_project_name = 'myProject'

def get_project_files():
    headers = {'PRIVATE-TOKEN': PRIVATE_TOKEN}
    url = urljoin(GITLAB_API_URL, f'projects/{PROJECT_ID}/repository/tree')
    responses = []
    while url:
        response = requests.get(url, headers=headers, params={'recursive': True, 'per_page': 100})
        responses.extend(response.json())
        next_page = response.headers.get('X-Next-Page')
        url = urljoin(GITLAB_API_URL, f'projects/{PROJECT_ID}/repository/tree?page={next_page}') if next_page else None
    return responses

def download_file(file_path):
    headers = {'PRIVATE-TOKEN': PRIVATE_TOKEN}
    safe_file_path = quote_plus(file_path)
    url = urljoin(GITLAB_API_URL, f'projects/{PROJECT_ID}/repository/files/{safe_file_path}/raw')
    response = requests.get(url, headers=headers)
    return response.content

def calculate_checksum(file_content):
    return hashlib.sha256(file_content).hexdigest()

def read_existing_checksums(file_path):
    if not os.path.exists(file_path):
        return {}
    with open(file_path, 'r') as file:
        return dict(line.strip().split(': ') for line in file if line.strip())

def write_checksums(file_path, checksums):
    with open(file_path, 'w') as file:
        for file_name, checksum in checksums.items():
            file.write(f"{file_name}: {checksum}\n")

def run_subprocess(command, queue):
    try:
        res = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print('Command stdout:', res.stdout)
        print('Command stderr:', res.stderr)
        queue.put(res.returncode)
    except subprocess.CalledProcessError as e:
        print(f'Error during command execution: {e}')
        queue.put(e.returncode)

@task(log_stdout=True)
def process_files():
    existing_checksums = read_existing_checksums(checksum_file)
    new_checksums2 = {}
    files = get_project_files()
    for file in files:
        file_path = str(file['path'])
        checksum = None
        if file_path.endswith('.py'):
            file_content = download_file(file_path)
            with open(os.path.join(all_files_path, file_path), 'wb') as file:
                file.write(file_content)
            checksum = calculate_checksum(file_content)
            new_checksums2[file_path] = checksum
    write_checksums(checksum_file, new_checksums2)

    # Find and print discrepancies
    for file, checksum in new_checksums2.items():
        if file == 'CICD_Flow.py':
            continue
        if file in existing_checksums and existing_checksums[file] != checksum:
            print(f"File changed: {file}, Old checksum: {existing_checksums[file]}, New checksum: {checksum}")
            try:
                subprocess.check_call(f"flake8 {os.path.join(all_files_path, file)}", shell=True)
                print(f'Flake8 has succeeded for {file}')
            except Exception as e:
                print(f'Flake8 has FAILED for {file} - {e}')
                continue
            try:
                queue = multiprocessing.Queue()
                command = f"python3 {os.path.join(all_files_path, file)}"
                process = multiprocessing.Process(target=run_subprocess, args=(command, queue))
                process.start()
                process.join()
                if queue.get() != 0:
                    raise BaseException('Flow run FAILED!')
                print('Flow run succeeded!')
            except Exception as e:
                print(f'Flow run FAILED! - {e}')
                continue
            try:
                queue = multiprocessing.Queue()
                command = f"prefect register --project {prefect_project_name} -p {os.path.join(all_files_path, file)}"
                process = multiprocessing.Process(target=run_subprocess, args=(command, queue))
                process.start()
                process.join()
                if queue.get() == 0:
                    print(f'Registration flow successful')
                else:
                    raise Exception('Registration flow ERROR')
            except Exception as e:
                print(f'Registration flow FAILED! - {e}')

with Flow("CICD Flow") as flow:
    process_files()

f_state = flow.run()
# Uncomment the next line to assert that the flow run is successful
# assert f_state.is_successful()

flow.register(project_name="frequent")
