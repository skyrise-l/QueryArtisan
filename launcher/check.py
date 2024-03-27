import requests
import time
import subprocess

status_url = "http://localhost:9111" 

def restart_program():
    print("Restarting the program...")
    subprocess.Popen(["python -m GPT3", "your_python_program.py"])

while True:
    try:
    
        response = requests.get(status_url)
        if response.status_code == 200:  
            print("Program is running.")
        else:
            print("Program is not responding. Attempting to restart...")
            restart_program() 
    except Exception as e:
        print(f"Error occurred: {e}. Attempting to restart...")
        restart_program() 

    time.sleep(60)