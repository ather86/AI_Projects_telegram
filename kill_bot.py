import psutil
import os

def find_and_kill_bot_process():
    """
    Finds and terminates any running Python processes that are executing 'telegram_gate.py'.
    """
    killed = False
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            # Check if the process is a python process and is running the telegram_gate.py script
            if 'python' in proc.info['name'].lower() and proc.info['cmdline'] and 'telegram_gate.py' in proc.info['cmdline']:
                print(f"Found running bot process with PID: {proc.info['pid']}. Terminating it.")
                p = psutil.Process(proc.info['pid'])
                p.terminate()
                p.wait() # Wait for the process to terminate
                print(f"Process {proc.info['pid']} terminated.")
                killed = True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
            print(f"Could not terminate process {proc.info.get('pid', '?')}: {e}")
    if not killed:
        print("No running 'telegram_gate.py' process found.")

if __name__ == "__main__":
    find_and_kill_bot_process()
