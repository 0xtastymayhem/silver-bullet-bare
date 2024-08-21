import socket
import json

# Server configuration
SERVER_HOST = 'localhost'
SERVER_PORT = 65432
UID = 'TERMINAL'
SID = "MAINFRAME"

def send_command(command):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((SERVER_HOST, SERVER_PORT))
            s.sendall(UID.encode('utf-8'))  # Send UID
            s.recv(1024)  # Wait for UID acknowledgment
            s.sendall(json.dumps({'command': command}).encode('utf-8'))
            response = s.recv(4096)  # Increase buffer size if needed
            if response:
                response_data = json.loads(response.decode('utf-8'))
                if response_data['status'] == 'success':
                    #print("Command Output:\n", response_data['output'])
                    print(f"[: {SID} :]\t\t::     SENT     ::\t   :: EXECUTE :: => {command}\n", response_data['output'])
                    if response_data.get('error'):
                        #print("Command Error Output:\n", response_data['error'])
                        print(f"[: {SID} :]\t\t::    ERROR     ::\t   ::     COMMAND     ::\n", response_data['error'])
                else:
                    #print("Error executing command:\n", response_data['message'])
                    print(f"[: {SID} :]\t\t::    ERROR     ::\t   ::    EXECUTION   ::\n", response_data['message'])
    except socket.error as e:
        print(f"[: {UID} :]\t\t::    ERROR     ::\t   :: SEND ::\n{e}")

if __name__ == "__main__":
    print(f"[: {UID} :]\t\t::   STARTED    ::")
    while True:
        command = input("Enter command to execute on server (or 'exit' to quit): ")
        if command.lower() == 'exit':
            break
        send_command(command)
