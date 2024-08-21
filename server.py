import socket
import json
import threading
import signal
import sys

# Server configuration
HOST = '127.0.0.1'
PORT = 65432
SID = "MAINFRAME"

clients = []
shutdown_event = threading.Event()

# Define custom functions
def custom_function_1():
    return "Custom function 1 executed."

def custom_function_2(arg):
    return f"Custom function 2 executed with argument: {arg}"

def shutdown_server():
    print("Shutdown command received. Initiating graceful shutdown...")
    shutdown_event.set()
    return "Server shutting down..."

# Command dispatcher
COMMANDS = {
    'custom1': custom_function_1,
    'custom2': custom_function_2,
    'shutdown': shutdown_server
}

def handle_client(uid, conn, addr):
    global clients
    print(f"[: {uid} :]\t\t::  CONNECTED   ::\t   @{addr}")
    clients.append(conn)
    try:
        while not shutdown_event.is_set():
            data = conn.recv(1024)
            if not data:
                print(f"[: {uid} :]\t\t:: DISCONNECTED ::\t   @{addr}")
                break
            message = json.loads(data.decode('utf-8'))

            if 'command' in message:
                command = message['command'].split()
                cmd_name = command[0]
                args = command[1:]
                if cmd_name in COMMANDS:
                    print(f"[: {uid} :]\t\t::     SENT     ::\t   :: EXECUTE :: => {message['command']}")
                    try:
                        if args:
                            response_output = COMMANDS[cmd_name](*args)
                        else:
                            response_output = COMMANDS[cmd_name]()
                        response = {
                            'status': 'success',
                            'output': response_output,
                            'error': None
                        }
                        conn.sendall(json.dumps(response).encode('utf-8'))
                        if cmd_name == "shutdown":
                            break  # Exit after sending the shutdown response
                    except Exception as e:
                        response = {
                            'status': 'error',
                            'message': str(e)
                        }
                        conn.sendall(json.dumps(response).encode('utf-8'))
                else:
                    response = {
                        'status': 'error',
                        'message': f"Command '{cmd_name}' is not recognized."
                    }
                    conn.sendall(json.dumps(response).encode('utf-8'))
            else:
                funding_info = message.get('data')
                print(f"[: {uid} :]\t\t::     SENT     ::\n{funding_info}")
                # Send acknowledgment
                conn.sendall(json.dumps({'status': 'received'}).encode('utf-8'))
    except Exception as e:
        print(f"[: {uid} :]\t\t::    ERROR     ::\t   @{addr}\n{e}")
    finally:
        try:
            conn.close()
        except Exception as e:
            print(f"Error closing connection for {uid}: {e}")
        clients.remove(conn)
        print(f"[: {SID} :]\t\t::    CLOSED    ::\t   [: {uid} :]@{addr}")

def start_listener():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        print(f"[: {SID} :]\t\t::  LISTENING   ::\t   @{HOST}:{PORT}")
        while not shutdown_event.is_set():
            try:
                s.settimeout(1)
                conn, addr = s.accept()
                uid = conn.recv(1024).decode('utf-8')
                conn.sendall(b'UID received')  # Acknowledge UID reception
                client_thread = threading.Thread(target=handle_client, args=(uid, conn, addr))
                client_thread.start()
            except socket.timeout:
                continue
            except Exception as e:
                print(f"[: {SID} :]\t\t::    ERROR    :: {e}")

def graceful_shutdown():
    print("Received signal to shut down. Shutting down gracefully...")
    shutdown_event.set()
    # Notify all clients of shutdown
    for client in clients:
        try:
            client.sendall(json.dumps({'status': 'shutdown'}).encode('utf-8'))
            client.shutdown(socket.SHUT_RDWR)
            client.close()
        except Exception as e:
            print(f"Error shutting down client: {e}")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda signum, frame: graceful_shutdown())
    signal.signal(signal.SIGTERM, lambda signum, frame: graceful_shutdown())
    print(f"[: {SID} :]\t\t::   STARTED    ::")
    listener_thread = threading.Thread(target=start_listener)
    listener_thread.start()
    listener_thread.join()

    # Ensure proper shutdown if the thread finishes
    graceful_shutdown()
