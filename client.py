import threading
import websocket
import json
import socket
import time
import sys

# Server configuration
MAIN_SERVER_HOST = '127.0.0.1'
MAIN_SERVER_PORT = 65432
UID = 'BITMEX'
SID = 'MAINFRAME'
WEBSOCKET_HOST_FUNDING = 'wss://www.bitmex.com/realtime'
WEBSOCKET_HOST_GENERAL = WEBSOCKET_HOST_FUNDING  # Use the same WebSocket URL for both connections

server_socket = None
ws_funding = None
ws_general = None
tickers = []  # Global tickers list initially empty

def on_message_funding(ws_funding, message):
    try:
        data = json.loads(message)
        if 'table' in data and data['table'] == 'funding':
            if 'data' in data:
                for funding_info in data['data']:
                    send_to_main_server(funding_info)
    except json.JSONDecodeError as e:
        print(f"[: {UID} :]\t\t::     F_WS     ::\t   :: ERROR ::\nWebSocket JSON error: {e}")
        #print(f"Funding - WebSocket JSON error: {e}")

def on_message_general(ws_general, message):
    global tickers
    try:
        data = json.loads(message)
        
        # Ticker retrieval logic
        if data.get('table') == 'instrument':
            for instrument in data['data']:
                symbol = instrument.get('symbol')
                typ = instrument.get('typ')
                if typ == 'FFWCSX':  # Perpetual contracts type
                    #print(f"Perpetual Ticker: {symbol}")
                    if symbol not in tickers:
                        tickers.append(symbol)
                        #print(f"Updated tickers list: {tickers}")
                        #print(f"Ticker appended to the ticker list: {symbol}")
                        print(f"[: {UID} :]\t\t::     G_WS     ::\t   :: INFO ::\nUpdated tickers list with:\t{symbol}")
    except json.JSONDecodeError as e:
        print(f"[: {UID} :]\t\t::     G_WS     ::\t   :: ERROR ::\nWebSocket JSON error: {e}")
        #print(f"General - WebSocket JSON error: {e}")

def send_to_main_server(funding_info):
    global server_socket
    data_to_server = {
        'data': funding_info
    }
    try:
        server_socket.sendall(json.dumps(data_to_server).encode('utf-8'))
        print(f"[: {UID} :]\t\t::     SENT     ::\t   {{'data': funding_info}}")
    except socket.error as e:
        print(f"[: {UID} :]\t\t::    ERROR     ::\t   :: SEND :: => [: {SID} :]@({MAIN_SERVER_HOST}, {MAIN_SERVER_PORT})\n{e}.\nFailed to send data to the main server: {e}")
        #print(f"Failed to send data to the main server: {e}")
        reconnect_to_server()

def listen_for_server_messages():
    global server_socket, ws_funding, ws_general
    try:
        while True:
            message = server_socket.recv(1024)
            if message:
                response_data = json.loads(message.decode('utf-8'))
                if response_data.get('status') == 'shutdown':
                    print(f"[: {SID} :]\t\t::     SENT     ::\t   :: EXECUTE :: => shutdown\nServer is shutting down. Exiting client...")
                    if ws_funding:
                        ws_funding.close()
                    if ws_general:
                        ws_general.close()
                    server_socket.close()
                    sys.exit(0)
                else:
                    print(f"[: {SID} :]\t\t::     SENT     ::\t   {response_data}")
            else:
                break
    except socket.error as e:
        print(f"[: {UID} :]\t\t::    ERROR     ::\t   :: RECEIVE :: => [: {SID} :]@({MAIN_SERVER_HOST}, {MAIN_SERVER_PORT})\n{e}.\nError receiving data from server: {e}")
        #print(f"[: {UID} :]\t\t::    ERROR     ::\t   :: CONNECT :: => [: {SID} :]@({MAIN_SERVER_HOST}, {MAIN_SERVER_PORT})\n{e}.\n
        #print(f"Error receiving data from server: {e}")
    except Exception as e:
        print(f"[: {UID} :]\t\t::    ERROR     ::\t   :: [: {SID} :]@({MAIN_SERVER_HOST}, {MAIN_SERVER_PORT})\n{e}.\nUnexpected error: {e}")
        #print(f"Unexpected error: {e}")
    finally:
        print(f"[: {UID} :]\t\t::     INFO     :\t   :: [: {SID} :]@({MAIN_SERVER_HOST}, {MAIN_SERVER_PORT})\nServer connection closed.")
        #print("Server connection closed.")
        sys.exit(0)

def reconnect_to_server():
    global server_socket
    while True:
        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.connect((MAIN_SERVER_HOST, MAIN_SERVER_PORT))
            server_socket.sendall(UID.encode('utf-8'))  # Send UID immediately after connecting
            ack = server_socket.recv(1024)  # Receive UID acknowledgment
            if ack == b'UID received':
                print(f"[: {UID} :]\t\t::   CONNECTED  ::\t   [TO] server on port {MAIN_SERVER_PORT}\n\t\t\t\t\t\t   [FROM] client port {server_socket.getsockname()[1]}")
                break
            else:
                print(f"[: {UID} :]\t\t::    ERROR     ::\t   :: RECEIVE :: => [: {SID} :]@({MAIN_SERVER_HOST}, {MAIN_SERVER_PORT})\n{e}.\nUnexpected response: {ack}")
                #print(f"Unexpected response: {ack}")
        except socket.error as e:
            print(f"[: {UID} :]\t\t::    ERROR     ::\t   :: CONNECT :: => [: {SID} :]@({MAIN_SERVER_HOST}, {MAIN_SERVER_PORT})\n{e}.\nRetrying in 5 seconds...")
            time.sleep(5)

def on_error_funding(ws_funding, error):
    print(f"[: {UID} :]\t\t::     F_WS     ::\t   :: ERROR ::\nWebSocket error: {error}")
    #print(f"Funding - WebSocket error: {error}")

def on_error_general(ws_general, error):
    print(f"[: {UID} :]\t\t::     G_WS     ::\t   :: ERROR ::\nWebSocket error: {error}")
    #print(f"General - WebSocket error: {error}")

def on_close_funding(ws_funding, close_status_code, close_msg):
    print(f"[: {UID} :]\t\t::     F_WS     ::\t   :: CLOSED :: => {WEBSOCKET_HOST_FUNDING}\nWebSocket closed with code: {close_status_code}, message: {close_msg}")
    #print(f"Funding - WebSocket closed with code: {close_status_code}, message: {close_msg}")
    sys.exit(0)  # Exit when WebSocket is closed

def on_close_general(ws_general, close_status_code, close_msg):
    print(f"[: {UID} :]\t\t::     G_WS     ::\t   :: CLOSED :: => {WEBSOCKET_HOST_GENERAL}\nWebSocket closed with code: {close_status_code}, message: {close_msg}")
    #print(f"General - WebSocket closed with code: {close_status_code}, message: {close_msg}")
    sys.exit(0)  # Exit when WebSocket is closed

def on_open_funding(ws_funding):
    global tickers
    # Loop until tickers are populated
    while not tickers:
        time_wait = 5
        print(f"[: {UID} :]\t\t::     INFO     :\t   \nWaiting {time_wait}s for tickers to be populated...")
        #print(f"Waiting {time_wait}s for tickers to be populated...")
        time.sleep(time_wait)  # Wait before checking again

    max_batch_size = 20  # Adjust based on the websocket server
    for i in range(0, len(tickers), max_batch_size):
        batch = tickers[i:i + max_batch_size]
        subscribe_message = {
            "op": "subscribe",
            "args": [f"funding:{ticker}" for ticker in batch]
        }
        ws_funding.send(json.dumps(subscribe_message))
    keep_alive_thread_funding = threading.Thread(target=keep_alive_funding, args=(ws_funding,))
    keep_alive_thread_funding.start()

def on_open_general(ws_general):
    subscribe_message = {
        "op": "subscribe",
        "args": ["instrument"]  # Subscribe to instrument table for general tickers
    }
    ws_general.send(json.dumps(subscribe_message))
    keep_alive_thread_general = threading.Thread(target=keep_alive_general, args=(ws_general,))
    keep_alive_thread_general.start()

def keep_alive_funding(ws_funding):
    while True:
        time.sleep(30)  # Adjust the interval as needed
        try:
            ws_funding.send(json.dumps({"op": "ping"}))
            print(f"[: {UID} :]\t\t::    F_PING    ::\t   {WEBSOCKET_HOST_FUNDING}")
        except Exception as e:
            print(f"[: {UID} :]\t\t::     F_WS     ::\t   :: ERROR ::\nPing error: {e}")
            #print(f"Funding - Ping error: {e}")
            break

def keep_alive_general(ws_general):
    while True:
        time.sleep(30)  # Adjust the interval as needed
        try:
            ws_general.send(json.dumps({"op": "ping"}))
            print(f"[: {UID} :]\t\t::    G_PING    ::\t   {WEBSOCKET_HOST_GENERAL}")
        except Exception as e:
            print(f"[: {UID} :]\t\t::     G_WS     ::\t   :: ERROR ::\nPing error: {e}")
            #print(f"General - Ping error: {e}")
            break

def run_websocket_funding():
    global ws_funding
    ws_funding = websocket.WebSocketApp(WEBSOCKET_HOST_FUNDING,
                                        on_open=on_open_funding,
                                        on_message=on_message_funding,
                                        on_error=on_error_funding,
                                        on_close=on_close_funding)
    ws_funding.run_forever()

def run_websocket_general():
    global ws_general
    ws_general = websocket.WebSocketApp(WEBSOCKET_HOST_GENERAL,
                                        on_open=on_open_general,
                                        on_message=on_message_general,
                                        on_error=on_error_general,
                                        on_close=on_close_general)
    ws_general.run_forever()

if __name__ == "__main__":
    reconnect_to_server()

    # Start a thread to listen for server messages
    listener_thread = threading.Thread(target=listen_for_server_messages)
    listener_thread.daemon = True  # Ensure it exits when the main thread exits
    listener_thread.start()

    # Start the secondary (general) WebSocket thread first
    websocket_general_thread = threading.Thread(target=run_websocket_general)
    websocket_general_thread.start()

    # Start the primary (funding) WebSocket thread second
    websocket_funding_thread = threading.Thread(target=run_websocket_funding)
    websocket_funding_thread.start()

    websocket_general_thread.join()
    websocket_funding_thread.join()
