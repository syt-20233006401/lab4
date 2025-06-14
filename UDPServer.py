import socket
import threading
import random
import os
import base64

class UDPServer:
    def __init__(self, port):  #Initialize the server
        self.server_port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_socket.bind(('', port))
        print(f"[Server] Started successfully, listening on port {port}")

    def start(self):
        while True:
            try:
                data, client_addr = self.server_socket.recvfrom(1024)
                message = data.decode().strip()
                print(f"[Server] Received message from {client_addr}: {message}")

                if message.startswith("DOWNLOAD"):
                    threading.Thread(  #Process download requests
                        target=self.handle_download_request,
                        args=(message, client_addr)
                    ).start()

            except Exception as e:
                print(f"[Server] Error: {e}")

    def handle_download_request(self, message, client_addr):
        try: #Processing download request
            filename = message.split()[1]#Get file name
            if os.path.exists(filename):
                file_size = os.path.getsize(filename) #Get file size
                worker_port = random.randint(50000, 51000)
                response = f"OK {filename} SIZE {file_size} PORT {worker_port}"
                self.server_socket.sendto(response.encode(), client_addr)
                print(f"[Server] Sent OK response to {client_addr}, port {worker_port}")
                transfer_thread = threading.Thread(
                    target=self.handle_file_transfer,
                    args=(filename, worker_port, client_addr[0]),
                    daemon=True
                )
                transfer_thread.start() #Start the file transfer thread
            else:
                response = f"ERR {filename} NOT_FOUND"
                self.server_socket.sendto(response.encode(), client_addr)
                print(f"[Server] File {filename} not found")
        except IndexError:
            print(f"[Server] Invalid DOWNLOAD message format: {message}")
            response = "ERR INVALID_FORMAT"
            self.server_socket.sendto(response.encode(), client_addr)
        except Exception as e:
            print(f"[Server] Download request error: {e}")

    def handle_file_transfer(self, filename, port, client_host):
        # Handle file transfer
        transfer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        transfer_socket.bind(('', port))  # Bind transfer port
        print(f"[File Transfer] Starting file {filename} transfer on port {port}")
