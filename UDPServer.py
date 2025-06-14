import socket
import threading
import random
import os
import base64

class UDPServer:
    def __init__(self, port): #Initialize the server
        self.server_port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #Create a UDP socket
        self.server_socket.bind(('', port))
        print(f"[Server] Started successfully, listening on port {port}")

    def start(self):
        while True: #Start the server main loop
            try:
                data, client_addr = self.server_socket.recvfrom(1024)
                message = data.decode().strip()
                print(f"[Server] Received message from {client_addr}: {message}")
                if message.startswith("DOWNLOAD"):
                    threading.Thread( #Process download requests
                        target=self.handle_download_request,
                        args=(message, client_addr)
                    ).start()
            except Exception as e:
                print(f"[Server] Error: {e}")
