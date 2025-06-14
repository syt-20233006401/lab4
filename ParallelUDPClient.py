import base64
import os
import socket
import threading
from queue import Queue

class ParallelUDPClient:
    def __init__(self, host, port, file_list, max_threads=4):#Initialize the client
        self.server_host = host
        self.server_port = port
        self.file_list = file_list
        self.max_threads = max_threads
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(5.0)
        self.max_retries = 5
        self.file_queue = Queue()
        self.lock = threading.Lock()

    def run(self): #The main run method
        try:
            with open(self.file_list, 'r', encoding='utf-8') as f:
                filenames = [line.strip() for line in f if line.strip()]
            for filename in filenames:
                self.file_queue.put(filename)
            threads = []
            for _ in range(min(self.max_threads, len(filenames))):
                t = threading.Thread(target=self.worker)
                t.start()
                threads.append(t)
            for t in threads:
                t.join()
        except FileNotFoundError:
            print(f"[Parallel Client] Error: File list {self.file_list} not found")
        except Exception as e:
            print(f"[Parallel Client] Error: {e}")
        finally:
            self.socket.close()#Close the socket

    def worker(self):#Working thread method
        while not self.file_queue.empty():
            filename = self.file_queue.get()
            try:
                self.download_file(filename)
            finally:
                self.file_queue.task_done()

    def download_file(self, filename):#Client file download logic
        with self.lock:
            print(f"[Parallel Client] Starting download: {filename}")
        response = self.send_with_retry( #Send download request and retry
            f"DOWNLOAD {filename}",
            (self.server_host, self.server_port),
            lambda r: r.startswith(("OK", "ERR")))
        if not response:
            with self.lock:
                print(f"[Parallel Client] Download {filename} failed: Cannot connect to server")
            return
        if response.startswith("ERR"):
            with self.lock:
                print(f"[Parallel Client] Download {filename} failed: {response}")
            return
        parts = response.split()#Parse server response
        file_size = int(parts[parts.index("SIZE") + 1])
        port = int(parts[parts.index("PORT") + 1])
        with self.lock:
            print(f"[Parallel Client] Receiving file {filename} (size: {file_size} bytes)")
        if not self.receive_file(filename, file_size, port):#Receive file content
            with self.lock:
                print(f"[Parallel Client] File {filename} download incomplete")
            return
        with self.lock:
            print(f"[Parallel Client] File {filename} download complete")

    def receive_file(self, filename, file_size, port):
        # Receive file content
        transfer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        transfer_socket.settimeout(10.0)  # Set transfer socket timeout
        temp_filename = filename + ".download"  # Temporary filename
        try:
            with open(temp_filename, 'wb') as file:
                total_received = 0
                while total_received < file_size:
                    start = total_received
                    end = min(start + 999, file_size - 1)
                    # Try up to 5 times to get data chunk
                    for attempt in range(5):
                        try:
                            # Request file chunk
                            request = f"FILE {filename} GET START {start} END {end}"
                            transfer_socket.sendto(request.encode(), (self.server_host, port))
                            data, _ = transfer_socket.recvfrom(4096)
                            response = data.decode()
                            if "DATA" in response:
                                # Decode and write to file
                                data_part = response.split("DATA ")[1]
                                chunk = base64.b64decode(data_part.encode('utf-8'))
                                if len(chunk) == (end - start + 1):
                                    file.write(chunk)
                                    total_received += len(chunk)
                                    with self.lock:
                                        # Print progress
                                        print(f"\r[Progress {filename}] {total_received}/{file_size} ({total_received / file_size:.1%})", end='', flush=True)
                                    break
                        except socket.timeout:
                            with self.lock:
                                print(f"\n[Retry {filename}] Chunk {start}-{end} timeout ({attempt + 1}/5)")
                            continue
            # Check if file is fully downloaded
            if total_received == file_size:
                if os.path.exists(filename):
                    os.replace(temp_filename, filename)  # Replace existing file
                else:
                    os.rename(temp_filename, filename)   # Rename temp file
                return True
            return False
        except Exception as e:
            with self.lock:
                print(f"\n[Critical Error {filename}] {str(e)}")
            return False
        finally:
            transfer_socket.close()  # Close transfer socket
            if os.path.exists(temp_filename):
                os.remove(temp_filename)  # Delete temp file
    def send_with_retry(self, message, address, response_validator):
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                self.socket.sendto(message.encode(), address)
                data, _ = self.socket.recvfrom(2048)
                response = data.decode()
                if response_validator(response):
                    return response
            except socket.timeout:
                retry_count += 1
                new_timeout = min(5.0 * (retry_count + 1), 30)
                self.socket.settimeout(new_timeout)
                with self.lock:
                    print(f"[Parallel Client] Timeout, retry {retry_count}/{self.max_retries}")
        return None

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 4 or len(sys.argv) > 5:
        print("Usage: python3 ParallelUDPClient.py <host> <port> <file_list> [threads]")
        sys.exit(1)
    host = sys.argv[1]
    port = int(sys.argv[2])
    file_list = sys.argv[3]
    max_threads = int(sys.argv[4]) if len(sys.argv) == 5 else 4
    client = ParallelUDPClient(host, port, file_list, max_threads)
    client.run()
