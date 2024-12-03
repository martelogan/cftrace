import socket
import ssl
import time

HOST = '34.49.121.93'
PORT = 443
PATH = '/internal-echo'
SAMPLES = 5

def create_ssl_context():
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context

def perform_https_request(sock, context, keep_alive=True, recreate_ssl_context=False):
    ssock = None
    try:
        if not sock:
            sock = socket.create_connection((HOST, PORT))
            ssock = context.wrap_socket(sock, server_hostname=HOST)
        elif recreate_ssl_context:
            sock = sock.unwrap()  # Unwrapping for new SSL context
            ssock = context.wrap_socket(sock, server_hostname=HOST)
        else:
            ssock = sock  # Reuse SSL socket

        request_header = (
            f"GET {PATH} HTTP/1.1\r\n"
            f"Host: {HOST}\r\n"
            f"Connection: {'keep-alive' if keep_alive else 'close'}\r\n"
            "\r\n"
        )
        ssock.send(request_header.encode())
        start_time = time.time()
        response = ssock.recv(4096)
        elapsed_time = (time.time() - start_time) * 1000

        if len(response) == 0 or elapsed_time < 0.8:
            print("Received an empty response, indicating no real transmission occurred.")
            elapsed_time = None  # Insufficient transmission time

    except Exception as e:
        print(f"Error during data exchange: {e}")
        elapsed_time = None

    finally:
        if not keep_alive:
            try:
                ssock.shutdown(socket.SHUT_RDWR)
                ssock.close()
            except Exception as e:
                print(f"Error while shutting down the socket: {e}")

    return elapsed_time, ssock if keep_alive else None

def log_experiment(title, results):
    valid_results = [res for res in results if res is not None]
    if valid_results:
        average_time = sum(valid_results) / len(valid_results)
    else:
        average_time = 0
    print(f"\n{title}:")
    for elapsed_time in valid_results:
        print(f"Elapsed Time: {elapsed_time:.2f} ms")
    print(f"Average Time: {average_time:.2f} ms")

def main():
    context = create_ssl_context()
    ss_socket = None

    cold_results = [perform_https_request(None, context, keep_alive=False)[0] for _ in range(SAMPLES)]
    log_experiment("Cold TCP & Cold TLS", cold_results)

    warm_cold_tls_results = []
    for _ in range(SAMPLES):
        elapsed_time, ss_socket = perform_https_request(ss_socket, context, keep_alive=True, recreate_ssl_context=True)
        warm_cold_tls_results.append(elapsed_time)
    ss_socket.close()
    log_experiment("Warm TCP & Cold TLS", warm_cold_tls_results)

    warm_warm_tls_results = []
    ss_socket = None
    for _ in range(SAMPLES):
        elapsed_time, ss_socket = perform_https_request(ss_socket, context, keep_alive=True, recreate_ssl_context=False)
        warm_warm_tls_results.append(elapsed_time)
    ss_socket.close()
    log_experiment("Warm TCP & Warm TLS", warm_warm_tls_results)

if __name__ == "__main__":
    main()
