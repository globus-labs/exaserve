import socket
import sys

def get_hsn_ip():
    """
    Connects to a dummy internal IP to force the OS to pick the 
    default route interface (High Speed Network).
    """
    try:
        # We don't actually send data, just open a socket to determine routing.
        # 10.255.255.255 is a safe dummy target for internal routing.
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception as e:
        # Fallback for login nodes or single-node testing
        return socket.gethostbyname(socket.gethostname())

if __name__ == "__main__":
    print(get_hsn_ip())
