import torch
import intel_extension_for_pytorch as ipex
import time
import os
import sys

def clear_screen():
    # A more aggressive clear that works in 99% of terminals
    os.system('cls' if os.name == 'nt' else 'clear')

def main():
    if not torch.xpu.is_available():
        print("CRITICAL: No XPU (Intel GPU) detected via PyTorch!")
        return

    device_count = torch.xpu.device_count()
    
    try:
        while True:
            clear_screen()
            
            # --- HEADER ---
            # Fixed width: 80 chars total
            print("+" + "-"*78 + "+")
            print(f"| AURORA XPU MONITOR (Python/IPEX)       {time.strftime('%H:%M:%S')}   GPUs: {device_count:<2}           |")
            print("+" + "-"*78 + "+")
            print(f"| {'ID':<4} | {'Name':<30} | {'Memory (Used / Total)':<22} | {'Util':<8} |")
            print("+" + "-"*78 + "+")

            # --- ROW DATA ---
            for i in range(device_count):
                # 1. Get Name (Truncate to fit 30 chars)
                props = torch.xpu.get_device_properties(i)
                name = props.name
                if len(name) > 30: 
                    name = name[:27] + "..."
                
                # 2. Get Global Memory Stats
                # mem_get_info returns (free, total) in bytes
                try:
                    mem_free, mem_total = torch.xpu.mem_get_info(i)
                    mem_used = mem_total - mem_free
                    
                    # Convert to GiB
                    used_gib = mem_used / (1024**3)
                    total_gib = mem_total / (1024**3)
                    util_pct = (mem_used / mem_total) * 100
                except:
                    used_gib = 0.0
                    total_gib = 0.0
                    util_pct = 0.0

                # 3. Print Row (Strict Formatting)
                # :<4  = Left align, width 4
                # :<30 = Left align, width 30
                # :>5.2f = Right align, width 5, 2 decimals
                print(f"| {i:<4} | {name:<30} | {used_gib:>6.2f} / {total_gib:<6.2f} GiB   | {util_pct:>5.1f}%  |")

            print("+" + "-"*78 + "+")
            print("| [Ctrl+C] to Exit                                                           |")
            print("+" + "-"*78 + "+")
            
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nMonitor stopped.")

if __name__ == "__main__":
    main()
