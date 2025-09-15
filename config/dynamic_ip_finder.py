import subprocess
import re

def get_vpn_ip(adapter_keyword="Common_javeriana_na_res"):
    try:
        # Ejecutar ipconfig y capturar la salida
        result = subprocess.run(["ipconfig"], capture_output=True, text=True, check=True)
        output = result.stdout

        # Buscar la sección del adaptador que contenga el keyword
        adapter_regex = rf"Adaptador.*{adapter_keyword}.*?(\n\s*\S.*?)*?(?=\n\n|\Z)"
        match = re.search(adapter_regex, output, re.DOTALL)
        if not match:
            print("[ERROR] No se encontró el adaptador VPN")
            return None

        adapter_block = match.group(0)

        # Buscar la IP dentro de ese bloque
        ip_match = re.search(r"Dirección IPv4 .*?: (\d+\.\d+\.\d+\.\d+)", adapter_block)
        if ip_match:
            ip_address = ip_match.group(1)
            print(f"[INFO] IP VPN encontrada: {ip_address}")
            return ip_address
        else:
            print("[ERROR] No se encontró la IP en el adaptador")
            return None

    except subprocess.CalledProcessError as e:
        print("[ERROR] Falló al ejecutar ipconfig:", e)
        return None

if __name__ == "__main__":
    get_vpn_ip()
