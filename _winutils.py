"""_winutils.py — Windows HADOOP_HOME fix. No-op on Linux/Mac."""
import os, sys, urllib.request

def setup():
    if sys.platform != "win32":
        return
    root    = os.path.dirname(os.path.abspath(__file__))
    bin_dir = os.path.join(root, "hadoop", "bin")
    os.makedirs(bin_dir, exist_ok=True)
    for name, url in [
        ("winutils.exe", "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe"),
        ("hadoop.dll",   "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/hadoop.dll"),
    ]:
        dest = os.path.join(bin_dir, name)
        if not os.path.exists(dest):
            print(f"Baixando {name}...")
            urllib.request.urlretrieve(url, dest)
    os.environ["HADOOP_HOME"] = os.path.join(root, "hadoop")
    os.environ["PATH"]        = bin_dir + os.pathsep + os.environ.get("PATH", "")
