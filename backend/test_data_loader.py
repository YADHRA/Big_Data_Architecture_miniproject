from pathlib import Path
from tempfile import TemporaryDirectory

from app.utils.data_loader import DataLoader


print("=" * 70)
print("DATA LOADER ERROR-HANDLING TEST")
print("=" * 70)

with TemporaryDirectory() as temp_dir:
    loader = DataLoader(Path(temp_dir))

    try:
        loader.load_csv("missing_file.csv")
    except FileNotFoundError as exc:
        print("PASS | Missing CSV raises FileNotFoundError")
        print("     ", exc)
    else:
        raise AssertionError(
            "Missing CSV did not raise FileNotFoundError"
        )

print("=" * 70)
print("DATA LOADER ERROR-HANDLING TEST PASSED")
print("=" * 70)
