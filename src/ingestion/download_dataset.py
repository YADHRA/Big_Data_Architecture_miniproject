"""Stage 2: download the main dataset from Hugging Face into data/raw/."""
from pathlib import Path
from huggingface_hub import list_repo_files, hf_hub_download

REPO_ID = "electricsheepafrica/africa-synth-telecom-application-data-consumption-nigeria"
TARGET_NAME = "application_data_consumption_breakdown.parquet"
RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    files = list_repo_files(REPO_ID, repo_type="dataset")
    print("Files in the dataset repo:")
    for f in files:
        print("  -", f)

    matches = [f for f in files if f.endswith(TARGET_NAME)]
    if not matches:
        raise SystemExit(f"Could not find {TARGET_NAME} in the repo. Check the list above.")

    print(f"\nDownloading: {matches[0]}")
    local_path = hf_hub_download(
        repo_id=REPO_ID,
        filename=matches[0],
        repo_type="dataset",
        local_dir=RAW_DIR,
    )
    print("Saved to:", local_path)


if __name__ == "__main__":
    main()
