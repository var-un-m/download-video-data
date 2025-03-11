# download-video-data

## Installation

- Prereq: install aws cli and configure it with the provided credentials

```bash
# Clone the repository
git clone https://github.com/var-un-m/download-video-data.git
cd download-video-data

# Install dependencies
pip install -r requirements.txt
```

## Usage

```python
python download_dataset.py --table HDTF-crops --output-csv ./hdtf-meta.csv --download-dir ./hdtf --quality-threshold xx           # HDTF
python download_dataset.py --table celebv-HQ-crops --output-csv ./celebv-hq-meta.csv --download-dir ./cvhq --quality-threshold xx # Celebv-HQ
python download_dataset.py --table crops_v5 --output-csv ./celebv-text-meta.csv --download-dir ./cvtext --quality-threshold xx    # Celebv-text
```