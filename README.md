
# Yandex Market Reviews Scraper

This project is a Python script that scrapes reviews from Yandex Market.

## Setup

1. Ensure you have Python 3.7+ installed on your system.

2. Clone this repository:
   
   ```bash
    https://github.com/AlbinAimleap/yandex
    cd yandex
   ```
   

3. Install the required dependencies:
   
   ```bash
    pip install -r requirements.txt
    ```
   

## Usage

Run the script with the following command:


```bash
    python main.py <output_file>
```


Replace `<output_file>` with the desired output file name. The script supports .csv, .json, and .xlsx file formats.

For example:

```bash
    python main.py reviews_output.csv
```


## Output

The script will generate two files:

1. The main output file (specified by you) containing the scraped reviews.
2. A `yandex_missing.json` file containing information about any items that couldn't be scraped.

## Note

Make sure you have the necessary permissions to scrape data from Yandex Market and that you comply with their terms of service.
