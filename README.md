# cc_news
Creating corpora from CC News dumps with a bit of py
Step 1 is performed by `get_parquetfiles.py`
This step involves downloading the parquet files we want from HF.

Step 2 is performed by `make_conll.py`
This step takes parquet files retrieved in Step1, and extracts articles from them, exporting the data to json files as an intermediate step. This means we do the slower parquet processing step once.
The files can thus be inspected and any changes made much more speedily than from the parquet, before exporting the data to the conll files the parsing script wants.

Step 3 is performed by `runStanza.py`
This is the longest step, sending the files to the parser.

Step 4 is performed by `send_to_xml.py`
This script takes conll files and converts them to XML, using the metadata stored in the conll comment lines.
