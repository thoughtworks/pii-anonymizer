# Data Protection Framework
Data Protection Framework is a python library/command line application for identification, anonymization and de-anonymization of Personally Identifiable Information data.

The framework aims to work on a two-fold principle for detecting PII:
1. Using RegularExpressions using a pattern
2. Using NLP for detecting NER (Named Entity Recognitions)

## Common Usage
1. `pip install pii-anonymizer`
2. Specify configs in `pii-anonymizer.json`
3. Choose whether to run in standalone or spark mode with `python -m pii_anonymizer.standalone` or `python -m pii_anonymizer.spark`

## Features and Current Status

### Completed
 * Following Global detectors have been completed:
   * [x] EMAIL_ADDRESS :  An email address identifies the mailbox that emails are sent to or from. The maximum length of the domain name is 255 characters, and the maximum length of the local-part is 64 characters.
   * [x] CREDIT_CARD_NUMBER : A credit card number is 12 to 19 digits long. They are used for payment transactions globally.

 * Following detectors specific to Singapore have been completed:
   * [x] PHONE_NUMBER : A telephone number.
   * [x] FIN/NRIC : A unique set of nine alpha-numeric characters on the Singapore National Registration Identity Card.
   * [x] THAI_ID : 13 numeric digits of Thai Citizen ID

 * Following anonymizers have been added
    * [x] Replacement ('replace'): Replaces a detected sensitive value with a specified surrogate value. Leave the value empty to simply delete detected sensitive value.
    * [x] Hash ('hash'): Hash detected sensitive value with sha256.
    * [x] Encryption: Encrypts the original sensitive data value using a Fernet (AES based).

Currently supported file formats: `csv, parquet`

## Encryption
To use encryption as anonymize mode, a compatible encryption key needs to be created and assigned to `PII_SECRET` environment variables. Compatible key can be generated with

`python -m pii_anonymizer.key`

This will generate output similar to
```
Keep this encrypt key safe
81AOjk7NV66O62QpnFsvCXH8BDB26KM9TIH7pBfZ6PQ=
```
To set this key as an environment variable run

`export PII_SECRET=81AOjk7NV66O62QpnFsvCXH8BDB26KM9TIH7pBfZ6PQ=`
### TO-DO
Following features  are part of the backlog with more features coming soon
 * Detectors:
    * [ ] NAME
    * [ ] ADDRESS
 * Anonymizers:
    * [ ] Masking: Replaces a number of characters of a sensitive value with a specified surrogate character, such as a hash (#) or asterisk (*).
    * [ ] Bucketing: "Generalizes" a sensitive value by replacing it with a range of values. (For example, replacing a specific age with an age range,
    or temperatures with ranges corresponding to "Hot," "Medium," and "Cold.")


You can have a detailed at upcoming features and backlog in this [Github Board](https://github.com/thoughtworks-datakind/anonymizer/projects/1?fullscreen=true)

## Development setup
1. Install [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)
2. Setup hooks and install packages with `make install`

### Config JSON
Limitation: when reading multiple files, all files that matches the file_path must have same headers. Additionally, when file format is not given anonymizer will assume that the file format is the first matched filename. Thus, when the file_path ends with `/*` and the folder contains mixed file format, the operation will fail.

An example for the config JSON is located at `<PROJECT_ROOT>/pii-anonymizer.json`
```
{
  "acquire": {
    "file_path": <FILE PATH TO YOUR INPUT CSV>, -> ./input_data/file.csv or ./input_data/*.csv to read all files that matches
    "delimiter": <YOUR CSV DELIMITER>
  },
  "analyze": {
    "exclude": ['Exception']
  },
  "anonymize": {
    "mode": <replace|hash|encrypt>,
    "value": "string to replace",
    "output_file_path" : <PATH TO YOUR CSV OUTPUT FOLDER>,
    "output_file_format": <csv|parquet>,
    "output_file_name": "anonymized" -> optionally, specify the output filename.
  },
  "report" : {
    "location" : <PATH TO YOUR REPORT OUTPUT FOLDER>,
    "level" : <LOG LEVEL>
  }
}
```

### Running Tests
You can run the tests by running `make test` or triggering shell script located at `<PROJECT_ROOT>/bin/run_tests.sh`

### Trying out on local

##### Anonymizing a delimited csv file
1. Set up a JSON config file similar to the one seen at the project root.
In the 'acquire' section of the json, populate the input file path and the delimiter.
In the 'report' section, provide the output path, where you want the PII detection report to be generated.
A 'high' level report just calls out which columns have PII attributes.
A 'medium' level report calls out the percentage of PII in each column and the associated PII (email, credit card, etc)type for the same.
2. Run the main class - `python -m pii_anonymizer.standalone --config <optionally, path of the config file or leave blank to defaults to pii-anonymizer.json>`
You should see the report being appended to the file named 'report_\<date\>.log' in the output path specified in the
config file.

### Packaging
Run `poetry build` and the `.whl` file will be created in the `dist` folder.

### Licensing
Distributed under the MIT license. See ``LICENSE`` for more information.

### Contributing

You want to help out? _Awesome_!
