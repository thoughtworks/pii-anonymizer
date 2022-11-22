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

 * Following anonymizers have been added
    * [x] Replacement ('replace'): Replaces a detected sensitive value with a specified surrogate value. Leave the value empty to simply delete detected sensitive value.
    * [x] Hash ('hash'): Hash detected sensitive value with sha256.


### TO-DO
Following features  are part of the backlog with more features coming soon
 * Detectors:
    * [ ] NAME
    * [ ] ADDRESS
 * Anonymizers:
    * [ ] Encryption :  Encrypts the original sensitive data value using a cryptographic key. Cloud DLP supports several types of tokenization, including transformations that can be reversed, or "re-identified."
    * [ ] Masking: Replaces a number of characters of a sensitive value with a specified surrogate character, such as a hash (#) or asterisk (*).
    * [ ] Bucketing: "Generalizes" a sensitive value by replacing it with a range of values. (For example, replacing a specific age with an age range,
    or temperatures with ranges corresponding to "Hot," "Medium," and "Cold.")


You can have a detailed at upcoming features and backlog in this [Github Board](https://github.com/thoughtworks-datakind/anonymizer/projects/1?fullscreen=true)

## Development setup
1. Install [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)
2. Setup hooks and install packages with `make install`

### Config JSON
An example for the config JSON is located at `<PROJECT_ROOT>/pii-anonymizer.json`
```
{
  "acquire": {
    "file_path": <FILE PATH TO YOUR INPUT CSV>,
    "delimiter": <YOUR CSV DELIMITER>
  },
  "analyze": {

  },
  "anonymize": {
    "mode": <replace|hash>,
    "value": "string to replace",
    "output_file_path" : <PATH TO YOUR CSV OUTPUT FOLDER>
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
