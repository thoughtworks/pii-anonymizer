import glob

allowed_format = ["csv", "parquet", "parq"]


def format_glob(input_path):
    filename = input_path.split("/")[-1]
    if "*" in filename:
        return input_path
    has_extension = True if len(filename.split(".")) > 1 else False
    if has_extension != True:
        if input_path[-1] != "/":
            input_path = input_path + "/"
        input_path = input_path + "*"

    return input_path


def get_format(input_path):
    file_format = None
    all_files = glob.glob(format_glob(input_path))
    for filename in all_files:
        format = filename.split(".")[-1]
        if format in allowed_format:
            file_format = format if format != "parq" else "parquet"
            break

    return file_format
