from pii_anonymizer.standalone.dpf_main import DPFMain
from pii_anonymizer.common.get_args import get_args


def main():
    config_file_path = get_args()
    DPFMain(config_file_path).run()


if __name__ == "__main__":
    main()
