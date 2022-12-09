from cryptography.fernet import Fernet


def main():
    secret = Fernet.generate_key().decode()
    print(f"Keep this encrypt key safe\n{secret}")


if __name__ == "__main__":
    main()
