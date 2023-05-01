import json
import subprocess


def main():
    with open("src/env.json", "r") as env_file:
        env = json.load(env_file)

    subprocess.Popen(["mlflow", "ui", "--backend-store-uri", env["mlflow_tracking_uri"]])


if __name__ == "__main__":
    main()
