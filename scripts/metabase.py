"""Fire up metabase and ngrok for the frontend to be public."""
import os
import subprocess

def main():
    os.chdir("../metabase")
    subprocess.Popen(["java", "-jar", "metabase.jar"])
    subprocess.Popen(["ngrok", "http", "--domain=judahwilson.ngrok.io", "3000"])

if __name__ == '__main__':
    main()