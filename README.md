### Data Engineering Interview - Peter Musonye
This repo contains code to implement the interview problems outlined in
Data Engineering Technical Interview.docx

### How to see the solutions:
The .ipynb jupyter notebooks can be opened directly in Github

Alternatively:
  1. with VSCode
  2. With docker:

     From this directory, open a terminal and type:
     ```
     docker run --rm -it -v $(pwd)/:/home/jovyan -p 8888:8888 --name=jupyter --platform linux/arm64/v8 jupyter/datascience-notebook:latest
     ```
     Remove `--platform` flag if not running on Apple Silicon Mac

     You terminal will then show instructions to open the jupyter server in your browser: For example

     ```
     To access the server, open this file in a browser:
        file:///home/jovyan/.local/share/jupyter/runtime/jpserver-7-open.html
    Or copy and paste one of these URLs:
        http://3ba4bc2f2de9:8888/lab?token=020eea6e4a5e39f3f87d831ee557d43612fd1723981c2aba
        http://127.0.0.1:8888/lab?token=020eea6e4a5e39f3f87d831ee557d43612fd1723981c2aba
     ```

### How to Run Problem 2's Script:

Follow these steps:
1. Open an terminal and install python-dotenv
    ```
    pip install python-dotenv
    ```
2. Ensure you populate .env (use examples from .env.sample):

    ```
    API_URL=https://xecdapi.xe.com/v1/convert_from/
    USERNAME=username
    PASSWORD=password
    ```

3. Run

    ```
    python currency_data.py --manual
    ```
   Remove the --manual flag if you'd prefer to wait for the scheduler instead.
