FROM python:3.10-buster

WORKDIR /app

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY 3019_server_experimental_ext2.py ./main.py

CMD ["python3", "main.py"]
