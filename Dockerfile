FROM python:3.10-buster

WORKDIR /app

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY management_db_migrations.py ./management_db_migrations.py
COPY 3019_server_experimental_ext2.py ./main.py
COPY lib_utils ./lib_utils/
COPY static ./static/
COPY templates ./templates/

CMD ["python3", "main.py"]
