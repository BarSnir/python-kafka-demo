FROM python

WORKDIR /app
COPY . .

RUN rm -r venv
RUN python3 -m pip install --upgrade pip
RUN pip3 install -r requirements.txt